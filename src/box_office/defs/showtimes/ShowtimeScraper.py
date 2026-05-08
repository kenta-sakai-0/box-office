from curl_cffi.requests import AsyncSession
from datetime import datetime, timedelta
import asyncio
# from core.data_collection.showtimes.ShowtimesData import ShowtimesData
# from core.data_collection.theaters.TheatersData import TheatersData
import numpy as np

# import logging
# logging.basicConfig(level=logging.INFO)
# log = logging.getLogger(__name__)

# file_handler = logging.FileHandler('.logs/showtimes/out.log')
# file_handler.setLevel(logging.INFO)
# file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
# log.addHandler(file_handler)

# error_handler = logging.FileHandler('.logs/showtimes/err.log')
# error_handler.setLevel(logging.ERROR)
# error_handler .setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
# log.addHandler(error_handler)


class ShowtimeScraper():

    def __init__(self, proxy_url):
        self.sema = asyncio.Semaphore(15)
        self.SData = ShowtimesData()
        self.proxy_url = proxy_url
        self._403 = asyncio.Event()
  
    async def fetch_showtimes(self, start_date, end_date, tids, num_theaters_per_chunk: int, run_id):
        
        for i in range(0, len(tids), num_theaters_per_chunk):
            if self._403.is_set():
                log.info('Killing process')
                break
                
            chunk = tids[i:i + num_theaters_per_chunk]
            log.info(f"Processing chunk {i//num_theaters_per_chunk + 1} | theaters {i+1}-{i+len(chunk)} of {len(tids)}")
            await self.fetch_showtimes_for(chunk, start_date, end_date, run_id)
            await asyncio.sleep(5)  # pause between chunks

    async def fetch_showtimes_for(self, tids, start_date, end_date, run_id):
        if self._403.is_set():
            return
        
        date_list = [start_date + timedelta(n) for n in range((end_date - start_date).days + 1)]
        
        
        async with AsyncSession(proxies={'http': self.proxy_url, 'https': self.proxy_url}) as client:
            tasks = [
                self.fetch_showtimes_on_page(client, theater_id, date)
                for theater_id in tids
                for date in date_list
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        page_payloads = [r for r in results if not isinstance(r, Exception)]
        await self.process_showtimes(page_payloads, run_id)
        return page_payloads

    async def fetch_showtimes_on_page(
        self,
        client: AsyncSession,
        theater_id: str,
        date: datetime,
        timeout: int = 10,
    ) -> None:
        """
            Fetch showtimes for a specific theater and date from Fandango's API, then save response to temp storage
        """ 
        try:
            date = date.strftime('%Y-%m-%d')
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': f'https://www.fandango.com/{theater_id}/theater-page?date={date}',
                'X-Requested-With': 'XMLHttpRequest',
                'Sec-GPC': '1',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'Connection': 'keep-alive',
                'TE': 'trailers',
            }
            params = {
                'startDate': date
            }
            url = f"https://www.fandango.com/napi/theaterMovieShowtimes/{theater_id.upper()}"
            
            async with self.sema:
                if self._403.is_set():
                    log.error(f'Hit 403 limit | Skipping {theater_id} | {date}')
                    return {}
                
                await asyncio.sleep(np.random.uniform(1, 1.5))
                response = await client.get(url, headers=headers, params=params, impersonate='chrome', timeout=timeout, verify=False)
                
                if response.status_code == 200:
                    log.info(f"[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] | Fetched {theater_id} | {date} ")
                    return response.json()
                else:
                    if response.status_code == 403:
                        self._403.set()
                    log.error(f"[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] | Error: {response.status_code} | {theater_id} | {date} ")
                    raise Exception(f"{response.status_code} {response.text[:200]}")
            
        except Exception as e:
            log.error(e)
        
    async def process_showtimes(self, page_payloads: list[dict], run_id: str):
        await self.SData.save_page_lvl_payloads(page_payloads, run_id)

if __name__ == '__main__':
        
    theaters_df = TheatersData().fetch_theaters_df()
    tids = theaters_df['theater_id'].to_list()
    tids_arrays = np.array_split(tids, 6)
    tids1, tids2, tids3, tids4, tids5, tids6 = tids_arrays
    
    start_date = datetime.today()
    end_date = start_date + timedelta(days=30)
    run_id = round( datetime.now().timestamp())
    
    scraper1 = ShowtimeScraper('http://mthkqgja:eti35uwjj7p8@45.56.175.48:5722')
    scraper2 = ShowtimeScraper('http://mthkqgja:eti35uwjj7p8@107.172.116.105:5561')
    scraper3 = ShowtimeScraper('http://mthkqgja:eti35uwjj7p8@142.111.124.176:6196')
    scraper4 = ShowtimeScraper('http://mthkqgja:eti35uwjj7p8@23.95.250.36:6309')
    scraper5 = ShowtimeScraper('http://mthkqgja:eti35uwjj7p8@104.232.211.237:5850')
    scraper6 = ShowtimeScraper('http://mthkqgja:eti35uwjj7p8@136.0.207.177:6754')
        
    async def main():
        await asyncio.gather(
            scraper1.fetch_showtimes(start_date, end_date, tids1, 50, run_id),
            scraper2.fetch_showtimes(start_date, end_date, tids2, 50, run_id),
            scraper3.fetch_showtimes(start_date, end_date, tids3, 50, run_id),
            scraper4.fetch_showtimes(start_date, end_date, tids4, 50, run_id),
            scraper5.fetch_showtimes(start_date, end_date, tids5, 50, run_id),
            scraper6.fetch_showtimes(start_date, end_date, tids6, 50, run_id)
        )
    
    asyncio.run(main())

"""
3/24 run - 2.5hrs
    - semaphore = 15
    - agent = 1x
    - requests = 85,901
    - ~572 requests / min

3/26 run 
    7:53 ~ @ 3xAgent, semaphore = 15

3/27 run - 42 mins
    - semaphore = 15
    - agent = 6x
    - 42 mins

3/27 - 34 mins
    - async writes
    - chunk size = 100
    - semaphore = 15
    - agent = 6x
    - 2,529 requests/min


Showtimes: 
    total: 1,543,445 
    < 7 days
        645,257 -> 40%
    7~14 days
        251,125 -> 16%
    > 14~30 days
        522,509 -> 30%

    With 10 executors, full scrape will take 270 mins ~ 4.5hrs
"""