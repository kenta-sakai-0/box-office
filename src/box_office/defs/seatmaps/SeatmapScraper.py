from curl_cffi.requests import AsyncSession
from datetime import datetime, timedelta, timezone
import requests
import asyncio
from core.data_collection.seatmaps.SeatmapData import SeatmapData
from core.data_collection.showtimes.ShowtimesData import ShowtimesData
import numpy as np  

import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

file_handler = logging.FileHandler('.logs/seatmaps/out.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
log.addHandler(file_handler)

error_handler = logging.FileHandler('.logs/seatmaps/err.log')
error_handler.setLevel(logging.ERROR)
error_handler .setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
log.addHandler(error_handler)

class SeatmapScraper():

    def __init__(self, proxy_url):
        self.sema = asyncio.Semaphore(15)
        self.SData = SeatmapData()
        self.proxy_url = proxy_url
        self._403_max_retries = 5 # Max number of tolerated failures before putting proxy on timeoutk
  
    async def fetch_seatmap(self, showtimeHashCodez, num_showtimes_per_chunk: int, run_id):
        
        for i in range(0, len(showtimeHashCodez), num_showtimes_per_chunk):
            # if self._403.is_set():
            #     log.info('Killing process')
            #     break
                
            chunk = showtimeHashCodez[i:i + num_showtimes_per_chunk]
            log.info(f"Processing chunk {i//num_showtimes_per_chunk + 1}")
            
            await self.fetch_seatmap_for_chunk(chunk, run_id)

    async def fetch_seatmap_for_chunk(self, showtimeHashCodez, run_id):
        # if self._403.is_set():
        #     return
              
        async with AsyncSession(proxies={'http': self.proxy_url, 'https': self.proxy_url}) as client:
            tasks = [self.__seatmap(client, showtimeHashCode[0], showtimeHashCode[1]) for showtimeHashCode in showtimeHashCodez]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        payload = [r for r in results if not isinstance(r, Exception)]
        
        await self.__write(payload, run_id)
        return payload

    async def __seatmap(
        self,
        client: AsyncSession,
        showtimeHashCode,
        theater_url,
        timeout: int = 10,
    ) -> None:
        """
            Fetch seatmap for one showtimeHashCode
        """ 
        try: 
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': theater_url,
                'X-Requested-With': 'XMLHttpRequest',
                'Sec-GPC': '1',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'If-None-Match': 'W/"d21a-pyHkEhChConYvCdA48akD6azBfs"',
            }
        
            url = f'https://www.fandango.com/napi/seatMap/{showtimeHashCode}'
            
            async with self.sema:
                # if self._403.is_set():
                #     log.error(f'Hit 403 limit | Skipping {showtimeHashCode}')
                #     return {}
                
                await asyncio.sleep(np.random.uniform(1.0, 2.5))
                response = await client.get(url, headers=headers, impersonate='chrome', timeout=timeout, verify=False)
                
                if response.status_code == 200:
                    log.info(f"[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] | Fetched {showtimeHashCode}")
                    return response.json() | {'snapshot_ts': int(datetime.now().timestamp())}
                elif response.status_code == 403:
                    # self._403_max_retries += 1
                    await asyncio.sleep(300)
                log.error(f"[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] | Response {response.status_code} | {showtimeHashCode} | {self.proxy_url} | {response.json()}") 
            
        except Exception as e:
            log.error(f"[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] | {e} | {showtimeHashCode} | proxy: {self.proxy_url} | {e}") 
            
    async def __write(self, payload: list[dict], run_id: str):
        await self.SData.dump_payload(payload, run_id)


if __name__ == '__main__':
    showtimes_df = SeatmapData().fetch_showtimes_df()
        
    def read_proxies(file_path='config/webshare_proxies.txt'):
        """Read proxies from a text file and format as http URLs"""
        proxies = []
        try:
            with open(file_path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        parts = line.split(':')
                        if len(parts) == 4:
                            ip, port, username, password = parts
                            proxy_url = f'http://{username}:{password}@{ip}:{port}'
                            proxies.append(proxy_url)
        except FileNotFoundError:
            print(f"Error: {file_path} not found")
        return proxies
    
    proxies = read_proxies()[0:75]
    run_id = round(datetime.now().timestamp())
    
    HashCodes = np.array_split(showtimes_df.select(['showtimeHashCode', 'theater_url']).rows(), len(proxies))
    
    scrapers = []
    for p, h in zip(proxies, HashCodes):
        scraper = SeatmapScraper(p)
        scrapers.append(scraper)
    
    async def main():
        await asyncio.gather(*[scrapers[i].fetch_seatmap(HashCodes[i], 500, run_id) for i in range(len(scrapers))])
    
    asyncio.run(main())

"""
3/29 run - 7 mins
    - semaphore = 15
    - agent = 6x
    - showtimes = 10000
"""