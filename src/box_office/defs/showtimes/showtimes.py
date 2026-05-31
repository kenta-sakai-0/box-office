import dagster as dg
from curl_cffi.requests import AsyncSession
from datetime import datetime, timedelta
import asyncio
import numpy as np
from dataclasses import dataclass
import yaml
import os
import uuid
from importlib.resources import files

from box_office.resources.common.proxies import ProxyResource, ProxyClient, Proxy
from box_office.resources.databricks.Databricks import DatabricksResource
from box_office.resources.common.RequestNode import RequestNode

env = os.getenv("DAGSTER_ENV")

config_path = files("box_office") / "config.yaml"
with open(config_path, 'r') as file:
    config = yaml.safe_load(file).get(env)

catalog = config.get('catalog')

showtimes_config = config.get('showtimes')
numNodes = showtimes_config.get('numNodes')
chunkSize = showtimes_config.get('chunkSize')
timeout = showtimes_config.get('timeout')
maxRequestsFailedInRow = showtimes_config.get('maxRequestsFailedInRow')
nDaysFromToday = showtimes_config.get('nDaysFromToday')

startDate = datetime.today()
endDate = startDate + timedelta(days=nDaysFromToday)

@dg.asset(deps=['theaters'])
def showtimes_raw(context: dg.AssetExecutionContext, databricks: DatabricksResource, proxy: dg.ResourceParam[ProxyClient]):
    run_id = str(round(datetime.now().timestamp()))
 
    theaters_df = databricks.query(f'select * from {catalog}.base.theaters')
    context.log.info(f'Theaters sample:\n{theaters_df.head(5)}')
    
    tids = theaters_df['theater_id'].to_list()
    date_list = [startDate + timedelta(n) for n in range((endDate - startDate).days + 1)]

    proxyClient = proxy

    async def _run():
        # Build the queue inside the event loop so Queue is bound to the right loop
        requestQ: asyncio.Queue = asyncio.Queue()
        for t in tids:
            for d in date_list:
                requestQ.put_nowait({
                    'url': f'https://www.fandango.com/napi/theaterMovieShowtimes/{t.upper()}',
                    'headers': headers(t, d),
                    'params': params(d),
                    'impersonate': 'chrome',
                    'timeout': timeout,
                    'verify': False
                    })
        context.log.info(f'Total pages queued: {requestQ.qsize()}')

        nodes = [
            RequestNode(
                nodeID=i,
                proxyClient=proxyClient,
                requestQ=requestQ,
                run_id=run_id,
                databricks=databricks,
                context=context,
            )
            for i in range(numNodes)
        ]
 
        await asyncio.gather(*[
            node.run(
                maxRequestsFailedInRow=maxRequestsFailedInRow,
                chunkSize=chunkSize,
                timeout=timeout,
                file_destination= showtimes_snapshot_folderpath(run_id=run_id)
            )
            for node in nodes])
 
    asyncio.run(_run())

def showtimes_snapshot_folderpath(run_id)-> str:
    return showtimes_config.get('showtimes_snapshot_folderpath').format(run_id=run_id)

def headers(theater_id: str, date: datetime) -> dict:
    date = date.strftime('%Y-%m-%d')
    return {
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

def params(date: datetime) -> dict:
    date = date.strftime('%Y-%m-%d')
    return {'startDate': date}
