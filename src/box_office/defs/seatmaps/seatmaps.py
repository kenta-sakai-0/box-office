import dagster as dg
from datetime import datetime, timedelta
import asyncio
import yaml
import os
from pathlib import Path
import polars as pl
import numpy as np 

from importlib.resources import files
import papermill

from box_office.resources.common.proxies import ProxyResource, ProxyClient, Proxy
from box_office.resources.databricks.Databricks import DatabricksResource
from box_office.resources.common.ClusterManager import ClusterManager
env = os.getenv("DAGSTER_ENV")

config_path = files("box_office") / "config.yaml"
with open(config_path, 'r') as file:
    config = yaml.safe_load(file).get(env)

catalog = config.get('catalog')

seatmaps_config = config.get('seatmaps')
numNodes = seatmaps_config.get('numNodes')
chunkSize = seatmaps_config.get('chunkSize')
timeout = seatmaps_config.get('timeout')
maxRequestsFailedInRow = seatmaps_config.get('maxRequestsFailedInRow')
nDaysFromToday = seatmaps_config.get('nDaysFromToday')

startDate = datetime.today()
endDate = startDate + timedelta(days=nDaysFromToday)

class seatmapsConfig(dg.Config):
    run_id: str | None = None

@dg.asset(deps=['showtimes'])
async def seatmaps_raw(context: dg.AssetExecutionContext, databricks: DatabricksResource, proxy: dg.ResourceParam[ProxyClient]):
    run_id = str(round(datetime.now().timestamp()))
    proxyClient = proxy
    requestQ = seatmaps_requestQ(context=context, databricks=databricks)
    
    cluster = ClusterManager(
        proxyClient=proxyClient,
        databricks=databricks,
        context=context
    )
    
    await cluster.run(
        requestQ=requestQ,
        run_id=run_id,
        numNodes=numNodes,
        maxRequestsFailedInRow=maxRequestsFailedInRow,
        chunkSize=chunkSize,
        file_destination=seatmaps_snapshot_folderpath(run_id)
    )

    return run_id

@dg.asset()
def seatmaps(context: dg.AssetExecutionContext, config: seatmapsConfig, seatmaps_raw):
    run_id = config.run_id or seatmaps_raw
    papermill.execute_notebook(
        input_path=seatmaps_notebook_filepath(),
        output_path=None,
        parameters={'run_id': run_id, 'env': env}
    )



def seatmaps_requestQ(context, databricks) -> asyncio.Queue:
    """
        Fetch showtimes and create a request queue for seatmaps
    """
    requestQ: asyncio.Queue = asyncio.Queue()

    showtimes_df = fetch_showtimes_df(databricks)
    context.log.info(f'Showtimes sample:\n{showtimes_df.head(5)}')

    for showtimeHashCode, theater_url in showtimes_df.select(['showtimeHashCode', 'theater_url']).rows():
        request = {
            'url': f'https://www.fandango.com/napi/seatMap/{showtimeHashCode}',
            'headers': headers(theater_url),
            'impersonate': 'chrome', 
            'timeout': timeout, 
            'verify': False
        }
        requestQ.put_nowait(request)

    context.log.info(f'Total pages queued: {requestQ.qsize()}')
    return requestQ



def fetch_showtimes_df(databricks) -> pl.DataFrame:
    q = f"""
        select st.showtimeHashCode, st.ticketingDate, t.theater_url
        from {catalog}.base.showtimes st
        left join {catalog}.base.theaters t
            on lower(st.theater_id) = lower(t.theater_id)
        where st.ticketingDate between to_date(to_utc_timestamp(current_timestamp(), 'America/Los_Angeles')) and to_date(to_utc_timestamp(current_timestamp() + interval 30 days, 'America/Los_Angeles'))
    """
    return databricks.query(q)

def seatmaps_snapshot_folderpath(run_id)-> str:
    return seatmaps_config.get('snapshot_folderpath').format(run_id=run_id)

def seatmaps_notebook_filepath() -> str:
    return Path(__file__).parent.joinpath('data_cleaning', 'seatmaps.ipynb')

def headers(theater_url):
    return {
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