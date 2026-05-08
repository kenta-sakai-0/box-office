import dagster as dg

import requests
from bs4 import BeautifulSoup
import polars as pl
from datetime import datetime, timezone
from box_office.resources.databricks.Databricks import DatabricksResource
import yaml
import os
from pathlib import Path

env = os.getenv("DAGSTER_ENV")
with open(f'{Path(__file__).parent.joinpath("config.yaml")}', 'r') as file:
    config = yaml.safe_load(file).get(env)

catalog = config.get('catalog')


@dg.asset
async def franchises_snapshot(databricks: DatabricksResource) -> str:
    """
        Takes snapshot of https://www.fandango.com/movie-theaters and make it available as flat file

    """
    run_id = str(int(datetime.now().timestamp()))
    
    franchises_snapshot = scrape_franchises()
    await upload_franchises_snapshot(
        run_id=run_id,
        df=franchises_snapshot, 
        databricks=databricks
        )
    
    return run_id

@dg.asset
async def franchises(franchises_snapshot:str, databricks: DatabricksResource):
    refresh_franchises(
        run_id=franchises_snapshot,
        catalog=catalog,
        databricks=databricks
    )

def scrape_franchises() -> pl.DataFrame:    
    """
        Scrape franchises page and returns polars DF
    """
    url = 'https://www.fandango.com/movie-theaters'
    
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    chains = soup.select('li.movie-theaters__chain a')
    
    results = []
    for a in chains:
        url = f'https://www.fandango.com{a.get("href")}'
        franchiseName = a.get('alt')
        results.append({"url": url, "name": franchiseName})
    
    df = (
        pl.DataFrame(results)
        .select(pl.col('name').alias('franchise_name'), pl.col('url').alias('franchise_url'))
        .with_columns(pl.lit(datetime.now(timezone.utc)).alias('snapshot_ts'))
    )
    return df

async def upload_franchises_snapshot(run_id, df: pl.DataFrame, databricks:DatabricksResource) -> None:
    franchises_snapshot_location = config.get('franchises_snapshot_location').format(
        catalog=catalog,
        run_id=run_id
        )
    
    await databricks.upload_polars(
        df = df,
        astype='parquet',
        targetPath=franchises_snapshot_location,
        overwrite=True
    )

def refresh_franchises(run_id, catalog:str, databricks:DatabricksResource) -> pl.DataFrame:
    franchises_snapshot_location = config.get('franchises_snapshot_location').format(
        catalog=catalog,
        run_id=run_id
        )
    q = f"""
        CREATE OR REPLACE TABLE {catalog}.base.franchises AS
        SELECT * FROM read_files('{franchises_snapshot_location}')
    """
    
    databricks.submit_query(q)