import dagster as dg
from box_office.resources.databricks.Databricks import DatabricksResource

import requests
from bs4 import BeautifulSoup
import polars as pl
from datetime import datetime, timezone
import yaml
import os
from importlib.resources import files

env = os.getenv("DAGSTER_ENV")

config_path = files("box_office") / "config.yaml"
with open(config_path, 'r') as file:
    config = yaml.safe_load(file).get(env)
catalog = config.get('catalog')

theaters_config = config.get('theaters')

@dg.asset(deps=['franchises'])
async def theaters_snapshot(context, databricks: DatabricksResource) -> str:
    """
        Takes snapshot of theaters from Fandango and make it available as flat file
    """

    franchises = fetch_franchises_df(databricks)
    context.log.info("Success: Franchises table fetched")

    run_id = str(int(datetime.now().timestamp()))
    
    theaters_snapshot_df = scrape_theaters(franchises)
    context.log.info("Success: Theaters scraped")

    await upload_theaters_snapshot(
        run_id=run_id,
        df=theaters_snapshot_df, 
        databricks=databricks
        )
    context.log.info("Success: Theaters snapshot uploaded")

    return run_id

@dg.asset()
async def theaters(context, theaters_snapshot: str, databricks: DatabricksResource):
    refresh_theaters(
        run_id=theaters_snapshot,
        catalog=catalog,
        databricks=databricks
    )
    context.log.info("Success: Theaters refreshed with latest data")

def scrape_theaters(franchises: pl.DataFrame) -> pl.DataFrame:
    """
        Scrape theaters from franchises and return processed polars DF
    """
    theater_data = []

    for row in franchises.iter_rows(named=True):
        print(f"Scraping {row.get('franchise_name')}")
        franchise_name, url = row['franchise_name'], row['franchise_url']
        response = requests.get(url)
        if response.status_code == 404:
            continue

        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            rows = soup.select('.theaters-by-chain__state-section span.theaters-by-chain__theater-name')

            for row in rows:
                link_tag = row.find('a')
                if link_tag:
                    entry = {
                        "franchise_name": franchise_name,
                        "theater_name": link_tag.get_text(strip=True),
                        "theater_url": "https://www.fandango.com" + link_tag['href'],
                    }
                    theater_data.append(entry)
            
        except Exception:
            pass
    
    df = pl.DataFrame(theater_data)
    df = df.with_columns(
        pl.col('theater_url')
            .str.extract(r'-([a-z0-9]+)/theater-page$', 1)
            .alias('theater_id'),
        
        pl.lit(datetime.now(timezone.utc)).alias('snapshot_ts')
    )
    return df

async def upload_theaters_snapshot(run_id: str, df: pl.DataFrame, databricks: DatabricksResource) -> None:
    theaters_snapshot_location = theaters_config.get('theaters_snapshot_location').format(
        catalog=catalog,
        run_id=run_id
        )
    
    await databricks.upload_polars(
        df=df,
        astype='parquet',
        targetPath=theaters_snapshot_location,
        overwrite=True
    )

def refresh_theaters(run_id: str, catalog: str, databricks: DatabricksResource) -> None:
    theaters_snapshot_location = theaters_config.get('theaters_snapshot_location').format(
        catalog=catalog,
        run_id=run_id
        )
    q = f"""
        CREATE OR REPLACE TABLE {catalog}.base.theaters AS
        SELECT * FROM read_files('{theaters_snapshot_location}')
    """
    databricks.submit_query(q)

def fetch_franchises_df(databricks: DatabricksResource) -> pl.DataFrame:
    q = f"""
        select * from {catalog}.base.franchises
    """
    return databricks.query(q)