# from core.db.databricks.DataBricks import DataBricks
# import yaml
# from datetime import datetime
# import polars as pl
# import uuid
# import logging
# logging.basicConfig(level=logging.INFO)
# log = logging.getLogger(__name__)

# class SeatmapData():
#     def __init__(self, config=None):

#         config = yaml.safe_load(open('config/config.yaml'))
#         self.config = config or yaml.safe_load(open('config/config.yaml'))
        
#         self.landing_databricks_vol = self.config.get('data').get('seatmaps').get('landing_databricks_vol')
#         self.catalog = self.config.get('databricks').get('dev').get('catalog')

#     async def dump_payload(self, payload: list[dict], run_id):
        
#         snapshot_ts = str(int(datetime.now().timestamp()))
#         id = uuid.uuid4().hex[:8]
#         targetPath = self.landing_databricks_vol.format(catalog=self.catalog, run_id = run_id, snapshot_ts=snapshot_ts, id = id)
            
#         dB = DataBricks()
#         await dB.upload_dict_as_jsonl(payload, targetPath, overwrite=False)
    
#     def fetch_showtimes_df(self) -> pl.DataFrame:
#         q = """
#             select st.showtimeHashCode, st.ticketingDate, t.theater_url
#             from fandango_dev.base.showtimes st
#             left join fandango_dev.base.theaters t
#                 on lower(st.theater_id) = lower(t.theater_id)
#             where st.ticketingDate >= to_utc_timestamp(current_timestamp(), 'America/Los_Angeles')
#         """
#         return DataBricks().query(q)