# from core.db.databricks.DataBricks import DataBricks
# import yaml
# from datetime import datetime
# import polars as pl
# import uuid
# import logging
# logging.basicConfig(level=logging.INFO)
# log = logging.getLogger(__name__)

# class ShowtimesData():
#     def __init__(self, config=None):

#         config = yaml.safe_load(open('config/config.yaml'))
#         self.config = config or yaml.safe_load(open('config/config.yaml'))
        
#         self.landing_databricks_vol = self.config.get('data').get('showtimes').get('landing_databricks_vol')
#         self.catalog = self.config.get('databricks').get('dev').get('catalog')

#     async def save_page_lvl_payloads(self, page_payloads: list[dict], run_id):
        
#         snapshot_ts = str(int(datetime.now().timestamp()))
#         id = uuid.uuid4().hex[:8] 
#         targetPath = self.landing_databricks_vol.format(catalog=self.catalog, run_id = run_id, snashot_ts=snapshot_ts, id = id)
            
#         dB = DataBricks()
#         await dB.upload_dict_as_jsonl(page_payloads, targetPath, overwrite=False)
        
