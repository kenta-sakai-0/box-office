from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql
from pydantic import PrivateAttr
import pyarrow as pa
import requests
import json
import polars as pl
import io
import asyncio
import time

import dagster as dg

class DatabricksResource(dg.ConfigurableResource):
    _w =PrivateAttr()
    _logger = PrivateAttr()

    def model_post_init(self, context):
        self._w = WorkspaceClient(profile='main')
        self._logger = dg.get_dagster_logger()
        
    async def upload(self, fileContents: io.BytesIO, targetPath, overwrite=False):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._upload_blocking, fileContents, targetPath, overwrite)
        
        
    def _upload_blocking(self, fileContents: io.BytesIO, targetPath, overwrite=False):
        self._logger.info(f"Uploading to Databricks | {targetPath} | overwrite={overwrite}")
        self._w.files.upload(
            file_path=targetPath,
            contents=fileContents,
            overwrite=overwrite
        )
        self._logger.info(f"Success | Uploaded to Databricks | {targetPath} | overwrite={overwrite}")
    
    async def upload_raw_jsonl(self, data: list[bytes], targetPath: str, overwrite=False):
        joined = b'\n'.join(data)
        await self.upload(
            fileContents=io.BytesIO(joined),
            targetPath=targetPath,
            overwrite=overwrite
        )

    async def upload_dict_as_json(self, data: dict, targetPath: str, overwrite=False):
        
        await self.upload(
            fileContents=io.BytesIO(json.dumps(data).encode("utf-8")),
            targetPath=targetPath,
            overwrite=overwrite
        )

    async def upload_dict_as_jsonl(self, data: dict, targetPath: str, overwrite=False):
        
        jsonl_bytes = '\n'.join(json.dumps(record) for record in data).encode("utf-8")
        
        await self.upload(
            fileContents=io.BytesIO(jsonl_bytes),
            targetPath=targetPath,
            overwrite=overwrite
        )
        
    async def upload_polars(self, df: pl.DataFrame, astype:str, targetPath: str, overwrite: bool = False):
        if astype == 'parquet':
            buffer = io.BytesIO()
            df.write_parquet(buffer)
            buffer.seek(0)
            await self.upload(buffer, targetPath, overwrite)
    
    def submit_query(self, query: str, warehouse_id: str = 'dd367767ecda1b31'):
        try:
            result = self._w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=query,
                wait_timeout="50s",
                disposition=sql.Disposition.EXTERNAL_LINKS,
                format=sql.Format.ARROW_STREAM
            )
            print(result.status.state)
            if result.status.state != sql.StatementState.SUCCEEDED:
                raise Exception(f"Query failed with state {result.status.state}: {result.status.error.message}\n{query}")
            return result
        except Exception as e:
            raise Exception(f"Query failed: {e}\n{query}") from e
    
    def query(self, query: str, warehouse_id: str = 'dd367767ecda1b31') -> pl.DataFrame:
        self._logger.info(f"Databricks | Submitting query")
        result = self.submit_query(query=query, warehouse_id=warehouse_id)
        
        self._logger.info(f"Databricks | Fetching data")
        chunks = []
        for chunk_index in range(result.manifest.total_chunk_count):
            self._logger.info(f"Databricks | Fetching chunk {len(chunks)+1}")
            chunks.append(self._fetch_chunk_table(result.statement_id, chunk_index))

        if not chunks:
            columns = [col.name for col in result.manifest.schema.columns]
            return pl.DataFrame(schema=columns)
        
        self._logger.info(f"Databricks | Success | Queried and fetched data")
        return pl.from_arrow(pa.concat_tables(chunks))

    def _fetch_chunk_table(self, statement_id: str, chunk_index: int, max_attempts: int = 3) -> pa.Table:
        last_error = None
        for attempt in range(1, max_attempts + 1):
            chunk = self._w.statement_execution.get_statement_result_chunk_n(
                statement_id=statement_id,
                chunk_index=chunk_index
            )
            try:
                response = requests.get(chunk.external_links[0].external_link)
                response.raise_for_status()
                reader = pa.ipc.open_stream(response.content)
                return reader.read_all()
            except (requests.exceptions.RequestException, pa.lib.ArrowInvalid) as e:
                last_error = e
                if attempt < max_attempts:
                    backoff = 2 ** (attempt - 1)
                    self._logger.warning(
                        f"Databricks | Chunk {chunk_index} download failed (attempt {attempt}/{max_attempts}): {e} | Retrying in {backoff}s"
                    )
                    time.sleep(backoff)
        raise last_error
