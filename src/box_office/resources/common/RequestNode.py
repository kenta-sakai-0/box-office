import dagster as dg
import asyncio
import numpy as np
from curl_cffi.requests import AsyncSession
import uuid

from box_office.resources.common.proxies import ProxyResource, ProxyClient, Proxy
from box_office.resources.databricks.Databricks import DatabricksResource


class RequestNode():
    def __init__(
        self,
        nodeID,
        proxyClient: ProxyClient,
        requestQ: asyncio.Queue,
        run_id: str,
        databricks: DatabricksResource,
        context: dg.AssetExecutionContext
    ):
        self.nodeID = nodeID
        self.context = context
        self.context.log.info('Spinning up node')
        self.requestQ = requestQ
        self.run_id = run_id
        self.databricks = databricks

        self.proxyClient = proxyClient
        self.proxy: Proxy = None

    async def request_proxy(self):
        self.context.log.info(f'NodeID: {self.nodeID} | Requesting proxy...')
        self.proxy = await self.proxyClient.get()
        self.context.log.info(f'NodeID: {self.nodeID} | Proxy fetched: {self.proxy.url}')
        return True

    async def run(
            self,
            maxRequestsFailedInRow: int,
            chunkSize: int,
            file_destination: str,
            cooldown_seconds: int=180
        ) -> bool:
        """
            Main job run for node. Requests a proxy and processes the page queue.
            Creates a new AsyncSession whenever the proxy is rotated due to a 403.
        """

        try:
            responses = []
            # Outer loop: re-enter whenever we need a new proxy/session
            while not self.requestQ.empty():

                proxy_blocked = False
                rotate_proxy = False
                numRequestsFailedInRow = 0
                await self.request_proxy()

                async with AsyncSession(
                    proxies={'http': self.proxy.url, 'https': self.proxy.url}
                ) as session:
                    
                    # Inner loop terminates when we need new proxy when old one died for whatever reason
                    while (not self.requestQ.empty()) and (not rotate_proxy):
                        try:
                            r = self.requestQ.get_nowait()
                        except asyncio.QueueEmpty:
                            break
                        
                        try:
                            response = await session.get(**r)

                            if response.status_code == 200:
                                responses.append(response.content)
                                # self.context.log.info(f'NodeID: {self.nodeID} | OK | {response.status_code} | Request {r} | {len(responses)}/{chunkSize}')
                                numRequestsFailedInRow = 0

                            elif response.status_code in (403, 407):
                                self.context.log.error(
                                    f'NodeID: {self.nodeID} | NOT OK | {response.status_code} | Request {r}'
                                )
                                proxy_blocked = True

                                raise Exception
                            
                            else:
                                raise Exception
                                
                        except Exception as e:
                            self.context.log.error(f'NodeID: {self.nodeID} | Request error | Request {r} | {e}')
                            numRequestsFailedInRow += 1
                        
                        if proxy_blocked or (numRequestsFailedInRow >= maxRequestsFailedInRow):
                            rotate_proxy = True
                            self.context.log.info(f"NodeID: {self.nodeID} | proxy_blocked: {proxy_blocked} | {numRequestsFailedInRow} requests failed in a row: {self.proxy.url}")
                            await self.proxyClient.releaseBroken(self.proxy, cooldown_seconds=cooldown_seconds)
                        
                        # Flush to Databricks every chunkSize responses
                        if len(responses) > 0 and len(responses) % chunkSize == 0:
                            await self._flush(responses, file_destination)
                            responses = []

                        await asyncio.sleep(np.random.uniform(0.75, 1.5))
            
            # Flush any remaining responses after the queue is drained
            await self._flush(responses, file_destination)

        finally:
            if self.proxy:
                await self.proxyClient.release(self.proxy)

    async def _flush(self, responses:list[dict], file_destination:str):
        if not responses:
            return
        
        await self.databricks.upload_raw_jsonl(
            data=responses, 
            targetPath=f'{file_destination}/{uuid.uuid4()}.jsonl'
            )
        self.context.log.info(f'NodeID: {self.nodeID} | Flushed {len(responses)} pages to Databricks: {file_destination}')