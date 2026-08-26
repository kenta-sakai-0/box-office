import dagster as dg
import asyncio
from collections import deque
 
from box_office.resources.common.RequestNode import RequestNode
from datetime import datetime, timedelta

class ClusterManager():
    def __init__(self, proxyClient, databricks, context):
        self._logger = dg.get_dagster_logger()
        self.proxyClient = proxyClient
        self.databricks = databricks
        self.context = context

        self.counter = []

    async def run(self, requestQ, run_id, numNodes, maxRequestsFailedInRow, chunkSize, file_destination):
        nodes = [
            RequestNode(
                nodeID=i,
                proxyClient=self.proxyClient,
                requestQ=requestQ,
                run_id=run_id,
                databricks=self.databricks,
                context=self.context
            )
            for i in range(numNodes)
        ]

        await asyncio.gather(*[
            node.run(
                maxRequestsFailedInRow=maxRequestsFailedInRow,
                chunkSize=chunkSize,
                file_destination=file_destination
            )
            for node in nodes
        ])
    
    async def monitor(self, interval=60):
        while self.counter != [] and self.counter[0] < datetime.now() - timedelta(seconds=interval):
            self.counter.pop()

        self.context.info(f"Requests processed in last minute: {len(self.counter)}")