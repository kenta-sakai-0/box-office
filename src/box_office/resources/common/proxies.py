import dagster as dg
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta

maxStrikes = 3
cooldown = timedelta(minutes=5)


class ProxyResource(dg.ConfigurableResource):
    file_path: str = 'src/box_office/resources/common/Webshare 100 proxies.txt'

    def create_resource(self, context: dg.InitResourceContext) -> 'ProxyClient':
        return ProxyClient(self.file_path, context)
    

@dataclass
class Proxy():
    url: str
    released_at: datetime = datetime.min
    strikes: int = 0
    health: bool = True


class ProxyClient:

    def __init__(self, file_path: str, context: dg.InitResourceContext):
        self._proxyQ = self.__loadWebshareProxies(file_path)
        self._brokenQ = asyncio.Queue()
        self.context = context


    def __loadWebshareProxies(self, file_path: str) -> asyncio.Queue:
        """This is for the Webshare .txt file"""

        Q = asyncio.Queue()
        try:
            with open(file_path) as f:
                for line in f:
                    parts = line.strip().split(':')
                    if len(parts) == 4:
                        ip, port, username, password = parts
                        prox = Proxy(url=f'http://{username}:{password}@{ip}:{port}')
                        Q.put_nowait(prox)

        except FileNotFoundError:
            print(f"Error: {file_path} not found")

        return Q


    async def get(self, timeout=30) -> Proxy:
        """Fetches proxy from queue. If there's nothing available in main Q, fetch something from brokenQ. timeouts in N minutes"""
        request_ts = datetime.now()
        
        while datetime.now() - request_ts < timedelta(seconds=timeout):
            self.context.log.info(f'available proxies: {self._proxyQ.qsize()}, timeout queries: {self._brokenQ.qsize()}')
            try:
                p = self._proxyQ.get_nowait()
                if p: # Race condition

                    return p

                p = self._brokenQ.get_nowait()
                if p:
                    if p.released_at <= datetime.now():
                        return p
                    else:
                        await self._brokenQ.put(p)
            except:
                self.context.log.info(f"No proxies available. Retrying in 5...")
            
            await asyncio.sleep(5)        

        raise Exception("No available proxy")


    async def release(self, p: Proxy) -> None:
        await self._proxyQ.put(p)


    async def releaseBroken(self, p:Proxy) -> None:
        
        if p.strikes >= maxStrikes:
            return
        
        p.released_at = datetime.now() + cooldown
        p.strikes += 1
        await self._brokenQ.put(p)