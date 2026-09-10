import dagster as dg
import os
import requests

WEBSHARE_PROXY_LIST_URL = 'https://proxy.webshare.io/api/v2/proxy/list/'


class WebshareResource(dg.ConfigurableResource):
    api_key: str = os.getenv("WEBSHARE_API_KEY")

    def create_resource(self, context: dg.InitResourceContext) -> 'WebshareClient':
        return WebshareClient(self.api_key)


class WebshareClient:

    def __init__(self, api_key: str):
        self.api_key = api_key

    def fetch_valid_proxies(self) -> list[str]:
        """Fetches every valid proxy on the account as 'ip:port:username:password' lines"""

        headers = {"Authorization": f"Token {self.api_key}"}
        params = {"mode": "direct", "valid": "true", "page_size": 100}

        proxies = []
        url = WEBSHARE_PROXY_LIST_URL
        while url:
            resp = requests.get(url, headers=headers, params=params)
            resp.raise_for_status()
            data = resp.json()

            for p in data.get('results', []):
                proxies.append(f"{p['proxy_address']}:{p['port']}:{p['username']}:{p['password']}")

            url = data.get('next')
            params = None  # 'next' is already a full URL with query params baked in

        return proxies
