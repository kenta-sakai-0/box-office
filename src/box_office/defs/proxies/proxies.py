import dagster as dg
import os

from box_office.resources.common.webshare import WebshareClient
from box_office.resources.common.proxies import WEBSHARE_PROXIES_FILE_PATH


@dg.asset
def proxy_refresh(context: dg.AssetExecutionContext, webshare: dg.ResourceParam[WebshareClient]) -> None:
    """Refreshes the local Webshare proxies file with the account's currently valid proxies"""

    proxies = webshare.fetch_valid_proxies()

    tmp_path = WEBSHARE_PROXIES_FILE_PATH + ".tmp"
    with open(tmp_path, "w") as f:
        f.write("\n".join(proxies))
    os.replace(tmp_path, WEBSHARE_PROXIES_FILE_PATH)

    context.log.info(f"Refreshed {len(proxies)} proxies")
