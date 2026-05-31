import dagster as dg

from box_office.resources.databricks.Databricks import DatabricksResource
from box_office.resources.common.proxies import ProxyResource

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "databricks": DatabricksResource(),
            "proxy": ProxyResource()
        }
    )