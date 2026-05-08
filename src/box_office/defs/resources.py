import dagster as dg

from box_office.resources.databricks.Databricks import DatabricksResource

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "databricks": DatabricksResource()
        }
    )