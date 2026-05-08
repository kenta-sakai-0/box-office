# debug_job.py
from dagster import materialize
from box_office.defs.theaters.theaters import theaters, theaters_snapshot
from box_office.resources.databricks.Databricks import DatabricksResource

if __name__ == "__main__":
    result = materialize(
        assets=[theaters_snapshot, theaters],
        resources={
            "databricks": DatabricksResource()
        }
    )