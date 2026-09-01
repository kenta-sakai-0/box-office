import dagster as dg

pipeline_job = dg.define_asset_job(
    name="pipeline_job",
    selection=dg.AssetSelection.all(),
)

pipeline_schedule = dg.ScheduleDefinition(
    name="nightly_pipeline_schedule",
    job=pipeline_job,
    cron_schedule="0 1 * * *",
    execution_timezone="America/Los_Angeles",
)


@dg.definitions
def schedules():
    return dg.Definitions(
        jobs=[pipeline_job],
        schedules=[pipeline_schedule],
    )
