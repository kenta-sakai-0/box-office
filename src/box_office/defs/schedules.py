import dagster as dg

# Retry each failed step up to 3 times (30s, 60s, 120s ± jitter) to ride out
# transient scrape/proxy errors and Databricks warehouse cold starts
job_retry_policy = dg.RetryPolicy(
    max_retries=3,
    delay=30,
    backoff=dg.Backoff.EXPONENTIAL,
    jitter=dg.Jitter.PLUS_MINUS,
)

pipeline_job = dg.define_asset_job(
    name="pipeline_job",
    selection=dg.AssetSelection.all(),
    op_retry_policy=job_retry_policy,
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
