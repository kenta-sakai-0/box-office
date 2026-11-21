import os

import dagster as dg

# Emails on any failed run in this code location (after step retries are exhausted).
# Configure in .env; with Gmail, ALERT_EMAIL_PASSWORD must be an app password, not the account password.
email_from = os.getenv("ALERT_EMAIL_FROM")
email_password = os.getenv("ALERT_EMAIL_PASSWORD")
email_to = [addr.strip() for addr in os.getenv("ALERT_EMAIL_TO", email_from or "").split(",") if addr.strip()]


def failure_email_subject(context: dg.RunFailureSensorContext) -> str:
    return f"[box-office] {context.dagster_run.job_name} failed ({context.dagster_run.run_id[:8]})"


@dg.definitions
def sensors():
    if not (email_from and email_password and email_to):
        return dg.Definitions()

    return dg.Definitions(
        sensors=[
            dg.make_email_on_run_failure_sensor(
                name="email_on_run_failure",
                email_from=email_from,
                email_password=email_password,
                email_to=email_to,
                email_subject_fn=failure_email_subject,
                webserver_base_url=os.getenv("DAGSTER_WEBSERVER_URL", "http://localhost:3000"),
                default_status=dg.DefaultSensorStatus.RUNNING,
            )
        ]
    )
