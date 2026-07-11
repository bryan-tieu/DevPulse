import os
from datetime import datetime

import requests


def build_alert_payload(
    dag_id: str, task_id: str, logical_date: str, try_number: int, exception: str
) -> dict:

    alert_payload = {
        "content": (
            f"DAG: {dag_id}\n"
            f"Task: {task_id}\n"
            f"Logical date: {logical_date}\n"
            f"Try: {try_number}\n"
            f"Exception: {exception}"
        )
    }

    return alert_payload


def notify_failure(context: dict) -> None:

    task_instance = context["ti"]

    dag_id = task_instance.dag_id
    task_id = task_instance.task_id

    # Convert datetime to string to parse properly in JSON
    logical_date = context.get("logical_date")
    if isinstance(logical_date, datetime):
        logical_date = logical_date.isoformat()

    try_number = task_instance.try_number
    exception = context.get("exception")
    to_string_exception = str(exception)

    alert_payload_result = build_alert_payload(
        dag_id, task_id, logical_date, try_number, to_string_exception
    )
    print(alert_payload_result)

    url = os.environ.get("ALERT_WEBHOOK_URL")

    if not url:
        print("ALERT_WEBHOOK_URL not set. Failure alert has no destination")
        return

    try:
        response = requests.post(
            url,
            json=alert_payload_result,
            # Timeout because this is a failure alert.
            # We don't want to be stuck here if we're
            # monitoring a failure. This becomes the failure
            # if it doesn't timeout.
            timeout=15,
        )
        response.raise_for_status()

    except requests.RequestException as e:

        # Log and continue, not raise
        # This runs through on_failure_callback which means
        # the task already failed. Raising would do nothing useful
        # We log the failure and keep moving
        print(f"POST failed with {e}")
