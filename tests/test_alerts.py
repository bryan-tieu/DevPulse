from airflow.dags.alerts import build_alert_payload


def test_build_alert_payload():
    dag_id = "devpulse"
    task_id = "load_silver"
    logical_date = "2024-01-01T00:00:00"
    try_number = 1
    exception = KeyError("missing column 'id'")

    assert build_alert_payload(dag_id, task_id, logical_date, try_number, exception) == {
        "content": (
            "DAG: devpulse\n"
            "Task: load_silver\n"
            "Logical date: 2024-01-01T00:00:00\n"
            "Try: 1\n"
            "Exception: \"missing column 'id'\""
        )
    }


def test_payload_format():
    test_payload = build_alert_payload(
        dag_id="devpulse",
        task_id="load_silver",
        logical_date="2024-01-01T00:00:00",
        try_number=1,
        exception=str(KeyError("missing column 'id'")),
    )

    assert set(test_payload) == {"content"}
    assert isinstance(test_payload["content"], str)
