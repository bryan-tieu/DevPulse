import datetime
from unittest.mock import patch

import pytest
import requests

from dashboard import api_client
from dashboard.api_client import (
    DEFAULT_TIMEOUT,
    RUNS_TIMEOUT,
    DashboardError,
    _get,
    get_leaderboard,
    get_runs,
)


class FakeResponse:

    def __init__(self, status_code: int, json_body=None, json_raises: bool = False):
        self.status_code = status_code
        self.ok = 200 <= status_code < 400
        self._json_body = json_body
        self._json_raises = json_raises

    def json(self):
        if self._json_raises:
            raise ValueError("No JSON object could be decoded")

        return self._json_body


def test_success_returns_body_unchanged():
    body = {"rows": [{"repo": "test/python", "stars": 10}]}
    with patch.object(
        api_client.requests, "get", return_value=FakeResponse(status_code=200, json_body=body)
    ):
        result = _get("/leaderboard", {"date": datetime.date(2024, 1, 1), "limit": 3})

    assert result == body


@pytest.mark.parametrize(
    "exc, expected",
    [
        (requests.ConnectionError("connection refused"), "Can't reach"),
        (requests.Timeout("slow"), "timed out"),
        (requests.RequestException("boom"), "failed"),
    ],
    ids=["connection", "timeout", "generic"],
)
def test_transport_failure_becomes_dashboard_error(exc, expected):
    with patch.object(api_client.requests, "get", side_effect=exc):
        with pytest.raises(DashboardError) as e:
            _get("/leaderboard", {})

    error = e.value

    assert expected in error.detail
    assert error.status_code is None
    assert not isinstance(error, requests.RequestException)


def test_non_json_error_body_still_yields_dashboard_error():
    with patch.object(
        api_client.requests, "get", return_value=FakeResponse(status_code=500, json_raises=True)
    ):
        with pytest.raises(DashboardError) as e:
            _get("/leaderboard", {})

    error = e.value

    assert error.status_code == 500
    assert error.detail is None


def test_502_carries_api_detail():
    body = {"detail": "BigQuery upstream failed"}

    with patch.object(
        api_client.requests, "get", return_value=FakeResponse(status_code=502, json_body=body)
    ):
        with pytest.raises(DashboardError) as e:
            _get("/leaderboard", {})

    error = e.value

    assert error.detail == "BigQuery upstream failed"
    assert error.status_code == 502


def test_timeout_is_passed_to_leaderboard_get():
    with patch.object(
        api_client.requests, "get", return_value=FakeResponse(status_code=200, json_body={})
    ) as mock_get:
        get_leaderboard(datetime.date(2024, 1, 1), limit=3)

    assert mock_get.call_args.kwargs.get("timeout") == DEFAULT_TIMEOUT


def test_timeout_is_passed_to_runs_get():

    with patch.object(
        api_client.requests, "get", return_value=FakeResponse(status_code=200, json_body={})
    ) as mock_get:
        get_runs(limit=3)

    assert mock_get.call_args.kwargs.get("timeout") == RUNS_TIMEOUT
