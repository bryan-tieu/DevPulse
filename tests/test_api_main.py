"""Endpoint tests for /trending — no GCP anywhere.

dependency_overrides swaps get_bq_client for a Mock before the endpoint runs:
the API-layer version of the injected-client seam test_queries.py proves for
run_query. If any test here needs live BigQuery, the dependency injection has
failed at its one job.
"""

from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
from google.api_core import exceptions as gcp_exceptions

from api.main import app, get_bq_client

FAKE_ROWS = [
    {
        "date_key": 20240101,
        "repo_id": 101,
        "repo_name": "octo/hello",
        "stars": 42,
        "daily_rank": 1,
    },
    {
        "date_key": 20240101,
        "repo_id": 99,
        "repo_name": "octo/world",
        "stars": 42,
        "daily_rank": 1,
    },
]


@pytest.fixture
def bq_mock():
    # Cleared in teardown so an override can't leak into another test module.
    mock = Mock()
    mock.query.return_value.result.return_value = FAKE_ROWS
    app.dependency_overrides[get_bq_client] = lambda: mock
    yield mock
    app.dependency_overrides.clear()


@pytest.fixture
def client(bq_mock):
    return TestClient(app)


def test_trending_happy_path(client):
    resp = client.get("/trending", params={"date": "2024-01-01", "limit": 2})

    assert resp.status_code == 200
    body = resp.json()
    assert body["date"] == "2024-01-01"
    assert body["limit"] == 2
    assert body["offset"] == 0
    assert body["results"] == FAKE_ROWS


def test_trending_converts_iso_date_to_smart_key(client, bq_mock):
    # The edge decision made real: ISO in the URL, YYYYMMDD INT64 on the wire
    # to BQ. Asserted on the bound parameter, not the internals.
    client.get("/trending", params={"date": "2024-01-01"})

    job_config = bq_mock.query.call_args.kwargs["job_config"]
    bound = {p.name: p.value for p in job_config.query_parameters}
    assert bound["date_key"] == 20240101


def test_trending_limit_out_of_bounds_is_422(client, bq_mock):
    resp = client.get("/trending", params={"date": "2024-01-01", "limit": 101})

    assert resp.status_code == 422
    # Validation must reject at the edge — before a query job can exist.
    bq_mock.query.assert_not_called()


def test_trending_impossible_date_is_422(client, bq_mock):
    # The reason ?date= is typed date: a raw int param would bind 20241301,
    # scan, and 200 an empty page — a silent drop wearing a success code.
    resp = client.get("/trending", params={"date": "2024-13-01"})

    assert resp.status_code == 422
    bq_mock.query.assert_not_called()


def test_trending_date_is_required(client, bq_mock):
    resp = client.get("/trending")

    assert resp.status_code == 422
    bq_mock.query.assert_not_called()


def test_trending_byte_cap_trip_is_500(client, bq_mock):
    # Verified empirically (1-byte cap, free — fails before scanning): the
    # client library has no mapping for reason=bytesBilledLimitExceeded, so it
    # falls back to InternalServerError. The reason string is the contract.
    bq_mock.query.return_value.result.side_effect = gcp_exceptions.InternalServerError(
        "Query exceeded limit for bytes billed: 1.",
        errors=[{"reason": "bytesBilledLimitExceeded"}],
    )

    resp = client.get("/trending", params={"date": "2024-01-01"})

    assert resp.status_code == 500
    assert "byte budget" in resp.json()["detail"]


def test_trending_backend_error_is_502(client, bq_mock):
    # Same exception class as the cap trip, different reason — must NOT be
    # reported as a byte-budget problem.
    bq_mock.query.return_value.result.side_effect = gcp_exceptions.InternalServerError(
        "backend error", errors=[{"reason": "internalError"}]
    )

    resp = client.get("/trending", params={"date": "2024-01-01"})

    assert resp.status_code == 502
    assert resp.json()["detail"] == "Upstream warehouse error"
