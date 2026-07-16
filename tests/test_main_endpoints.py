from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from api.cache import QueryCache, get_query_cache
from api.main import app, get_bq_client


class FakeClock:

    def __init__(self, start: float = 1000.0):
        self._now = start

    def __call__(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


CASES = [
    (
        "/trending",
        [{"date_key": 20260101, "repo_id": 1, "repo_name": "a/b", "stars": 10, "daily_rank": 1}],
    ),
    (
        "/languages/momentum",
        [
            {
                "date_key": 20260101,
                "language": "Python",
                "event_count": 5,
                "active_repos": 3,
                "momentum_delta": None,
                "daily_rank": 1,
            }
        ],
    ),
    (
        "/leaderboard",
        [
            {
                "date_key": 20260101,
                "actor_id": 1,
                "actor_login": "me",
                "contributions": 7,
                "daily_rank": 1,
            }
        ],
    ),
]


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app.dependency_overrides.clear()


def _wire(fed_rows):

    client = Mock()
    client.query.return_value.result.return_value = fed_rows
    fresh_cache = QueryCache(ttl=300, clock=FakeClock())
    app.dependency_overrides[get_bq_client] = lambda: client
    app.dependency_overrides[get_query_cache] = lambda: fresh_cache
    return TestClient(app), client


@pytest.mark.parametrize("path, fed", CASES)
def test_consecutive_identical_requests_miss_then_hit(path, fed):
    tc, mock_client = _wire(fed)

    r1 = tc.get(path, params={"date": "2026-01-01"})
    r2 = tc.get(path, params={"date": "2026-01-01"})

    assert r1.status_code == 200 and r2.status_code == 200
    assert r1.headers["X-Cache"] == "miss"
    assert r2.headers["X-Cache"] == "hit"
    assert mock_client.query.call_count == 1


@pytest.mark.parametrize("path, fed", CASES)
def test_different_date_misses(path, fed):
    tc, mock_client = _wire(fed)

    tc.get(path, params={"date": "2026-01-01"})
    r = tc.get(path, params={"date": "2026-01-02"})

    assert r.headers["X-Cache"] == "miss"
    assert mock_client.query.call_count == 2
