import pytest
from google.cloud import bigquery
from unittest.mock import Mock
from api.cache import QueryCache, cache_run_query

class FakeClock:
    
    def __init__(self, start: float = 1000.0):
        self._now = start
    
    def __call__(self) -> float:
        return self._now
    
    def advance(self, seconds: float) -> None:
        self._now += seconds

@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()

@pytest.fixture
def cache(clock) -> QueryCache:
    return QueryCache(ttl=300, clock=clock)

KEY_A = ("SELECT 1", ())
KEY_B = ("SELECT 2", ())
ROWS_A = [{"id": 1, "name": "ana"}, {"id": 2, "name": "ben"}]
ROWS_B = [{"count": 7}]

SP = bigquery.ScalarQueryParameter
SQL = "SELECT * FROM events WHERE day = @day"

def test_get_on_empty_cache_raises(cache):
    
    with pytest.raises(KeyError):
        cache.get(KEY_A)

def test_set_then_get_returns(cache):
    cache.set(KEY_A, ROWS_A)
    assert cache.get(KEY_A) == ROWS_A

def test_get_after_expiry_raises(cache, clock):
    cache.set(KEY_A, ROWS_A)
    clock.advance(301)
    with pytest.raises(KeyError):
        cache.get(KEY_A)

def test_get_before_expiry_returns(cache, clock):
    cache.set(KEY_A, ROWS_A)
    clock.advance(299)
    assert cache.get(KEY_A) == ROWS_A

def test_distinct_keys_do_not_collide(cache):
    cache.set(KEY_A, ROWS_A)
    cache.set(KEY_B, ROWS_B)
    assert cache.get(KEY_A) == ROWS_A
    assert cache.get(KEY_B) == ROWS_B

def _params(day: str):
    return [SP("day", "STRING", day), SP("region", "STRING", "us")]

def test_param_values_become_cache_key_identity():
    
    cache = QueryCache(ttl=300, clock=FakeClock())
    
    client = Mock()
    client.query.return_value.result.return_value = [{"id": 1}, {"id": 2}]
    
    rows1, hit1 = cache_run_query(client, SQL, _params("2026-01-01"), cache)
    rows2, hit2 = cache_run_query(client, SQL, _params("2026-01-01"), cache)
    
    assert client.query.call_count == 1
    assert (hit1, hit2) == (False, True)
    
    # rows1 and rows2 should be identical since the second hit 
    # should return the same object as in rows1, same key
    assert rows1 is rows2
    
    rows3, hit3 = cache_run_query(client, SQL, _params("2026-01-02"), cache)
    assert client.query.call_count == 2
    assert hit3 is False