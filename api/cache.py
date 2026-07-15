import json
import time
from functools import lru_cache
from typing import Callable

from api.queries import run_query


# unbounded dict keyed on user input + read-time eviction
# allows for abandoned keys to never be reclaimed
# okay for localhost, moved to Redis at scale
class QueryCache:
    
    # use monotonic to prevent steps backwards;
    # backward step would allow an entry to keep serving past its expiry date
    def __init__(self, ttl: float, clock: Callable[[], float] = time.monotonic):
        self.ttl = ttl
        self._clock = clock
        self._store: dict = {}

    # expired must call KeyError
    # Shouldn't be able to call a dead key again
    def get(self, key: tuple[str, tuple[str, ...]]):
        expires_at, rows = self._store[key]

        if self._clock() >= expires_at:

            del self._store[key]
            raise KeyError(key)
        return rows

    def set(self, key: tuple[str, tuple[str, ...]], rows: list[dict]):
        expires_at = self._clock() + self.ttl
        self._store[key] = (expires_at, rows)


@lru_cache(maxsize=1)
def get_query_cache() -> QueryCache:
    return QueryCache(300)


def cache_run_query(client, sql, params, cache):

    dict_key = (sql, tuple(json.dumps(p.to_api_repr(), sort_keys=True) for p in params))

    try:

        rows = cache.get(dict_key)
        return (rows, True)

    except KeyError:

        rows = run_query(client, sql, params)
        cache.set(dict_key, rows)
        return (rows, False)
