from datetime import date

import requests

BASE_URL = "http://127.0.0.1:8000"

# Connection to the API should be near instant
# Getting a response body takes longer since it's a BigQuery query
DEFAULT_TIMEOUT = (1, 6)

# Runs should be quick regardless; Same idea with the connection
# Still relatively quick since no query (list_rows())
RUNS_TIMEOUT = (1, 3)


class DashboardError(Exception):

    def __init__(self, detail: str, status_code: int | None = None) -> None:
        self.detail = detail
        self.status_code = status_code
        super().__init__(self.detail)


def _get_detail(resp: requests.Response) -> str | None:
    try:
        return resp.json().get("detail")
    except ValueError:
        return None


def _get(path: str, params: dict, timeout=DEFAULT_TIMEOUT) -> dict:

    url = f"{BASE_URL}{path}"

    try:
        resp = requests.get(url, params=params, timeout=timeout)

    except requests.ConnectionError as e:
        raise DashboardError(f"Can't reach the dashboard API at {BASE_URL}") from e

    except requests.Timeout as e:
        raise DashboardError("The dashboard API timed out; Request aborted") from e

    except requests.RequestException as e:
        raise DashboardError(f"The request to the dashboard API failed: {e}") from e

    if resp.ok:
        return resp.json()

    raise DashboardError(_get_detail(resp), status_code=resp.status_code)


def get_leaderboard(day: date, limit: int, offset: int = 0) -> dict:
    return _get("/leaderboard", {"date": day, "limit": limit, "offset": offset})


def get_trending(day: date, limit: int, offset: int = 0) -> dict:
    return _get("/trending", {"date": day, "limit": limit, "offset": offset})


def get_runs(limit: int) -> dict:
    return _get("/runs", {"limit": limit}, timeout=RUNS_TIMEOUT)


def get_language_momentum(day: date, limit: int, offset: int = 0) -> dict:
    return _get("/languages/momentum", {"date": day, "limit": limit, "offset": offset})
