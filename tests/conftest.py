"""Make the Spark job modules importable from the tests, and skip the Spark
tests when PySpark isn't available.

The silver job lives in `spark/` (not an installed package), so put that dir on
the import path. Resolved relative to this file, it works both inside the Spark
container (/opt/devpulse/spark) and on a host checkout.

PySpark + Java only exist in the Spark container, so on a host checkout (e.g. the
.venv) we skip collecting the Spark tests — the pure-Python tests still run.
"""

import os
import sys

import pytest

from api.cache import get_query_cache


@pytest.fixture(autouse=True)
def _fresh_query_cache():
    get_query_cache.cache_clear()
    yield


try:
    import pyspark  # noqa: F401

    _HAS_PYSPARK = True
except ImportError:
    _HAS_PYSPARK = False

_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, _ROOT)  # repo root: config, transform.* importable
sys.path.insert(0, os.path.join(_ROOT, "spark"))  # spark job modules
sys.path.insert(0, os.path.join(_ROOT, "airflow/dags"))

collect_ignore = [] if _HAS_PYSPARK else ["test_silver_events.py"]
