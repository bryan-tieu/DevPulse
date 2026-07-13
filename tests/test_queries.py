from unittest.mock import Mock

from google.cloud import bigquery

from api.queries import (
    build_language_momentum_query,
    build_leaderboard_query,
    build_trending_query,
    run_query,
)
from config import BQ_GOLD_DATASET, GCP_PROJECT


def normalize_string(sql) -> str:
    return " ".join(sql.split())


def test_build_trending_query():
    sql, _ = build_trending_query(20240101, 25, 50)

    expected_string = f"FROM `{GCP_PROJECT}.{BQ_GOLD_DATASET}.trending_repos_daily` AS trd"

    assert expected_string in normalize_string(sql)


def test_build_language_momentum_query():
    sql, _ = build_language_momentum_query(20240101, 25, 50)

    expected_string = f"FROM `{GCP_PROJECT}.{BQ_GOLD_DATASET}.language_momentum` AS lm"

    assert expected_string in normalize_string(sql)


def test_leaderboard_query():
    sql, _ = build_leaderboard_query(20240101, 25, 50)

    expected_string = f"FROM `{GCP_PROJECT}.{BQ_GOLD_DATASET}.contributor_leaderboard` AS cl"

    assert expected_string in normalize_string(sql)


def test_trending_query_placeholders():
    sql, _ = build_trending_query(20240101, 25, 50)

    assert "WHERE trd.date_key = @date_key" in sql
    assert "ORDER BY daily_rank ASC, repo_id DESC" in sql
    assert "LIMIT @limit" in sql
    assert "OFFSET @offset" in sql


def test_language_momentum_query_placeholders():
    sql, _ = build_language_momentum_query(20240101, 25, 50)

    assert "WHERE lm.date_key = @date_key" in sql
    assert "ORDER BY daily_rank ASC, language DESC" in sql
    assert "LIMIT @limit" in sql
    assert "OFFSET @offset" in sql


def test_leaderboard_query_placeholders():
    sql, _ = build_leaderboard_query(20240101, 25, 50)

    assert "WHERE cl.date_key = @date_key" in sql
    assert "ORDER BY daily_rank ASC, actor_id DESC" in sql
    assert "LIMIT @limit" in sql
    assert "OFFSET @offset" in sql


def test_trending_query_params_and_types():
    _, params = build_trending_query(20240101, 25, 50)

    assert all(isinstance(param, bigquery.ScalarQueryParameter) for param in params)

    assert {p.name: (p.type_, p.value) for p in params} == {
        "date_key": ("INT64", 20240101),
        "limit": ("INT64", 25),
        "offset": ("INT64", 50),
    }


def test_language_momentum_query_params_and_types():
    _, params = build_language_momentum_query(20240101, 25, 50)

    assert all(isinstance(param, bigquery.ScalarQueryParameter) for param in params)

    assert {p.name: (p.type_, p.value) for p in params} == {
        "date_key": ("INT64", 20240101),
        "limit": ("INT64", 25),
        "offset": ("INT64", 50),
    }


def test_leaderboard_query_params_and_types():
    _, params = build_leaderboard_query(20240101, 25, 50)

    assert all(isinstance(param, bigquery.ScalarQueryParameter) for param in params)

    assert {p.name: (p.type_, p.value) for p in params} == {
        "date_key": ("INT64", 20240101),
        "limit": ("INT64", 25),
        "offset": ("INT64", 50),
    }


def test_run_query_uses_injected_client_and_cap():
    # The injected client is the test seam: run_query must talk to THIS object,
    # never construct its own (a self-made client silently routes tests to
    # production GCP — the shadow-binding bug class).
    client = Mock()
    client.query.return_value.result.return_value = [{"repo_id": 1, "stars": 5}]

    params = [bigquery.ScalarQueryParameter("date_key", "INT64", 20240101)]
    rows = run_query(client, "SELECT 1", params)

    client.query.assert_called_once()
    assert client.query.call_args.args[0] == "SELECT 1"

    job_config = client.query.call_args.kwargs["job_config"]
    assert job_config.maximum_bytes_billed == 100_000_000
    assert job_config.query_parameters == params

    assert rows == [{"repo_id": 1, "stars": 5}]
