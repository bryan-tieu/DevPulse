from google.cloud import bigquery

from config import BQ_GOLD_DATASET, GCP_PROJECT


def build_trending_query(
    date_key: int, limit: int, offset: int
) -> tuple[str, list[bigquery.ScalarQueryParameter]]:

    sql = f"""
        SELECT 
            trd.date_key,
            trd.repo_id,
            trd.repo_name,
            trd.stars,
            trd.daily_rank
        FROM `{GCP_PROJECT}.{BQ_GOLD_DATASET}.trending_repos_daily` AS trd
        WHERE trd.date_key = @date_key
        ORDER BY daily_rank ASC, repo_id DESC
        LIMIT @limit
        OFFSET @offset
    """

    query_parameters = [
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
        bigquery.ScalarQueryParameter("offset", "INT64", offset),
        bigquery.ScalarQueryParameter("date_key", "INT64", date_key),
    ]
    return sql, query_parameters


def build_language_momentum_query(
    date_key: int, limit: int, offset: int
) -> tuple[str, list[bigquery.ScalarQueryParameter]]:
    sql = f"""
        SELECT 
            lm.date_key,
            lm.language,
            lm.event_count,
            lm.active_repos,
            lm.momentum_delta,
            lm.daily_rank
        FROM `{GCP_PROJECT}.{BQ_GOLD_DATASET}.language_momentum` AS lm
        WHERE lm.date_key = @date_key
        ORDER BY daily_rank ASC, language DESC
        LIMIT @limit
        OFFSET @offset
    """
    query_parameters = [
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
        bigquery.ScalarQueryParameter("offset", "INT64", offset),
        bigquery.ScalarQueryParameter("date_key", "INT64", date_key),
    ]

    return sql, query_parameters


def build_leaderboard_query(
    date_key: int, limit: int, offset: int
) -> tuple[str, list[bigquery.ScalarQueryParameter]]:

    sql = f"""
        SELECT 
            cl.date_key,
            cl.actor_id,
            cl.actor_login,
            cl.contributions,
            cl.daily_rank
        FROM `{GCP_PROJECT}.{BQ_GOLD_DATASET}.contributor_leaderboard` AS cl
        WHERE cl.date_key = @date_key
        ORDER BY daily_rank ASC, actor_id DESC
        LIMIT @limit
        OFFSET @offset
    """

    query_parameters = [
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
        bigquery.ScalarQueryParameter("offset", "INT64", offset),
        bigquery.ScalarQueryParameter("date_key", "INT64", date_key),
    ]

    return sql, query_parameters


def run_query(
    client: bigquery.Client, sql: str, params: list[bigquery.ScalarQueryParameter]
) -> list[dict]:

    # Limit to 100MB. Failing any job if its estimate
    # exceeds this budget

    job_config = bigquery.QueryJobConfig(query_parameters=params, maximum_bytes_billed=100_000_000)
    query_job = client.query(sql, job_config=job_config)
    rows = query_job.result()

    return [dict(row) for row in rows]
