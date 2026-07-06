-- Mart: trending repositories per day. Grain = one row per (repo, day).
--
-- The first model that JOINS the fact to a dim and AGGREGATES — the shape every
-- serving query (API / dashboard) takes. It reads ref('fact_events') and
-- ref('dim_repo') ONLY — never the source: a mart reaching back to stg_events /
-- silver_events would bypass the star + the incremental fact (a bug).
--
-- "Trending" = stars gained that day. On GitHub the "star" action fires as a
-- WatchEvent (historical naming — Watch is the star, not repo-watching), so the
-- trending signal is COUNT(WatchEvent) per repo per day. Repos with zero stars
-- that day aren't "trending", so we filter to WatchEvent before aggregating.
--
-- daily_rank ranks repos within each day by stars — the leaderboard shape, and a
-- window function computed in the mart (not the fact). RANK (not ROW_NUMBER) so
-- ties share a rank.
--
-- Materialized as a TABLE (marts default): stable + fast for the API to read.
--
-- SCOPE NOTE: over one ingested hour "daily" is degenerate (one date, one hour of
-- events) — the mart SHAPE is exercised, not a real multi-day trend. It comes
-- alive when the backfill window expands. (Same honesty as Day 9's Type-1 caveat.)

with stars as (

    select
        date_key,
        repo_sk,
        count(*) as stars
    from {{ ref('fact_events') }}
    where event_type = 'WatchEvent'
    group by date_key, repo_sk

)

select
    s.date_key,
    s.repo_sk,
    r.repo_id,
    r.repo_name,
    s.stars,
    rank() over (
        partition by s.date_key order by s.stars desc
    ) as daily_rank
from stars as s
inner join {{ ref('dim_repo') }} as r
    on s.repo_sk = r.repo_sk
