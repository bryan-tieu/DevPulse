-- Mart: programming-language momentum over time. Grain = one row per (language, day).
--
-- Language is NOT in the event stream (payload was dropped from silver on Day 6 —
-- shape drift + author-email PII), so it is ENRICHED in from the repo_languages
-- SEED (a stand-in for a real GitHub-repos-API repo_metadata source).
--
-- KEY ROUTING: fact_events is lean — it carries repo_sk (the surrogate), NOT the
-- natural repo_id. The seed is keyed on repo_id (what an external enrichment
-- source knows). So we re-join dim_repo to recover repo_id from repo_sk, then
-- LEFT JOIN the seed. That the fact holds only the surrogate is a deliberate
-- Day-10 choice; enriching by natural key is its consequence. The fact->dim_repo
-- join is safe (Day-10 relationships test proved every repo_sk matches) — it
-- drops nothing, so SUM(event_count) still reconciles to fact COUNT(*).
--
-- LEFT JOIN + COALESCE('Unknown'), never INNER: an INNER join to a PARTIAL seed
-- would SILENTLY DROP every event whose repo isn't seeded (over one hour, most of
-- them), corrupting the totals into "only the languages we happened to seed".
-- LEFT keeps every event and makes the coverage gap VISIBLE as an 'Unknown'
-- bucket — the "never drop silently" rule applied to a join.
--
-- Reads ref('fact_events')/ref('dim_repo')/ref('repo_languages') — never the
-- source (a mart reaching past the fact bypasses the star + incremental fact).
--
-- Materialized as a TABLE (marts default).
--
-- SCOPE NOTE: "momentum" (period-over-period) is DEGENERATE over one hour — there
-- is only one date_key, so lag() is NULL and there is no prior period to compare.
-- The mart SHAPE (the window function) is exercised, not a real trend; it comes
-- alive once the backfill spans multiple days. (Same honesty as trending_repos_daily.)

with enriched as (

    select
        f.date_key,
        f.repo_sk,
        -- noqa: RF04 — sqlfluff flags `language` as a keyword, but BigQuery allows
        -- it as an identifier (same false-positive as Day 9's `quarter`).
        coalesce(l.language, 'Unknown') as language  -- noqa: RF04
    from {{ ref('fact_events') }} as f
    inner join {{ ref('dim_repo') }} as r
        on f.repo_sk = r.repo_sk
    left join {{ ref('repo_languages') }} as l
        on r.repo_id = l.repo_id

),

by_language_day as (

    select
        date_key,
        language,
        count(*) as event_count,
        count(distinct repo_sk) as active_repos
    from enriched
    group by date_key, language

)

select
    date_key,
    language,
    event_count,
    active_repos,
    -- period-over-period momentum: this day's events minus the prior day's for the
    -- same language. NULL for the first date per language (no prior period) — so
    -- this column is deliberately NOT tested not_null (degenerate over one hour).
    event_count - lag(event_count) over (
        partition by language order by date_key
    ) as momentum_delta,
    -- the day's language leaderboard (RANK — ties share a rank)
    rank() over (
        partition by date_key order by event_count desc
    ) as daily_rank
from by_language_day
