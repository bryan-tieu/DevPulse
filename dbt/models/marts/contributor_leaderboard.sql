-- Mart: contributor leaderboard per day. Grain = one row per (actor, day).
--
-- Structurally a near-clone of trending_repos_daily (fact -> dim -> aggregate ->
-- rank()). Reads ref('fact_events') + ref('dim_actor') ONLY — never the source.
-- The fact is lean (actor_sk only, no actor_login), so we join dim_actor to
-- recover the public login, exactly as trending_repos_daily joins dim_repo.
--
-- THE DECISION — not every event is a "contribution". A leaderboard of
-- CONTRIBUTORS shouldn't count a WatchEvent (starring) or ForkEvent as a
-- contribution, so we filter the fact to a CONTRIBUTION event-type allowlist
-- below. HONEST TRADEOFF: every allowed event counts EQUALLY — a merged
-- PullRequestEvent weighs the same as a single IssueCommentEvent. Real
-- leaderboards WEIGHT contributions (a CASE assigning points per type); we keep
-- the flat count today (clearer; weighting is business-rule tuning, not an
-- engineering gap). The allowlist + the flat-count choice are logged in decisions.md.
--
-- Materialized as a TABLE (marts default): stable + fast for the API to read.
--
-- SCOPE NOTE: over one ingested hour "daily" is degenerate (one date). The mart
-- SHAPE is exercised; it comes alive when the backfill expands.
-- PII: actor identity is actor_id/actor_sk/actor_login (public username) only —
-- no email. dim_actor drew this line on Day 9; the leaderboard doesn't widen it.

with contributions as (

    select
        date_key,
        actor_sk,
        count(*) as contributions
    from {{ ref('fact_events') }}
    where
        event_type in (
            'PushEvent',
            'PullRequestEvent',
            'IssuesEvent',
            'PullRequestReviewEvent',
            'PullRequestReviewCommentEvent',
            'IssueCommentEvent',
            'CommitCommentEvent',
            'CreateEvent'
        )
    group by date_key, actor_sk

)

select
    c.date_key,
    c.actor_sk,
    a.actor_id,
    a.actor_login,
    c.contributions,
    rank() over (
        partition by c.date_key order by c.contributions desc
    ) as daily_rank
from contributions as c
inner join {{ ref('dim_actor') }} as a
    on c.actor_sk = a.actor_sk
