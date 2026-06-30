-- Dimension: one row per repository (grain = repo_id).
--
-- repo_sk: a deterministic md5 surrogate key (dbt_utils.generate_surrogate_key)
-- over the natural key repo_id. It is this dim's PK and the single-column FK that
-- fact_events will join on; repo_id (the natural/business key) is kept as a column.
-- Honest tradeoff: repo_id is already a clean single-source integer that would
-- serve as the key as-is -- the surrogate is the Kimball pattern, adopted to
-- practise it and give the fact a uniform key shape, not out of strict necessity.
--
-- repo_name is a slowly-changing attribute (repos get renamed/transferred) while
-- repo_id is stable, so "one row per repo" needs a rule for WHICH name to keep.
-- We take the LATEST by created_at -- a Type-1 (overwrite) dimension: current
-- value only, no history. SCD Type-2 (versioned rows with valid-from/valid-to) is
-- the deferred production alternative. QUALIFY filters the window function without
-- a wrapping subquery (BigQuery sugar).

with latest_repo as (

    select
        repo_id,
        repo_name
    from {{ ref('stg_events') }}
    qualify row_number() over (
        partition by repo_id order by created_at desc
    ) = 1

)

select
    {{ dbt_utils.generate_surrogate_key(['repo_id']) }} as repo_sk,
    repo_id,
    repo_name
from latest_repo
