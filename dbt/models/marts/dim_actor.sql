-- Dimension: one row per actor/user (grain = actor_id). Same pattern as dim_repo.
--
-- actor_sk: md5 surrogate key over the natural key actor_id (the dim PK + the FK
-- fact_events joins on); actor_id kept as the natural/business key. Same honest
-- caveat as dim_repo -- the integer id would serve as the key as-is.
--
-- actor_login is slowly-changing (users rename) while actor_id is stable: keep the
-- LATEST login by created_at (Type-1 / overwrite). QUALIFY = window-function filter.
--
-- PII boundary (the deliberate checkpoint): actor identity in gold is actor_id +
-- actor_login (a PUBLIC username) ONLY -- never an email. Silver already drops
-- payload/author emails, and this dim does not reach back to bronze to widen it.

with latest_actor as (

    select
        actor_id,
        actor_login
    from {{ ref('stg_events') }}
    qualify row_number() over (
        partition by actor_id order by created_at desc
    ) = 1

)

select
    {{ dbt_utils.generate_surrogate_key(['actor_id']) }} as actor_sk,
    actor_id,
    actor_login
from latest_actor
