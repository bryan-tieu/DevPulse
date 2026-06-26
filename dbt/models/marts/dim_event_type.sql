-- Dimension: the distinct set of GitHub event types.
-- The smallest dim, on purpose: event_type IS its own natural key, so there's no
-- surrogate key to generate yet (that machinery arrives with dim_repo/dim_actor).
-- First use of ref() — the staging -> mart edge in the DAG. Materialized as a
-- table (marts default): small, stable, fast for downstream joins.

with events as (

    select distinct event_type
    from {{ ref('stg_events') }}

)

select event_type
from events
