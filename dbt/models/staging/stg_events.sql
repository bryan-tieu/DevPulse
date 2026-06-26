-- Staging: 1:1 cleaned pass-through over the silver_events source.
-- Discipline: rename/standardize + light derivation only — NO joins, NO
-- aggregation, NO business logic. Dims/facts (marts) do the modeling.
-- Materialized as a view (dbt_project.yml staging default): cheap, always fresh.

with source as (

    select * from {{ source('silver', 'silver_events') }}

),

renamed as (

    select
        -- identifiers / attributes (already typed + deduped in silver)
        event_id,
        event_type,
        actor_id,
        actor_login,
        repo_id,
        repo_name,
        public,

        -- event timestamp (UTC)
        created_at,

        -- Recover the partition columns Spark's partitionBy stripped into the
        -- GCS path (so they never reached the BQ table). UTC, matching Spark's
        -- to_date()/hour() — keeps event_date/event_hour consistent across layers.
        date(created_at) as event_date,
        extract(hour from created_at) as event_hour

    from source

)

select * from renamed
