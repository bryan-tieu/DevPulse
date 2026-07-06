-- Fact: the center of the star. ONE row per GitHub event (grain = event_id, a
-- DEGENERATE dimension — a natural key kept on the fact itself, no dim table).
-- A fact holds KEYS + MEASURES, not wide attributes: the four FKs below point at
-- the dims for everything descriptive.
--
-- FACTLESS. A GitHub event has no natural additive quantity (no amount, no
-- duration), so there is NO measure column — the marts express volume as
-- COUNT(*). The classic Kimball "1 as event_count" convenience is deliberately
-- skipped (a plain COUNT(*) is clearer and costs nothing extra).
--
-- Incremental materialization (the day's lesson):
-- materialized='incremental' + strategy 'merge' on unique_key='event_id':
--   * First build (table doesn't exist yet → is_incremental() is FALSE): full
--     rebuild from all of stg_events.
--   * Later builds: only the trailing window (the is_incremental() filter
--     below) is recomputed, then MERGEd into the existing table on event_id —
--     an UPSERT. A re-appearing event_id is overwritten, never appended, so
--     re-runs can't duplicate rows. That row-grain idempotency is why we chose
--     merge over insert_overwrite here.
--
-- HONEST TRADEOFF: for immutable, append-only events, insert_overwrite by
-- event_date partition would scan fewer bytes (replace whole partitions — the
-- direct heir of Day 5's $YYYYMMDDHH decorator + Day 6's dynamic overwrite). We
-- take merge anyway to exercise the unique_key upsert and get row-grain dedupe.
-- We keep partition_by=event_date REGARDLESS: BigQuery uses it to PRUNE the
-- MERGE's target scan to the affected day(s) instead of the whole table — the
-- concrete reason partitioning still pays off under merge.
--
-- on_schema_change='fail': a new/dropped column should BREAK LOUDLY (forcing a
-- conscious `dbt build --full-refresh`), never silently drift the table.
--
-- cluster_by=['event_type']: the marts filter on event_type (e.g. WatchEvent for
-- trending) — clustering co-locates those rows so the mart reads scan less.
-- Cheap and read-side only; drop it if you'd rather keep the config minimal.

{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="event_id",
        partition_by={"field": "event_date", "data_type": "date"},
        cluster_by=["event_type"],
        on_schema_change="fail",
    )
}}

with events as (

    select
        src.event_id,
        src.repo_id,
        src.actor_id,
        src.event_type,
        src.event_date,
        src.created_at
    from {{ ref('stg_events') }} as src

    {% if is_incremental() %}
    -- Watermark + LOOKBACK: reprocess only events at/after (latest loaded ts − 1h)
    -- rather than everything. The 1-hour lookback (not a strict `> max`) is the
    -- LATE-ARRIVING-ROWS hedge — events that showed up after the watermark had
    -- already advanced get re-swept and MERGEd (upsert dedupes them). Bigger
    -- lookback = safer against lateness, more bytes scanned each run.
        where src.created_at >= (
            select timestamp_sub(max(dst.created_at), interval 1 hour)
            from {{ this }} as dst
        )
    {% endif %}

)

select
    -- degenerate dimension + the merge unique_key
    event_id,

    -- FKs, REGENERATED DETERMINISTICALLY from the natural keys — no dim join.
    -- generate_surrogate_key is a pure md5, so the same repo_id/actor_id here
    -- hashes to the SAME value the dim stored; the relationships tests (Step 2)
    -- prove every key lands. The classic ETL alternative is a surrogate-key
    -- LOOKUP JOIN against the dim — needed only for sequential surrogates or
    -- early-arriving facts; unnecessary here since both come from stg_events.
    {{ dbt_utils.generate_surrogate_key(['repo_id']) }} as repo_sk,
    {{ dbt_utils.generate_surrogate_key(['actor_id']) }} as actor_sk,

    -- FK to dim_date, matching its SMART YYYYMMDD key by the same derivation.
    cast(format_date('%Y%m%d', event_date) as int64) as date_key,

    -- FK to dim_event_type (whose PK is the NATURAL key) — passes straight through.
    event_type,

    -- partition column + the incremental watermark source
    event_date,
    created_at

from events
