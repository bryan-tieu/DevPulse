# 04 · Modeling & dbt

Skills-map rows: **Warehouse modeling (Kimball star schema)** · **dbt (models, tests, docs, seeds, packages)**

> ⚠️ **Highest-risk topic for the "claims your repo contradicts" pattern.** Both 2026-08-06 misses landed here. On a portfolio project the interviewer may have the code open — every claim must be pointable.

---

## Topic: Warehouse modeling (Kimball star schema)

### Q: `dim_repo` uses a surrogate key and is Type-1. (a) Defend both choices. (b) Repos get renamed — what does your star schema report, what have you destroyed, and which mart starts lying?

`Asked 2026-08-06 · skills-map quiz Q6 · 6/10`

**My answer:**
> a. The surrogate key buys us one less join between the fact and dim_repo. Because the surrogate is an md5, the fact can deterministically generate the surrogate key for dim_repo. Type 1 SCD means we keep no history of changes, we only keep a record of the most active record.
> b. My star schema reports the new repo name and destroys the old one. I'm going to guess language momentum but I dont know the reason why

**Verdict:** The determinism insight is real. But **"one less join" is contradicted by your own model** — [language_momentum.sql](../../dbt/models/marts/language_momentum.sql) documents re-joining `dim_repo` to recover `repo_id` for the seed. Net-neutral at best. And the mart guess was **wrong**: language survives renames *because* the enrichment keys on `repo_id`, the stable key. Minor: Type-1 keeps the most **recent** record (`qualify row_number() … order by created_at desc`), not the "most active."

**Model answer:**
> "The surrogate is a deterministic md5 over `repo_id`, which means the fact computes its own FK without a lookup join to the dim — that's real. I'll be honest that it's net-neutral overall, because my language mart re-joins `dim_repo` to recover the natural `repo_id` for the enrichment seed; my model comment says outright that `repo_id` is a clean single-source integer that would have served as the key, and the surrogate was adopted to practise the pattern and give the fact a uniform key shape. Type-1 means the dim is recomputed to current values every build, so a rename retroactively relabels history: `trending_repos_daily` shows a 2024 event under its 2026 name, counts still reconciling perfectly, no test failing — history silently restated. My language mart *survived* renames only because I keyed the enrichment on `repo_id`, not `repo_name`. And I couldn't move to Type-2 with this key — hashing the natural key alone gives a durable key that's identical across versions; I'd need to hash key plus effective date."

**Follow-ups to expect:**
- *"Could you move to Type-2 with your current key?"* → **No.** Durable key, identical across versions. This is the answer that salvages the question.
- *"When does a surrogate genuinely earn its keep?"* → composite natural keys, multi-source integration (GitLab + GitHub colliding), and any Type-2 dimension. Not clean single-source integers.
- *"Which mart lies on a rename?"* → `trending_repos_daily` (and `contributor_leaderboard` via `actor_login`). **Not** `language_momentum`.

**Drill:** before defending any benefit, ask *"can I point at the line that proves it?"* If not, concede — your docs already hold the stronger concession.

---

### Q: Your fact has no measure column. Explain that.

`❓ Predicted probe — not yet drilled`

**Model answer:**
> "It's a **factless fact table** — a GitHub event carries no natural additive quantity, so there's nothing honest to sum; marts express volume as `COUNT(*)`. `event_id` is a **degenerate dimension**: a natural key kept on the fact with no dimension table behind it, and it doubles as the merge `unique_key`. I deliberately skipped the Kimball convenience of adding a constant `1 as event_count` — a plain `COUNT(*)` is clearer and costs nothing, and a constant column invites someone to `SUM` it and think they've measured something."

**Follow-ups:** *"When is a factless fact the right model?"* → coverage/event tables generally: attendance, clicks, sensor pings — anything where the *occurrence* is the fact.

---

## Topic: dbt

### Q: `fact_events` is incremental with `merge` on a `unique_key`. (a) Defend merge over `insert_overwrite`. (b) You measured them — what happened?

`Asked 2026-08-06 · skills-map quiz Q7 · 6/10`

**My answer:**
> a. merge matches rows on the unique key, and updates it or inserts a new one, row level. insert overwrite replaces the entire partition/table and inserts the new record. Merge buys correctness on the identity. An upsert on a unique key cannot produce duplicates no matter the window.
> b. MERGE scans more at this scale compared to insert overwrite. At scale, we have more partitions to prune and eventually the tradeoff becomes a win for merge

**Verdict:** (a) is strong — *"cannot produce duplicates no matter the window"* is a crisp statement of the guarantee. **(b) threw away your best asset.** The whole value of this story is that you *measured* it; told without figures it's indistinguishable from having read the dbt docs. Worse, you asserted a comparison you never ran — the measurement was **CTAS vs MERGE**, not merge vs `insert_overwrite`.

**Model answer:**
> "Merge upserts row-by-row on `event_id`, so a re-appearing event overwrites rather than duplicates — row-grain idempotency regardless of the lookback window. `insert_overwrite` swaps whole partitions, and it's correct only when the incoming batch is the *complete* contents of each partition it touches; a partial batch silently deletes rows that were already there. For immutable append-only events `insert_overwrite` is genuinely the cheaper strategy, and I chose merge anyway — deliberately, to get row-grain dedupe and exercise the `unique_key` machinery — and I logged the bill. First build as a CTAS scanned **8.5 MiB**; the MERGE run scanned **40.4 MiB**, about **4.75× more**. A CTAS scans one thing; a MERGE scans three — source, target for key matching, and the `max(created_at)` watermark subquery against the target, which is the one nobody predicts. Pruning couldn't help because all 180,386 rows were in one `event_date` partition that the one-hour lookback reprocessed entirely. The payoff only appears at months of partitions where a MERGE prunes to a sliver against a full rebuild — over one hour I paid the overhead with none of the benefit."

**Follow-ups to expect:**
- *"How did you measure that?"* ← the seam. Answer honestly: CTAS first-build vs MERGE second-build, from job bytes-billed.
- *"So would you switch to `insert_overwrite`?"* → for this workload, on cost grounds, yes — and the reason I haven't is that the project's purpose is to exercise the pattern. That concession is strong, not weak.
- *"What's the lookback for?"* → late-arriving rows. Bigger lookback = safer against lateness, more bytes scanned. Over one static hour it only proves the mechanism — exercised, not stress-tested.

**Drill:** rehearse aloud until **8.5 → 40.4** arrives without effort. Your best story, currently your worst-delivered.

---

### Q: Walk me through what `dbt build` actually does, and why it's a gate.

`❓ Predicted probe — not yet drilled` *(prior slip 2026-07-30: answered "builds/updates" on swing one — lead with the mechanism)*

**Model answer:**
> "`dbt build` runs models and their tests **interleaved in DAG order** — not all models then all tests. So a model's tests run immediately after it's built and before anything downstream of it, which means a bad row stops the build at the point it enters rather than propagating into three marts first. That's what makes it a *gate*: it's wired as a DockerOperator task with `retries=0`, so a failure fails the DAG task and pages, rather than producing a dashboard nobody reads. My baseline is **PASS=69** — and that number doubles as an idempotency proof, because the grain tests would go red if a re-run appended instead of merging. On the incremental fact specifically, the first build is a `CREATE TABLE` because `is_incremental()` is false with no table present; later builds compile to a MERGE against the watermark window."

**Follow-ups to expect:**
- *"Has it ever caught anything?"* → yes, first live catch on Day 12 was real bad data.
- *"What's `on_schema_change='fail'` for?"* → a column change breaks loudly and forces a conscious `--full-refresh` rather than silently reshaping the table.
- *"Green means what, exactly?"* → nothing **errored**, and every test passed. A gate *failing* is red, not green. (Flagged as a prior precision slip — "green = nothing broke" is not the same claim.)

---

### Q: Every mart LEFT JOINs the enrichment seed with `COALESCE('Unknown')`. Why not INNER?

`❓ Predicted probe — not yet drilled · this is the project's signature honesty story`

**Model answer:**
> "Because the seed is **partial** — it covers a handful of repos out of tens of thousands. An INNER join to a partial reference table silently drops every event whose repo isn't seeded, which over one hour is **178,081 of 180,386 events — 98.7%**. The totals would still reconcile internally and look perfectly clean; they'd just be answering a different question: 'the languages we happened to seed' rather than 'the languages in the data.' LEFT JOIN plus `COALESCE('Unknown')` keeps every event and makes the coverage gap **visible as a bucket** instead of invisible as an absence. And I carried that through to the dashboard — the `Unknown` bar is shown and labelled with its share, never filtered out, because filtering it in the presentation layer is the same lie relocated one hop downstream."

**Follow-ups to expect:**
- *"98.7% Unknown — is that mart even useful?"* → **Honest answer: barely, as a metric.** It's structurally correct and coverage-limited. The fix is real enrichment (a GitHub repos-API source), not a different join.
- *"How would you build that enrichment at scale?"* → GraphQL batch fetch behind an incremental anti-join, fetch-once per repo, keyed on row *presence* rather than `language IS NULL` (so a 404-tombstone doesn't get re-fetched forever).
