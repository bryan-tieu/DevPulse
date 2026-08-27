# 06 · Testing, serving & observability

Skills-map rows: **Testing (pytest)** · **Serving (FastAPI) & dashboards** · **Observability / run metadata & alerting**

---

## Topic: Testing (pytest)

### Q: 93 tests, 0.43 seconds, zero credentials. Name the specific seams that make that possible — at four layers of the stack.

`Asked 2026-08-06 · skills-map quiz Q13 · 0/10 — retrieval failure`

**My answer:**
> Teach

**Verdict:** The largest gap between what's built and what can be sold. This row's repo evidence is **excellent**, and "93 tests in 0.43 seconds with no credentials" is a number that stops an interviewer. Mutation-testing is something very few candidates can claim.

**Model answer:**
> "The rule is that I inject the collaborator and never mock the code under test. The API takes its BigQuery client through FastAPI's dependency system, so a test swaps in a fake and the real routing, validation and error handling still execute — only the warehouse is fake. The cache takes a clock as a parameter, so a 300-second TTL test advances time instantly instead of sleeping. The dashboard client takes a transport. Spark and GE logic are pure functions over plain data — `compute_residual(180387, 180386, 0) == 1` needs no GE runtime at all. That's 93 tests in 0.43 seconds with no credentials, which is also why CI is cheap for me: **hermetic CI isn't something you configure at the end, it's something the architecture permits or forbids.** None of it is retrofittable — a function that constructs its own client can't be tested without patching globals. Honest gaps: Spark tests only run in the container, and Streamlit render code is untested by choice, which is *why* all the real logic lives in the client and pure helpers. And I mutation-test: four of four mutations turned my newest suite red, because I'd previously written an assertion that passed while asserting nothing."

**The four seams, as a table:**

| Layer | Seam | Substitutes for |
|---|---|---|
| Spark / load path | Pure-transform ÷ I/O split | A cluster, GCS, a real load job |
| GE | `quality/checks.py` is pure arithmetic | The GE runtime, pandas, a Parquet read |
| API | Pure SQL builders · `dependency_overrides` · injected clock | BigQuery, and the passage of time |
| Dashboard | Fake transport · pure display helpers | A running uvicorn / live API |

**Follow-ups to expect:**
- *"How do you know your tests are any good?"* → **mutation testing.** 4/4 red, including one producing `DID NOT RAISE`. Plus the born-vacuous `assert error.detail in error.detail` you caught passing while asserting nothing.
- *"What don't you test?"* → Streamlit render code, deliberately. Say *why*: it's a view layer with no logic worth pinning, which is precisely the constraint that forced logic out of the panels.
- *"Why do Spark tests skip on the host?"* → `conftest.py` sets `collect_ignore` when PySpark is absent. They run in the container — and they have no CI home, which is an honest gap.

**Drill:** this is retrieval, not knowledge. Explain the four seams to a wall, then diff against the table above.

---

## Topic: Serving (FastAPI) & dashboards

### Q: Why does the dashboard call your API instead of querying BigQuery directly?

`❓ Predicted probe — not yet drilled` *(answered well on the 2026-07-27 tradeoffs quiz — steelmanned both directions unprompted)*

**Model answer:**
> "Because everything that makes warehouse access safe lives in the API: bound parameters, the 100 MB `maximum_bytes_billed` cap, the TTL cache, deterministic ORDER BY for stable pagination. A dashboard with its own `bigquery.Client` is a **second, uncontrolled path to the warehouse** — uncapped, unparameterized, uncached, and carrying a duplicate copy of SQL that will drift from the API's. One door, guarded. I enforced it structurally, not by convention: there is no `bigquery` import and no SQL string anywhere under `dashboard/`, and because Streamlit lives in a separate venv, an illegal import is a *failing test*. The case where direct-to-warehouse genuinely wins is a single consumer with no API — one hop instead of two, nothing to keep in sync. The moment a second consumer appears, it loses."

**Follow-ups to expect:**
- *"Wouldn't Looker Studio be better?"* → **Yes, for a company** — self-serve exploration, scheduled delivery, no code to maintain. It's the wrong answer *here* because it connects straight to BQ and skips the API, leaving nothing engineered to defend.
- *"What's simplified about Streamlit?"* → single-process, single-user, server-rendered, re-runs the whole script on every interaction — which is exactly why caching is load-bearing architecture rather than an optimization. No auth, no multi-tenancy, no horizontal story.
- *"Is CORS a problem?"* → **No, and knowing why is the point.** Streamlit renders server-side, so `requests` runs in the Streamlit process and the browser never issues a cross-origin call. CORS is a *browser* policy constraining which origins' JavaScript may read a response — it authenticates nobody and stops no server-side client. It becomes required the day a JS frontend calls the API, and it is never a substitute for auth.

---

### Q: Show me how you handle a NULL in the UI.

`❓ Predicted probe — not yet drilled · the project's honesty story, presentation-layer edition`

**Model answer:**
> "Empty, NULL, and unknown are three different states and each has to look like itself. `momentum_delta` is NULL on every row — a single ingested hour makes the day-over-day window degenerate — so it renders as an em dash, never `0`, because `0` asserts 'no change measured,' a claim the data doesn't support. `verdict` is `bool | None`, so the panel is **tri-state**: pass, fail, and *unknown* — collapsing null into either one is a lie an operator would act on. An empty date shows a stated message, not a blank chart, because a blank chart reads as 'zero activity.' And I separated **schema drift from NULL** with a `SchemaError` guard: a missing *key* is a bug — version skew or a typo — while a NULL *value* is data. Using `.get()` would have silently rendered them identically."

**Follow-ups to expect:**
- *"Why not just filter the Unknown bucket out of the chart?"* → that's the INNER-join lie relocated to the presentation layer. The table keeps every row; the chart narrows to resolved languages **with the Unknown count captioned adjacent**. Never a silent filter.
- *"Your API returns `errors[]` — what do you do with it?"* → render it **independently of `results`**. An `errors[]` you don't render is an in-band channel you've turned into `/dev/null` — and that was a real bug I found and fixed, where the partition case printed "No runs available" and returned before showing the errors.

---

## Topic: Observability / run metadata & alerting

### Q: How do you know your pipeline ran, and how do you find out when it didn't?

`❓ Predicted probe — not yet drilled`

**Model answer:**
> "Two mechanisms. **Run metadata**: an observer task with `all_done` trigger rule writes a row to `pipeline_run_metadata` for every run — run id, logical date, per-task states, the counted identity, and a run summary — and it uses `all_done` deliberately so the row lands *even when the run failed*, which is the case you most need the record for. Writes go through free `load_table_from_json` and reads through free `list_rows`, so my ops window costs zero query jobs — verified against `bq ls -j`. **Alerting**: an `on_failure_callback` in `default_args` fires a webhook, and because it fires only after retries are exhausted, the retry routing *is* the alerting latency policy — gates at `retries=0` page in the same minute, where retries had made it 11 minutes. Both paths are proven: one page on red, and the metadata row lands anyway."

**Follow-ups to expect:**
- *"What's the observer's own state in that row?"* → `"running"`, because it's writing the row while it runs. Accepted limitation, documented — not pretended away.
- *"What's missing from this observability story?"* → no metrics backend, no dashboards on task duration, no SLA monitors, no log aggregation. The webhook has no delivery guarantee. It's a run *ledger*, not observability at scale.
- *"Why not just read Airflow's own metadata DB?"* → it exists, but it's the orchestrator's view of task states, not the *data's* view of counts and reconciliation. Mine records the identity the gate checked.
