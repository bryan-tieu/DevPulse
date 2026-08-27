# 02 · Orchestration & idempotency

Skills-map rows: **Orchestration (Airflow)** · **Idempotent pipeline design**

> These two are one topic wearing two hats. Idempotency is a *consequence* of interval purity — say that connection out loud whenever either comes up.

---

## Topic: Orchestration (Airflow)

### Q: A colleague writes an ingestion task computing its target hour as `datetime.now() - timedelta(hours=1)`. It runs correctly in production for six months. Name everything broken about it — specifically on a backfill and on a retry.

`Asked 2026-08-06 · skills-map quiz Q3 · 8/10`

**My answer:**
> A backfill doesn't go through because that target hour function only generates an hour before. A backfill from last week doesn't know what interval to backfill, it only knows the previous hour interval from the current time. When a task retries after a failure the time shifts from the duration of the original failure. That shift can make that task retry on a different time interval.

**Verdict:** Both failure modes identified, and the retry answer is precise. **Under-sold the backfill severity** — it doesn't fail, it *succeeds*. And missed the deepest consequence: this destroys idempotency.

**Model answer:**
> "It's not that it breaks — it's that it succeeds while being wrong, which is worse. Backfilling 168 runs, every task computes the same `now() - 1h` and ingests one identical hour. All 168 go green. You get a wall of successful task instances and a week of data that was never fetched. On a retry crossing an hour boundary, attempt two targets a different hour than attempt one, leaving one partition partial and another double-written. And the deep problem is that my idempotency is a *consequence* of interval purity — my partition decorator is derived from the logical date, so wall-clock input means re-runs stop overwriting and start accumulating. Every guarantee in my hard rules is downstream of that. The fix is `data_interval_start` passed as a parameter, which also makes it unit-testable — same reason my cache takes an injected clock."

**Follow-ups to expect:**
- *"How would you catch this in code review?"* → any DAG reading `datetime.now()`, `date.today()`, or `time.time()` in task logic is an automatic block; the interval is in context and must be passed as a parameter.
- *"Does normal operation drift too?"* → **Yes.** A run delayed by scheduler queueing or worker starvation computes from when it *started executing*, not when it was scheduled. Plus DST and timezones.
- *"Why does purity make it testable?"* → same seam as the injected clock in the API cache. `datetime.now()` in a DAG and `time.time()` in a cache are one bug in two costumes.

**Drill:** always land the cache connection. It's what makes you sound like you think in patterns rather than facts.

---

### Q: Your `wait_for_archive` sensor uses `reschedule` mode, not the default `poke`. What changes mechanically, why is it right here, and when does `poke` win?

`Asked 2026-08-06 · skills-map quiz Q4 · 8/10`

**My answer:**
> Reschedule allows the task to release the worker slot and sleeps until the next. Poke keeps the worker slot active so other tasks can't use it while it's active. Reschedule changes it's state to "Up for reschedule" so that the system knows that the next interval should give the slot back to wait_for_archive when the time comes. Since we do hourly runs, reschedule is the right mode for this. It frees up an hour of worker active time for other tasks. Keeping poke here would mean an hour of no work being done until the next interval. Poke would suffice if we had a condition that would be met within minutes. Short active time.

**Verdict:** Mechanically correct, right state name, right direction on the tradeoff. Sounds like excellent documentation reading rather than operational scar tissue — because the **deadlock** wasn't named, the cost of reschedule wasn't quantified, and **deferrable operators** weren't mentioned. Minor: reschedule re-queues after `poke_interval`, an independent dial, not "the next DAG interval."

**Model answer:**
> "Poke holds the worker slot for the whole wait; reschedule releases it, marks the task `up_for_reschedule`, and re-queues it after `poke_interval`. The reason it mattered here: my bounded backfill spans 48 hourly runs, so in poke mode that's ~48 concurrent sensors holding slots — and once sensors ≥ pool size you get deadlock, where every slot is occupied by something waiting and nothing can run, including the tasks the sensors are waiting for. Reschedule isn't free though: each cycle is a fresh task instance with real startup and DB overhead, so the crossover is roughly wait-time versus startup-cost — poke for seconds, reschedule for minutes and up. The modern answer beats both: a deferrable operator on the triggerer holds no slot and pays no per-poke startup. One constraint to know — reschedule discards in-memory state between pokes, so `poke()` must be stateless."

**Follow-ups to expect:**
- *"What's your pool size, and what happens when sensors exceed it?"* → the deadlock question asked directly. Volunteer it first.
- *"Why not deferrable everywhere?"* → needs a running triggerer process and an async-capable operator; for one sensor on a local single-node Airflow it's ceremony. Name it as what you'd use at scale.

**Drill:** for every config choice in this project, have the *number* that forced it. Here it's **48**.

---

### Q: Explain your retry routing.

`❓ Predicted probe — not yet drilled` *(strong material — measured, and rarely offered by candidates)*

**Model answer:**
> "Retry policy is a routing decision by failure class, not a global setting. Infrastructure tasks get `retries=2` because they fail transiently and my idempotency makes a re-run free — re-running costs nothing and usually fixes it. **Data quality gates get `retries=0`**, because a deterministic failure doesn't heal by being repeated: bad data is still bad two minutes later, and retrying just delays the page. I measured that difference — with retries on the gate, notification took **11 minutes**; with `retries=0` it became same-minute. The `on_failure_callback` lives in `default_args` and fires only after retries are exhausted, which is why the routing matters: it's the retry count that decides how fast a human hears about it. One subtlety worth knowing — `upstream_failed` never fires the callback, so a downstream task doesn't double-page you for the same incident."

**Follow-ups to expect:**
- *"How do you know the alert path works?"* → both paths proven on Day 14: one page on red, and the metadata row lands anyway via the `all_done` observer.
- *"What if the webhook is down?"* → not solved. Alerting has no delivery guarantee here; at scale you'd want a queue or a paging service with its own retry.

---

## Topic: Idempotent pipeline design

### Q: Name the idempotency mechanism at each of the four layers.

`Asked 2026-08-06 · skills-map quiz Q8a · 4/4 — perfect`

**My answer:**
> bronze is blob.exists(). silver lake is dynamic partition overwrite. silver warehouse is table$YYYYMMDDHH and WRITE_TRUNCATE. gold is dbt merge

**Verdict:** Perfect. Rare — most candidates have one mechanism, not a per-layer scheme. No notes.

**Model answer:** (as given, plus the unifying rule)
> "Four layers, four mechanisms: `blob.exists()` skip at bronze, Spark dynamic partition overwrite in the lake, the BigQuery partition decorator `table$YYYYMMDDHH` with WRITE_TRUNCATE at the warehouse, and dbt incremental `merge` on `unique_key` at gold. The rule that ties them together is **partition grain = load grain** — each layer replaces exactly the unit it loads, which is what makes 'run it twice, get 180,386 both times' true at every stage."

---

### Q: You found a stray silver partition and deleted it with a BigQuery partition delete. What happened downstream, and why couldn't you fix it the same way?

`Asked 2026-08-06 · skills-map quiz Q8b · partial`

**My answer:**
> Downstream, the dbt table stayed because we used merge and we can't unmerge without a full-refresh. The rule from this is we follow the grain for cleanup. Partition grain = load grain = delete grain

**Verdict:** Headline and rule both correct. **Missed the sharpest of the three edges** — the silent no-op — which is the best debugging story in the incident.

**Model answer:**
> "Three things bit me. First, a merge can't un-merge — deleting the stray silver partition did nothing to the rows the incremental fact had already absorbed; restoring it required `dbt build --full-refresh`, verified by PASS=69 and 180,386. Second, and this is the one I'd flag in any review: **`bq rm -f -t` on a *nonexistent* partition succeeds silently.** My first delete aimed at a misread hour, removed nothing, and reported nothing. What caught it was `INFORMATION_SCHEMA.PARTITIONS` — a free metadata query — as an artifact-first check rather than trusting the exit code. Third, cleanup has to walk every layer the lineage touched: a GCS sweep can't reach a BQ partition, and a BQ partition delete can't reach merged gold rows. The rule I took away is partition grain = load grain = **delete grain**."

**Follow-ups to expect:**
- *"How did you know the first delete hadn't worked?"* ← this is where the good material lives. `INFORMATION_SCHEMA.PARTITIONS`, free metadata query, artifact-first.
- *"What class of bug is a silently-succeeding destructive command?"* → the same class as an INNER join dropping rows — **fail loudly, never drop silently**, arriving from the *tooling* side instead of the data side.

**Drill:** bank the `bq rm` story as a standalone anecdote. Silent-success failures are the strongest debugging stories you own.
