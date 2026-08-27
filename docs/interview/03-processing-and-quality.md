# 03 · Processing & quality

Skills-map rows: **Distributed processing (Spark/PySpark)** 🔶 · **Data quality gates**

---

## Topic: Distributed processing (Spark/PySpark)

### Q: Your silver job reads 180,387 records and writes 180,386 — it found one duplicate. (a) What does Spark physically do to find it? (b) At 100× data on a real cluster, what becomes the problem and what do you change?

`Asked 2026-08-06 · skills-map quiz Q5 · 8/10`

**My answer:**
> a. Deduping is a wide transformation. It distributes those records across executor nodes and then matches key's together. This involves data transfers over the network since we're moving the records across different executor nodes to find duplicates. Once that executor has 2 of the same record, it's able to then do it's own deduping, removing the duplicate record.
> b. At scale, the executor's memory starts becoming a problem. I also redesign the shuffle so we don't have skew. Salting a hot key is one solution to preventing skew

**Verdict:** A genuinely correct shuffle model — better than most candidates. **But "salting" is a trap you walked toward:** salting is a *join* technique, and applied naively to a dedupe it **breaks correctness** — different salts scatter identical rows into different partitions and both survive. Also missing: `spark.sql.shuffle.partitions` (the first knob, before memory), AQE, and the redesign that beats all the tuning.

**Model answer:**
> "Dedupe is a wide transformation, so Spark hash-partitions on the dedupe key — `hash(key) % numPartitions` — which *guarantees* identical keys land in the same partition; that guarantee is why the local dedupe is correct rather than approximate. Physically the map side writes shuffle files to local disk and the reduce side fetches them over the network, so it's disk plus network, and partitions exceeding executor memory spill. At 100× my first move isn't memory, it's `spark.sql.shuffle.partitions` — the default 200 is wrong in both directions; I'd target ~128–200 MB per shuffle partition. AQE handles most skew automatically now; salting is the manual fallback, and for a *dedupe* specifically it needs two stages, since one-pass salting scatters duplicates and both survive. But the best answer is to delete the shuffle: dupes only occur within an hour and I'm already hour-partitioned, so dedupe within partition and the wide transformation becomes narrow. Honestly, though — 100× of 20 MB/hour is 2 GB/hour. That's still not big data; a single node handles it."

**Follow-ups to expect:**
- *"Show me how you'd salt a dedupe."* ← the trap. Answer: **you can't in one pass.** salt → partial dedupe → strip salt → final dedupe.
- *"Why single-node?"* → deliberate. Then name the cluster deltas: executor sizing, shuffle cost, Dataproc/EMR submission — and that 2 GB/hour doesn't justify a cluster.
- *"How many tasks does a stage have?"* → **one task per partition.** (Job → stages at shuffle boundaries → tasks per partition.) Flagged as a previous slip on 2026-07-31 — re-drill it.

**Drill:** never say "salting" without immediately adding "for joins — for dedupe it needs two stages."

---

### Q: Why `PERMISSIVE` mode, and what does it do with an unparseable line?

`❓ Predicted probe — not yet drilled · OPEN QUESTION IN THE REPO`

**Model answer (honest, because this is genuinely unresolved):**
> "The reader is `PERMISSIVE`, which puts unparseable content into a `_corrupt_record` column rather than failing the job — and here's the honest part: **I don't currently declare that column in my explicit schema**, which raises a real question I've logged rather than answered — whether Spark is silently *dropping* those rows instead of surfacing them. It's an open experiment in my decisions log. What I can say is what the alternatives are: `FAILFAST` aborts the whole job on one bad line, which is wrong for a firehose where a single malformed record shouldn't kill an hour; `DROPMALFORMED` discards silently, which violates my own fail-loudly rule. The correct shape is `PERMISSIVE` **with** `_corrupt_record` declared and routed to quarantine, which is what my GE identity is already built to count."

**Follow-ups to expect:**
- *"So you might be dropping data and not know?"* → **Yes, possibly, and it's logged as an open question.** This is a good answer, not a bad one — it shows you know the difference between what you've verified and what you've assumed.
- *"How would you find out?"* → declare `_corrupt_record` in the schema, feed a deliberately malformed line, and check whether the count changes. One experiment.

---

## Topic: Data quality gates

### Q: Your GE gate asserts 180,387 raw = 180,386 hour_rows + 0 quarantine + 1 residual. (a) What is each term, and why is residual 1 rather than 0? (b) Why an identity at all, when you already have 8 column expectations?

`Asked 2026-08-06 · skills-map quiz Q9 · 6/10`

**My answer:**
> a. raw is the rows in bronze. hour_rows are the rows that actuakly landed in silver. quarantine covers malformed rows. residual are rows unaccounted for.
> b. Counted identities catch additions or deletions. Expectations don't care about the counted identity, just that the columns satisfy the requirements of the gate

**Verdict:** **"Residual = rows unaccounted for" is a circular definition** — it restates the word, and the question specifically asked *why it's 1*. That makes an interviewer suspect you know the check exists without knowing what it caught. (b) is directionally right but shallow — the killer sentence is missing.

**Model answer:**
> "Raw is what bronze delivered — 180,387. Hour_rows is what landed in the silver partition — 180,386. Quarantine is rows I rejected explicitly — 0. Residual is what's left over, and it's 1 **because of the deduplicated event**: raw contained one repeated `event_id` and Spark removed it. The important framing is that residual isn't 'rows we lost,' it's **'rows we must be able to explain'** — mine is explained, so `residual_ok()` passes it under a threshold rather than demanding zero, and it also guards `residual >= 0`, because negative residual means silver has *more* rows than bronze, which is duplication — an entirely different emergency. As for why the identity earns its place: **per-column expectations only ever run on the rows that survived.** If a transform silently drops 40% of the data, every column expectation still passes — the survivors are all valid. Expectations validate the rows you have; the identity validates the rows you *should* have. Expectations are single-table; the identity is cross-layer, and only a cross-layer reconciliation catches a boundary leaking."

**Follow-ups to expect:**
- *"What would a residual of −1 mean?"* → silver has more rows than bronze: duplication or corruption. Exactly why `residual_ok()` guards `residual >= 0` alongside the threshold.
- *"Why a threshold instead of exactly zero?"* → because *explained* loss exists (dedupe). Demanding zero would either fail every run or force you to fudge the count — both worse.
- *"Has this gate ever caught anything real?"* → **yes** — the dbt gate's first live catch was genuine bad data, and the GE red path was re-proven under alerting on Day 14.

**Drill:** never define a term using the term. If your definition contains the word, you haven't answered.

---

### Q: You have two gates — dbt tests and Great Expectations. Why two? Isn't that redundant?

`❓ Predicted probe — not yet drilled`

**Model answer:**
> "They guard different boundaries and catch different classes. GE gates **bronze → silver**: it's a cross-layer reconciliation, checking that the rows bronze delivered are all accounted for in silver — an *arithmetic* check about completeness. dbt tests gate **silver → gold**: grain uniqueness, not-null on keys, and `relationships` on every FK, which dbt compiles to a left-anti-join so a surrogate that didn't land breaks the build. That's a *structural* check about model integrity. GE can't see a broken FK; dbt can't see that 40% of bronze never arrived. And both are **gates, not dashboards** — they fail the DAG task, with `retries=0` so a deterministic failure pages immediately rather than being retried into a delay."
