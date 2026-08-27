# Interview Prep — DevPulse

> **Purpose:** everything an interviewer might probe about this project, with the question asked, **Bryan's actual answer**, the honest verdict, a rehearsable model answer, and the follow-ups that come next. Strictly interview preparation — design rationale lives in [decisions.md](../decisions.md), the evidence map in [skills-map.md](../skills-map.md), the steelmen in [tradeoffs.md](../tradeoffs.md).
>
> **This file is an index. Questions live in the topic files.**

## How to use this

1. **Cover the model answer.** Answer out loud, from memory, in full sentences. Time yourself — 60–90 seconds is the target for a technical answer.
2. **Then read your own recorded answer**, not the model one. The gap between what you *just said* and what you said *last time* is the actual measurement.
3. **Only then** read the model answer, and note which specific number or failure-name you dropped.
4. **Never study by re-reading.** Recognition feels like knowing and isn't — Q12 and Q13 below were both material Bryan had authored within 24 hours and could not produce.

## Topic files

| File | Skills-map rows covered | Questions |
|---|---|---|
| [01 · Platform, architecture & cost](01-platform-architecture-and-cost.md) | Cloud data platforms · Lakehouse/medallion · Cost engineering | 3 |
| [02 · Orchestration & idempotency](02-orchestration-and-idempotency.md) | Orchestration (Airflow) · Idempotent pipeline design | 4 |
| [03 · Processing & quality](03-processing-and-quality.md) | Distributed processing (Spark) · Data quality gates | 3 |
| [04 · Modeling & dbt](04-modeling-and-dbt.md) | Warehouse modeling (Kimball) · dbt | 4 |
| [05 · Infra, security & tooling](05-infra-security-and-tooling.md) | IaC (Terraform) · Security & credentials · Docker · CI/CD | 5 |
| [06 · Testing, serving & observability](06-testing-serving-and-observability.md) | Testing (pytest) · Serving (FastAPI/dashboard) · Observability & alerting | 5 |
| [07 · Behavioral & at-scale](07-behavioral-and-at-scale.md) | The pitch · what's simplified · what would you do differently | 4 |

## Coverage against the skills map

| Row | Drilled? | Best score | Note |
|---|---|---|---|
| Cloud data platforms | ✅ | 7/10 | GCS-vs-BigQuery billing confusion |
| Lakehouse / medallion | ✅ | 7/10 | Units error; missed the PII steelman |
| Orchestration (Airflow) | ✅ ×2 | 8/10 | Strongest area |
| Distributed processing (Spark) | ✅ | 8/10 | Salting-breaks-dedupe trap open |
| Warehouse modeling (Kimball) | ✅ | 6/10 | SCD reopened after closing on 07-30 |
| dbt | ✅ | 6/10 | Flagship story told without numbers |
| Idempotent pipeline design | ✅ | 8/10 | 4/4 on mechanisms |
| Data quality gates | ✅ | 6/10 | Circular definition of residual |
| IaC (Terraform) | ✅ | 8/10 | Precise |
| Security & credentials | ❌ | 0/10 | **Genuine knowledge gap — top priority** |
| Docker / containerized tooling | ❌ | 0/10 | Retrieval failure on own decision |
| Testing (pytest) | ❌ | 0/10 | Retrieval failure; repo evidence is excellent |
| Cost engineering | ⬜ | — | Predicted questions seeded, not yet asked |
| Serving (FastAPI) & dashboards | ⬜ | — | Predicted questions seeded, not yet asked |
| Observability / alerting | ⬜ | — | Predicted questions seeded, not yet asked |
| CI/CD (GitHub Actions) | ⬜ | — | Day 17 not built — answer as design, not experience |
| Streaming (Kafka) | ⬜ | — | Phase 4 not built — answer as design, not experience |

## The three standing delivery weaknesses

These cost more points than any knowledge gap. They appear in *every* quiz.

1. **🔴 Numbers vanish under pressure.** Third recurrence of the identical miss (the 8.5 → 40.4 MiB merge figure, told qualitatively on 2026-07-27 *and* 2026-08-06). Every claim in the skills map is *claim + number + mechanism*; strip the number and it sounds like a blog post. **Canonical figures are in [CLAUDE.md](../../CLAUDE.md) → Reference values.**
2. **🟡 Describes the mechanism, doesn't name the failure.** "A backfill doesn't go through" vs "**it succeeds and lies** — 168 green tasks, one hour of data." Interviewers score the failure mode, because only operators have seen one.
3. **🔴 Claims the repo contradicts.** Twice on 2026-08-06 (the surrogate "one less join"; a merge-vs-`insert_overwrite` comparison never run). This is a **trust** problem, not a knowledge one — and it's worse on a portfolio project, where they may have the code open. The honest concession is already written in the repo and is *stronger* than the defense.

**Standing strength to keep:** doesn't bluff. Four logged instances of declining rather than guessing. In a real interview, convert that into the *graceful partial* — state what you know, mark the boundary, name how you'd close it.

## Convention for adding questions

Append to the relevant topic file under its topic heading, using this block:

```markdown
### Q: <the question, phrased as an interviewer would ask it>

`Asked 2026-MM-DD · <source> · <score>/10`

**My answer:**
> <verbatim — no cleanup, that's the point>

**Verdict:** <what was right · what was wrong>

**Model answer:**
> <rehearsable, 60–90 seconds spoken>

**Follow-ups to expect:**
- <the next question, and whether the answer above survives it>

**Drill:** <one concrete practice instruction>
```

Questions that have **not** been asked yet are marked `❓ Predicted probe — not yet drilled` and carry a model answer but no recorded answer. When one gets asked, fill in the answer and score in place.

**Sources so far:** `/quiz` skills-map sweep 2026-08-06 (13 Q) · `/quiz` tradeoffs set 2026-07-27 (6 Q) · `/quiz` modeling+dbt 2026-07-30 (6 Q) · `/quiz` retention retest 2026-07-31 (6 Q). Only the 2026-08-06 set has verbatim answers recorded; earlier sets are summarised from the coaching log and marked as such.
