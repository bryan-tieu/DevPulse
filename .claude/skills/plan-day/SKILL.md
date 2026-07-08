---
name: plan-day
description: Generate the next DevPulse build-day plan (docs/daily/day-NN.md) in the established house format — scoped to one lesson, with build steps, constraints, done criteria, and learning goals. Use when the user asks to plan the next day/session or "what's next".
---

# Plan-day: write the next day plan

The day plans are the engine of this project: each is a self-contained lesson + runbook that any model can execute faithfully. Match the house format exactly — read the most recent `docs/daily/day-NN.md` first as the style reference.

## Before writing

1. Read `CLAUDE.md` → *Current status* → **Next up** (that's the topic; don't invent a different one without asking).
2. Read the latest day plan end-to-end (format + what it deferred) and the relevant `docs/decisions.md` entries.
3. Scope check: **one day = one core lesson**, ~3–5 focused hours, a handful of small commits. If Next up holds two big topics (e.g. "dbt DAG gate + Great Expectations"), propose splitting and ask which comes first.

## Required structure (in this order)

```markdown
# Day NN — <imperative title: the thing being built + the lesson inside it>

> **Phase X · Week Y.** Picks up from Day NN-1 (1–3 sentence recap with the key numbers). What today adds and why it's next.
>
> **Scope call (read first):** exactly what is IN and what is deliberately OUT/deferred to which day.
>
> **<The day's central decision>, stated up front and honestly:** the pattern being taught, the alternative(s) rejected and why, and the production-vs-simplified line.

## Environment notes (Windows 10 · PowerShell · Docker Desktop)
What carries from the previous day; auth posture (keyless ADC, no SA key); which stacks/images are needed; the rehydrate reminder (terraform destroy emptied everything → /start-session chain before anything reads data); any new tool introduced today and its one-paragraph orientation.

## Focus
One paragraph: the concrete deliverables and the target end state.

## Build steps
Numbered; **one small commit per step**; steps 0 (rehydrate/baseline-green) and the final verify step are setup, not commits.
Each step: what to build, the *why* inline (the teaching), the gotchas to expect, and the verification (exact counts where known).

## Constraints & conventions
Emoji-bulleted hard lines for the day (🧱 scope, 🚫 rejected approaches, 🔗 test requirements, 🔏 PII, 💸 cost, 📐 lint, 📦 commit granularity).

## Done criteria
- [ ] Checkboxes — each independently verifiable, with exact expected numbers (row counts, PASS counts) where derivable. Include: build green, reconciliation holds, lint clean, docs/status updated, teardown done.

## Learning goals
Numbered list (4–7): the concepts Bryan should be able to explain in an interview after today — each phrased as the concept + why it matters, not just a keyword.
```

## Quality bar

- **Exact numbers everywhere they're knowable** (the canonical hour is 180,386 rows; current dbt baseline PASS=69 — pull from CLAUDE.md Reference values, update if the day changes them).
- **State simplifications loudly** ("exercised, not stress-tested" is the house phrase for patterns that only come alive at scale).
- **Every day ends with**: reconciliation, lint, docs/status/decisions updates, teardown. Bake those into the steps and done criteria.
- **Carry the security backlog** forward: still-pending items (pipeline-SA impersonation, silver-bucket grant, Phase-3 API hardening) get a one-line 🔐 reminder if the day touches their area.
- After writing, save to `docs/daily/day-NN.md` and update `CLAUDE.md` Current status → point "Next up" at the new plan.
