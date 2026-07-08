---
name: quiz
description: Run an interview-prep quiz over what has been built in DevPulse so far — concepts, tradeoffs, debugging war stories, and behavioral framing. Use when the user asks to be quizzed, tested, or to practice for interviews.
---

# Quiz: interview-prep over the real project

The goal is retrieval practice: Bryan must be able to *produce* explanations of his own project under interview pressure, not just recognize them. Source material = `docs/decisions.md`, `docs/glossary.md`, `docs/history.md`, and the code itself.

## Session shape

1. **Scope it:** ask (or infer from context) — a specific topic (e.g. "Day 10", "idempotency", "dbt"), or a general mixed set. Default: 6 questions, mixed.
2. **One question at a time. Wait for the answer.** Never dump all questions at once; never answer for him.
3. **Grade honestly:** what was right, what was missing/wrong, then the model answer in interview-length form (2–4 sentences). Cite where the material lives (decisions.md entry, file) so he can review.
4. **End with a scorecard:** strong areas, weak areas, and 1–3 concrete review pointers. Offer `/teach` on the weakest one.

## Question mix (vary across these types)

- **Concept:** "What is a partition decorator and why does the load target `silver_events$2024010115`?"
- **Tradeoff/decision:** "You chose `merge` over `insert_overwrite` for the fact — defend it, then argue the other side." (These come straight from decisions.md — the richest source.)
- **War story / debugging:** "Your sensor never fired for a week's backfill. Walk me through how you'd debug it." (Real incidents: the `%M`/`%m` typo, the silent empty bind-mount dir, the GCS-connector `chmod`, the Spark OOM, the MERGE that scanned more.)
- **At scale:** "This runs single-node Spark in Docker. What changes when it's 100× the data?"
- **Numbers check:** "Your mart sums to fewer events than the fact. What's your first hypothesis?" (LEFT vs INNER, allowlist filters, dedupe.)
- **Behavioral framing:** "Tell me about this project in 60 seconds" / "What would you do differently?"

## Calibration

- Push past the first answer: one follow-up ("why?", "what breaks if not?") per question, like a real interviewer.
- Interview-realistic tone — supportive but not soft. A vague answer gets "can you be more specific?" before the reveal.
- Track recurring weak spots across quizzes if evident and suggest they become review items or a `/teach` session.
- Keep the whole quiz grounded in what has actually been built (check CLAUDE.md status) — don't quiz on Phase 3/4 material that doesn't exist yet, except as "how *would* you…" stretch questions clearly labeled as such.
