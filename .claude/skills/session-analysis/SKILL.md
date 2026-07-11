---
name: session-analysis
description: Run a learning retrospective over the current working session — what went well, what went badly, what Bryan must review, new mental models formed, and tradeoffs encountered. Use at the end of a session (after or alongside /end-session), or whenever the user asks "how did today go" / for a session review. Distinct from /end-session, which is operational (verify, docs, teardown); this skill is about the learning, not the pipeline.
---

# Session-analysis: the learning retrospective

`/end-session` closes the *pipeline's* day; this closes *Bryan's*. The output is a written
retrospective in the conversation (not a doc — the durable bits get routed to the files that
already own them). The audience is Bryan-in-one-month prepping for an interview.

## Sources (consult, don't guess)

- The session transcript itself — checkpoints, review rounds, hint-ladder events, probing questions and his answers.
- `git log` for the day — commits tell the true build sequence and discipline.
- `docs/daily/day-NN.md` — planned vs. delivered.
- `docs/warmups.md` — drill grades and the "solved for me" queue.
- The coaching-profile memory — today's evidence for/against each known pattern.

## The five sections (all mandatory, in this order)

1. **What went well.** Concrete and evidenced — "X landed unprompted," "concept check passed in one round," "self-initiated production question." Skill growth counts double when it shows up *unprompted* where it previously needed prompting.
2. **What went badly.** Equally concrete, never softened. Recurring bug classes get named and counted ("signature-truth violation ×5"). Note *when* in the session errors clustered (fatigue curve). A bad day honestly logged is worth more than a good day inflated — same rule as the warmup grades.
3. **Bryan's review queue.** Things he must revisit *himself*: level-4 handouts (from `warmups.md`), concepts that needed >2 rounds, anything he got right only after being told. Each item: what to do and how to prove it stuck (rebuild cold / explain from memory / verify during a future run).
4. **New mental models.** Named, one-line definitions, each anchored to where it showed up today. If Bryan coined a compression himself ("B before A = problem"), record *his* phrasing — self-built models stick.
5. **Tradeoffs encountered.** Every "we chose X over Y because Z" from the session, one line each with the *when-the-alternative-wins* clause — these feed `docs/tradeoffs.md` and `/quiz` directly.

## Routing the durable bits (do it, don't just mention it)

- Coaching patterns (new bug class, habit shift, fatigue evidence) → update the coaching-profile memory.
- Drill-worthy gaps → `docs/warmups.md` queue if not already there.
- Interview-grade tradeoffs not yet in `docs/tradeoffs.md` → flag them for the next docs commit (list them explicitly).
- End with **one interview story from today** — situation, decision, number, outcome — in words Bryan can say out loud.

## Rules

- Honesty over encouragement; specifics over vibes ("3 mechanical errors after hour 5" beats "got tired").
- Evidence or it didn't happen — every claim traceable to a checkpoint, commit, or test run.
- Keep it to one screen-ish per section; this is a debrief, not a transcript replay.
- If the session was short or trivial, say so and keep the retro proportionally short.
