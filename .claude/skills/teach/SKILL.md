---
name: teach
description: Explain a data-engineering concept the DevPulse way — grounded in this repo's code, with tradeoffs, at-scale differences, and interview framing. Use when the user asks to explain/understand a concept, tool, or decision, or says "teach me X".
---

# Teach: the DevPulse explanation protocol

Bryan is self-taught and building interview-grade understanding. A good explanation here is grounded in *his* codebase, honest about simplifications, and ends in words he can say to an interviewer. Never a generic textbook answer.

## Structure every explanation as

1. **The concept in one paragraph** — plain language first, precise terms second.
2. **Where it lives in DevPulse** — the actual file/model/config, quoted or linked (e.g. `spark/silver_events.py`, `dbt/models/marts/fact_events.sql`). If it doesn't exist here yet, say which phase introduces it.
3. **The decision behind it** — what alternative was rejected and why (check `docs/decisions.md` first; if this decision is logged, teach *that* reasoning, don't invent a new one).
4. **What changes at real scale** — the honest production delta (single-node → cluster, seed → API enrichment source, LocalExecutor → Celery/K8s, local TF state → remote backend…). This articulation is a core interview skill.
5. **The failure mode** — how this goes wrong and how you'd notice (this project has real war stories: sensors waiting forever on a typo'd URL, the MERGE that scanned *more*, the OOM on parallel GCS upload — use them).
6. **Interview version** — 2–4 sentences, first person, as Bryan would answer "tell me about X" or "why did you choose X". Include one likely follow-up question and its answer.

## Rules

- Check `docs/glossary.md` first; teach consistently with it. If the term is missing, **add a glossary entry** after teaching (same style: general meaning + DevPulse-specific role), and mention you did.
- Depth over breadth: one concept taught to defensible depth beats a survey. If the question is broad ("teach me Spark"), propose a sequence and start with the piece DevPulse actually uses.
- Use the project's real numbers (180,386-row hour, 55,245 repos, PASS=69) in examples — concrete beats abstract.
- If the concept exposes a gap or improvement in the current build, flag it honestly and suggest logging it (decisions.md or the next day plan) — don't silently fix or silently ignore.
- Close by offering a `/quiz` on the topic.
