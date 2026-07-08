# DevPulse Operating Manual

> **For Bryan.** How to run a working session with Claude Code under the structure introduced 2026-07-08 (root `CLAUDE.md` + project skills + coach mode). `CLAUDE.md` is the *Claude-facing* contract; this file is the *you-facing* manual. If the two ever disagree, fix `CLAUDE.md` — it's what Claude actually obeys.

---

## 1. The mental model

```
CLAUDE.md (repo root)          ← auto-loads every session: rules, commands, current status
├── .claude/skills/            ← 7 workflows triggered by /name (or plain English)
└── docs/
    ├── daily/day-NN.md        ← the plan you execute each build day
    ├── decisions.md           ← every "why" — interview ammunition
    ├── glossary.md            ← every term + the canonical numbers
    ├── history.md             ← what each day delivered
    ├── skills-map.md          ← job-skill → evidence → interview claim
    ├── warmups.md             ← your re-implementation drill scorecard
    └── operating-manual.md    ← this file
```

Division of labor: **you write pipeline code; the skills maintain the docs.** You should almost never hand-edit status/history/decisions — `/end-session` does it. Everything lives in files and git, **not** in any one conversation — which is why conversations are disposable (§5).

## 2. Starting a session (the part that goes wrong)

1. Open Claude Code **from `C:\Users\Bryan\Downloads\DevPulse\DevPulse`** — the inner folder, the git repo. (The outer `Downloads\DevPulse` is just a wrapper; sessions started there don't load the skills.)
2. Sanity check: type `/` — you should see all seven skills (`start-session`, `plan-day`, `end-session`, `verify-pipeline`, `teach`, `quiz`, `warmup`). See **none**? Wrong folder. See six but not a newly added one? Skills scan at session start — restart the conversation.
3. `CLAUDE.md` loaded silently — Claude already knows the rules and where the project stands. Don't re-explain the project.

## 3. The seven skills

| Command | What it does | Cloud cost |
|---|---|---|
| `/warmup` | One re-implementation drill from the 9-item ladder — you rebuild an existing component from a blank file, graded cold/warm/repeat, logged in `warmups.md` | none |
| `/start-session` | Preflight + `terraform apply` + rehydrate the canonical hour (bronze → Spark → BQ → dbt), verifying counts at each step | ~$0 but real infra |
| `/plan-day` | Writes the next `docs/daily/day-NN.md` in the house format, steps marked 🧑‍💻/🤖 | none |
| `/verify-pipeline` | The two proofs for anything built: run-twice idempotency + count reconciliation | queries only |
| `/end-session` | Lint/tests → update all docs in order → teardown (asks first) → commit checklist | none (saves cost) |
| `/teach <topic>` | Concept deep-dive grounded in this repo, ending with the interview version | none |
| `/quiz` | Interview-style questions, one at a time, from your decisions and war stories | none |

All of them also trigger from plain English ("let's wrap up", "quiz me on Spark", "warm-up"). The `/` form is just explicit — and works even if you paraphrase badly.

## 4. Session recipes

**Build day (the main loop):**
1. `/warmup` — one drill (~30 min). 2. `/start-session` — only if today reads/writes live data. 3. Work `docs/daily/day-NN.md` step by step. 4. `/verify-pipeline` on anything built. 5. `/end-session` — never skip; it's what keeps the next session oriented and the cloud bill at $0.

**Zero-energy evening (no infra, no cost):** `/quiz`, `/teach`, `/warmup` in any mix. These are the interview-prep muscle and need nothing running.

**Planning only:** `/plan-day`, discuss/adjust scope, done. Push back on scope *before* the day starts, not mid-day.

## 5. Conversations: one per session, then discard

Open a fresh conversation for each working session; close it after `/end-session`. Everything worth keeping went to disk (docs + git + Claude's persistent memory of your preferences). Long-running chats accumulate stale context and eventually get summarized — files don't. A fresh conversation starts *better* oriented than an old one continues.

## 6. Coach mode — how the no-handouts rule works in practice

From Day 12 on, day-plan steps are marked:
- **🧑‍💻 you implement** — Claude frames the problem (pattern, pitfalls, where to look) then stops. You write. Your code gets reviewed like a PR: questions first, never a silent rewrite.
- **🤖 Claude scaffolds** — boilerplate you've already built twice, and repo chores.

**The hint ladder** (you control escalation — say "hint" to go up one level):
1. Concept + where to look
2. The shape (pseudocode / signature / skeleton)
3. A targeted snippet for the specific stuck line
4. Full solution — **only if you explicitly ask**; it's logged as "solved for me — revisit" and becomes a warm-up target

Debugging is yours too: expect "what do you observe, what's your hypothesis?" before an explanation. The struggle is the rep — that's the product you asked for, not friction to remove.

**Your half of the contract:** any model will fold and write the code if you push. Watch for the tell — a day that finishes fast and feels smooth probably means you didn't write enough of it. The `/end-session` "who wrote what" accounting exists to keep this honest.

## 7. Warm-ups — closing the Days 1–11 gap

Days 1–11 were ~90% Claude-written: you know the code conceptually, not cold. `/warmup` treats you as starting from Day 1 *strictly coding-wise* — a 9-item ladder (`ingest_hour` → BQ load → Spark transform → DAG → dbt models → Terraform) in `docs/warmups.md`. Blank file in `warmups/` (gitignored), original closed, 20–40 min timebox, verified against the real pytest suite where one exists, then diffed against the original. An item retires only after passing **cold twice, ≥1 week apart**. Expect the first drills to feel bad; retrieval failing then succeeding is the mechanism, and the tracker makes improvement visible.

## 8. Standing rules that protect you (memorize these five)

1. **Never end a session with infra up** without recording it in Current status — `terraform destroy` + `docker compose down` is the default.
2. **Never commit**: `.env`, `terraform.tfvars`, `tfstate`, `profiles.yml`, ADC files, `dbt/target/`, `dbt_packages/`, logs.
3. **Nothing is "done" without the two proofs** — idempotent re-run + reconciled counts (canonical hour = 180,386).
4. **No SA keys, ever** — ADC only; the SA switch is impersonation.
5. **Small commits, one concept each**, imperative mood.

## 9. Troubleshooting

| Symptom | Cause → fix |
|---|---|
| `/` shows no project skills | Session opened in the outer wrapper folder → reopen from `DevPulse\DevPulse` |
| A skill exists on disk but isn't listed | Created after this session's scan → new conversation |
| Claude ignores coach mode / writes lesson code unasked | Point it at `CLAUDE.md` → Coach mode; it reloads rules from there |
| dbt/Spark can't read data | You `terraform destroy`ed (good!) → `/start-session` rehydrates |
| Counts don't match 180,386 / PASS≠69 | Stop and investigate — see `/verify-pipeline` failure interpretations |
| Claude re-litigates a settled decision | Tell it to check `docs/decisions.md` first — the why is logged |
