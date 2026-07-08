---
name: end-session
description: Close out a DevPulse working session — verify, lint, update status/history/decisions docs, tear down cloud infra, and walk the commit checklist. Use at the end of every session, and whenever the user says they're done or wrapping up.
---

# End-session: wrap the day

Work through this in order. Skipping a step is allowed only if you tell the user explicitly what was skipped and why.

## 1. Verify before you document

- Anything built today: run its proof (see `/verify-pipeline`) — idempotency re-run + reconciliation counts. Don't document unverified claims.
- Lint: `ruff check . && black --check .`; if dbt models changed: `docker compose -f dbt/docker-compose.yaml run --rm --entrypoint sqlfluff dbt lint models`.
- Tests: `python -m pytest tests/` (host; Spark tests need the container).
- If dbt models changed: `docker compose -f dbt/docker-compose.yaml run --rm dbt docs generate` so lineage stays current.

## 2. Update the docs (in this order)

1. **Day log** — check off the done-criteria boxes in `docs/daily/day-NN.md` that were actually met; note anything deferred.
2. **`docs/decisions.md`** — one entry per non-obvious choice made today: the decision, why, the rejected alternative, what changes at scale. Include honest negative findings.
3. **`docs/history.md`** — append one Day-NN entry (dense paragraph, same style as existing entries: what landed, the key numbers, PASS counts, gotchas banked).
4. **`CLAUDE.md` → Current status** — rewrite the ≤15-line block: phase, last completed day (link), next up, known issues, open decisions. **Narrative goes to history.md, not here.** Update Reference values / Milestones checkboxes if they changed.
5. **`docs/glossary.md`** — add any new term introduced today (definition + how it's used here).
6. **`docs/skills-map.md`** — flip any skill newly exercised/proven and add its evidence.

## 3. Teardown (cost hygiene — never skip silently)

```
docker compose -f spark/docker-compose.yaml down
docker compose -f dbt/docker-compose.yaml down
docker compose -f airflow/docker-compose.yaml down   # if it was up
terraform -chdir=terraform destroy                    # empties buckets + gold dataset — confirm with user first
```
If the user wants to keep infra up (e.g. continuing tomorrow morning), record that in Current status → Known issues so the next session knows.

## 4. Commit checklist

- One commit per concept, imperative mood. Suggest the commit list; the user commits (or asks you to).
- Confirm nothing forbidden is staged: `profiles.yml`, `.env`, `terraform.tfvars`, `terraform.tfstate*`, ADC files, `dbt/target/`, `dbt_packages/`, logs. `git status` + `git check-ignore` if unsure.
- Docs updates (status/history/decisions/day log) go in a final `docs:` commit.

## 5. Sign off

Give the user a 5-line summary: what landed, the proof numbers, what's next (Day N+1 topic), teardown state, and one interview-ready sentence about today's work.
