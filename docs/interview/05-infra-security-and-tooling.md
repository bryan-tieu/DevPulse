# 05 · Infra, security & tooling

Skills-map rows: **IaC (Terraform)** · **Security & credentials** · **Docker / containerized tooling** · **CI/CD (GitHub Actions)**

> 🔴 **This file contains the project's only genuine knowledge gap** (credentials) and one retrieval failure on self-authored material (Docker). Highest study priority.

---

## Topic: IaC (Terraform)

### Q: A teammate grants the pipeline SA BigQuery Data Editor using `google_project_iam_binding` because the docs example does. What happens on `terraform apply`?

`Asked 2026-08-06 · skills-map quiz Q10 · 8/10`

**My answer:**
> After terraform apply, every member in the members list gets the role and those who previously had the role and aren't in the members list get their role removed. The blast radius becomes the members list and those who aren't on it but previously had the role. iam policy only when terraform manages the entierety of a resource. iam binding for a specific role. iam member for adding a member to a role

**Verdict:** Precise and correct — clearly careful provider-doc reading. To sound like someone who's *been burned*, it needs a **victim** and a **timeline**.

**Model answer:**
> "`_binding` is authoritative for that one role — apply it and every principal holding that role who isn't in your list gets stripped, including Google-managed service agents and, if you'd granted it to yourself, your own account. Locking yourself out of your own project with `terraform apply` is the canonical incident, and without a second admin it can be genuinely unrecoverable. It's also not a one-time event: once a `_binding` owns a role, every subsequent apply re-strips whatever anyone added through the console since, so Terraform silently becomes the enforcer of a list nobody else knows exists. `_policy` is worse — authoritative over the resource's entire IAM policy, all roles, all members. My rule is `_member` unless Terraform is genuinely the sole source of truth for that role, which is rarely true anywhere real people and Google's own automation also touch the project."

**Follow-ups to expect:**
- *"Has this ever bitten you?"* → have an answer ready; frame as what you checked for and why you chose `_member` preemptively.
- *"How would you recover?"* → another admin, or org-level access. If neither exists, you're opening a support case.
- *"What else in your Terraform is pinned?"* → provider versions plus a committed `.terraform.lock.hcl` — the same reproducibility argument as pinned Python deps.

**Drill:** for every "authoritative" resource in any IaC tool, name the **victim** out loud.

---

## Topic: Security & credentials 🔴

### Q: Your dbt container authenticates to BigQuery with no key baked into the image. Walk me through the mechanism. Then: what's actually wrong with a service account key?

`Asked 2026-08-06 · skills-map quiz Q11 · 0/10 — declined, genuine gap`

**My answer:**
> This one I don't know so I'm not going to answer with a wrong answer. Teach this

**Verdict:** Honest, and better than bluffing — but **in a real interview you can't pass**, and this is a near-certain question for anyone whose project touches cloud infra. Two things to build: the content, and the **graceful partial**.

**The graceful partial** (use this structure whenever knowledge runs out mid-answer):
> "I know the shape but not every detail — let me give you what I'm confident in. My containers mount ADC read-only rather than baking a key, and the library resolves credentials through `GOOGLE_APPLICATION_CREDENTIALS` to get short-lived tokens. What I'd want to check before asserting is the exact resolution order and how impersonation differs mechanically from federation."

**Model answer — part (a), the mechanism:**
> "Compose bind-mounts my local ADC file read-only into the container and sets `GOOGLE_APPLICATION_CREDENTIALS` to point at it; dbt's profile uses `method: oauth`, which just means 'resolve via ADC, don't look for a keyfile.' At call time `google.auth.default()` walks a fixed chain — the env var first, then gcloud's well-known path, then the metadata server on GCP compute. The part that matters is **what's in that file**: it's not a service account key, it's `type: authorized_user` — a **refresh token** tied to my human identity. The library exchanges it at Google's OAuth endpoint for a **short-lived access token**, roughly an hour, sent as a bearer token and auto-renewed. So the container never holds a credential to the BigQuery API; it holds a token-minting instrument. Read-only mount means nothing can rewrite it, and never `COPY`ing it means it can't leak in an image layer or a registry push."

**Model answer — part (b), what's wrong with a key:**
> "A service account key is a bearer secret with **no expiry** — a key leaked in 2021 still works today, whereas an access token dies in an hour. It's a secret at rest, so it proliferates: file, laptop, Slack, CI secret store, `.env`, Docker layer, public repo — and **every copy is fully privileged** with no way to tell which one was used. It has **no identity binding**: impersonation binds to a real principal and federation binds to a specific repo and ref, but a key binds to nothing, so audit logs show 'the SA did it' and never which human or system. Rotation means re-distributing the file everywhere it ever landed, which is why keys live forever in practice. And compromise is silent — no session to expire, nothing to revoke short of finding it. The alternatives are **impersonation** — my short-lived credential calls `generateAccessToken` on the target SA via `serviceAccountTokenCreator`, so no key exists and the delegation chain is logged — and **workload identity federation**, where the platform signs an OIDC token attesting what's running and GCP exchanges it for a short-lived token, so trust lives in the platform's attestation rather than in a secret. A key is still right for systems with **no OIDC support** — legacy on-prem runners, air-gapped boxes, a SaaS whose integration only accepts JSON. Then you scope it to one role on one resource, enforce key expiry by org policy, store it in a secrets manager, and alert on its use."

**Follow-ups to expect:**
- *"So what's in that JSON file?"* → a refresh token, `type: authorized_user`. **Not** a key.
- *"How would you do this in CI?"* → WIF. And the footgun: an `attribute_condition` that doesn't pin the repository lets **any** GitHub repo assume your SA.
- *"Why haven't you switched to the pipeline SA?"* → backlogged, deliberately — the switch must be impersonation, never a minted key.

**Drill:** `/teach` this, then be re-quizzed next session. Knowledge gaps close reliably on teach + next-day retest; this is the *easy* kind of gap.

---

## Topic: Docker / containerized tooling

### Q: dbt got its own image. Streamlit got its own venv. Identical reasoning — why two different mechanisms, and what security consideration decided it?

`Asked 2026-08-06 · skills-map quiz Q12 · 0/10 — retrieval failure on own decision (authored 24h earlier)`

**My answer:**
> Teach this too

**Verdict:** The dangerous one — this is the question they ask *after you bring it up yourself*. Saying "I isolated Streamlit in its own venv" and then failing to defend it is worse than never mentioning it. **Re-reading won't fix this; you already read it — you wrote it.** Close the docs, explain to a wall, diff against [decisions.md](../decisions.md).

**Model answer:**
> "Same reasoning — heavy transitive deps near `protobuf` and `pyarrow` sitting next to `google-cloud-*` — different mechanism, because the mechanism should be the cheapest thing that solves it. dbt needed an image because it's also a DockerOperator task in my DAG: the image *is* the deployment artifact, and a venv can't be a DAG task. Streamlit is a host process nobody orchestrates, so a venv gave identical isolation for free — and the install proved the risk was real, pulling protobuf 7.35, pyarrow 24 and pandas 3. The security piece decided it: a container reaching my host API would need `host.docker.internal` **and** force uvicorn from `127.0.0.1` to `0.0.0.0`, exposing an unauthenticated BigQuery-backed endpoint to the LAN — data exposure and a **billing-DoS**, since every request runs a paid query job. Zero learning for a real regression. Once the API is containerized and authenticated, the container becomes the right answer — the decision is time-indexed, not permanent. And the split enforces the layer boundary mechanically: streamlit isn't in the host venv, so an illegal import in the client is a **failing test**, not a convention."

**Follow-ups to expect:**
- *"Why not bind `0.0.0.0` and firewall it?"* → defense in depth: don't widen the binding when you don't have to, and a host firewall is one config change from not existing.
- *"Why did dbt get an image in the first place?"* → because its deps **actually broke the shared venv** once. Not theory.
- *"Isn't four images excessive?"* → Airflow, Spark, dbt, GE — each isolated for a named dependency reason, and three of them are DAG tasks.

---

## Topic: CI/CD (GitHub Actions) ⬜

> **Not built as of 2026-08-06.** [Day 17](../daily/day-17.md) is planned. Answer these as **design**, never as experience — claiming built CI you don't have is the fastest way to fail a follow-up.

### Q: How would you set up CI for this project?

`❓ Predicted probe — not yet drilled`

**Model answer:**
> "The first decision is what CI is allowed to *need*. I'd build a **hermetic** pipeline — no credential, no cloud, no data — with three independent jobs: ruff plus black, pytest, and a dbt gate. My suite already runs 93 tests in 0.43 seconds with zero credentials, so the test job is nearly free. For dbt I'd run `dbt deps` plus **`dbt parse`** rather than `dbt build`: parse needs no warehouse and still catches a broken `ref()`, a model missing its `.yml`, a YAML error, a missing package — most of what actually breaks in a dbt PR. What it *can't* catch is SQL that compiles but returns wrong rows, and that's the honest boundary: **CI proves the code is well-formed, not that the pipeline produced the right numbers.** The credentialed tier would need Workload Identity Federation, not a key in GitHub Secrets — that's a separate piece of work with a real footgun in the trust policy."

**Follow-ups to expect:**
- *"Why not just put a service account key in GitHub Secrets?"* ← **the whole point.** See the credentials answer above.
- *"How do you run tests that need config?"* → job-level `env:` with **deliberately fake** values (`ci-project`). Config isn't a secret, and fake values mean an accidental network call fails against a nonexistent project instead of touching real infra.
- *"Does a green check mean it's safe to merge?"* → only if it's a **required status check on a PR**. A workflow on push to main is a *notification* — the broken code is already the trunk when the email arrives.
