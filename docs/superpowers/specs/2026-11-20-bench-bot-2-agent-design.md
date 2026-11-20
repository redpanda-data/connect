# Bench Bot Part 2: Slack agent on ADP

Status: draft, revised 2026-11-20 after review. Fresh runs are end to end and build latest `main`. My calls are marked *(my call)*.
Parent spec: `docs/superpowers/specs/2026-11-20-bench-bot-design.md` (decisions there still bind). Part 1 (results catalogue, `benchmarking/aws/catalog` + `cmd/catalog`) is PR #5271.

## Intent

Get Bench Bot working end to end in Slack so sales and engineering can try it:

1. Someone asks in Slack, with a pasted config or a sizing question.
2. The bot asks follow-up questions until the request is answerable.
3. It answers from the catalogue when a current result exists. Otherwise it shows the cost of a fresh run.
4. On a thumbs-up from anyone, it starts a benchmark and posts the result in the same thread.

## Goals (this part)

- An ADP agent, `bench-bot`, reachable from Slack in a chosen channel and by DM.
- Catalogue answers come from the tested part 1 code (`catalog.FindMatches`, `Recommend`, `CheckStaleness`), never from the model's own arithmetic.
- The answer format from the parent spec, with every caveat in the headline.
- **Fresh runs are end to end** *(decided 2026-11-20)*. They run the customer's config (input, processors and output) at 1/2/4/8 vCPU against real infrastructure where we have it, and approximate the rest with a clear caveat. See §7.
- **Fresh runs build latest `main`** *(decided 2026-11-20)*. If that build isn't in a release yet, the answer calls it out (§3).
- Results posted back into the requesting thread when the run finishes, 25–60 minutes later.

## Non-goals (later parts)

- Porting the fork's other source stacks (sqlserver, oracle, mongodb, dynamodb), more SaaS sink accounts, and the 0.5 vCPU point (part 4). Until then those sources are approximated and those sinks fall back (§7).
- Parallel isolated runs and the separate AWS account (part 3). Here, runs queue on the existing shared stack.
- Per-user identity. The relay acts as a service account, as RPCN Bot does.
- Spend caps or second approvers (decided against in the parent spec).

## Anti-goals

- The model computes or rounds a CU number itself.
- Customer secrets reach the agent, its transcripts or the catalogue.
- A run starts without a visible cost and a thumbs-up in the thread.
- A result is posted to the wrong thread, or never posted, without anyone being told.

## Constraints found (ADP and internal-agents, 2026-11-20)

- **Agent definition:** agents are YAML manifests (`rpk ai agent apply`).
  - Prompts are Go templates of at most 50 KB. A literal `${NAME}` in a prompt crash-loops the pod.
  - Every apply restarts the pod, so the first calls after an apply may return 502.
- **Native Slack is off in production.** ADP's channel-bridge is integration-only, so the working pattern is a Connect relay pipeline. The reference is `internal-agents/agents/customer-adp/ingest.yaml`:
  - Socket Mode input, then `dedupe` on `event_id`.
  - A 👀 reaction, then a non-blocking A2A `message/send` over `http` with oauth2 client credentials.
  - The agent posts its own reply and swaps reactions through a write-only Slack MCP.
- **Tool calls are authorised as the caller.** On the Slack path that is the relay's service account, so it needs Cedar grants for `Agent.invoke`, `LLMProvider.invoke`, and `McpServer.*` plus `McpServerTool.call` on every Bench Bot MCP. Keep `code_mode` off, because it bypasses per-tool Cedar.
- **Timeouts:** an MCP tool call times out at 10 minutes, and a synchronous A2A call at 15 minutes. A bench takes 25–60 minutes, so dispatch is fire-and-forget and completion is pushed back.
- **No ready-made workflow dispatch.** No managed MCP can dispatch a GitHub workflow. An `openapi` managed MCP over a trimmed GitHub REST spec, with a static PAT, can (`internal-agents/agents/obs-triage/mcp-servers/obs-github.yaml` is the pattern).
- **Panda Platform hosts our tool server.** This is internal-agents' Cloud Run behind IAP. It hosts Go MCP apps via an `mcp:` manifest block, and CI registers each as gateway server `pandaplatform-<app>`.
  - The default `identity: google` needs per-user consent, which a relay service account can't give. Use `identity: none`.
  - There is a fixed 30-second load-balancer timeout.
- **The `a2a_message` Connect processor won't do.** It has a hard 5-minute timeout and sends no `contextId`. Use raw `http` with `blocking: false`.

## Approaches considered (catalogue tool)

- **A. Go MCP app on Panda Platform importing `catalog` (chosen).** Exact reuse of the tested logic. Open items: AWS credentials from Cloud Run, and the ADP team owns the platform.
- **B. Published JSON index plus ADP's managed `aws_s3` MCP.** No hosting, but the model would do matching and CU arithmetic. Rejected (anti-goal).
- **C. Self-hosted `redpanda-connect mcp-server` wrapping the CLI with the `command` processor.** `command` is not allowed in Cloud, so we would host and secure it ourselves. Rejected.

## Design

Three repos are involved:

- `internal-agents` holds the agent, the relay, the tool servers and the policies, under `agents/bench-bot/` and `apps/bench-catalog/`.
- `connect` holds the bench workflow and a small part 1 addition.
- ADP holds the runtime config.

### 1. `bench-catalog` MCP app (internal-agents `apps/bench-catalog/`)

A Go Panda Platform app. It is copied from `apps/mcp-example`, uses `identity: none`, and imports `github.com/redpanda-data/connect/v4/benchmarking/aws/catalog` pinned to a commit.

It exposes these tools, each returning the part 1 CLI's JSON shapes:

| Tool | Input | Output |
|---|---|---|
| `catalog_query` | input, output, processors, message size, mode, and optionally a config YAML (fingerprinted server-side, never stored) | up to 5 matches, best first, each with kind, differences and staleness |
| `catalog_recommend` | entry_id, target MB/s | recommendation, connect_version, build_sha, instance type, points and staleness |
| `bench_plan` | config YAML or base scenario name, target workload (message size, target MB/s), channel, thread ts | a stored plan with a `plan_id`: base scenario, input overrides, row size, customer processors, sweep 1/2/4/8, caveats, estimated cost and duration. Unsupported shapes return `runnable: false` with a reason |
| `bench_dispatch` | plan_id, approver | dispatches the workflow for that plan exactly once and returns the run URL. A second call for the same plan returns the existing run (idempotent) |
| `bench_status` | plan_id | queued, waiting for stack, running, done or failed, with the run URL |
| `lint_config` | config YAML | Connect lint result (catalog-side schema check) |

Supporting infrastructure:

- **Staleness** needs a full git clone with tags. The container clones `redpanda-data/connect` at start-up and fetches tags every hour, which is about 130 MB.
- **AWS access** is read-only on `redpanda-connect-bench-catalog`. The preferred route is GCP service account → AWS web-identity federation, with no static keys. The fallback is a read-only IAM user key held in Panda Platform `secrets:`. The ADP team decides.
- **Cost estimate** comes from a new `catalog/estimates.json`: per-stack base cost and duration for the 4-point sweep, taken from recent runs (postgres about $2–3 and about 25 minutes). A small part 1 addition, with `func EstimateRun(stack string, points int) (Estimate, error)`. *(My call.)* The answer labels it "estimate, excludes queue wait". The file is owned like `sizing.json`. Actual-cost reporting waits for the cost work in part 3.
- **Plans are stored server-side** *(my call)*, as JSON under `plans/<plan_id>.json` in the catalogue bucket, so the cost message, the approval and the completion message all refer to one `plan_id`. The relay's thread history (customer-adp caps it at about 4,000 characters) never has to carry the plan. `bench_dispatch` writes `dispatched_run_id` with an S3 conditional put, so a double 👍 or a redelivered approval can't start two paid runs. This needs **write** access to the `plans/` prefix only.
- **Dispatch lives in this app** *(my call)*, not in a generic OpenAPI MCP, so idempotency and the scenario allow-list are enforced in code. The GitHub PAT is a secret of this app.
- **Latency:** every tool must answer in under 30 seconds. Catalogue `List` is cached in memory for 60 seconds.

### 2. GitHub dispatch (folded into `bench-catalog`)

*(My call.)* There is no separate OpenAPI MCP. `bench-catalog` calls `POST /repos/redpanda-data/connect/actions/workflows/bench_bot.yml/dispatches` itself with a fine-grained PAT (Actions read/write on `redpanda-data/connect` only, owned by a bot account), passing `plan_id` as an input. It finds the run by listing `workflow_dispatch` runs whose `display_title` carries the `plan_id`. The workflow sets `run-name: bench-bot ${{ inputs.plan_id }}`.

### 3. Bench workflow (connect `.github/workflows/bench_bot.yml`)

**Inputs** (`workflow_dispatch`): `plan_id` only. The workflow reads the plan from S3, so nothing a model wrote goes straight into the workflow.

**Which build:** latest `main` *(decided)*.
- The runner builds from the checkout as it does today. Results are stamped with `git describe` (e.g. `v4.119.0-12-gabc1234`) and the commit SHA.
- **Unreleased callout.** When the version has commits after the last tag (a `-N-g` suffix), every answer from that entry says "measured on unreleased build v4.119.0-12-gabc1234 (after v4.119.0)". Part 1 gains `Entry.Unreleased()` and the caveat `unreleased_build`.
- **Staleness.** A build newer than the latest tag reads `current` until code under its components changes again on `main`. Staleness compares against `main` HEAD for unreleased entries, and against the latest tag for released ones. This is a small part 1 change.

**Workload from the plan:** the customer's whole config, run end to end. See §7.

**Steps:**
- Authenticate to AWS via OIDC and fetch the licence, the same as the soak workflows.
- Render the plan's scenario into `$RUNNER_TEMP`, outside the repo, so the tree stays clean and the runner's dirty-tree guard passes. It sets `infra.memory_per_vcpu_bytes: 4000000000`, so bot runs get memory sizing.
- `task aws:bench`. It provisions the source and sink infrastructure the plan needs (§7), and the run writes its catalogue entry (part 1).

**Steps:**
- Authenticate to AWS via OIDC and fetch the licence, the same as the soak workflows.
- Check out with `fetch-depth: 0` so `git describe` works.
- Render a temporary scenario from base plus overrides, with `infra.memory_per_vcpu_bytes: 4000000000`, so bot runs get memory sizing.
- `task aws:bench`. The run writes its catalogue entry (part 1).
- **Queueing** *(my call)*. There is no shared `concurrency:` group, because GitHub keeps only one pending run per group and silently cancels the older one. Instead, an "acquire stack" step loops every 2 minutes for up to 4 hours. It continues only when two things are both true:
  - no `Project=redpanda-connect-bench` EC2 instance exists, which also covers soaks;
  - it can create the lock object `locks/shared-stack` in the catalogue bucket with a conditional put (If-None-Match). The lock lives 2 hours.

  The final step releases the lock. While waiting, the plan's status is `waiting for stack`. A timeout fails the run and the completion message says so.
- **Watchdog** *(my call)*. A scheduled workflow every 30 minutes finds plans dispatched more than 6 hours ago without a completion, and messages the agent to post "this run never reported back: <run URL>".

**Completion:** a final step that runs even on failure sends an A2A `message/send` to `bench-bot`. The message carries `plan_id`, run URL, outcome and the new entry ID; the plan holds the channel, thread and target MB/s. The agent then calls `catalog_recommend` and posts the formatted answer in the thread. This keeps answer formatting in one place, the agent prompt.

The workflow authenticates to ADP with a Cloud service-account client ID and secret held as repo secrets.

### 4. Slack relay pipeline (internal-agents `agents/bench-bot/pipelines/relay.yaml`)

Based on customer-adp's `ingest.yaml`:

- **Events:** a Socket Mode `slack` input handling `app_mention`, `message.im` and `reaction_added`, with `dedupe` on `event_id`.
- **Follow-ups must @-mention the bot** *(my call)*, as customer-adp does after its 2026-09-30 incident. DMs need no mention. The bot's questions end with "reply with @Bench Bot …".
- **Slack Connect:** messages from externally shared channels are dropped. Costs and CU numbers are internal.
- **Secret redaction:** before anything leaves the pipeline, a Bloblang step redacts secret-shaped values. That covers DSN passwords, `password`/`secret`/`token`/`key`/`credentials` values, `sasl` blocks and anything matching AWS key, Slack token or JWT patterns. The redacted message goes into the thread history sent to the agent. The bot also asks people to paste configs with secrets removed.
- **Thread context:** the pipeline reads thread history (`slack_thread`) and sends it with the new message. There is no A2A `contextId`, matching the working relays.
- **Approval:** a `reaction_added` of 👍 or ✅ on a message the bot posted with a cost prompt becomes an agent message: `APPROVAL by <user> on message <ts>`. The cost message text carries `plan <plan_id>`, which the relay copies into the approval. Reactions on other messages are dropped.
- **Hand-off:** a 👀 reaction, then a non-blocking A2A submit. The agent replies and swaps reactions itself through the write-only Slack MCP.

Hosting needs a Redpanda Cloud cluster and service account; the ADP team decides which.

### 5. Agent `bench-bot` (internal-agents `agents/bench-bot/agent.yaml`)

**MCP servers:**
- `pandaplatform-bench-catalog`
- a write-only Slack MCP (post and react), copied from `slack-customer-agent.mcp.yaml`

**System prompt rules:**
- Classify each request as a config check or a sizing question.
- Lint configs. Ask for source, sink, message size, target MB/s, and CDC vs bulk until known.
- Always call `catalog_query`, and for sizing `catalog_recommend`. Never compute, round or estimate CUs yourself.
- **Answer format:** the parent spec's headline and curve lines, with caveats in the headline. The CU figure is primary, vCPU secondary, and the Connect version, instance type and cost are always shown. A closest match is shown first, labelled, with the fresh-run cost.
- **Fresh run:** call `bench_plan`. If it isn't runnable, say why and offer to log a request; Jira filing is a later add. If it is runnable, post the plan, its caveats, cost and duration, plus `plan <plan_id>`, and ask for a 👍. On an `APPROVAL` naming that plan, call `bench_dispatch(plan_id, approver)`, then reply "queued, starts when the shared bench stack is free, ~N min once running, I'll post here".
- **Completion message:** call `catalog_recommend` on the new entry and post the answer. On failure, post the run URL and outcome.
- Refuse requests to use customer credentials or hosts.

### 7. End-to-end runs (runner, connect repo)

This brings the parent spec's end-to-end topology (Design §4) forward from part 4.

**Topology.** A new `e2eTopology` sits beside `sourceTopology` behind the runner's existing `Topology` interface:
- The customer's input, with identity swapped for bench infrastructure, then their processors as written, then their output, with identity swapped too.
- Throughput is measured at the output. A `benchmark` processor is appended last in `output.processors`, so backpressure from a slow sink shows up as lower throughput. The runner adds a sink-side ground-truth count where a sink exposes one (rows or objects written), and fails the point if the two disagree by more than 2%.
- Catalogue identity is the customer's pipeline. The harness `benchmark` processor is excluded, as in part 1's `BenchSourceOutput` rule.

**Inputs** *(my call on which are "real")*:

| Customer input | How it's run | Caveat |
|---|---|---|
| `postgres_cdc`, `mysql_cdc` | Existing RDS stacks with the sustained write workload | none |
| `kafka_franz`, `kafka`, `redpanda`, `redpanda_common` | The bench Redpanda cluster, pre-loaded with synthetic messages of the customer's size (enough for the whole sweep) | none |
| anything else (sqlserver/oracle/mongodb/dynamodb CDC, http, S3, etc.) | **Approximated:** a Redpanda topic with synthetic messages of the customer's size and shape, read with `redpanda` | `source_approximated` in the headline |

**Outputs** (accounts that exist today: Snowflake, BigQuery, AWS):

| Customer output | How it's run | Caveat |
|---|---|---|
| `redpanda`, `kafka_franz`, `kafka` | The bench Redpanda cluster, with a per-run topic | none |
| `aws_s3` | A per-run prefix in a bench bucket, deleted after the run | none |
| `iceberg` (S3 + Glue/REST catalog) | A per-run namespace on S3, dropped after the run | none |
| `snowflake_streaming`, `snowflake_put` | Our Snowflake account, a per-run schema dropped after the run, at the fixed reference warehouse size (the user can raise it before the run) | none |
| `gcp_bigquery` | Our BigQuery project, a per-run dataset deleted after the run | none |
| anything else | **Fallback:** a `drop` output, so source plus processors are measured | `sink_not_measured` in the headline, and a request is logged to add the sink |

**Processors.** The customer's processors run as written. Processors that call customer systems (`http`, `sql_*`, cache lookups, `branch`/`workflow` calling out) are swapped for canned stand-in responses, with the caveat `external_calls_stubbed`.

**Identity swap.** Endpoints, credentials, TLS, topics, tables, schemas and slot names always come from the bench. Performance settings (batching, `max_in_flight`, checkpoint limits, compression, partitioning, etc.) come from the customer. `stream_snapshot: true` is honoured for CDC sources with a seeded table. Seeding before the reset is a framework change; see `docs/superpowers/specs/2026-09-22-inc-snapshot-soak-design.md`.

**Credentials.** Snowflake, BigQuery and Iceberg catalog credentials live in the bench account's Secrets Manager. The runner fetches them like the licence, and they are never in the plan or the catalogue.

**Teardown.** Per-run SaaS scratch (schemas, datasets, namespaces, prefixes) is removed in the runner's deferred teardown. The orphan reaper is extended to sweep scratch older than 4 hours, using a name prefix of `bench_<session>`.

**Cost.** `estimates.json` gains a per-sink SaaS cost for the 4-point sweep. Snowflake's scales with warehouse size, BigQuery's with streaming bytes. The cost prompt shows AWS and SaaS separately.

**Plans.** Two plans:
- **2a:** this section (runner end-to-end topology, sinks, credentials, teardown) plus the bench workflow, in connect.
- **2b:** the agent, the `bench-catalog` app, the relay and the policies, in internal-agents.

2a comes first, because 2b's fresh-run path depends on it. 2b's catalogue-answer path can be tested in Slack before 2a lands.

### 6. Grants and secrets (who creates what)

| Item | Owner |
|---|---|
| Slack app (Socket Mode; scopes `app_mentions:read`, `chat:write`, `reactions:read`, `reactions:write`, `channels:history`, `groups:history`, `im:history`, `im:read`), plus a test channel | Slack admin / IT |
| Cloud service account for the relay and for the workflow's A2A call | ADP team |
| Cedar policies: `Agent.invoke` on bench-bot, `LLMProvider.invoke`, `McpServer.*` on the three MCPs | ADP team (we write the YAML) |
| GitHub bot PAT (Actions read/write on connect) as a `bench-catalog` app secret | Connect repo admin |
| AWS access for `bench-catalog`: read the catalogue, read/write `plans/` only | Bench AWS account owner + ADP team |
| Repo secrets for `bench_bot.yml` (ADP client ID and secret) | Connect repo admin |

## Left to the builder

- Exact Bloblang redaction patterns, and the redaction test fixtures.
- The allow-list of overridable input keys per base scenario.
- Cache and refresh intervals inside the MCP app.
- Prompt wording, as long as it follows the rules above.
- Whether `lint_config` uses the Connect linter as a library or shells out.
