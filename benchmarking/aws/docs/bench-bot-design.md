# Bench Bot: on-demand Connect benchmarks and sizing from Slack

Status: draft, awaiting approval (2026-11-20)

## Intent

Sales and engineering need two answers about Redpanda Connect workloads, today only available by asking an engineer to hand-run a benchmark:

1. **Config check**: "here is a customer's pipeline config. Does it work, and how does it perform?"
2. **Sizing**: "the customer has workload X. How many compute units (CUs) do we sell them?"

Bench Bot is a Slack agent, built like RPCN Bot, that takes either kind of request. It asks for missing information. If a still-current benchmark result exists, it answers from that. Otherwise it runs a new benchmark at 1, 2, 4 and 8 vCPU in AWS and answers from the result.

Success means a salesperson can get a defensible CU recommendation for a customer workload without involving an engineer, and every number they quote carries what it was measured on.

## Goals

- Accept a pasted Connect config or a plain-language workload description in Slack.
- Ask follow-up questions until the request is benchmarkable (source, sink, processors, message size, target throughput, CDC vs bulk).
- Answer from the results catalogue when a matching, still-current result exists.
- Otherwise run a benchmark across 1, 2, 4 and 8 vCPU (10, 20, 40, 80 CUs) and post the result in the thread.
- Benchmark the **full pipeline**: the customer's input, processors and their **real sink**.
- Support all connectors over time, not only postgres_cdc and mysql_cdc.
- Run several benchmarks at the same time.

## Non-goals

- Running against customer infrastructure, customer credentials or customer data.
- Guaranteeing performance contractually. Answers are measured evidence with stated conditions.
- Replacing the nightly and `/soak` PR soak runs. Those keep their own workflows.
- Approval workflows or spend caps (explicitly decided against; see Decisions).

## Anti-goals (failure even if it technically works)

- A number gets quoted to a customer without the conditions it was measured under. For example, the sink wasn't measured, it was an estimate, or it came from an old Connect version.
- An old result is presented as current after the connector has been improved.
- Customer secrets end up stored in our systems.
- Benchmarks leak AWS or SaaS resources, or one run's resources collide with another's.

## Decisions made (by the requester)

| Topic | Decision |
|---|---|
| Users | Sales and engineering. |
| Connectors | All connectors, added over time. |
| Concurrency | Multiple benchmark runs at the same time. |
| Pipeline scope | Full pipeline including the customer's real sink (option B). We hold our own test accounts for SaaS sinks. |
| Unsupported sink | Fall back to measuring source and processors with a null sink. Label the result "sink not measured" **in the headline**, and log a request to add that sink. |
| Connect version | Every answer lists the Connect version measured. Older results may lack improvements. |
| Sizing output | Recommend a number. The curve is shown underneath. |
| Headroom | Size so the workload uses at most **70%** of measured peak. This is a configurable setting owned by the Connect PM, and is shown in every answer. |
| Units | Answers are centred on CUs, with vCPU alongside. **1 CU = 0.1 vCPU + 400 MB memory**, so 1 vCPU = 10 CUs = 4 GB. CUs matter mostly for self-hosted (sold by core pack) but also for Cloud (usage-based, where cores still matter). The bot gives the CU figure and leaves the commercial translation to the user. |
| Binary | Bench the full `redpanda-connect` distribution, **not** the cloud distribution. |
| Sink sizing | Each SaaS sink uses a **fixed reference size** (warehouse size, slots, etc.). The user may raise it before the run, which updates the cost estimate. Answers state the sink size used, and flag when the sink was the bottleneck. |
| Between sweep points | Estimate along a straight line between the two measured points and round up to the next 10 CUs. The answer says it is an estimate. |
| Below 10 CUs | Don't extrapolate. Answer "10 CUs or less" and offer a run with a 0.5 vCPU (5 CU) point added. |
| Spend approval | Show the estimated cost (AWS + SaaS) before every run. **Anyone** may approve with a thumbs-up. No second approver, no cap. |
| Architecture | Approach A (below). |

## Constraints (from the existing framework, `benchmarking/aws`)

- The runner already sweeps `matrix.cpu_points: [1, 2, 4, 8]` and provisions VPC, runner EC2, load-gen EC2, 3 Redpanda brokers, the source and a results bucket. It then tears all of it down. Roughly 25 min and ~$2–3 per postgres bench.
- **Only postgres and mysql stacks exist in this tree.** sqlserver, oracle, mongodb, dynamodb and iceberg live on the original development fork and must be ported.
- **Only the source topology exists** (`runner/topology.go`, `topologyFor`). It measures source → Connect → Redpanda. `direction: sink` was cut, and there is no end-to-end (source → processors → real sink) topology at all.
- **Runs are not isolated.** Terraform state keys are fixed (`"shared"` and the stack name, `runner/main.go`), pre-flight refuses to start while bench EC2 exists, and the soak workflows serialise on `concurrency: bench-soak`. Session-scoped isolation is the documented scaling path but is unbuilt.
- The orphan-reaper Lambda destroys `Project=redpanda-connect-bench` resources older than 4h. Bot runs must finish well inside that or tag themselves exempt.
- Soak workflows already authenticate to AWS via GitHub OIDC and fetch the enterprise licence from Secrets Manager. Reuse this rather than adding stored keys.
- Known framework traps: reset runs after seed at every sweep point, brokers are not on the runner host, and there is no quiesce window between workload end and Connect shutdown.
- ADP's custom Slack integration has no user identity, so the agent acts with its own service-account credentials. The shared `github-read` MCP is read-only, so dispatching workflows needs a separate tool with a write-scoped token (RPCN Bot hit both, Oct 2026).
- GitHub Actions jobs are capped at 6h.
- **Sweep points share one machine.** The runner pins Connect to N cores with `taskset` on a single c8g.4xlarge (16 vCPU), with memory unconstrained (`runner/matrix.go`). A CU includes memory, so each point must also be limited to 4 GB per vCPU (e.g. a cgroup memory limit) for results to map to CUs.
- **Existing bench results for other connectors** (sqlserver, oracle, mongodb, dynamodb, iceberg) and their stacks live in the `connect_prakhar` repo. Their format and metadata were not checked when this was written.
- **SaaS accounts that exist today:** Snowflake and BigQuery, plus AWS (S3, Iceberg on S3, Redpanda).

## Approaches considered

**A. New ADP agent shaped like RPCN Bot (chosen).** A Connect pipeline relays Slack to and from an ADP agent. The agent dispatches a GitHub Actions workflow that runs the bench runner on an isolated stack, and searches an S3 results catalogue. This reuses tooling the team knows, and Actions gives run history and logs for free. It needs a small write-scoped workflow-dispatch tool. The 6h job cap is fine for ~25–60 min benches.

**B. Add benchmarking to RPCN Bot.** One bot in the channel, but it mixes a read-only engineering help bot with a spend-incurring, sales-facing one in one prompt and toolset, and every change routes through RPCN Bot's owner. Rejected. It can be merged later, once ADP's native Slack integration and sub-agent support arrive.

**C. Standalone Slack app plus our own job orchestrator on AWS.** Full control and no 6h limit, but we would build and run conversation handling, queueing and hosting ourselves. Rejected as too much to own for what A gives nearly free.

## Design

Four parts, built in this order. Each is useful on its own. The bot can answer from the catalogue before parallel runs or new connectors exist.

### 1. Results catalogue

A durable, searchable record of every bench result, including existing and future runs.

Each record captures:
- **Identity:** connectors (input, processors, output) and their settings, normalised so that equivalent configs match.
- **Workload:** message or row size, CDC vs bulk, offered load.
- **Environment:** Connect version (release tag or commit), instance types, region.
- **Results per sweep point:** peak throughput (MB/s and msgs/s), CPU use, memory, and whether the sink was measured.
- **Cost and links:** AWS and SaaS cost of the run, plus links to the run and the raw artefacts.

Lives alongside the existing results/archive buckets. The storage and query mechanism is left to the builder. It must be queryable by the agent via a tool.

**Staleness.** A stored result is current for a request if nothing relevant to its connectors changed between its Connect version and the latest release. "Relevant" means a release-note entry naming that connector, or a merged change in that connector's code path. If something changed, the bot still shows the old result, labelled "measured on vX.Y, connector changed since", and offers a fresh run on the latest release.

### 2. Bench Bot agent (ADP)

- **Slack relay:** a Connect pipeline, same pattern as RPCN Bot.
- **Tools:**
  - catalogue search
  - workflow dispatch (write-scoped GitHub token, limited to the bench workflow)
  - run status
  - config validation (`rpk connect lint` or equivalent)
- **Request flow:**
  1. Classify the request: config check or sizing.
  2. Validate the config if one is given. Ask about anything not inferable: source, sink, processors, message size, target throughput, CDC vs bulk.
  3. Search the catalogue. If an exact match is current, answer.
  4. Otherwise post the planned run and its estimated cost (AWS + SaaS), then wait for a thumbs-up from anyone.
  5. Dispatch the run, acknowledge ("running, ~N min, I'll reply here"), and post the result in the thread when done.
- **Config handling:** replace customer endpoints and secrets with our test infrastructure, and generate synthetic data shaped like the customer's messages.
- **Secrets** *(my call)*: Slack history and agent transcripts count as our systems. The Connect relay pipeline redacts secret-shaped values before the message reaches the agent, and the bot asks people to paste configs with secrets removed. Nothing stored in the catalogue contains customer secrets.
- **Processors that call customer systems** *(my call)*: `http`, sql or cache lookups, and `branch`/`workflow` calls out to customer services are replaced with canned stand-in responses. The headline says "external calls stubbed".
- **Close matches** *(my call)*: if a stored result is close but not exact, the bot shows it first, labelled "closest match, not exact", together with the cost of a fresh run. So the catalogue needs similarity search, not just equality.
- **Approval signal** *(my call)*: a thumbs-up reaction from anyone. If the ADP relay can't deliver reaction events, a thread reply of "go" is the fallback.
- **CU recommendation:** the bot computes the CUs needed against both measured peak throughput (at 70% headroom) and peak memory (at 400 MB per CU), and recommends the larger. The answer says which one bound.
- **Answer format** (every answer):

  > **Recommend 40 CUs (4 vCPU)** for 40 MB/s Postgres CDC → Snowflake, 1 KB rows
  > Peak measured: 10 CU 18 MB/s · 20 CU 35 MB/s · 40 CU 70 MB/s · 80 CU 120 MB/s
  > Sized so the workload uses at most 70% of peak · Connect v4.118.0 · c8g · cost $6.40 · exact match

  The headline carries any of these caveats when they apply: "sink not measured", "estimate between measured points", "measured on older version, connector changed since", "closest match, not exact".

### 3. Parallel, isolated runs

- **Separate AWS account** *(my call)*: bot runs live in their own AWS account, with a second copy of the persistent stack (reaper, OIDC role, alarms, archive). In the shared account, a bench's EC2 makes the soak pre-flight (`runner/preflight.go`) refuse to start, so bot runs would block the nightly and `/soak` runs.
- **Per-run isolation:** each run gets its own session ID. That ID drives the Terraform state key, resource names and tags, so runs never share state or resources. Replace the fixed `"shared"` and stack-name state keys and the bench-EC2 pre-flight with session-scoped equivalents.
- **Workflow concurrency:** a bot-facing workflow with per-session concurrency, not the global `bench-soak` group. Soak workflows keep their own lane.
- **Sink isolation:** each run gets its own scratch database, dataset or table in each SaaS sink, torn down at the end, with the reaper extended to sweep stale ones.
- **Account quotas:** AWS quotas (vCPU, VPCs, EIPs, RDS instances) set a practical concurrency ceiling. The builder determines it and the bot queues beyond it.

### 4. Connector coverage

- **End-to-end topology:** add one alongside `sourceTopology`: real source → customer processors → real sink. Measure throughput at the sink, with a sink-side ground-truth count per sink type, plus Connect's own metrics.
- **Null-sink fallback:** the same topology with output swapped for a null sink, used when the sink is unsupported.
- **Port the fork's stacks:** sqlserver, oracle, mongodb, dynamodb and iceberg. Then add the remaining sources and sinks. Each needs a source stack and a workload generator. Kafka/Redpanda as a source likely covers the largest share of customer configs and may be worth prioritising.
- **SaaS sink accounts:** the first release supports real sinks with existing accounts: Snowflake, BigQuery, and AWS-hosted S3, Iceberg on S3 and Redpanda. Every other sink uses the null-sink fallback until an account is added, with credentials in Secrets Manager.
- **Backfill:** import the `connect_prakhar` results into the catalogue only where they record Connect version and instance type. Results without those are left out, not guessed.
- **Missing-sink requests:** each fallback logs a request to add that sink. The destination is left to the builder: a Jira ticket in CON (the bot can file these the way RPCN Bot does) or a tracked list.

## Left to the builder

- RPCN Bot's setup (the Slack relay pipeline, tool hosting, reaction events) lives in ADP, not in a repo. Get it from Joe Woodward and the ADP team.
- The fixed reference size for each SaaS sink.

- Catalogue storage and query technology, and the config-normalisation rules for "same pipeline".
- How cost is estimated before a run.
- How synthetic data is derived from a customer config.
- The concurrency ceiling and queueing behaviour.
- Exact sink-side measurement per sink.
- Ordering of connector ports after the fork's stacks.
