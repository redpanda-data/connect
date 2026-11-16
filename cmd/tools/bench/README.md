# Benchmark Suite Tool

Discovers the connector benchmark suites under `internal/impl`, checks them against one contract, runs them, and turns their logs into a result JSON document. It exists so that a person or an agent can find out what can be benchmarked, what it needs, how to run it and what the numbers were, without reading each suite's Taskfile and README.

```bash
go run ./cmd/tools/bench list --json           # what exists, what each suite needs
go run ./cmd/tools/bench run aws_dynamodb_cdc  # run it, get result.json
go run ./cmd/tools/bench check                 # validate the manifests
go run ./cmd/tools/bench parse < connect.log   # summarise a log captured by hand
```

Suites are migrated to the contract one at a time. `list` shows all of them: the ones that are not migrated are listed with what they lack, and are still run by hand from their README.

## The suite contract

A suite is a directory with three things.

### 1. `bench.yaml`

```yaml
name: aws_dynamodb_cdc            # unique, used on the command line
component: aws_dynamodb_cdc       # the connector under test, by its config name
kind: input                       # input, output, processor, cache or pipeline
summary: Read throughput of the DynamoDB CDC input on DynamoDB Local
results: docs/benchmark-results/dynamodb-cdc.md   # optional, where past runs are recorded
requires:
  tools: [docker, go]             # executables the tasks call
  env: [HANA_DSN]                 # optional, variables that must be set
  notes: Free text for what cannot be checked (a licence, an account).
scenarios:                        # at least one, the first is the default
  - name: streams
    description: Drain the stream records of 450k seeded items
    task: bench:run               # optional, the task that runs it
    vars: {CORES: "4"}            # optional, Taskfile variables for every task
    max_duration: 5m              # optional, default 10m
```

Unknown keys are rejected. `requires.tools` is checked with a `PATH` lookup before anything starts, and a `docker` entry also checks that the daemon answers. `task` itself is always required.

### 2. A Taskfile with the `bench:*` interface

| Task | Does |
|---|---|
| `bench:up` | Starts the dependencies and creates the schema. Safe to run when a previous run was left up. |
| `bench:load` | Loads the dataset. |
| `bench:reset` | Clears what a previous run left behind (checkpoints, replication slots, consumer groups), keeps the data. |
| `bench:run` | Runs the pipeline under test in the foreground. A scenario can name another task. |
| `bench:down` | Removes everything `bench:up` started. |

All five exist in every suite, as a no-op when there is nothing to do, so the same sequence works everywhere. The scenario's `vars` are passed to each of them as `NAME=value`.

### 3. A pipeline that logs its throughput

The pipeline under test has a [`benchmark` processor](https://docs.redpanda.com/redpanda-connect/components/processors/benchmark/) with `interval: 1s` and `count_bytes: true`. Its `rolling stats` lines are the samples and its `total stats` line, logged at shutdown, is the total. Either log format works.

A `README.md` is still required: it holds what a manifest cannot, such as how to read the numbers and what limits them.

## Running a suite

```bash
go run ./cmd/tools/bench run [flags] <suite>
```

`<suite>` is a manifest name or a suite directory. The run is `bench:up`, `bench:load`, `bench:reset`, the scenario's task, then `bench:down`. Teardown also happens when a step fails or the run is interrupted.

Most suites read a source that never ends, so the tool decides when the measurement is over. It interrupts the scenario's task when either happens:

- messages have flowed and then `--idle-samples` samples in a row saw none (the source is drained), or
- `--max-duration` has passed.

The suite then shuts down and logs its total stats. A task that exits by itself ends the measurement too.

| Flag | Description |
|---|---|
| `--scenario` | Scenario to run (default: the first one of the suite) |
| `--var NAME=value` | Taskfile variable for every task, overrides the scenario's `vars` (repeatable) |
| `--max-duration` | Stop the run phase after this long (default: the scenario's `max_duration`, or `10m`) |
| `--idle-samples` | Samples in a row without a message that end the run, once messages have flowed (default: 5, 0 disables) |
| `--skip-setup` | Skip `bench:up`, `bench:load` and `bench:down`: measure again on a suite that is already up |
| `--keep` | Skip `bench:down`, to run again with `--skip-setup` or to inspect |
| `--out` | Directory for `result.json` and `run.log` (default: `target/bench/<suite>/<scenario>/<timestamp>`) |

The output of the tasks goes to stderr and to `run.log`. Stdout carries only the result, without its samples, so it can be piped to `jq`. The exit code is non-zero unless the status is `ok`.

## The result document

`result.json`, schema version 1:

```json
{
  "schema_version": 1,
  "suite": "aws_dynamodb_cdc",
  "component": "aws_dynamodb_cdc",
  "scenario": "streams",
  "status": "ok",
  "git_sha": "030e5f5e4ec424202a3e50d590384cc482418282",
  "git_dirty": false,
  "started_at": "2026-11-16T16:53:56Z",
  "finished_at": "2026-11-16T16:54:40Z",
  "duration_seconds": 43.7,
  "stop_reason": "idle",
  "host": {"os": "darwin", "arch": "arm64", "cpus": 14, "go_version": "go1.27.4"},
  "vars": {"CORES": "4"},
  "summary": {
    "samples": 5,
    "median_mb_s": 237.2, "p5_mb_s": 54.1, "p95_mb_s": 256.7, "peak_mb_s": 256.7, "mean_mb_s": 186.3,
    "median_msg_s": 114500, "p5_msg_s": 26070, "p95_msg_s": 124000, "peak_msg_s": 124000, "mean_msg_s": 90000
  },
  "total": {"msg_per_sec": 40906.9, "mb_per_sec": 84.7},
  "samples": [{"t": 0, "msg_per_sec": 0, "mb_per_sec": 0}],
  "result_path": "target/bench/aws_dynamodb_cdc/streams/20261116T165317Z/result.json",
  "log_path": "target/bench/aws_dynamodb_cdc/streams/20261116T165317Z/run.log"
}
```

| Field | Meaning |
|---|---|
| `status` | `ok`: messages were measured. `no_samples`: the run logged no sample with messages. `failed`: a prerequisite was missing or a task failed, see `error`. |
| `stop_reason` | Why the run phase ended: `idle`, `max_duration`, `exited` or `interrupted`. |
| `started_at`, `finished_at`, `duration_seconds` | The run phase only, without setup and teardown. It includes the time `go run` takes to build Connect. |
| `git_sha`, `git_dirty` | The commit that was measured, and whether the tree had uncommitted changes. |
| `vars` | The Taskfile variables of the run: the scenario's, overridden by `--var`. |
| `samples` | One entry per `rolling stats` line. `t` is the index of the sample, which is seconds with the required `interval: 1s`. |
| `summary` | Aggregates over the **active window**: the samples from the first to the last one that saw messages. Warm-up and the idle tail are left out, a stall in the middle is not. `samples` is the size of that window. |
| `total` | The lifetime average the processor logs at shutdown. It includes warm-up and idle time, so it is lower than the summary. Absent when the suite was killed before it logged it. |

All `mb` figures are decimal megabytes (1 MB = 1,000,000 bytes), the unit the `benchmark` processor logs. The `summary` and `samples` field names and the percentile method are those of the AWS harness in [`benchmarking/aws`](../../../benchmarking/aws/README.md), so a reader of one result reads the other.

Adding a field does not change `schema_version`. Renaming or removing one does.

Compare `summary.median_msg_s` between runs, not `peak` or `total`. A run with a `summary.samples` of a few seconds is too short to compare: load more data.

## Migrating a suite

1. Add the five `bench:*` tasks to its Taskfile. Keep the old task names as aliases if people use them.
2. Make sure the pipeline has the `benchmark` processor and that `bench:run` keeps it in the foreground.
3. Add `bench.yaml`.
4. `go run ./cmd/tools/bench check`, then `go run ./cmd/tools/bench run <name>` and look at `result.json`.

`TestRepositorySuites` runs the same validation as `check` in the unit tests, so a manifest that drifts from its Taskfile fails CI.

A suite that compares against another engine (Kafka Connect, Debezium) measures it some other way than the `benchmark` processor. Those are out of the contract for now: only the Redpanda Connect side of a comparison gets a manifest.
