# Integration Test Runner

Runs integration tests package-by-package with caching, resume, and structured output.

```bash
go run ./cmd/tools/integration run [flags] [filter...]
```

Or via task runner:

```bash
task test:integration
```

## Flags

| Flag | Description |
|---|---|
| `--output-dir` | Directory for test output (default: `.integration/<timestamp>`) |
| `--clean` | Ignore cache, start a fresh run |
| `--debug` | Enable debug logging to stderr |
| `--race` | Enable race detector (sets `CGO_ENABLED=1`) |
| `--unit` | Run all tests, not just integration tests |
| `--cover-profile` | Generate coverage profile per package (`.cov` files) |
| `--parallel` | Number of packages to test at the same time (default: 1) |

`--output-dir` and `--clean` are mutually exclusive.

Positional arguments are substring filters on package paths:

```bash
go run ./cmd/tools/integration run kafka redis
```

## Features

- **Pause/Resume** -- The runner automatically resumes from the latest run directory. Tests that already passed or were skipped are excluded via `-skip` regex. Use `--clean` to force a fresh run.

- **Error Links** -- Failed tests include a direct file:line reference to the output log (e.g. `FAIL TestFoo (0.5s) -> .integration/20260326094101/kafka.txt:42`), allowing immediate navigation to the failure context.

- **Subtest Compaction** -- When all subtests of a parent pass, they are collapsed into a single parent entry. When any subtest fails, all subtests are shown individually so failures remain visible.

- **Timeout Detection** -- Tests that start (`=== RUN`) but never finish (`--- PASS/FAIL/SKIP`) are detected as timed out and reported as failures with an error link.

- **Per-Package Timeout** -- Each package in [`packages.json`](packages.json) can specify a custom `timeout` field. Default is 5 minutes.

- **Docker Cleanup** -- Ryuk, the testcontainers reaper, is on by default. It removes the containers, networks, and volumes of each package about 10 seconds after its test binary exits, also when the run is interrupted or crashes. If you disable it (`TESTCONTAINERS_RYUK_DISABLED=true`), containers will not be cleaned up and will accumulate.

- **Parallel Packages** -- `--parallel N` tests up to N packages at the same time. The runner starts them in the order of [`packages.json`](packages.json). With N above 1, each package writes its output as one block when it is done, so the packages show in the order they finish. With the default of 1, a package writes its lines as they happen. Keep the default of 1 on a small Docker VM: packages that run at the same time share its CPU and memory.

- **Race Detection** -- The `--race` flag enables Go's race detector with `CGO_ENABLED=1` set automatically.

## Output Directory

All state is stored under `.integration/`:

```
.integration/
  20260326094101/         # timestamped run directory
    index.md              # Markdown index with structured results
    kafka.txt             # raw go test JSON output per package
    redis.txt
    aws-s3.txt
```

On resume, the runner picks the latest timestamped directory and appends new results to existing output files. Line numbers in error links remain stable across resumes.

## Package List

The list of packages and their timeouts is defined in [`packages.json`](packages.json).

## Affected Packages

The `affected` subcommand reads changed file paths on stdin and prints the packages in `packages.json` whose tests depend on them, as `GITHUB_OUTPUT` lines. The pull request job in `integration_test.yml` uses it to build its filter.

```bash
git diff --name-only origin/main...HEAD | go run ./cmd/tools/integration affected
```

A change to a Go package selects every package whose test binary (`go list -test -deps`) includes it. A `_test.go` file or a file under `testdata/` selects only its own package. When the run would select more than `--max-packages` packages (default 20), `filters` is empty and `budget_exceeded=true`.
