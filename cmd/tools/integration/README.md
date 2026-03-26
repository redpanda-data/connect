# Integration Test Runner

Runs integration tests package-by-package with caching, resume, and structured output.

```bash
go run ./cmd/tools/integration run [--clean] [--debug] [--race] [filter...]
```

## Flags

| Flag | Description |
|---|---|
| `--clean` | Ignore cache, start a fresh run |
| `--debug` | Enable debug logging to stderr |
| `--race` | Enable race detector (sets `CGO_ENABLED=1`) |

Positional arguments are substring filters on package paths:

```bash
go run ./cmd/tools/integration run kafka redis
```

## Features

- **Pause/Resume** -- The runner automatically resumes from the latest run. Tests that already passed or were skipped are excluded via `-skip` regex. Use `--clean` to force a fresh run.

- **Context Tags** -- XML-like tags provide structure at two levels. In the index file, each package section is wrapped in tags (e.g. `<kafka>...</kafka>`). In per-package output files, each test run is wrapped in `<results timestamp="..." package="...">...</results>` tags, separating multiple runs within the same file while keeping them together for resume.

- **Error Links** -- Failed tests include a direct file:line reference to the output log (e.g. `FAIL TestFoo (0.5s) -> .integration/20260326094101/kafka.txt:42`), allowing immediate navigation to the failure context.

- **SubTest Grouping** -- When all subtests of a parent pass, they are compacted into a single parent entry. When any subtest fails, all subtests are shown individually so failures remain visible.

- **Timeout Detection** -- Tests that start (`=== RUN`) but never finish (`--- PASS/FAIL/SKIP`) are detected as timed out and reported as failures with an error link.

- **Per-Package Timeout** -- Each package in [`packages.json`](packages.json) can specify a custom `timeout` field. Default is 5 minutes.

- **Index Files** -- Each run produces a Markdown index file at `.integration/<timestamp>/index-<timestamp>.md` with a summary of all package results, test outcomes, and a final pass/fail/skip count.

- **Docker Cleanup** -- Testcontainers are removed between packages to prevent resource leaks. Skipped in CI environments.

- **Race Detection** -- The `--race` flag enables Go's race detector with `CGO_ENABLED=1` set automatically.

## Output Directory

All state is stored under `.integration/`:

```
.integration/
  20260326094101/              # timestamped run directory
    index-20260326094101.md    # index file with structured results
    kafka.txt                  # raw go test output per package
    redis.txt
    aws-s3.txt
```

On resume, the runner picks the latest timestamped directory and appends new results to existing output files. Line numbers in error links remain stable across resumes.
