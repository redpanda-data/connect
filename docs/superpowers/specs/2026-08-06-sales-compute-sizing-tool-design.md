# Sales Compute Sizing Tool — Design

**Date:** 2026-08-06
**Status:** Approved for planning

## Problem

Sales needs a licensable core count for a Redpanda Connect deployment, live, without
asking the perf team. The benchmark data to answer this exists in
`benchmarking/aws/results/` but is unusable by a non-engineer: 45+ timestamped JSON
files, a majority of them smokes or broken runs reading 0 MiB/s, with no indication of
which run is authoritative.

## Form factor

A single self-contained HTML page at `benchmarking/aws/sizing/index.html`, published as
an Artifact so reps get a bookmarkable URL.

Decided against a Claude skill: the operator is a sales rep or SE self-serving, possibly
mid-call, with no terminal and no repo. Decided against a generator that derives the page
from `results/` at build time: the curated dataset is six rows, so the maintenance saving
does not pay for the extra moving part. Numbers are typed into the page as a JS constant,
each carrying the provenance of the run it came from.

## Units

**Superseded — corrected 2026-08-06.** This section originally claimed every throughput
number in `results/` is MiB/s. That was wrong, and generalised from one call site.

The unit depended on how a point was summarised (`runner/matrix.go:288`):

| Summarised from | Unit |
|---|---|
| Connect arm of a source scenario — Connect's own `rolling stats:` log, SI decimal via `humanize.Bytes` | **MB/s** (10⁶) |
| Sink scenario, or any `kafka_connect` arm — broker/Iceberg byte counters divided by `(1 << 20)` | **MiB/s** (2²⁰) |

Two consequences followed. First, five of the six blessed runs are decimal MB/s, so a page
converting with `2^20` under-sized on about 1.8% of plausible inputs — including cases
where it produced a core count instead of a refusal. Second, and beyond this tool, every
Connect-vs-Kafka-Connect ratio in `results/` is inflated 4.86%, because Connect's
source-side figure was decimal while the KC arm it was compared against was binary.
Conclusions survive (postgres at 4 vCPU: 6.0x → 5.7x; mysql: 2.04x → 1.94x) but the
printed figures are biased in Connect's favour.

The harness now divides by a single `bytesPerMB = 1_000_000` everywhere
(`runner/stats.go`), chosen because it matches both the `mb_per_sec` field name and what
Connect reports about itself. Result files written before that fix keep their original
unit, so each connector on the page carries a `curveUnit` tag and the target is converted
into that connector's unit — which keeps every curve value byte-identical to its run file.

The page accepts volume as events/sec x size **or** as a throughput figure (MB/s, MiB/s,
GB/day, TB/day). Everything funnels through bytes/sec before being expressed in a
connector's curve unit, so there is exactly one place a unit conversion happens.

## The calculation

1. Rep enters events/sec and average event size (default 1200 B, editable, labelled as
   the size the benchmarks used).
2. Target throughput = events/sec x event size, in MiB/s.
3. Look up the connector's measured Connect median MiB/s at 1, 2, 4, 8 vCPU.
4. Divide every point on that curve by the processing-tax multiplier.
5. Inflate the target, not the curve, by the headroom factor: `required = target x (1 + headroom)`,
   default 30%.
6. Return the smallest vCPU point that clears the target, reported as licensable cores.

### Why median, not p5

The obvious choice is p5 — a rep should not quote a number the pipeline misses half the
time. It does not survive contact with the data. The postgres run holds a warm-window p5
of 25–32 MiB/s against a median of 83–102, with dips to ~2 MiB/s scattered through the
entire run, not just startup. MySQL, on the same harness and instance, is tight: p5 95
against median 102. That asymmetry is evidence the postgres dips come from the load
generator's pacing rather than from Connect, so sizing on p5 would penalise postgres for
a harness artifact.

Median is therefore the basis, and the headroom slider carries the safety margin
explicitly where the rep can see and defend it.

### Guard: flat curves

Where the measured curve is flat, the tool must not return a larger vCPU count for a
target above the ceiling. It reports the ceiling and the actual fix:

- **oracledb_cdc** — ~13 MiB/s regardless of vCPU. The ceiling is per LogMiner reader.
  Fix is more readers: measured 19, 25, 30 MiB/s at 4 vCPU as readers increase.
- **mongodb_cdc** — ~33 MiB/s from 2 vCPU up. The ceiling is the single change-stream
  cursor. Fix is sharding.

### Guard: event size out of range

Every curve is MiB/s measured at 1.2 KB events (4 KB for dynamodb). Below roughly 0.5 KB
per-message overhead dominates and the achievable MiB/s drops; above roughly 5 KB the
curves are untested. Outside 0.5–5 KB the page warns that the number is extrapolated.

## Data baked in

Six connectors, one blessed run each. Connect median MiB/s at 1 / 2 / 4 / 8 vCPU, all on
a `c8g.4xlarge` runner:

| Connector | 1 | 2 | 4 | 8 | Shape | Blessed run | git sha |
|---|---|---|---|---|---|---|---|
| postgres_cdc | 51 | 83 | 102 | 102 | plateaus at 4 | `postgres/orders-cdc/2026-06-01T20-55-50Z` | 25057d693 |
| mysql_cdc | 70 | 102 | 108 | 111 | plateaus at 2 | `mysql/orders-cdc/2026-06-02T14-13-52Z` | 25057d693 |
| mongodb_cdc | 26 | 33 | 33 | 33 | single-cursor ceiling | `mongodb/orders-cdc/2026-07-17T17-11-10Z` | 156a11081 |
| dynamodb_cdc | 40 | 72 | 81 | 82 | source-bound past 4 | `dynamodb/cdc/2026-06-15T17-44-43Z` | d9d2b3c98 |
| oracledb_cdc | 13 | 13 | 13 | 13 | per-reader ceiling | `oracle/orders-cdc/2026-06-22T16-31-22Z` | 63ea466c5 |
| iceberg (sink) | 34 | 65 | 97 | 128 | scales; ~2.5 GB heap | `iceberg/orders-sink-recipe-b/2026-07-09T15-57-38Z` | 62f50196b |

Row size is 1200 B for all except dynamodb (4096 B).

### Exclusions

- **kinesis** — the only runs are GOMAXPROCS tuning A/Bs at 1–2 vCPU, not throughput
  sweeps. Publishing a curve from them would fabricate data.
- **`iceberg/orders-sink`** (the non-`recipe-b` family) — its 8-vCPU point reads 241, 78,
  49, and 128 MiB/s across four runs. Not reproducible. `recipe-b` is blessed instead and
  its displayed confidence is capped.

## Processing tax

The blocking honesty problem: no bench arm in `results/` includes a heavy Bloblang
mapping or schema-registry encode/decode. Every measured number is near-passthrough. A
rep asking about "50k events/sec with Avro and a mapping" cannot be answered from
measured data.

The page ships a dropdown whose multipliers are marked as unmeasured estimates,
visually distinct from measured numbers:

| Setting | Multiplier | Basis |
|---|---|---|
| Passthrough | 1.0x | measured |
| Light mapping (field renames, filters, JSON in/out) | 1.2x | **estimate** |
| Heavy mapping + schema registry (Avro/Protobuf codec, nested restructuring) | 2.0x | **estimate** |

**Follow-up that retires the estimates:** add mapping and schema-registry arms to
`benchmarking/aws/scenarios/postgres/orders-cdc` — postgres is the cheapest stack to
stand up and its curve already plateaus, so the tax shows cleanly. Three arms:
passthrough (control), a representative `mapping` processor, and Avro with schema
registry. Replace the multipliers with measured ratios and drop the estimate badges.

## Coverage boundary

Connectors outside the six get a hard "not benchmarked — ask the perf team" state with no
number. No analogues, no generic floor. The tool's value is that a rep can defend every
number in it; a plausible-looking guess destroys that for the whole page.

## Output

Headline is the licensable core count. Below it:

- Provenance for the connector in play: run date and git SHA.
- A "measured" or "estimate" badge on every contributing number.
- A note that cores are counted off the instance's CPU, not `GOMAXPROCS`, so raising
  `GOMAXPROCS` for I/O-bound work does not change the quote.
- The ceiling warning where one applies.

Memory footprint is shown where it is load-bearing — iceberg peaks near 2.5 GB heap,
which affects instance choice independently of cores.

## Out of scope for v1

- Redpanda cluster sizing, broker count, partition counts.
- Redpanda Cloud unit conversion. Self-hosted core licensing only.
- Instance-type shopping lists and cost modelling.
- Kafka Connect comparison figures. The data exists for postgres, mysql, mongodb and
  iceberg and is compelling, but the ask is a core count.
- Charts. The table plus the flat-curve refusal message conveys the plateau.

## Acceptance cases

Verified by hand in the browser:

1. postgres_cdc, 40k events/sec at 1200 B (= 45.8 MiB/s), passthrough, 30% headroom
   → target 59.5 MiB/s → clears at 2 vCPU (83) → **2 cores**.
2. Same but heavy mapping + SR (2.0x) → effective curve 25.5/41.5/51/51 → target
   59.5 MiB/s exceeds every point → refuses with "exceeds measured ceiling at 8 vCPU".
3. oracledb_cdc, any target above ~13 MiB/s → per-reader ceiling message, no larger core
   count offered, points at more readers.
4. mongodb_cdc, target 40 MiB/s → single-cursor ceiling message naming sharding.
5. Any connector at 200 B events → extrapolation warning shown.
6. snowflake (or any unlisted connector) → "not benchmarked" state, no number rendered.
