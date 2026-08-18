# Compute sizing page

Self-serve tool: pick an input and an output, state the volume, set a processing-tax
setting and a headroom percentage; get back a licensable core count.

The tool sizes **pipelines**, not bare connectors. Most measured entries pair one side
with a Redpanda topic (a CDC source producing into it, or a sink draining it); one pair —
Oracle CDC → SQL Server (`sql_insert`) — was measured end to end with no broker in the
middle. Picking a pair nobody ran (say `postgres_cdc` → Snowflake) gets a refusal that
names whichever halves WERE measured; the tool never composes two half-curves into a
pipeline number, because chaining through a topic is a deployment shape, not a
measurement.

Volume can be typed either way a customer states it. **Events/sec and throughput are two
views of one quantity and stay linked** — edit either and the other follows, with the
average event size as the anchor between them (the size is never rewritten). Throughput
takes MB/s, MiB/s, GB/day or TB/day; switching the unit restates the same rate. Because the
size is always known, the event-size sanity check always runs. It exists so a rep
or SE can answer "how many cores do I need" live, on a call, without paging the perf
team or reading `benchmarking/aws/results/` (45+ timestamped JSON files, most of them
smokes).

Design spec (why every decision below was made, including why median over p5 and why
no charts): `docs/superpowers/specs/2026-08-06-sales-compute-sizing-tool-design.md`.

Published URL: https://claude.ai/code/artifact/febb2295-eb57-4115-b2ab-94c1717f761e
(private until shared from the page's share menu). To update it, republish this same
file path and pass that URL as `url` — a fresh conversation that skips the `url` mints a
new link, which strands anyone who bookmarked the old one.

## 2026-08-18 data refresh — what changed and why

- **Oracle's vCPU curve went 13-flat → 19-flat.** Every oracle run before 2026-08-05 is
  void: its load generator was self-throttling (a Ticker gating a blocking insert), so the
  old curve measured the generator. 19 MB/s is the saturated single-reader figure from the
  reader run; the page's always-on caveat says a reader keeping up in real time sustains
  7-12 MB/s and that cores were measured irrelevant.
- **SQL Server CDC added** from the collated 2026-08-10..11 sweep. Window MEANS, not
  medians — the capture is bursty (duty cycle 44-86%) and medians read ~0 between bursts.
  2 vCPU is the sweet spot; the 8 vCPU dip is an artifact window.
- **Snowflake sink added** from the 2026-08-18 sweep (post-unit-fix, so decimal MB/s).
  The curve requires the benchmarked memory-buffer recipe — without it throughput
  collapses to 0.04 MB/s (measured) because input acks chain to Snowpipe commit latency.
  Plateau ~66-71 MB/s from 2 vCPU up is the commit path, not compute.
- **Oracle → SQL Server end-to-end added** (2026-08-18): flat ~6.4 MB/s at any core count,
  ~0.12 cores used — size it at 1 core.

## Oracle scales with readers, not cores

Oracle is the one connector where the sizing dimension is not vCPU. A LogMiner reader is an
`oracledb_cdc` input with its own disjoint table list, so **readers partition tables**. The
page therefore shows a reader-count control for Oracle only, sizes off the measured reader
curve, and always displays that curve with its caveats.

Measured on `scenarios/oracle/orders-5table-readers` — the same 5-table, 36.7 MB/s workload
redistributed across N readers at a fixed 4 vCPU:

| Readers | Measured | vs 1 reader |
|---|---|---|
| 1 | 19 MB/s | 1.00x |
| 2 | 25 MB/s | 1.32x |
| 5 | 29 MB/s | 1.53x |

Three things the page states and you should not let a quote drop:

1. **A single hot table cannot be split.** Readers partition tables, so if the writes sit in
   one table, no reader count and no core count beats the 13 MB/s from the single-table
   `orders-cdc` run. That is why the vCPU table (13, one table) and the reader table (19 at
   one reader, five tables) disagree — they are different workloads, and the page says so.
2. **Scaling is sublinear and never caught the offered load.** 5x the readers bought 1.53x,
   topping out at 29-30 MB/s against a 36.7 MB/s write rate. A per-database ceiling near
   31 MB/s is the likely explanation, so treat ~30 MB/s as the practical Oracle ceiling.
3. **Only 1, 2 and 5 readers were run.** The select offers exactly those. Asking for 3 or 4
   returns a refusal rather than an interpolated number, because nothing measured them.

Two runs on different source instances (`db.r5.2xlarge` and `db.r5.4xlarge`) agreed within
1 MB/s, which is what rules out source I/O as the constraint. Both are cited on the page.

Adding reader scaling to another connector means giving it a `readerScaling` block of the
same shape; `sizeFor` picks up the reader path from its presence alone.

## The hard boundary

Only nine pipelines have a number: `postgres_cdc`, `mysql_cdc`, `mongodb_cdc`,
`sqlserver_cdc`, `snowflake_sink`, `oracle_to_sqlserver`,
`dynamodb_cdc`, `oracledb_cdc`, and the `iceberg` sink. Anything else — including
`kinesis`, which has tuning runs but no throughput sweep — renders "not benchmarked,
ask the perf team" and no number. This is deliberate: every figure on the page must be
defensible back to a specific run, and one plausible-looking guess for an unbenched
pipeline would destroy that guarantee for all nine real ones. Do not add a tenth
row by analogy or interpolation.

## Testing

```
node --test benchmarking/aws/sizing/sizing.test.mjs
```

36 tests cover the calculation core: unit conversion in both directions, rate/throughput
round-tripping without drift across all four input units, the "smallest clearing point" rule, headroom semantics, ceiling
refusals, the no-answer guard on blank/negative/non-finite input, event-size caveats, and
per-connector provenance. Run them after any change to the data or the calculation.

Two are worth knowing about specifically:

- A test asserting that a **MiB/s-stated volume is not treated as MB/s**. Against a
  decimal-MB curve, 100 MiB/s is 104.86 MB/s — enough to flip a refusal into a core count.
- A **bridge-integrity test**. The page is two module scripts joined by a
  `window.__sizingCore = { ... }` line. Every other test imports the fenced core directly,
  so a stale name in that bridge throws at load, blanks the entire UI, and leaves the suite
  green. That happened once; this test checks both sides of the bridge against the file.

One of them is a **literal data snapshot** pinning all 36 curve points, the SHAs, run
paths, dates, peak-heap figures and `benchedEventBytes`, the event-size constants, and the
`measured` flag on all three tax settings. It exists because these numbers go straight into
customer quotes and the earlier `typeof`-only checks let a transposed digit through. The
benchmark JSONs under `benchmarking/aws/results/` are gitignored, so the test cannot
re-derive the values from them — if you change the data on purpose, update the snapshot in
the same commit.

This is deliberately **not** wired into CI. The page is a standalone sales artifact,
not a build target — there's no pipeline that would run it, and adding one is not
worth the maintenance for a six-row dataset that changes a few times a year.

## Why the logic is fenced by sentinel comments

`index.html` must stay a single self-contained file: the published Artifact's CSP
blocks external `<script src>`, so there's no separate JS file to import. The
calculation logic (`CONNECTORS`, `TAX`, `sizeFor`, etc.) sits between
`// <sizing-core>` and `// </sizing-core>` inside the page's first `<script type="module">`.

`sizing.test.mjs` reads `index.html`, slices out everything between those two
sentinels, and imports it as a `data:` URL module. That's the entire trick: one source
of truth, no build step, and the tests exercise the exact bytes that ship, not a copy
kept in sync by hand.

Consequences:
- The fenced region must stay a side-effect-free ES module — no DOM access, no
  `window`, no top-level code that isn't `export`. The UI script below the fence reads
  it via `window.__sizingCore` (see the bridge line right after `</sizing-core>`).
- Moving calculation logic out of the fence, or moving DOM/UI code into it, breaks
  `loadCore()` in the test file (either the export the test wants disappears, or the
  module throws on import because it touches `window`).
- If you ever need a second sentinel pair or rename these, update the split logic in
  `sizing.test.mjs`'s `loadCore()` at the same time.

## Adding a connector once a new bench lands

1. **Pick one blessed run. Do not glob `benchmarking/aws/results/`.** Most files
   there are smokes or broken runs reading 0; averaging across whatever's in a
   directory will silently poison the number. Find the run yourself, confirm it looks
   healthy (non-zero, no crossed-out anomalies), and pick exactly one JSON file the
   same way the six existing rows were picked — see the spec's "Exclusions" section
   for the kind of run to reject (e.g. `iceberg/orders-sink`'s non-`recipe-b` family,
   whose 8-vCPU point read 241/78/49/128 MiB/s across four runs — not reproducible).

2. **Read the curve.** Each result file is `{ scenario, git_sha, points: [...] }`,
   where `points` has one entry per `(vcpu, engine)` pair. For each of the 1/2/4/8
   vCPU points where `engine == "connect"`, take `point.summary.median_mb_s`.
   Use the field as-is — never re-derive it from `msg_per_sec x bytes`.

   **Then set `curveUnit`, and get it right — a wrong tag is a silent 4.86% error.**
   Which unit `median_mb_s` holds depends on how the point was summarised
   (`runner/matrix.go:288`):

   | How the point was summarised | Unit | `curveUnit` |
   |---|---|---|
   | Connect arm of a **source** scenario — from Connect's own `rolling stats:` log, which is SI decimal (`humanize.Bytes`) | MB/s | `'MB'` |
   | **Sink** scenario, or **any `kafka_connect`** arm — from broker/Iceberg byte counters | see below | see below |

   The broker-side samplers used to divide by `(1 << 20)`, storing MiB/s in a field
   named MB. That is now fixed — `runner/stats.go` defines a single `bytesPerMB =
   1_000_000` and every sampler uses it — so **any run produced after that fix is
   `'MB'`**. Runs from before it keep MiB/s on their sink and `kafka_connect` points,
   which is why `iceberg_sink` is still tagged `'MiB'` here. If you are unsure which
   side of the fix a run falls on, check whether its `git_sha` predates the commit that
   introduced `bytesPerMB`.

   The page converts a rep's input into each connector's own `curveUnit` before
   comparing, so tagging correctly is all that is required — do not restate a curve in
   a different unit, because that would break its byte-for-byte match with the run file.

3. **Read the peak heap.** `peakHeapMB` is the *worst case across the whole run*, not
   just at the point you'll size against: for every `connect`-engine point in the
   run, take the max `heap_in_use_mb` in that point's `prom` array, then take the max
   of those per-vcpu maxima. (Concretely: `point.prom` is a time series of
   `{ t, heap_in_use_mb, ... }` samples; `max(p.heap_in_use_mb for p in point.prom)`
   per point, then `max` across the connect points.) Example: postgres_cdc's 1-vCPU
   point peaks at 118.6 MB while its 8-vCPU point only peaks at 102.1 MB — the stored
   `peakHeapMB: 118` is the larger of the two, because a customer could land on either.

4. **Add the `CONNECTORS` entry** in `index.html` (inside the sentinels), matching the
   shape of the existing six: `label`, `curve` (all four vCPU points), `benchedEventBytes`
   (the average event size the run used — the page warns when a user's event size is more
   than `EVENT_BYTES_DEVIATION_FACTOR` away from it in either direction, so it must be the
   real figure), `peakHeapMB`, `run: { path, date, sha }` (the run's relative path under
   `results/` **including the `.json` extension**, so it can be pasted straight into an
   editor, plus its date and the git SHA the run was taken at — `git_sha` is in the result
   JSON), `ceiling` (`null` if the curve keeps scaling, or `{ reason, fix }` if it
   plateaus — see `oracledb_cdc`/`mongodb_cdc` for the shape), `sourceKind` (`'cdc'` or
   `'sink'`, which selects the honest scale-out caveat in `SCALE_OUT_UNMEASURED` when
   `ceiling` is `null`), and `confidence` (`'high'` unless the run has a reproducibility
   caveat, in which case also set `confidenceNote`, as `iceberg_sink` does).

5. **Update the data snapshot test and add a behaviour test** in `sizing.test.mjs`. The
   snapshot ("data snapshot: curves, provenance, …") must gain a literal block for the new
   connector, or it will fail. Then add at minimum an assertion that `sizeFor` picks the
   right core count for a known target, mirroring the existing per-connector acceptance
   tests. Run `node --test benchmarking/aws/sizing/sizing.test.mjs` and confirm all tests
   pass, including the "six blessed connectors" list test, which you'll need to update to
   seven.

6. **Republish the Artifact** (same file, same URL — see the note in the Artifact
   tool's own docs about updating in place rather than minting a new URL) and update
   the "Published URL" line above if it's ever changed.

## Retiring the estimated processing tax

The `light` (1.2x) and `heavy` (2.0x) multipliers in `TAX` are **judgement, not
measurement**. No arm in `benchmarking/aws/results/` runs a heavy Bloblang mapping or
a schema-registry Avro/Protobuf encode/decode — every measured curve on this page is
near-passthrough. So "50k events/sec with Avro and a mapping" is answered today with
an estimate, badged as such in the UI (`taxMeasured: false`, the "Estimate" pill), not
with data.

The follow-up that fixes this, per the spec: add three arms to
`benchmarking/aws/scenarios/postgres/orders-cdc` (postgres is the cheapest stack to
stand up and its curve already plateaus, so the tax shows cleanly against a flat
baseline):

1. passthrough — control, should reproduce the existing curve
2. a representative `mapping` processor — renames/filters, JSON in/out
3. Avro with schema registry — encode or decode through the registry

Once those runs land, replace `TAX.light.multiplier` and `TAX.heavy.multiplier` with the
measured ratios and flip `measured: true`. The "Estimate" badge and note then disappear on
their own — both are keyed off `TAX[tax].measured` in `index.html`, so no other code
changes are needed.

**Get the ratio the right way round.** The multiplier *divides* the curve
(`effective = curve / multiplier` in `sizeFor`), so it expresses how many times *more*
compute the mapping needs, and it must be **greater than or equal to 1** for anything
slower than passthrough:

```
multiplier = passthrough median  ÷  mapped-arm median      (both in the same unit)
```

Concretely: passthrough measures 100 MB/s, the Avro arm measures 50 MB/s → heavy becomes
100 ÷ 50 = **2.0**. The inverse (50 ÷ 100 = 0.5) would make the page claim heavy mapping
runs *twice as fast* as passthrough. If you ever compute a multiplier below 1.0 for a
mapped arm, you have the division backwards. Sanity check after editing: for identical
inputs, heavy mapping must never return a smaller core count than passthrough.

Until that run exists, say so plainly to anyone asking: light and heavy mapping costs
on this page are estimates, not measurements.

## States the result card can render

| Status | Renders | Core count |
|---|---|---|
| `ok` | headline core count, measured rate at that point in the connector's own unit, peak heap, provenance, Measured/Estimate badge | yes |
| `ceiling` | refusal, the requirement, the **measured maximum** and (above 1.0x tax) the derated figure labelled as calculated, the limit box, provenance, badge | no |
| `unbenchmarked` | "not benchmarked — ask the perf team" | no |
| `no-input` | neutral "enter an event rate and an average event size" prompt, no badge, no provenance | no |

`no-input` exists because `Number('')` is `0`: an emptied events field used to size at
1 vCPU and render a **Measured** badge over a number the user never asked for. The guard is
`hasSizeableInput` inside the fence (so it is testable), not in the UI.

On the `ceiling` path, note the two distinct fields: `measuredCeilingRate` is a real curve
reading and `ceilingRate` is that reading divided by the processing multiplier. Only the
first may ever be called "measured" in copy; at 1.0x they are equal.

## Known gap (not this task's to fix)

`sizeFor`'s `ceiling` path returns no number for postgres, mysql, and iceberg once a
target exceeds the single-instance curve, because scaling across multiple Connect
instances was never benchmarked. A customer asking for ~500 MB/s of Postgres CDC is
realistic and the tool will decline it rather than multiply the single-instance curve
by an instance count (that would be an invented number). For the CDC sources it is not
even a given that a second instance would help — each needs its own replication slot or
log reader and may duplicate the stream rather than divide it, which is why
`SCALE_OUT_UNMEASURED.cdc` says so rather than implying a ready remedy. Worth deciding
deliberately whether to bench a two-instance arm before this comes up live on a call.
