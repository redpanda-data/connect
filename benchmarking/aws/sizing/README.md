# Compute sizing page

Self-serve tool: enter events/sec, average event size, a connector, a processing-tax
setting and a headroom percentage; get back a licensable core count. It exists so a rep
or SE can answer "how many cores do I need" live, on a call, without paging the perf
team or reading `benchmarking/aws/results/` (45+ timestamped JSON files, most of them
smokes).

Design spec (why every decision below was made, including why median over p5 and why
no charts): `docs/superpowers/specs/2026-08-06-sales-compute-sizing-tool-design.md`.

Published URL: recorded on first publish.

## The hard boundary

Only six connectors have a number: `postgres_cdc`, `mysql_cdc`, `mongodb_cdc`,
`dynamodb_cdc`, `oracledb_cdc`, and the `iceberg` sink. Anything else — including
`kinesis`, which has tuning runs but no throughput sweep — renders "not benchmarked,
ask the perf team" and no number. This is deliberate: every figure on the page must be
defensible back to a specific run, and one plausible-looking guess for an unbenched
connector would destroy that guarantee for all six real ones. Do not add a seventh
row by analogy or interpolation.

## Testing

```
node --test benchmarking/aws/sizing/sizing.test.mjs
```

15 tests cover the calculation core: unit conversion, the "smallest clearing point"
rule, headroom semantics, ceiling refusals, and per-connector provenance. Run them
after any change to the data or the calculation.

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
   there are smokes or broken runs reading 0 MiB/s; averaging across whatever's in a
   directory will silently poison the number. Find the run yourself, confirm it looks
   healthy (non-zero, no crossed-out anomalies), and pick exactly one JSON file the
   same way the six existing rows were picked — see the spec's "Exclusions" section
   for the kind of run to reject (e.g. `iceberg/orders-sink`'s non-`recipe-b` family,
   whose 8-vCPU point read 241/78/49/128 MiB/s across four runs — not reproducible).

2. **Read the curve.** Each result file is `{ scenario, git_sha, points: [...] }`,
   where `points` has one entry per `(vcpu, engine)` pair. For each of the 1/2/4/8
   vCPU points where `engine == "connect"`, take `point.summary.median_mb_s`.
   **The field is named `mb_per_sec`/`median_mb_s` but the value is MiB/s**, not
   MB/s — `benchmarking/aws/runner/brokermetrics.go:145` computes
   `deltaBytes / interval / (1 << 20)`. Don't re-derive it from `msg_per_sec x
   bytes` with `1e6`; use the field as-is and label it MiB/s.

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
   (the average event size the run used), `peakHeapMB`, `run: { path, date, sha }` (the
   run's relative path under `results/`, its date, and the git SHA the run was taken
   at — `git_sha` is in the result JSON), `ceiling` (`null` if the curve keeps scaling,
   or `{ mibps, reason, fix }` if it plateaus — see `oracledb_cdc`/`mongodb_cdc` for the
   shape), and `confidence` (`'high'` unless the run has a reproducibility caveat, in
   which case also set `confidenceNote`, as `iceberg_sink` does).

5. **Add a test** in `sizing.test.mjs` asserting the new curve — at minimum, that
   `sizeFor` picks the right core count for a known target, mirroring the existing
   per-connector acceptance tests. Run `node --test benchmarking/aws/sizing/sizing.test.mjs`
   and confirm all tests pass, including the "six blessed connectors" list test, which
   you'll need to update to seven.

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

Once those runs land, replace `TAX.light.multiplier` and `TAX.heavy.multiplier` with
the measured ratios (light/heavy median MiB/s divided by passthrough median MiB/s),
flip `measured: true`, and drop the "Estimate" badge and note from the UI — both are
keyed off `TAX[tax].measured` in `index.html`, so no other code changes are needed.

Until that run exists, say so plainly to anyone asking: light and heavy mapping costs
on this page are estimates, not measurements.

## Known gap (not this task's to fix)

`sizeFor`'s `ceiling` path returns no number for postgres, mysql, and iceberg once a
target exceeds the single-instance curve, because scaling across multiple Connect
instances was never benchmarked. A customer asking for ~500 MiB/s of Postgres CDC is
realistic and the tool will decline it rather than multiply the single-instance curve
by an instance count (that would be an invented number). Worth deciding deliberately
whether to bench a two-instance arm before this comes up live on a call.
