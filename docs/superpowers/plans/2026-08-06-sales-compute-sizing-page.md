# Sales Compute Sizing Page Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a single self-contained HTML page that turns a rep's events/sec into a defensible licensable core count for the six benchmarked connectors, refusing to answer where the data does not support one.

**Architecture:** One file, `benchmarking/aws/sizing/index.html`. Its pure sizing logic and data table live in an ES module `<script>` fenced by `// <sizing-core>` / `// </sizing-core>` sentinel comments. A Node test file extracts that fenced source and imports it as a `data:` URL module, so the logic is unit-tested without a build step and without a second copy of the source. The page is published as an Artifact; the Artifact CSP forbids external scripts, which is why everything is inline.

**Tech Stack:** Plain HTML + vanilla ES modules (no framework, no dependencies). Tests via `node --test` (Node 24 verified present; `data:` URL module import requires Node 20+). No CI wiring.

## Global Constraints

- Every throughput figure is **MiB/s**, computed as `bytes / (1 << 20)`, matching `benchmarking/aws/runner/brokermetrics.go:145`. Never use `1e6`. Label the unit `MiB/s` in all UI copy.
- Sizing basis is the **median** of the blessed run, never p5. Rationale is in the spec; do not "improve" this.
- Any number not derived from a blessed run must be visibly badged as an estimate. No unbadged guesses.
- Connectors outside the six render **no number at all** — no analogues, no generic floor.
- The six blessed runs, their SHAs, and their curves are fixed by the spec table. Do not re-derive them from `results/`; a majority of files there are smokes reading 0 MiB/s.
- Single file only. No external scripts, stylesheets, fonts, or images — the Artifact CSP blocks them.
- Do not add these tests to CI.

**Spec:** `docs/superpowers/specs/2026-08-06-sales-compute-sizing-tool-design.md`

## File Structure

- `benchmarking/aws/sizing/index.html` — the whole tool. Sentinel-fenced core (data + pure functions), then the UI script and markup. Created in Task 1, extended in Tasks 2 and 3.
- `benchmarking/aws/sizing/sizing.test.mjs` — extracts and tests the fenced core. Created in Task 1, extended in Task 2.
- `benchmarking/aws/sizing/README.md` — how to run tests, how to republish, how to add a connector when a bench lands. Created in Task 4.

---

### Task 1: Core sizing calculation

**Files:**
- Create: `benchmarking/aws/sizing/index.html`
- Test: `benchmarking/aws/sizing/sizing.test.mjs`

**Interfaces:**
- Consumes: nothing.
- Produces: `VCPU_POINTS: number[]`, `MIB: number`, `TAX: Record<string, {label: string, multiplier: number, measured: boolean}>`, `CONNECTORS: Record<string, Connector>`, `targetMiBps(eventsPerSec: number, eventBytes: number): number`, `sizeFor(opts): Result`.
  - `Connector = {label, curve: {1,2,4,8 → number}, benchedEventBytes, peakHeapMB, run: {path, date, sha}, ceiling: null | {mibps, reason, fix}, confidence: 'high'|'medium', confidenceNote?: string}`
  - `sizeFor({connector, eventsPerSec, eventBytes, tax, headroomPct})` returns one of:
    - `{status: 'ok', connector, cores, target, required, headroomPct, measuredMiBps, effectiveMiBps, peakHeapMB, run, confidence, taxMeasured, warnings}`
    - `{status: 'ceiling', connector, target, required, ceilingMiBps, ceiling, run, warnings}`
    - `{status: 'unbenchmarked', connector}`

- [ ] **Step 1: Write the failing test**

Create `benchmarking/aws/sizing/sizing.test.mjs`:

```js
import { readFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import test from 'node:test'
import assert from 'node:assert/strict'

const here = dirname(fileURLToPath(import.meta.url))

// The page must stay a single self-contained file (Artifact CSP blocks external
// scripts), so the pure logic is fenced by sentinel comments and imported here
// as a data: URL module. One source of truth, no build step.
async function loadCore() {
  const html = await readFile(join(here, 'index.html'), 'utf8')
  const parts = html.split('// <sizing-core>')
  assert.equal(parts.length, 2, 'index.html must contain exactly one // <sizing-core> sentinel')
  const src = parts[1].split('// </sizing-core>')[0]
  const b64 = Buffer.from(src).toString('base64')
  return import(`data:text/javascript;base64,${b64}`)
}

const core = await loadCore()

test('targetMiBps converts with 2^20, not 1e6', () => {
  // 40k events/sec at 1200 B is 48,000,000 B/s. In MiB/s that is 45.78, not 48.
  const t = core.targetMiBps(40_000, 1200)
  assert.ok(Math.abs(t - 45.776) < 0.01, `expected ~45.776 MiB/s, got ${t}`)
})

test('acceptance case 1: postgres 40k/s at 1200 B passthrough clears at 2 cores', () => {
  const r = core.sizeFor({
    connector: 'postgres_cdc', eventsPerSec: 40_000, eventBytes: 1200,
    tax: 'passthrough', headroomPct: 30,
  })
  assert.equal(r.status, 'ok')
  assert.equal(r.cores, 2)
  assert.equal(r.measuredMiBps, 83)
  assert.ok(Math.abs(r.required - 59.51) < 0.05, `required was ${r.required}`)
})

test('picks the smallest clearing point, not the biggest', () => {
  // mysql curve is 70/102/108/111; a 60 MiB/s requirement must not return 8.
  const r = core.sizeFor({
    connector: 'mysql_cdc', eventsPerSec: 40_000, eventBytes: 1200,
    tax: 'passthrough', headroomPct: 30,
  })
  assert.equal(r.status, 'ok')
  assert.equal(r.cores, 1)
})

test('headroom inflates the requirement, not the curve', () => {
  const low = core.sizeFor({ connector: 'postgres_cdc', eventsPerSec: 60_000, eventBytes: 1200, tax: 'passthrough', headroomPct: 0 })
  const high = core.sizeFor({ connector: 'postgres_cdc', eventsPerSec: 60_000, eventBytes: 1200, tax: 'passthrough', headroomPct: 50 })
  // 60k/s at 1200 B = 68.66 MiB/s. At 0% headroom the 83 point clears it (2 vCPU).
  // At 50% the requirement becomes 103 MiB/s, above the curve's 102 maximum, so
  // nothing clears — proving headroom moves the requirement rather than the curve.
  assert.equal(low.cores, 2)
  assert.equal(low.measuredMiBps, 83)
  assert.equal(high.status, 'ceiling')
})

test('every connector reports provenance and a peak heap figure', () => {
  for (const [key, c] of Object.entries(core.CONNECTORS)) {
    assert.ok(c.run.sha, `${key} missing git sha`)
    assert.ok(c.run.date, `${key} missing run date`)
    assert.ok(c.run.path, `${key} missing run path`)
    assert.ok(c.peakHeapMB > 0, `${key} missing peak heap`)
    for (const v of core.VCPU_POINTS) {
      assert.equal(typeof c.curve[v], 'number', `${key} missing curve point at ${v} vCPU`)
    }
  }
})

test('the six blessed connectors are present and kinesis is not', () => {
  assert.deepEqual(Object.keys(core.CONNECTORS).sort(), [
    'dynamodb_cdc', 'iceberg_sink', 'mongodb_cdc', 'mysql_cdc', 'oracledb_cdc', 'postgres_cdc',
  ])
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `node --test benchmarking/aws/sizing/sizing.test.mjs`
Expected: FAIL — `ENOENT` on `index.html`, since the page does not exist yet.

- [ ] **Step 3: Write the minimal implementation**

Create `benchmarking/aws/sizing/index.html` with only the fenced core for now (the UI arrives in Task 3):

```html
<title>Redpanda Connect — Compute Sizing</title>
<script type="module">
// <sizing-core>
// Sizing core. Every figure below comes from one blessed benchmark run; see
// docs/superpowers/specs/2026-08-06-sales-compute-sizing-tool-design.md for why
// each run was chosen and which runs were deliberately excluded.
//
// UNITS: the runner's `mb_per_sec` field is actually MiB/s — brokermetrics.go:145
// divides by (1 << 20). Curves and targets are both MiB/s. Never use 1e6.

export const MIB = 1024 * 1024
export const VCPU_POINTS = [1, 2, 4, 8]

export const EVENT_BYTES_MIN = 512
export const EVENT_BYTES_MAX = 5120

export const TAX = {
  passthrough: { label: 'Passthrough — no mapping', multiplier: 1.0, measured: true },
  light: { label: 'Light mapping — renames, filters, JSON in/out', multiplier: 1.2, measured: false },
  heavy: { label: 'Heavy mapping + schema registry — Avro/Protobuf codec', multiplier: 2.0, measured: false },
}

// Connect median MiB/s at 1/2/4/8 vCPU, all measured on a c8g.4xlarge runner.
export const CONNECTORS = {
  postgres_cdc: {
    label: 'PostgreSQL CDC (postgres_cdc)',
    curve: { 1: 51, 2: 83, 4: 102, 8: 102 },
    benchedEventBytes: 1200,
    peakHeapMB: 118,
    run: { path: 'postgres/orders-cdc/2026-06-01T20-55-50Z', date: '2026-06-01', sha: '25057d693' },
    ceiling: null,
    confidence: 'high',
  },
  mysql_cdc: {
    label: 'MySQL CDC (mysql_cdc)',
    curve: { 1: 70, 2: 102, 4: 108, 8: 111 },
    benchedEventBytes: 1200,
    peakHeapMB: 378,
    run: { path: 'mysql/orders-cdc/2026-06-02T14-13-52Z', date: '2026-06-02', sha: '25057d693' },
    ceiling: null,
    confidence: 'high',
  },
  mongodb_cdc: {
    label: 'MongoDB CDC (mongodb_cdc)',
    curve: { 1: 26, 2: 33, 4: 33, 8: 33 },
    benchedEventBytes: 1200,
    peakHeapMB: 157,
    run: { path: 'mongodb/orders-cdc/2026-07-17T17-11-10Z', date: '2026-07-17', sha: '156a11081' },
    ceiling: {
      mibps: 33,
      reason: 'a single change-stream cursor, which one replica set gives you regardless of vCPU',
      fix: 'Shard the collection — each shard adds a cursor. More cores will not move this number.',
    },
    confidence: 'high',
  },
  dynamodb_cdc: {
    label: 'DynamoDB CDC (aws_dynamodb_cdc)',
    curve: { 1: 40, 2: 72, 4: 81, 8: 82 },
    benchedEventBytes: 4096,
    peakHeapMB: 936,
    run: { path: 'dynamodb/cdc/2026-06-15T17-44-43Z', date: '2026-06-15', sha: 'd9d2b3c98' },
    ceiling: {
      mibps: 82,
      reason: 'the source, not Connect — the run hit the test account’s write-capacity limit past 4 vCPU',
      fix: 'Spread the load across more tables or streams. The 8 vCPU figure is a floor, not a Connect limit.',
    },
    confidence: 'high',
  },
  oracledb_cdc: {
    label: 'Oracle CDC (oracle_cdc)',
    curve: { 1: 13, 2: 13, 4: 13, 8: 13 },
    benchedEventBytes: 1200,
    peakHeapMB: 190,
    run: { path: 'oracle/orders-cdc/2026-06-22T16-31-22Z', date: '2026-06-22', sha: '63ea466c5' },
    ceiling: {
      mibps: 13,
      reason: 'one LogMiner reader — the ceiling is per reader, and vCPU count is irrelevant to it',
      fix: 'Add readers. Measured 19, then 25, then 30 MiB/s at a fixed 4 vCPU as readers were added.',
    },
    confidence: 'high',
  },
  iceberg_sink: {
    label: 'Iceberg sink (iceberg output)',
    curve: { 1: 34, 2: 65, 4: 97, 8: 128 },
    benchedEventBytes: 1200,
    peakHeapMB: 2681,
    run: { path: 'iceberg/orders-sink-recipe-b/2026-07-09T15-57-38Z', date: '2026-07-09', sha: '62f50196b' },
    ceiling: null,
    confidence: 'medium',
    confidenceNote: 'Medium confidence: a sibling Iceberg scenario produced 241, 78, 49 and 128 MiB/s at 8 vCPU across four runs. This blessed run is the well-behaved one; treat the 8 vCPU point as indicative.',
  },
}

export function targetMiBps(eventsPerSec, eventBytes) {
  return (eventsPerSec * eventBytes) / MIB
}

export function warningsFor(c, eventBytes) {
  const out = []
  if (eventBytes < EVENT_BYTES_MIN || eventBytes > EVENT_BYTES_MAX) {
    out.push(
      `Extrapolated. This curve was measured at ${c.benchedEventBytes} B events; outside ` +
      `${EVENT_BYTES_MIN}–${EVENT_BYTES_MAX} B no measurement backs it. Smaller events shift the ` +
      `cost to per-message overhead, so achievable MiB/s drops.`
    )
  }
  if (c.confidence !== 'high' && c.confidenceNote) out.push(c.confidenceNote)
  return out
}

export function sizeFor({ connector, eventsPerSec, eventBytes, tax = 'passthrough', headroomPct = 30 }) {
  const c = CONNECTORS[connector]
  if (!c) return { status: 'unbenchmarked', connector }

  const multiplier = TAX[tax].multiplier
  const target = targetMiBps(eventsPerSec, eventBytes)
  const required = target * (1 + headroomPct / 100)
  const warnings = warningsFor(c, eventBytes)

  const effective = VCPU_POINTS.map((vcpu) => ({ vcpu, mibps: c.curve[vcpu] / multiplier }))
  const fit = effective.find((p) => p.mibps >= required)

  if (!fit) {
    return {
      status: 'ceiling',
      connector, target, required,
      ceilingMiBps: effective[effective.length - 1].mibps,
      ceiling: c.ceiling,
      run: c.run,
      warnings,
    }
  }

  return {
    status: 'ok',
    connector,
    cores: fit.vcpu,
    target, required, headroomPct,
    measuredMiBps: c.curve[fit.vcpu],
    effectiveMiBps: fit.mibps,
    peakHeapMB: c.peakHeapMB,
    run: c.run,
    confidence: c.confidence,
    taxMeasured: TAX[tax].measured,
    warnings,
  }
}
// </sizing-core>
</script>
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `node --test benchmarking/aws/sizing/sizing.test.mjs`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/sizing/index.html benchmarking/aws/sizing/sizing.test.mjs
git commit -m "feat(sizing): core calculation over the six blessed bench runs"
```

---

### Task 2: Refusal paths — ceilings, unbenchmarked connectors, extrapolation

**Files:**
- Modify: `benchmarking/aws/sizing/sizing.test.mjs` (append tests)
- Modify: `benchmarking/aws/sizing/index.html` (only if a test exposes a gap)

**Interfaces:**
- Consumes: `sizeFor`, `CONNECTORS`, `TAX` from Task 1.
- Produces: no new exports. This task proves the Task 1 implementation satisfies acceptance cases 2–6 and hardens whatever it does not.

- [ ] **Step 1: Write the failing tests**

Append to `benchmarking/aws/sizing/sizing.test.mjs`:

```js
test('acceptance case 2: the heavy-mapping tax can push a target past the ceiling', () => {
  // postgres passthrough curve 51/83/102/102; at 2.0x it becomes 25.5/41.5/51/51.
  // A 59.51 MiB/s requirement clears at 2 vCPU passthrough but nothing clears at 2.0x.
  const r = core.sizeFor({
    connector: 'postgres_cdc', eventsPerSec: 40_000, eventBytes: 1200,
    tax: 'heavy', headroomPct: 30,
  })
  assert.equal(r.status, 'ceiling')
  assert.equal(r.ceilingMiBps, 51)
})

test('acceptance case 3: oracle refuses to grow cores and names readers as the fix', () => {
  const r = core.sizeFor({
    connector: 'oracledb_cdc', eventsPerSec: 20_000, eventBytes: 1200,
    tax: 'passthrough', headroomPct: 30,
  })
  assert.equal(r.status, 'ceiling')
  assert.equal(r.ceilingMiBps, 13)
  assert.match(r.ceiling.reason, /LogMiner/)
  assert.match(r.ceiling.fix, /readers/)
  assert.equal(r.cores, undefined, 'a refusal must not carry a core count')
})

test('acceptance case 4: mongo names sharding as the fix', () => {
  const eventsPerSec = Math.round((40 * core.MIB) / 1200) // ≈ 40 MiB/s of target
  const r = core.sizeFor({ connector: 'mongodb_cdc', eventsPerSec, eventBytes: 1200, tax: 'passthrough', headroomPct: 30 })
  assert.equal(r.status, 'ceiling')
  assert.match(r.ceiling.reason, /cursor/)
  assert.match(r.ceiling.fix, /[Ss]hard/)
})

test('acceptance case 5: tiny events raise an extrapolation warning but still size', () => {
  const r = core.sizeFor({ connector: 'postgres_cdc', eventsPerSec: 40_000, eventBytes: 200, tax: 'passthrough', headroomPct: 30 })
  assert.equal(r.status, 'ok')
  assert.equal(r.cores, 1)
  assert.equal(r.warnings.length, 1)
  assert.match(r.warnings[0], /Extrapolated/)
})

test('acceptance case 6: an unlisted connector yields no number at all', () => {
  const r = core.sizeFor({ connector: 'snowflake', eventsPerSec: 40_000, eventBytes: 1200 })
  assert.equal(r.status, 'unbenchmarked')
  assert.equal(r.cores, undefined)
  assert.equal(r.required, undefined)
})

test('an in-range event size raises no warning', () => {
  const r = core.sizeFor({ connector: 'mysql_cdc', eventsPerSec: 10_000, eventBytes: 1200, tax: 'passthrough', headroomPct: 30 })
  assert.deepEqual(r.warnings, [])
})

test('the iceberg curve always carries its reproducibility caveat', () => {
  const r = core.sizeFor({ connector: 'iceberg_sink', eventsPerSec: 20_000, eventBytes: 1200, tax: 'passthrough', headroomPct: 30 })
  assert.equal(r.status, 'ok')
  assert.equal(r.confidence, 'medium')
  assert.ok(r.warnings.some((w) => /Medium confidence/.test(w)))
})

test('estimated tax settings are flagged as unmeasured in the result', () => {
  const passthrough = core.sizeFor({ connector: 'mysql_cdc', eventsPerSec: 10_000, eventBytes: 1200, tax: 'passthrough' })
  const light = core.sizeFor({ connector: 'mysql_cdc', eventsPerSec: 10_000, eventBytes: 1200, tax: 'light' })
  assert.equal(passthrough.taxMeasured, true)
  assert.equal(light.taxMeasured, false)
})

test('dynamodb attributes its plateau to the source, not to Connect', () => {
  const eventsPerSec = Math.round((200 * core.MIB) / 4096)
  const r = core.sizeFor({ connector: 'dynamodb_cdc', eventsPerSec, eventBytes: 4096, tax: 'passthrough', headroomPct: 30 })
  assert.equal(r.status, 'ceiling')
  assert.match(r.ceiling.reason, /source/)
})
```

- [ ] **Step 2: Run the tests**

Run: `node --test benchmarking/aws/sizing/sizing.test.mjs`
Expected: all PASS against the Task 1 implementation. If any fails, fix `index.html` — do not weaken the test to match the code.

- [ ] **Step 3: Commit**

```bash
git add benchmarking/aws/sizing/sizing.test.mjs benchmarking/aws/sizing/index.html
git commit -m "test(sizing): cover ceiling refusals, extrapolation and unbenchmarked connectors"
```

---

### Task 3: The page UI

**Files:**
- Modify: `benchmarking/aws/sizing/index.html` (append UI markup and a render script below the fenced core)

**Interfaces:**
- Consumes: `sizeFor`, `CONNECTORS`, `TAX`, `VCPU_POINTS` from the fenced core in the same file.
- Produces: nothing importable. The fenced core must remain byte-for-byte importable on its own — do not put DOM references inside the sentinels, or Task 1's and Task 2's tests will break.

**Why this task specifies behaviour and copy but not literal markup:** every other task in this plan hands you code to type. This one deliberately does not, because Step 1 requires the `artifact-design` skill to drive the visual design, and pre-written CSS here would just be overridden. What is fixed and not yours to reinterpret: the input set and defaults, the three result states, which fields appear in each, and the exact wording of the refusal and `GOMAXPROCS` strings. How it looks is yours.

- [ ] **Step 1: Load the design skill**

Invoke the `artifact-design` skill before writing any markup or CSS. This page will be published as an Artifact and that skill calibrates the design investment. Do not skip it.

- [ ] **Step 2: Verify the tests still pass after any edit to the file**

Run: `node --test benchmarking/aws/sizing/sizing.test.mjs`
Expected: PASS. Re-run this after every UI edit — the sentinel extraction is the contract that keeps the tested logic and the shipped logic identical.

- [ ] **Step 3: Build the input panel**

Below the fenced core's `</script>`, add markup and a second module script. Inputs, all with visible labels:

- Connector `<select>`, options generated from `CONNECTORS` (use `c.label`), plus a final `<option value="__other">Something else…</option>`.
- Events/sec: `<input type="number" min="1" step="1000">`, default `40000`.
- Average event size in bytes: `<input type="number" min="1">`, default `1200`, with helper text `Benchmarks used 1200 B (4096 B for DynamoDB)`.
- Processing `<select>` over `TAX` keys, showing `label`, and appending ` (estimate)` to the option text where `measured === false`.
- Headroom `<input type="range" min="0" max="100" step="5" value="30">` with its current value rendered next to it as a percentage.

Recompute and re-render on every `input` event. No submit button.

- [ ] **Step 4: Render the three result states**

Call `sizeFor` with the current inputs and branch on `status`:

`status === 'ok'` — headline the core count as the largest element on the page, e.g. `2 cores`, with the words `licensable cores` beneath it. Then, in smaller supporting text:
- `Needs {required} MiB/s (target {target} MiB/s + {headroomPct}% headroom). Measured {measuredMiBps} MiB/s at {cores} vCPU.`
- Peak heap from the run: `{peakHeapMB} MB` — and where `peakHeapMB > 1024`, note that memory rather than cores may drive instance choice.
- Provenance: `Measured {run.date} · run {run.path} · {run.sha}`.
- A `measured` badge, or an `estimate` badge when `taxMeasured === false` with the text `Includes an unmeasured processing multiplier — confirm in a POC.`
- The `GOMAXPROCS` note: `Cores are counted off the instance's CPU, not GOMAXPROCS. Raising GOMAXPROCS for I/O-bound work does not change this number.`
- Every string in `warnings`, each visually marked as a caveat.

`status === 'ceiling'` — render **no core count**. Headline instead: `No core count — this exceeds what was measured.` Then `Needs {required} MiB/s; the measured ceiling is {ceilingMiBps} MiB/s.` When `ceiling` is non-null, render `The limit is {ceiling.reason}` and `{ceiling.fix}`. When `ceiling` is null (postgres, mysql, iceberg), render exactly: `Only one Connect instance was benchmarked. Scaling out across instances was not measured — ask the perf team before quoting.` Then provenance and warnings as above.

`status === 'unbenchmarked'` — render **no number and no curve**. Text: `Not benchmarked. This tool only answers for the six connectors with a blessed benchmark run. Ask the perf team rather than extrapolating.`

- [ ] **Step 5: Render the measured-curve table**

Under the result, a table of the selected connector's curve: a vCPU column and a measured MiB/s column, marking the row that was selected. Where the curve plateaus, the table is what shows the rep *why* the tool refused to grow the number. Give it `overflow-x: auto` in its own container so the page body never scrolls horizontally.

- [ ] **Step 6: Style for both themes**

Support light and dark: `@media (prefers-color-scheme: dark)` as the default signal plus `:root[data-theme="dark"]` and `:root[data-theme="light"]` overrides that win in both directions. Use relative units and flexbox/grid. `measured` and `estimate` badges must be distinguishable without relying on colour alone — give them different text.

- [ ] **Step 7: Verify the acceptance cases in a browser**

Run: `open benchmarking/aws/sizing/index.html`

Walk all six acceptance cases from the spec by hand and confirm the rendered output:
1. postgres_cdc, 40000 events/sec, 1200 B, passthrough, 30% → `2 cores`.
2. Same, processing set to heavy → no core count, ceiling 51 MiB/s.
3. oracledb_cdc, 20000 events/sec → ceiling message naming LogMiner readers, no core count.
4. mongodb_cdc, 35000 events/sec → ceiling message naming sharding.
5. postgres_cdc with event size 200 → `1 core` plus the extrapolation caveat.
6. Connector set to `Something else…` → the not-benchmarked state, no number anywhere.

Also confirm the page body does not scroll horizontally at a narrow window width, and that the dark theme renders legibly.

- [ ] **Step 8: Run the tests once more**

Run: `node --test benchmarking/aws/sizing/sizing.test.mjs`
Expected: PASS. This proves the UI work did not disturb the fenced core.

- [ ] **Step 9: Commit**

```bash
git add benchmarking/aws/sizing/index.html
git commit -m "feat(sizing): self-serve UI with provenance, badges and refusal states"
```

---

### Task 4: Publish and document

**Files:**
- Create: `benchmarking/aws/sizing/README.md`

- [ ] **Step 1: Write the README**

Create `benchmarking/aws/sizing/README.md` covering, in this order:

- What the page answers and who it is for: a rep or SE, self-serve, returning a licensable core count.
- The hard boundary: six connectors only; anything else returns no number by design.
- `node --test benchmarking/aws/sizing/sizing.test.mjs` to test. Not wired into CI deliberately.
- Why the logic sits inside sentinel comments: the Artifact CSP forbids external scripts, so the page must be one file, and the sentinels let the tests import the shipped source rather than a copy. Moving the logic out of the sentinels breaks the tests.
- **Adding a connector when a bench lands:** pick one blessed run (never glob `results/` — most files there are smokes reading 0 MiB/s); read `median_mb_s` per vCPU for the `connect` engine; remember the field is MiB/s despite its name; take `peakHeapMB` as the max `heap_in_use_mb` across that point's `prom` series; add the entry with its run path, date and git SHA; add a test asserting the new curve; republish.
- **Retiring the estimated processing tax:** name the follow-up from the spec — three arms on `benchmarking/aws/scenarios/postgres/orders-cdc` (passthrough control, a `mapping` processor, Avro with schema registry) — and note that until it runs, the `light` and `heavy` multipliers are judgement, not measurement.
- The link to the spec.

- [ ] **Step 2: Publish the Artifact**

Publish `benchmarking/aws/sizing/index.html` with the Artifact tool: a `favicon`, a one-sentence `description`, and a stable title. Record the returned URL in the README so future updates redeploy to the same URL rather than minting a new one.

- [ ] **Step 3: Commit**

```bash
git add benchmarking/aws/sizing/README.md
git commit -m "docs(sizing): usage, how to add a connector, how to retire the estimated tax"
```

---

## Known limitation to raise with Prakhar after Task 3

The `status === 'ceiling'` path returns no number for postgres, mysql and iceberg, because scaling across multiple Connect instances was never benchmarked. A customer at 500 MiB/s of Postgres CDC is a realistic ask and the tool will decline it. Multiplying the single-instance curve by an instance count would be an invented number, so the plan keeps the refusal — but this is the first wall sales will hit, and it is worth deciding deliberately whether to bench a two-instance arm rather than discovering it in front of a customer.
