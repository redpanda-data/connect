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
