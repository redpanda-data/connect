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

// Every number here becomes a customer quote, so the data table is pinned literally: a
// transposed digit, a garbled SHA or a flipped estimate flag must fail this suite rather
// than ship. Expected values are the ones verified against the benchmark JSONs under
// benchmarking/aws/results/ (which are gitignored, so they cannot be re-read here).
// If you change the data on purpose, change this snapshot in the same commit.
test('data snapshot: curves, provenance, heap, bench event sizes and tax flags are pinned', () => {
  const actual = {}
  for (const [key, c] of Object.entries(core.CONNECTORS)) {
    actual[key] = {
      curve: core.VCPU_POINTS.map((v) => c.curve[v]),
      benchedEventBytes: c.benchedEventBytes,
      peakHeapMB: c.peakHeapMB,
      runPath: c.run.path,
      runDate: c.run.date,
      runSha: c.run.sha,
      ceilingMiBps: c.ceiling ? c.ceiling.mibps : null,
      sourceKind: c.sourceKind,
      confidence: c.confidence,
    }
  }

  assert.deepEqual(actual, {
    postgres_cdc: {
      curve: [51, 83, 102, 102],
      benchedEventBytes: 1200,
      peakHeapMB: 118,
      runPath: 'postgres/orders-cdc/2026-06-01T20-55-50Z.json',
      runDate: '2026-06-01',
      runSha: '25057d693',
      ceilingMiBps: null,
      sourceKind: 'cdc',
      confidence: 'high',
    },
    mysql_cdc: {
      curve: [70, 102, 108, 111],
      benchedEventBytes: 1200,
      peakHeapMB: 378,
      runPath: 'mysql/orders-cdc/2026-06-02T14-13-52Z.json',
      runDate: '2026-06-02',
      runSha: '25057d693',
      ceilingMiBps: null,
      sourceKind: 'cdc',
      confidence: 'high',
    },
    mongodb_cdc: {
      curve: [26, 33, 33, 33],
      benchedEventBytes: 1200,
      peakHeapMB: 157,
      runPath: 'mongodb/orders-cdc/2026-07-17T17-11-10Z.json',
      runDate: '2026-07-17',
      runSha: '156a11081',
      ceilingMiBps: 33,
      sourceKind: 'cdc',
      confidence: 'high',
    },
    dynamodb_cdc: {
      curve: [40, 72, 81, 82],
      benchedEventBytes: 4096,
      peakHeapMB: 936,
      runPath: 'dynamodb/cdc/2026-06-15T17-44-43Z.json',
      runDate: '2026-06-15',
      runSha: 'd9d2b3c98',
      ceilingMiBps: 82,
      sourceKind: 'cdc',
      confidence: 'high',
    },
    oracledb_cdc: {
      curve: [13, 13, 13, 13],
      benchedEventBytes: 1200,
      peakHeapMB: 190,
      runPath: 'oracle/orders-cdc/2026-06-22T16-31-22Z.json',
      runDate: '2026-06-22',
      runSha: '63ea466c5',
      ceilingMiBps: 13,
      sourceKind: 'cdc',
      confidence: 'high',
    },
    iceberg_sink: {
      curve: [34, 65, 97, 128],
      benchedEventBytes: 1200,
      peakHeapMB: 2681,
      runPath: 'iceberg/orders-sink-recipe-b/2026-07-09T15-57-38Z.json',
      runDate: '2026-07-09',
      runSha: '62f50196b',
      ceilingMiBps: null,
      sourceKind: 'sink',
      confidence: 'medium',
    },
  })

  assert.deepEqual(core.VCPU_POINTS, [1, 2, 4, 8])
  assert.equal(core.EVENT_BYTES_MIN, 512)
  assert.equal(core.EVENT_BYTES_MAX, 5120)
  assert.equal(core.EVENT_BYTES_DEVIATION_FACTOR, 2)
  assert.equal(core.MIB, 1048576)

  // Only passthrough is measured. Flipping either estimate flag to true would silently
  // strip the Estimate badge from a number no benchmark produced.
  assert.deepEqual(
    Object.fromEntries(
      Object.entries(core.TAX).map(([k, t]) => [k, { multiplier: t.multiplier, measured: t.measured }]),
    ),
    {
      passthrough: { multiplier: 1.0, measured: true },
      light: { multiplier: 1.2, measured: false },
      heavy: { multiplier: 2.0, measured: false },
    },
  )
})

test('run paths point at real result files, extension included', () => {
  for (const [key, c] of Object.entries(core.CONNECTORS)) {
    assert.match(c.run.path, /^[a-z0-9-]+\/[a-z0-9-]+\/\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z\.json$/, `${key} run path is not a paste-able results/ path`)
  }
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

test('a ceiling refusal separates the measured maximum from the tax-derated figure', () => {
  // The reported bug: postgres + light mapping at 80k/s printed "the measured ceiling is
  // 85 MiB/s". 85 is 102/1.2 and appears in no benchmark; the table below said 102.
  const r = core.sizeFor({
    connector: 'postgres_cdc', eventsPerSec: 80_000, eventBytes: 1200,
    tax: 'light', headroomPct: 30,
  })
  assert.equal(r.status, 'ceiling')
  assert.equal(r.cores, undefined, 'a refusal must not carry a core count')
  assert.equal(r.measuredCeilingMiBps, 102, 'must be a real curve reading')
  assert.equal(r.measuredCeilingVcpu, 8, 'must name the vCPU point the table highlights')
  assert.equal(r.taxMultiplier, 1.2)
  assert.ok(Math.abs(r.ceilingMiBps - 85) < 0.001, `derated ceiling was ${r.ceilingMiBps}`)
  assert.notEqual(r.measuredCeilingMiBps, r.ceilingMiBps)
  // Without this the ceiling path renders no Estimate badge for an estimated number.
  assert.equal(r.taxMeasured, false)
  assert.equal(r.taxShort, 'light mapping')
  assert.equal(r.headroomPct, 30)
})

test('at 1.0x the ceiling figure is the measured maximum itself', () => {
  const r = core.sizeFor({
    connector: 'oracledb_cdc', eventsPerSec: 20_000, eventBytes: 1200,
    tax: 'passthrough', headroomPct: 30,
  })
  assert.equal(r.status, 'ceiling')
  assert.equal(r.taxMultiplier, 1)
  assert.equal(r.taxMeasured, true)
  assert.equal(r.measuredCeilingMiBps, r.ceilingMiBps)
  assert.equal(r.measuredCeilingMiBps, 13)
})

test('the measured maximum quoted on a refusal is the top of the curve, ties going to the largest vCPU', () => {
  // postgres is flat at 102 across 4 and 8 vCPU; the table tags the 8 vCPU row.
  const r = core.sizeFor({ connector: 'postgres_cdc', eventsPerSec: 200_000, eventBytes: 1200, tax: 'passthrough', headroomPct: 30 })
  assert.equal(r.status, 'ceiling')
  assert.equal(r.measuredCeilingVcpu, 8)
  assert.equal(r.measuredCeilingMiBps, 102)
})

test('blank, zero, negative and non-finite input yields no answer at all', () => {
  for (const bad of [0, -500, NaN, Infinity, -Infinity, Number('')]) {
    for (const field of ['eventsPerSec', 'eventBytes']) {
      const args = { connector: 'postgres_cdc', eventsPerSec: 40_000, eventBytes: 1200, tax: 'passthrough', headroomPct: 30 }
      args[field] = bad
      const r = core.sizeFor(args)
      assert.equal(r.status, 'no-input', `${field}=${bad} produced status ${r.status}`)
      assert.equal(r.cores, undefined, `${field}=${bad} produced a core count`)
      assert.equal(r.required, undefined)
      assert.equal(r.measuredMiBps, undefined)
      assert.equal(r.taxMeasured, undefined, 'a no-answer state must render no badge')
      assert.equal(r.run, undefined, 'a no-answer state must render no provenance')
    }
  }
})

test('hasSizeableInput is the single guard, and it accepts real input', () => {
  assert.equal(core.hasSizeableInput(40_000, 1200), true)
  assert.equal(core.hasSizeableInput(0, 1200), false)
  assert.equal(core.hasSizeableInput(40_000, 0), false)
  assert.equal(core.hasSizeableInput(-1, 1200), false)
  assert.equal(core.hasSizeableInput(NaN, 1200), false)
  assert.equal(core.hasSizeableInput(40_000, Infinity), false)
})

test('an event size far from the connector\'s own bench size warns even inside the global band', () => {
  // dynamodb's curve was measured at 4096 B. 600 B is inside 512-5120 but 6.8x smaller
  // than the size that produced the curve, so the global band check alone said nothing.
  const r = core.sizeFor({ connector: 'dynamodb_cdc', eventsPerSec: 20_000, eventBytes: 600, tax: 'passthrough', headroomPct: 30 })
  assert.equal(r.status, 'ok')
  assert.equal(r.warnings.length, 1, 'one caveat, not two near-identical ones')
  assert.match(r.warnings[0], /4096 B events/)
  assert.match(r.warnings[0], /6\.8x smaller/)
})

test('an event size within 2x of the bench size raises no deviation warning', () => {
  const near = core.sizeFor({ connector: 'dynamodb_cdc', eventsPerSec: 5_000, eventBytes: 2048, tax: 'passthrough', headroomPct: 30 })
  assert.deepEqual(near.warnings, [])
  // Outside the global band but within 2x of the bench size: the band warning still fires.
  const outOfBand = core.sizeFor({ connector: 'dynamodb_cdc', eventsPerSec: 1_000, eventBytes: 6000, tax: 'passthrough', headroomPct: 30 })
  assert.equal(outOfBand.warnings.length, 1)
  assert.match(outOfBand.warnings[0], /512–5120 B/)
})

test('the unmeasured-scale-out caveat is honest in both directions and CDC-aware', () => {
  const pg = core.sizeFor({ connector: 'postgres_cdc', eventsPerSec: 200_000, eventBytes: 1200, tax: 'passthrough', headroomPct: 30 })
  assert.equal(pg.ceiling, null)
  assert.match(pg.scaleOutNote, /was not measured/)
  assert.match(pg.scaleOutNote, /replication slot or log reader/)
  assert.match(pg.scaleOutNote, /duplicate the stream/)
  assert.match(pg.scaleOutNote, /proves scale-out is impossible/, 'must not overstate in the other direction')

  // The iceberg sink has no replication slot; it must not inherit the CDC sentence.
  const ice = core.sizeFor({ connector: 'iceberg_sink', eventsPerSec: 200_000, eventBytes: 1200, tax: 'passthrough', headroomPct: 30 })
  assert.equal(ice.ceiling, null)
  assert.match(ice.scaleOutNote, /was not measured/)
  assert.doesNotMatch(ice.scaleOutNote, /replication slot/)

  // A connector with a known hard ceiling explains that instead of scale-out.
  const ora = core.sizeFor({ connector: 'oracledb_cdc', eventsPerSec: 20_000, eventBytes: 1200, tax: 'passthrough', headroomPct: 30 })
  assert.equal(ora.scaleOutNote, null)
})
