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

test('everything funnels through bytes/sec, whichever way volume was stated', () => {
  // 40k events/sec at 1200 B, 48 MB/s, 45.776 MiB/s and 4.147 TB/day are the same rate.
  const viaEvents = core.bytesPerSecFromEvents(40_000, 1200)
  assert.equal(viaEvents, 48_000_000)
  assert.equal(core.bytesPerSecFromThroughput(48, 'MB/s'), 48_000_000)
  assert.ok(Math.abs(core.bytesPerSecFromThroughput(45.776, 'MiB/s') - 48_000_000) < 1000)
  assert.ok(Math.abs(core.bytesPerSecFromThroughput(4.1472, 'TB/day') - 48_000_000) < 1000)
  assert.ok(Math.abs(core.bytesPerSecFromThroughput(4147.2, 'GB/day') - 48_000_000) < 1000)
  assert.ok(Number.isNaN(core.bytesPerSecFromThroughput(48, 'bogus/s')))
})

test('a target is expressed in the unit of the curve it will be compared against', () => {
  // The five CDC runs stored decimal MB/s; the iceberg run stored MiB/s. 48e6 B/s is
  // 48 MB/s but only 45.776 MiB/s, and comparing across those is a 4.86% error.
  assert.equal(core.inCurveUnit(48_000_000, 'MB'), 48)
  assert.ok(Math.abs(core.inCurveUnit(48_000_000, 'MiB') - 45.776) < 0.01)
  assert.equal(core.CONNECTORS.postgres_cdc.curveUnit, 'MB')
  assert.equal(core.CONNECTORS.iceberg_sink.curveUnit, 'MiB')
})

test('acceptance case 1: postgres 40k/s at 1200 B passthrough clears at 2 cores', () => {
  const r = core.sizeFor({
    connector: 'postgres_cdc', eventsPerSec: 40_000, eventBytes: 1200,
    tax: 'passthrough', headroomPct: 30,
  })
  assert.equal(r.status, 'ok')
  assert.equal(r.cores, 2)
  assert.equal(r.measuredRate, 83)
  assert.equal(r.unit, 'MB')
  // 48 MB/s target + 30% = 62.4, cleared by the 83 MB/s point at 2 vCPU.
  assert.equal(r.target, 48)
  assert.ok(Math.abs(r.required - 62.4) < 0.05, `required was ${r.required}`)
})

test('rate and throughput round-trip through the event size without drift', () => {
  // The two fields are views of one volume, anchored by the event size. Editing either must
  // land on the same bytes/sec, or the page would disagree with itself as a rep typed.
  const t = core.throughputFromEvents(40_000, 1200, 'MB/s')
  assert.equal(t, 48)
  assert.equal(core.eventsPerSecFromThroughput(t, 'MB/s', 1200), 40_000)

  for (const unit of ['MB/s', 'MiB/s', 'GB/day', 'TB/day']) {
    const back = core.eventsPerSecFromThroughput(core.throughputFromEvents(40_000, 1200, unit), unit, 1200)
    assert.ok(Math.abs(back - 40_000) < 0.001, `${unit} round-trip gave ${back}`)
  }

  // A blank or zero size cannot yield a rate — the anchor is missing.
  assert.ok(Number.isNaN(core.eventsPerSecFromThroughput(48, 'MB/s', 0)))
  assert.ok(Number.isNaN(core.throughputFromEvents(40_000, 1200, 'bogus/s')))
})

test('a throughput-derived rate sizes identically to the rate typed directly', () => {
  const base = { connector: 'postgres_cdc', tax: 'passthrough', headroomPct: 30 }
  const direct = core.sizeFor({ ...base, eventsPerSec: 40_000, eventBytes: 1200 })
  const derived = core.sizeFor({
    ...base,
    eventsPerSec: core.eventsPerSecFromThroughput(48, 'MB/s', 1200),
    eventBytes: 1200,
  })
  assert.equal(derived.status, 'ok')
  assert.equal(derived.cores, direct.cores)
  assert.equal(derived.target, direct.target)
})

test('the event-size check always runs, whichever field the rep typed into', () => {
  // Sizing is always rate x size, so no input path can skip the size check. dynamodb was
  // benched at 4096 B, so a 600 B event must warn regardless of how the volume was entered.
  const typed = core.sizeFor({ connector: 'dynamodb_cdc', eventsPerSec: 20_000, eventBytes: 600 })
  const viaThroughput = core.sizeFor({
    connector: 'dynamodb_cdc',
    eventsPerSec: core.eventsPerSecFromThroughput(12, 'MB/s', 600),
    eventBytes: 600,
  })
  for (const r of [typed, viaThroughput]) {
    assert.equal(r.status, 'ok')
    assert.equal(r.warnings.length, 1)
    assert.match(r.warnings[0], /4096 B events/)
  }
})

test('a MiB/s-stated volume is not silently treated as MB/s', () => {
  // 100 MiB/s is 104.86 MB/s. Against a decimal-MB curve that is the difference between
  // clearing the 102 point and not — the error this whole unit split exists to prevent.
  const asMiB = core.eventsPerSecFromThroughput(100, 'MiB/s', 1200)
  const asMB = core.eventsPerSecFromThroughput(100, 'MB/s', 1200)
  assert.ok(asMiB > asMB, 'a MiB figure must yield a higher rate than the same MB figure')
  assert.ok(Math.abs(asMiB / asMB - 1.048576) < 1e-9)

  const base = { connector: 'postgres_cdc', eventBytes: 1200, headroomPct: 0 }
  const mib = core.sizeFor({ ...base, eventsPerSec: asMiB })
  const mb = core.sizeFor({ ...base, eventsPerSec: asMB })
  assert.ok(Math.abs(mib.target - 104.8576) < 0.01, `MiB target was ${mib.target}`)
  assert.ok(Math.abs(mb.target - 100) < 0.01)
  assert.equal(mb.status, 'ok')
  assert.equal(mib.status, 'ceiling')
})

test('picks the smallest clearing point, not the biggest', () => {
  // mysql curve is 70/102/108/111 MB/s; a 62.4 MB/s requirement must not return 8.
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
  // 60k/s at 1200 B = 72 MB/s. At 0% headroom the 83 point clears it (2 vCPU).
  // At 50% the requirement becomes 108 MB/s, above the curve's 102 maximum, so
  // nothing clears — proving headroom moves the requirement rather than the curve.
  assert.equal(low.cores, 2)
  assert.equal(low.measuredRate, 83)
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
      curveUnit: c.curveUnit,
      hasCeiling: Boolean(c.ceiling),
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
      curveUnit: 'MB',
      hasCeiling: false,
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
      curveUnit: 'MB',
      hasCeiling: false,
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
      curveUnit: 'MB',
      hasCeiling: true,
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
      curveUnit: 'MB',
      hasCeiling: true,
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
      curveUnit: 'MB',
      hasCeiling: true,
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
      curveUnit: 'MiB',
      hasCeiling: false,
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
  assert.equal(r.ceilingRate, 51)
})

test('acceptance case 3: oracle refuses to grow cores and names readers as the fix', () => {
  // 20k/s at 1200 B = 24 MB/s, +30% = 31.2 — past every measured reader point.
  const r = core.sizeFor({
    connector: 'oracledb_cdc', eventsPerSec: 20_000, eventBytes: 1200,
    readers: 1, tax: 'passthrough', headroomPct: 30,
  })
  assert.equal(r.status, 'ceiling')
  assert.equal(r.measuredCeilingRate, 19, 'the 1-reader measurement')
  assert.match(r.ceiling.reason, /LogMiner/)
  assert.match(r.ceiling.fix, /readers/)
  assert.equal(r.cores, undefined, 'a refusal must not carry a core count')
  // 31.2 exceeds even 5 readers (29), so there is no reader count to suggest.
  assert.equal(r.readerHint, null)
  assert.deepEqual(r.topMeasured, { readers: 5, rate: 29, effective: 29 })
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
  assert.equal(r.measuredCeilingRate, 102, 'must be a real curve reading')
  assert.equal(r.measuredCeilingVcpu, 8, 'must name the vCPU point the table highlights')
  assert.equal(r.taxMultiplier, 1.2)
  assert.ok(Math.abs(r.ceilingRate - 85) < 0.001, `derated ceiling was ${r.ceilingRate}`)
  assert.notEqual(r.measuredCeilingRate, r.ceilingRate)
  // Without this the ceiling path renders no Estimate badge for an estimated number.
  assert.equal(r.taxMeasured, false)
  assert.equal(r.taxShort, 'light mapping')
  assert.equal(r.headroomPct, 30)
})

test('at 1.0x the ceiling figure is the measured maximum itself', () => {
  // mongodb has a flat vCPU curve and no reader scaling, so it exercises the plain path.
  const r = core.sizeFor({
    connector: 'mongodb_cdc', eventsPerSec: 40_000, eventBytes: 1200,
    tax: 'passthrough', headroomPct: 30,
  })
  assert.equal(r.status, 'ceiling')
  assert.equal(r.taxMultiplier, 1)
  assert.equal(r.taxMeasured, true)
  assert.equal(r.measuredCeilingRate, r.ceilingRate)
  assert.equal(r.measuredCeilingRate, 33)
})

test('oracle sizes off reader count, and reports cores as the run value not a scaling knob', () => {
  const base = { connector: 'oracledb_cdc', eventBytes: 1200, tax: 'passthrough', headroomPct: 0 }
  // 12 MB/s target: clears at 1 reader (19).
  const one = core.sizeFor({ ...base, eventsPerSec: 10_000, readers: 1 })
  assert.equal(one.status, 'ok')
  assert.equal(one.measuredRate, 19)
  assert.equal(one.cores, 4, 'the reader runs held vCPU at 4')
  assert.equal(one.readers, 1)

  // 22 MB/s: past 1 reader (19), inside 2 readers (25).
  assert.equal(core.sizeFor({ ...base, eventsPerSec: 18_500, readers: 1 }).status, 'ceiling')
  const two = core.sizeFor({ ...base, eventsPerSec: 18_500, readers: 2 })
  assert.equal(two.status, 'ok')
  assert.equal(two.measuredRate, 25)

  // Same volume at 1 reader must point at the reader count that would clear it.
  assert.equal(core.sizeFor({ ...base, eventsPerSec: 18_500, readers: 1 }).readerHint, 2)
})

test('an unmeasured reader count is refused, never interpolated', () => {
  for (const readers of [3, 4, 6, 0]) {
    const r = core.sizeFor({ connector: 'oracledb_cdc', eventsPerSec: 5_000, eventBytes: 1200, readers })
    assert.equal(r.status, 'unmeasured-readers', `readers=${readers} must not produce a number`)
    assert.equal(r.cores, undefined)
    assert.deepEqual(r.measuredReaderCounts, [1, 2, 5])
  }
  assert.equal(core.readerRate(core.CONNECTORS.oracledb_cdc, 3), null)
})

test('the oracle reader curve is pinned, sublinear, and short of the offered load', () => {
  const rs = core.CONNECTORS.oracledb_cdc.readerScaling
  assert.deepEqual(rs.points, [
    { readers: 1, rate: 19 },
    { readers: 2, rate: 25 },
    { readers: 5, rate: 29 },
  ])
  assert.equal(rs.vcpu, 4)
  assert.equal(rs.tables, 5)
  assert.equal(rs.unit, 'MB')
  assert.equal(rs.run.sha, 'f1ccf5289')
  assert.equal(rs.corroboration.sha, '3e8bf51d4')
  // The two facts a rep must not lose: 5x readers is far from 5x throughput, and even the
  // best point never caught the write rate.
  assert.ok(rs.points[2].rate / rs.points[0].rate < 2, 'reader scaling must read as sublinear')
  assert.ok(rs.points[2].rate < rs.offeredMBs, 'top reader point must still trail offered load')
  assert.ok(rs.notes.some((n) => /single hot table cannot be split/.test(n)))
  assert.ok(rs.notes.some((n) => /per-database ceiling/.test(n)))
})

test('only oracle is reader-scaled; the others ignore a readers argument', () => {
  assert.deepEqual(core.measuredReaderCounts(core.CONNECTORS.oracledb_cdc), [1, 2, 5])
  for (const key of ['postgres_cdc', 'mysql_cdc', 'mongodb_cdc', 'dynamodb_cdc', 'iceberg_sink']) {
    assert.deepEqual(core.measuredReaderCounts(core.CONNECTORS[key]), [])
    const withReaders = core.sizeFor({ connector: key, eventsPerSec: 10_000, eventBytes: 1200, readers: 3 })
    const without = core.sizeFor({ connector: key, eventsPerSec: 10_000, eventBytes: 1200 })
    assert.equal(withReaders.status, without.status, `${key} must ignore readers`)
    assert.equal(withReaders.cores, without.cores)
  }
})

test('the measured maximum quoted on a refusal is the top of the curve, ties going to the largest vCPU', () => {
  // postgres is flat at 102 across 4 and 8 vCPU; the table tags the 8 vCPU row.
  const r = core.sizeFor({ connector: 'postgres_cdc', eventsPerSec: 200_000, eventBytes: 1200, tax: 'passthrough', headroomPct: 30 })
  assert.equal(r.status, 'ceiling')
  assert.equal(r.measuredCeilingVcpu, 8)
  assert.equal(r.measuredCeilingRate, 102)
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
      assert.equal(r.measuredRate, undefined)
      assert.equal(r.taxMeasured, undefined, 'a no-answer state must render no badge')
      assert.equal(r.run, undefined, 'a no-answer state must render no provenance')
    }
  }
})

test('hasSizeableInput is the single guard, and it accepts real input', () => {
  assert.equal(core.hasSizeableInput(48_000_000), true)
  assert.equal(core.hasSizeableInput(0), false)
  assert.equal(core.hasSizeableInput(-1), false)
  assert.equal(core.hasSizeableInput(NaN), false)
  assert.equal(core.hasSizeableInput(Infinity), false)
})

test('a positive rate times a blank size is still no-input, not zero cores', () => {
  // Number('') === 0, so this is the durable-empty-field case in events mode.
  const r = core.sizeFor({ connector: 'postgres_cdc', eventsPerSec: 40_000, eventBytes: 0 })
  assert.equal(r.status, 'no-input')
  assert.equal(r.cores, undefined)
  assert.equal(core.sizeFor({ connector: 'postgres_cdc', eventsPerSec: 0, eventBytes: 1200 }).status, 'no-input')
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

// The page is one file with two module scripts: the fenced core, then the UI. They are
// joined by a `window.__sizingCore = { ... }` bridge line. Every other test here imports
// the fence DIRECTLY, so a stale name in that bridge throws at page load, blanks the whole
// UI, and leaves this suite green — which is exactly what happened once. This test closes
// that gap by checking both sides of the bridge against the real file.
test('the UI bridge names match the core exports and cover what the UI destructures', async () => {
  const html = await readFile(join(here, 'index.html'), 'utf8')

  const bridge = html.match(/window\.__sizingCore = \{([\s\S]*?)\n\}/)
  assert.ok(bridge, 'window.__sizingCore bridge object not found')
  const bridged = bridge[1].split(',').map((s) => s.trim()).filter(Boolean)

  // Side 1: every bridged name must actually be exported by the core.
  for (const name of bridged) {
    assert.ok(name in core, `bridge exposes "${name}", which the sizing core does not export`)
  }

  // Side 2: every name the UI script destructures must be on the bridge.
  const destructure = html.match(/const \{([^}]*)\} = window\.__sizingCore/)
  assert.ok(destructure, 'UI script does not destructure window.__sizingCore')
  for (const name of destructure[1].split(',').map((s) => s.trim()).filter(Boolean)) {
    assert.ok(bridged.includes(name), `UI destructures "${name}", which the bridge does not provide`)
  }
})
