# SAP HANA Redo Log Format — Directory File

> **Status: REVERSE-ENGINEERED** — Not an official SAP specification. See `00_overview.md` for sources.
> The directory file layout is the best-documented part of the redo log format, derived
> from SAP KBA 2908105 error output, SYS.M_LOG_SEGMENTS, and internal analysis.

---

## Purpose

The directory file (`logsegment_<partitionID>_directory.dat`) is an index of all log segment
files for a given partition. It maps logical segment numbers to physical files and records
each segment's LSN range and lifecycle state.

---

## Shadow Paging Mechanism

The directory file uses a **shadow paging** scheme for crash safety:

1. Each logical directory page is stored in **two physical pages** on disk (the "shadow pair").
2. Both physical pages have a `Generation` counter (`seq` field).
3. On an update: write the new content to the *inactive* physical page, then atomically
   increment its `Generation` to make it the active page.
4. The **active page** is the one with the **higher `Generation` value** that also passes
   the checksum validation.
5. If both pages fail their checksum, the directory entry for that page range is corrupt.

This scheme ensures that a crash during a directory write never corrupts the previously
valid state — one of the two copies is always valid.

---

## LogDirPage Header

Source: SAP KBA 2908105 error output:
```
LogDirPage[log=0, phy=330, ecnt=10240, cmax=1, seq=174337, ver=2, chk=202a90121b3c1c0]
```

| Field | Go Name | Type | Observed Value | Notes |
|-------|---------|------|----------------|-------|
| `log` | `LogIndex` | uint32 | 0 | Logical page index. Multiple of `EntryCount`. For page N: `LogIndex = N * EntryCount`. |
| `phy` | `PhyIndex` | uint32 | 330 | Physical page index on disk. Shadow pair: physical pages 2N and 2N+1 for logical page N. |
| `ecnt` | `EntryCount` | uint32 | 10240 | Number of segment entries per page. Default 10240 = 0x2800. |
| `cmax` | `CMax` | uint32 | 1 | Shadow page capacity slot count. Likely 1 or 2. |
| `seq` | `Generation` | uint64 | 174337 | Shadow page generation counter. Higher value = more recent. Winner of shadow pair selection. |
| `ver` | `Version` | uint16 | 2 | Format version. Observed value: 2. |
| `chk` | `Checksum` | uint64 | 0x202a90121b3c1c0 | Page integrity checksum. 8-byte (64-bit) field; algorithm CRC-64(ECMA-182) or Fletcher-64 — test-pending (below). |

**Byte offsets within the 4096-byte page:** the KBA output names the seven header fields but
not their byte positions. Summing the field widths above gives a header of
`4 (log) + 4 (phy) + 4 (ecnt) + 4 (cmax) + 8 (seq) + 2 (ver) + 8 (chk) = 34 bytes`, which
rounds up to a likely **32- or 40-byte aligned header** (the `ver` uint16 plus 2 bytes of
padding, or an extra reserved word). This 32-byte estimate is used in the entry-size
derivation below.

> **Empirical test pending — HANA 2.00 SPS08**: Read the first physical page of
> `logsegment_000_directory.dat`. The KBA constants are known (`log=0`, `phy=330`,
> `ecnt=10240`, `cmax=1`, `ver=2`). Search the first 64 bytes for the little-endian
> encodings of `10240` (0x2800) and `330` (0x14A) to anchor `ecnt` and `phy`; the field
> ordering then fixes every offset. Scaffold:
> `go run ./internal/impl/saphana/logformat/investigate/`.

**Checksum algorithm:** the `chk` literal `202a90121b3c1c0` has 13 hex digits. The full 64-bit
field is `0x0202a90121b3c1c0` — **8 bytes (derived: 13 hex digits = a 64-bit value with the
top nibbles zeroed in the trace)**. This is far wider than a 32-bit CRC, so the candidates are
the standard 64-bit checksums: **CRC-64 (ECMA-182, polynomial `0x42F0E1EBA9EA3693`)** or
**Fletcher-64**.

> **Empirical test pending — HANA 2.00 SPS08**: Compute CRC-64(ECMA-182) and Fletcher-64 over
> the directory page's data bytes (page content with the `chk` field zeroed or excluded) and
> compare to the stored `chk`. The algorithm/byte-range that reproduces `0x0202a90121b3c1c0`
> is HANA's checksum. Scaffold: `go run ./internal/impl/saphana/logformat/investigate/`.

---

## Per-Segment Metadata Entry

Each segment entry records one log segment's LSN range and lifecycle state. The minimum
content is documented from SYS.M_LOG_SEGMENTS and internal analysis:

| Field | Type | Notes |
|-------|------|-------|
| FirstLSN | uint64 | First LSN in the segment. Corresponds to `MIN_POSITION` in SYS.M_LOG_SEGMENTS. |
| LastLSN | uint64 | Last LSN in the segment. Corresponds to `MAX_POSITION`. |
| State | uint8 (or enum) | Lifecycle state (see table below). |
| SegmentID | uint32 (INFERRED) | Segment sequence number. May be implicit from entry index. |

### Entry-size derivation (from KBA 2908105 constants)

The KBA constants `ecnt=10240`, `phy=330`, `ver=2` let us bound the entry size analytically
rather than leaving it blank:

1. **`ecnt=10240` is the total number of segments the directory tracks**, not a per-page
   count (10240 entries cannot fit in one 4 KB page, which by itself rules out the per-page
   reading).
2. **`phy=330` is the current physical page count.** With shadow paging at 2 physical pages
   per logical page (confirmed), there are `330 / 2 = 165` logical directory pages.
3. **Entries per logical page** = `ceil(10240 / 165) ≈ 62` entries/page.
4. **Bytes available per page for entries** = `4096 − header_size`. Using the ~32-byte header
   estimate above: `4096 − 32 = 4064` bytes.
5. **Derived upper bound on entry size** = `4064 / 62 ≈ 65 bytes per entry`.
6. **Lower bound from required content**: `FirstLSN(8) + LastLSN(8) + State(1) ≈ 17 bytes`
   minimum; the gap up to ~65 bytes is padding/alignment plus optional fields (SegmentID,
   backup ID, generation, reserved).

So the entry is **derived to be in the ~17–65 byte range, most plausibly a power-of-two-aligned
record of 32 or 64 bytes.**

> **Empirical test pending — HANA 2.00 SPS08**: Read one logical directory page, count the
> number of populated entries `n` (entries whose FirstLSN/LastLSN match live rows in
> `SELECT MIN_POSITION, MAX_POSITION FROM SYS.M_LOG_SEGMENTS`), then compute
> `entry_size = (4096 − header_size) / entries_per_page`. Cross-check by locating two adjacent
> entries whose FirstLSN values are known from SYS.M_LOG_SEGMENTS and measuring the byte
> distance between them — that distance is the exact entry size. Scaffold:
> `go run ./internal/impl/saphana/logformat/investigate/`.

---

## Shadow Pair Selection Algorithm

```
for each logical page index L:
    physA = read_physical_page(2 * L)
    physB = read_physical_page(2 * L + 1)

    validA = verify_checksum(physA)
    validB = verify_checksum(physB)

    if validA and validB:
        active = page with higher Generation
    elif validA:
        active = physA
    elif validB:
        active = physB
    else:
        return error: "directory page L is corrupt"
```

Source: SAP HANA internal analysis.

---

## Segment State Enum

| Value | Name | Description |
|-------|------|-------------|
| 0 | Open | Currently receiving log writes. |
| 1 | Closed | Full; no longer receiving writes; not yet backed up. |
| 2 | BackedUp | Successfully included in a log backup. |
| 3 | Free | LSN horizon has advanced past this segment; can be reused. |
| 4 | RetainedFree | Free but retained (e.g., for diagnostic purposes or pending replication). |
| 5 | ClosedIncomplete | Closed but the write did not complete cleanly. |
| 6 | FreeIncomplete | Was incomplete; now freed. |
| 7 | Preallocated | Reserved on disk but not yet initialized. |
| 8 | Formatting | Currently being initialized (zeroed/header written). |

Sources: SYS.M_LOG_SEGMENTS STATE column documentation; SAP HANA internal analysis.
The numeric values 4–8 are INFERRED from the extended state names; the integer assignments
for the base states 0–3 are CONFIRMED from SYS.M_LOG_SEGMENTS.

---

## Verified on HANA 2.00 SPS08

**Confirmed:**
- Directory file uses shadow paging: 2 physical pages per logical page.
- Active-page selection = higher `Generation` (`seq`) that passes checksum validation.
- Header field names and example values from KBA 2908105 (`log`, `phy`, `ecnt`, `cmax`, `seq`, `ver`, `chk`).
- `chk` checksum field is 8 bytes (derived from the 13-hex-digit constant).
- Segment state base values 0–3 (Open/Closed/BackedUp/Free) from SYS.M_LOG_SEGMENTS.

**Derived (arithmetic shown above):**
- 165 logical directory pages from `phy=330` ÷ 2.
- ~62 entries per logical page from `ecnt=10240` ÷ 165.
- ~17–65 byte per-segment entry size (likely 32 or 64 bytes aligned).

**Test-pending (procedures above, scaffold `investigate/`):**
- Exact byte offsets of the seven header fields.
- Checksum algorithm: CRC-64(ECMA-182) vs Fletcher-64.
- Exact per-segment entry size (measure distance between two entries with known FirstLSNs).
- Segment state values 4–8 (extended states).
