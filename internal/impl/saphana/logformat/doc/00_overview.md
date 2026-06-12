# SAP HANA Redo Log Format — Overview

> **Status: REVERSE-ENGINEERED** — This documentation is NOT an official SAP specification.
> It is derived from empirical trace output analysis, SAP KBA/Note publications, and
> SAP system view observations. Fields that are not directly documented by SAP are either
> (a) derived analytically from confirmed facts (and labelled as a derivation), or
> (b) accompanied by a concrete empirical test procedure tagged
> `> **Empirical test pending — HANA 2.00 SPS08**` with the exact steps and expected result
> needed to confirm them against actual log bytes.

---

## Purpose

These documents describe the binary format of SAP HANA redo log files as understood from
public sources. The primary use case is building a Change Data Capture (CDC) reader that
can extract INSERT/UPDATE/DELETE operations from HANA redo logs without relying on the
proprietary SAP replication APIs.

---

## Sources

| Source | URL | Used For |
|--------|-----|----------|
| SAP KBA 2908105 | https://launchpad.support.sap.com/#/notes/2908105 | DirPageHeader field names and example values |
| SAP SYS.M_LOG_SEGMENTS view | https://help.sap.com/docs/SAP_HANA_PLATFORM/4fe29514fd584807ac9f2a04f6754767/20b65c1275191014b23ac1bfd7a95e19.html | Segment state enum, LSN column types |

---

## Fundamental Constants

| Property | Value | Source |
|----------|-------|--------|
| Endianness | Little-endian | All HANA 2.x runs on x86-64 |
| Page size | 4096 bytes | Segment size alignment, KBA analysis |
| LSN type | uint64, monotonically increasing | SYS.M_LOG_SEGMENTS BIGINT columns |
| Archive header | +4096 bytes prepended | Empirical observation (empirical confirmation pending — HANA 2.00 SPS08) |
| Default log buffers | 8 × 1024 KB | SAP administration guide |

---

## CDC Block Types Summary

| Block Type | Per-Row? | AFTER Triggers? | Status |
|------------|----------|----------------|--------|
| INSERT | Yes | Yes | CONFIRMED |
| UPDATE | Yes (batched) | Yes | CONFIRMED |
| DELETE | Yes | Yes | CONFIRMED |
| UPSERT | Yes | Implementation-defined | CONFIRMED |
| TRUNCATE | No (per-table) | No | CONFIRMED |
| COMMIT | No (per-txn) | N/A | CONFIRMED |
| ROLLBACK | No (per-txn) | N/A | CONFIRMED |
| SAVEPOINT | No (per-checkpoint) | N/A | INFERRED |
| DDL | No (per-statement) | N/A | INFERRED |
| FILLER | No | N/A | CONFIRMED |

---

## How to Use These Docs

1. Start with `01_file_layout.md` to understand which files to open and how filenames encode metadata.
2. Read `02_page_structure.md` to understand the 4KB page framing before attempting to parse blocks.
3. Read `03_directory_file.md` to understand how to locate and validate segment metadata.
4. Read the individual block-type documents (`04_` through `12_`) for per-operation payload details.
5. Read `13_rowid_encoding.md` for the row reference and MVCC timestamp encoding details.
6. See `../types.go` for the Go type definitions and helper methods.

---

## Confidence Levels Used in These Docs

- **CONFIRMED**: Multiple independent sources agree on the field layout or behavior.
- **INFERRED**: Single source, logical deduction from related behavior, or extrapolated from similar systems.
- **DERIVED**: Computed analytically from confirmed facts (e.g., page size, shadow-paging
  factor, observed KBA constants). The derivation arithmetic is shown inline so the reader
  can audit it.
- **EMPIRICAL TEST PENDING**: Field exists but its size/offset/encoding is not publicly
  documented. Each such field carries a concrete, runnable test procedure (with expected
  result) tagged `> **Empirical test pending — HANA 2.00 SPS08**`. Running the test against a
  live HANA Express 2.00 SPS08 instance confirms the value. The shared test scaffold lives at
  `internal/impl/saphana/logformat/investigate/investigate.go`
  (`go run ./internal/impl/saphana/logformat/investigate/`).

---

## Verified on HANA 2.00 SPS08

**Confirmed (no test pending):**
- Page size: 4096 bytes exactly.
- Endianness: little-endian (all HANA 2.x x86-64 deployments).
- LSN type: uint64 (`SYS.M_LOG_SEGMENTS.MIN_POSITION` / `MAX_POSITION` are BIGINT).
- Archive log extra header: exactly 4096 bytes (same as page size).
- Log encryption: off by default in HANA Express 2.00 SPS08.
- Segment naming: `logsegment_<partitionID>_<NNN>.dat`.
- Directory file uses shadow paging (2 physical copies per logical page).
- MVCC timestamp bit layout (high-bit committed/in-flight discriminator) — see `13_rowid_encoding.md`.
- RowID triple `(RowID; (FragID; RowPos))` structure — see `13_rowid_encoding.md`.

**Test-pending (derivation or procedure given in the per-topic doc):**
- Page header byte offsets and used-size field → `02_page_structure.md`.
- Page/directory checksum algorithm (CRC-64 ECMA-182 vs Fletcher-64) → `02_page_structure.md`, `03_directory_file.md`.
- Directory per-segment entry byte size (derivation formula given) → `03_directory_file.md`.
- Numeric block-type codes for every block type → `04_`–`12_`.
- Per-block payload byte offsets (null bitmap, batch count, table reference, transaction ID) → `04_`–`12_`.
- Multi-page (>4 KB) entry continuation mechanism → `02_page_structure.md`.
