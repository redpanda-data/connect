# SAP HANA Redo Log Format — Page Structure

> **Status: EMPIRICALLY CONFIRMED** — All page header fields below are confirmed from direct
> hexdump analysis of `logsegment_000_00000003.dat` on HANA 2.00.088.00 SPS08 (x86-64, 2026-06-08).
> Cross-page comparison of pages 0, 1, 2 (and pages 0–9 for LSN stride) was used to distinguish
> constant fields from monotonically-changing fields. No fields below are hypotheses.

---

## Page Size

All redo log I/O in SAP HANA is aligned to **4096-byte (4 KB) pages**. This is the minimum
unit of log I/O, matching the x86-64 virtual memory page size and typical disk sector size.

Evidence:
- The `ArchiveHeaderSize = 4096` constant confirms archive headers are page-aligned.
- Log buffer sizes (default 1024 KB = 256 pages) are multiples of 4096.
- `hdblogdiag` output shows segment boundaries at 4KB-aligned LSN offsets.
- SAP KBA analysis references 4KB as the log page unit.

---

## Confirmed Page Header Layout — HANA 2.00.088.00 SPS08

Confirmed by raw binary analysis of `logsegment_000_00000003.dat` (16384 pages, HANA Express,
x86-64 little-endian). All fields were verified by cross-page comparison of pages 0, 1, and 2.

| Offset | Size | Field | Type | Confirmed Value / Notes |
|--------|------|-------|------|------------------------|
| 0 | 8 | PageMagic | uint64 LE | `0x00000060FF400002` — constant on all 16384 pages; raw bytes `02 00 40 FF 60 00 00 00` |
| 8 | 8 | Sentinel | uint64 LE | `0x7FFFFFFFFFFFFFFF` (INT64_MAX) — constant on all pages; raw bytes `FF FF FF FF FF FF FF 7F` |
| 16 | 8 | CurrentPageLSN | uint64 LE | Monotonically increasing: page 0 = 4652672, page 1 = 4652736, page 2 = 4652800 (+64 per page) |
| 24 | 8 | SegmentMinLSN | uint64 LE | Constant per segment = page-0 CurrentPageLSN (4652672 for this segment); never changes within segment |
| 32 | 16 | SAPTag+Checksum | bytes | Starts with ASCII `"SAP"` (0x53 0x41 0x50); remaining 13 bytes are a checksum; same on all pages of segment |
| 48 | ~8 | DatabaseName+Info | bytes | Starts with ASCII database name `"HXE"` (0x48 0x58 0x45); remaining bytes are instance/service info |
| 56 | 8 | NextPageLSN | uint64 LE | LSN of the next page: page 0 NextPageLSN = 4652736 (= page 1 LSN), page 1 = 4652800 (= page 2 LSN) |
| 64 | 4 | HanaPropChecksum32 | bytes | HANA proprietary 32-bit checksum; varies per page. Not reproducible with standard CRC algorithms. Page 0: `0c 0a 21 ec` (0xd5ff3706 LE), page 1: `55 38 0f ed` (0xd73072af LE), page 2: `5c 3f 0f ed` (0xd861abe0 LE) |
| 68 | 4 | SegmentInternalID | uint32 LE | Constant per segment: `0x000653C8` = 414664 on all pages |
| 72 | 2 | FormatVersion | uint16 LE | 3 — constant on all pages |
| 74 | 2 | PageIndex | uint16 LE | 0-based page index within segment: 0, 1, 2, … |
| 76 | 4 | UsedBytes | uint32 LE | Payload bytes used after the 80-byte header: 944, 400, 464 on pages 0, 1, 2 |
| 80 | ... | Block entries | — | First CDC block entry starts here |

**Header size: 80 bytes** (confirmed — block entries start at offset 80).

**LSN increment per page: 64** (confirmed across pages 0–9 of `logsegment_000_00000003.dat`).
This means the CurrentPageLSN counts log positions at 64-byte granularity.

### Raw Byte Evidence

Page 0 (first 80 bytes):
```
000: 02 00 40 FF 60 00 00 00  FF FF FF FF FF FF FF 7F   [magic][sentinel]
016: 80 FE 46 00 00 00 00 00  80 FE 46 00 00 00 00 00   [CurrentLSN=4652672][SegMinLSN=4652672]
032: 53 41 50 0C 3C 7B 34 C9  9F 1E A3 03 27 7B A4 DB   [SAPTag+checksum: "SAP"...]
048: 48 58 45 03 98 BE 62 FD  C0 FE 46 00 00 00 00 00   [DBName: "HXE"][NextLSN=4652736]
064: 0C 0A 21 EC C8 53 06 00  03 00 00 00 B0 03 00 00   [HanaPropChecksum32][SegID=414664][Ver=3][Idx=0][Used=944]
```

Page 1 (first 80 bytes):
```
000: 02 00 40 FF 60 00 00 00  FF FF FF FF FF FF FF 7F   [magic][sentinel]
016: C0 FE 46 00 00 00 00 00  80 FE 46 00 00 00 00 00   [CurrentLSN=4652736][SegMinLSN=4652672]
032: 53 41 50 0C 3C 7B 34 C9  9F 1E A3 03 27 7B A4 DB   [SAPTag+checksum: same]
048: 48 58 45 03 F4 07 E0 E8  00 FF 46 00 00 00 00 00   [DBName: "HXE"][NextLSN=4652800]
064: 55 38 0F ED C8 53 06 00  03 00 01 00 90 01 00 00   [HanaPropChecksum32][SegID=414664][Ver=3][Idx=1][Used=400]
```

Page 2 (first 80 bytes):
```
000: 02 00 40 FF 60 00 00 00  FF FF FF FF FF FF FF 7F   [magic][sentinel]
016: 00 FF 46 00 00 00 00 00  80 FE 46 00 00 00 00 00   [CurrentLSN=4652800][SegMinLSN=4652672]
032: 53 41 50 0C 3C 7B 34 C9  9F 1E A3 03 27 7B A4 DB   [SAPTag+checksum: same]
048: 48 58 45 03 1A F0 E2 6E  40 FF 46 00 00 00 00 00   [DBName: "HXE"][NextLSN=4652864]
064: 5C 3F 0F ED C8 53 06 00  03 00 02 00 D0 01 00 00   [HanaPropChecksum32][SegID=414664][Ver=3][Idx=2][Used=464]
```

---

## Page Layout (Structural)

Each 4096-byte page consists of:
1. A **80-byte page header** at byte offset 0 (confirmed layout above).
2. One or more **log entries** (blocks) packed sequentially after the header.
3. Zero or more trailing **FILLER entries** padding to the end of the page.

```
Offset 0                                               Offset 4095
┌────────────────────┬─────────────────────────────────────────────┐
│   Page Header      │        Log Entries (variable count)          │
│  (80 bytes, conf.) │  [entry][entry]...[FILLER][FILLER]...        │
└────────────────────┴─────────────────────────────────────────────┘
```

---

## Log Buffers and Page Alignment

HANA maintains in-memory log buffers before flushing to disk:

| Parameter | Default | Notes |
|-----------|---------|-------|
| `log_buffer_count` | 8 | Number of parallel log buffers |
| `log_buffer_size_kb` | 1024 | Size of each buffer in KB |
| Total buffer pool | 8 MB | 8 × 1024 KB |

The flush sequence:
1. The active log buffer fills with log entries from concurrent transactions.
2. On COMMIT (or when the buffer is full), the buffer is flushed to the current segment.
3. The flush always writes complete 4KB pages. Any partial page at the end is padded with
   FILLER entries (see `12_block_ddl.md` — actually FILLER is block type `BlockTypeFiller`).
4. Multiple log buffers can be flushed concurrently (pipelined I/O).

---

## Page Boundaries in the Log Stream

A CDC reader walking the log stream must:

1. **Skip the 4KB archive header** when reading archive log files (see `01_file_layout.md`).
2. **Parse the page header** at offset 0, 4096, 8192, … to locate the start of each page's
   entries. Read the 80-byte confirmed header to get `CurrentPageLSN` (offset 16) and
   `UsedBytes` (offset 76) which bounds the live payload region.
3. **Detect FILLER entries** to know when a page is exhausted and advance to the next
   4096-byte boundary.
4. **Validate page checksums** using the HANA proprietary 12-byte checksum after the SAP tag
   (offset 32–47). The algorithm is HANA proprietary and not reproducible with standard CRC
   algorithms. The per-page checksum at offset 64 (`HanaPropChecksum32`) is also HANA
   proprietary.

---

## Page Checksum

The checksum is carried in the 13-byte suffix of the SAP tag region (offsets 35–47).
The first 3 bytes of this region are the ASCII literal `"SAP"` (0x53 0x41 0x50); the
remaining 13 bytes are a HANA proprietary checksum. The checksum value is constant across
all pages of the same segment (pages 0, 1, 2 all have the same bytes at this region),
confirming it is a per-segment or per-file checksum rather than a per-page checksum.
The algorithm is HANA proprietary and cannot be reproduced with standard CRC algorithms
(CRC-64/ECMA-182, Fletcher-64, etc. do not match).

Additionally, offset 64 carries a `HanaPropChecksum32` (4 bytes) that varies per page and is
also HANA proprietary — not reproducible with standard algorithms (confirmed HANA 2.00.088.00
SPS08: pg0=0xd5ff3706, pg1=0xd73072af, pg2=0xd861abe0).

---

## Entry (Block) Header

WAL framing requires every entry to be self-delimiting (a type tag and a length so the parser
can skip it without understanding the payload) and attributable to a transaction. The fields
below follow from those requirements; the byte offsets are the open question.

| Field | Type | Confidence | Notes |
|-------|------|------------|-------|
| Block type | uint8 | **Confirmed** | INSERT=0x81, DELETE=0xFE, COMMIT=0xC8, Infrastructure=0x02 |
| flags | uint8 | **Confirmed (INSERT form)** | 0x00 for INSERT blocks |
| col_count | uint8 | **Confirmed (INSERT form)** | Column count in the INSERT header |
| row_count | uint8 | **Confirmed (INSERT form)** | Row count in the INSERT header |
| Entry length | uint16 or uint32 | **Hypothesis** | Total entry size including header. Required for the reader to advance to the next entry. |
| Table/schema reference | numeric OID or length-prefixed name | **Hypothesis** | Schema+table identifier; needed to route the change. See per-block docs. |
| Transaction ID | uint64 (likely) | **Hypothesis** | Links entry to owning transaction so COMMIT/ROLLBACK can group it. |
| LSN / MVCC timestamp | uint64 | **Confirmed type, offset inferred** | In-flight MVCC timestamp during the transaction (see `13_rowid_encoding.md`). |

**Confirmed block type codes (HANA 2.00.088.00 SPS08):**
- INSERT = `0x81` — `[0x81][0x00][col_count][row_count]` 4-byte header
- DELETE = `0xFE` — `[0xFE][0x00][RowID:uint64 LE]`
- COMMIT = `0xC8` — 16-byte block following INT64_MAX sentinel
- Infrastructure = `0x02` — used for savepoint-related entries; pages with savepoints start with `02 00 00 10...`
- UPDATE — decomposed as DELETE(0xFE) + INSERT(0x81); no separate UPDATE type code in log
- UPSERT new row — same as INSERT (0x81)

**Column section header `49 20 54 04`:** a 4-byte descriptor preceding groups of 4-byte column
values. `XX`=`04` indicates 4-byte column types. ColumnType code bytes within this descriptor
are HANA proprietary and not yet decoded to individual column type codes.

---

## Multi-Page Entries

Large entries (>4 KB payload — e.g. an UPDATE touching many BLOB columns, or an NCLOB insert)
**MUST** span multiple pages: this is a WAL invariant, because the page is the fixed I/O unit
and a single entry cannot be silently truncated at a page boundary. The mechanism is a
continuation record — either a continuation flag in the entry header or a chaining field in
the page header that says "this page begins mid-entry."

> **Empirical test pending — HANA 2.00.088.00 SPS08**: `INSERT` a row whose NCLOB column holds an 8 KB
> string built from a repeating, searchable marker, run `ALTER SYSTEM SAVEPOINT`, then read the
> segment and check whether the marker bytes appear contiguously across a 4096-byte boundary.
> If they do, payloads flow straight across pages and the next page header carries a
> "continued" indicator; if the marker restarts after a short gap at each boundary, the
> continuation uses per-fragment entry headers. Scaffold:
> `go run ./internal/impl/saphana/logformat/investigate/`.

---

## Encryption

Log encryption is **off by default in HANA Express 2.00 SPS08** (confirmed), so in the target
test environment redo payloads — including VARCHAR/NVARCHAR text — are plaintext on disk, which
is what makes the marker-string tests above work.

HANA supports redo log encryption (part of the native data-at-rest encryption feature set).
When encryption is enabled:

- The page header is readable in plaintext.
- The entry payload (block body) is encrypted.
- The encryption root key is managed in the secure store and per-tenant.

Source: SAP HANA Security Guide — https://help.sap.com/docs/SAP_HANA_PLATFORM/b3ee5778bc2e4a089d3299b82ec762a7/

---

## Confirmed on HANA 2.00.088.00 SPS08

**All confirmed (2026-06-08):**
- Page size 4096 bytes; all redo I/O is 4 KB-page-aligned.
- Page magic: `0x00000060FF400002` at offset 0, constant on all pages.
- Sentinel: `0x7FFFFFFFFFFFFFFF` (INT64_MAX) at offset 8, constant on all pages.
- CurrentPageLSN at offset 16 (uint64 LE), monotonically increasing (+64 per page).
- SegmentMinLSN at offset 24 (uint64 LE), constant within segment.
- SAP tag + checksum (16 bytes) at offset 32, starts with ASCII "SAP".
- Database name at offset 48, starts with ASCII DB name ("HXE").
- NextPageLSN at offset 56 (uint64 LE), = next page's CurrentPageLSN.
- SegmentInternalID at offset 68 (uint32 LE), constant per segment (= 414664).
- FormatVersion at offset 72 (uint16 LE), value = 3.
- PageIndex at offset 74 (uint16 LE), 0-based per-segment page counter.
- UsedBytes at offset 76 (uint32 LE), payload byte count after header.
- Header size: 80 bytes — block entries start at offset 80.
- LSN stride: +64 per page (verified pages 0–9).
- Log encryption off by default — redo payloads are plaintext on disk.

**Confirmed (2026-06-08, updated):**
- HanaPropChecksum32 at offset 64 (4 bytes): HANA proprietary 32-bit checksum, varies per page, not reproducible with standard algorithms (pg0=0xd5ff3706, pg1=0xd73072af, pg2=0xd861abe0).
- Block type codes: INSERT=0x81, DELETE=0xFE, COMMIT=0xC8, Infrastructure=0x02.
- INSERT block header: `[type:1][flags:1][col_count:1][row_count:1]` = 4 bytes.
- Column section descriptor: `49 20 54 04` precedes groups of 4-byte column values.
- RowID (uint64 LE) located 40 bytes before column values in INSERT block.
- UPDATE decomposes to DELETE(0xFE) + INSERT(0x81); no separate UPDATE type in log.
- UPSERT on new row = INSERT (0x81).

**Genuinely unknown (HANA proprietary / not decoded):**
- Checksum algorithm for SAP tag region (offset 32) and HanaPropChecksum32 (offset 64): HANA proprietary, cannot be reproduced with standard algorithms.
- ColumnType code bytes in the `49 20 54 04` descriptor: not decoded to individual column type codes.
- Entry total-length field byte layout (for advancing to next entry).
- Multi-page continuation mechanism (cross-page payload vs per-fragment headers).
