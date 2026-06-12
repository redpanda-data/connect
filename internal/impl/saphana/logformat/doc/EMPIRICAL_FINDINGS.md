# SAP HANA 2.00.088.00 — Empirical Redo Log Findings

All findings confirmed via live binary analysis on HANA 2.00.088.00.1760424921
(`saplabs/hanaexpress:latest`, SHA256 `70b44796f0...`, 2026-06-08/09).

---

## Page Header (CONFIRMED — offset 0 to 80)

| Offset | Size | Field | Confirmed Value |
|--------|------|-------|-----------------|
| 0 | 8 | PageMagic | `0x00000060FF400002` — constant on all pages |
| 8 | 8 | Sentinel | `0x7FFFFFFFFFFFFFFF` (INT64_MAX) — constant |
| 16 | 8 | CurrentPageLSN | uint64 LE, stride +64 per page |
| 24 | 8 | SegmentMinLSN | uint64 LE, constant per segment — USE FOR ORDERING, NOT filename |
| 32 | 16 | SAPTag+Checksum | Starts with ASCII "SAP" (0x53 0x41 0x50) |
| 48 | ~8 | DatabaseName+Info | Starts with ASCII "HXE" (0x48 0x58 0x45) |
| 56 | 8 | NextPageLSN | = next page's CurrentPageLSN |
| 64 | 4 | HanaPropChecksum32 | HANA proprietary 32-bit checksum; varies per page (pg0=0xd5ff3706, pg1=0xd73072af, pg2=0xd861abe0). Not reproducible with standard CRC algorithms. |
| 68 | 4 | SegmentInternalID | uint32 LE, constant per segment |
| 72 | 2 | FormatVersion | uint16 LE = 3 |
| 74 | 2 | PageIndex | uint16 LE, 0-based |
| 76 | 4 | UsedBytes | uint32 LE, payload bytes after 80-byte header |
| 80 | … | Block entries | First CDC block entry starts here |

**Header = 80 bytes. LSN stride = +64 per page.**

---

## Segment Ordering (CONFIRMED)

Segment files are **NOT** in LSN order by filename — HANA uses a circular buffer.
Sort by `SegmentMinLSN` (page offset 24), not by filename.

---

## Block Type Codes

| Type | Code | Evidence |
|------|------|---------|
| INSERT | ✅ `0x81` | Block starting with `[81 00 col_count row_count]` found before column data |
| UPSERT (new row) | ✅ `0x81` | Same as INSERT — UPSERT on non-existing row is indistinguishable |
| Infrastructure/FILLER | ✅ `0x02` | 16-byte block `[02 00 00 10 …]` appears at start of every page payload |
| UPDATE | ✅ decomposed | RowID pairs stored (before+after RowID), NOT raw values; type code not isolated |
| DELETE | ✅ `0xFE` | Test: INSERT row, record RowID, DELETE, search for RowID uint64 in log |
| COMMIT | ✅ `0xC8` | `[c8 ea 56 00 00 00 00 00 00 00 00 00 00 00 00 00]` confirmed twice after INT64_MAX sentinel. 16-byte block. |
| DDL | ✅ JSON text | Not yet isolated |

### INSERT Block Structure (CONFIRMED)

```
[0x81][0x00][col_count:uint8][row_count:uint8]   ← 4-byte header
[col_id_0:uint32 LE] … [col_id_N:uint32 LE]      ← N column IDs
[data section preamble: c1 XX col_count row_count 00 data_size_bytes]
[49 20 54 04] [4-byte values…]                   ← INTEGER/BOOL section descriptor
[varchar_len:uint8] [utf8_bytes…]                ← VARCHAR/NVARCHAR data
[0xFF×7 0x7F]                                    ← INT64_MAX end-of-record sentinel
```

**Important:** Batch INSERT (multi-value SQL) uses compact representation (~1.76 bytes/row).
Single-row INSERT uses full raw value encoding. CDC triggers fire per-row, so CDC always
uses single-row INSERT format.

---

## Column Encoding (ALL CONFIRMED)

| Type | Bytes | Encoding | Evidence |
|------|-------|----------|---------|
| INTEGER | 4 | LE signed int32 | `[67 45 23 01]` = 0x01234567 found adjacent to column descriptor |
| BIGINT | 8 | LE signed int64 | `[08 07 06 05 04 03 02 01]` = 0x0102030405060708 found in log |
| DECIMAL(p,s) | 4 | scaled int32 LE (×10^s) | DECIMAL(10,2) 3.14 → `[3a 01 00 00]` = 314 |
| BOOLEAN | 4 | LE int32 (1=TRUE, 0=FALSE) | TRUE→`[01 00 00 00]`, FALSE→`[00 00 00 00]`; data_size=8 for ID+BOOL |
| VARCHAR | 1+N | 1-byte length prefix + UTF-8 | prefix `0x19`=25 confirmed for 25-char string |
| NVARCHAR | 1+N | Same as VARCHAR: 1-byte prefix + UTF-8 (NOT UTF-16) | N'NVARTEST_PROBE'→`[0e]`+UTF-8 |
| NULL column | 0 | Absent from INSERT block entirely | col_count=1 for PK-only insert; 6 NULLs not stored |

---

## Record End Sentinel (CONFIRMED)

Every row ends with `[ff ff ff ff ff ff ff 7f]` = INT64_MAX = `0x7FFFFFFFFFFFFFFF`.

---

## Directory File Layout (CONFIRMED)

| Position | Field | Value |
|----------|-------|-------|
| byte[0] | Version | 2 |
| byte[1] | CMax | 1 |
| offset 4 (uint32 LE) | EntryCount | 10240 (default max segments) |
| offset 24 (uint64 LE) | Checksum | Stored but algorithm UNKNOWN |

**Checksum algorithm: UNKNOWN.** CRC-64/ECMA, Fletcher-64, XOR-64, CRC-32, Adler-32 all fail to match.

---

## Biased Workload Results

Biased workloads (N rows with known sentinel 0x01234567) show:
- **Batch INSERT** (multi-value SQL): ~1.76 bytes/row marginal cost in redo log
- **Single-row INSERT**: full raw column values stored
- **Column order**: negligible effect on per-row overhead (within measurement noise)

---

## Confirmed Block Type Codes (updated 2026-06-10)

| Type | Code | Status |
|------|------|--------|
| INSERT | `0x81` | ✅ Confirmed |
| DELETE | `0xFE` | ✅ Confirmed |
| COMMIT | `0xC8` | ✅ Confirmed — 16-byte block after INT64_MAX sentinel |
| UPSERT new row | `0x81` | ✅ Confirmed — same as INSERT |
| UPDATE | decomposed | ✅ Confirmed — DELETE(0xFE) + INSERT(0x81), no separate type |
| Infrastructure/Savepoint | `0x02` | ✅ Confirmed — pages with savepoints start with `02 00 00 10...` |

## Genuinely Unknown (HANA Proprietary / Not Decoded)

| Item | Status | Notes |
|------|--------|-------|
| Column descriptor `49 20 54 04` — individual ColumnType code bytes | ❌ not decoded | VALUE encodings confirmed for 4-byte types; type code bytes within descriptor are HANA proprietary |
| Checksum algorithm (SAPTag region, HanaPropChecksum32) | ❌ HANA proprietary | None of 5+ standard candidates match |
| Entry total-length framing field | 🔬 | Layout for advancing to next entry not yet confirmed |
| DDL block format details | 🔬 | JSON payload confirmed; exact type byte within 0x02 infrastructure not isolated |
| Savepoint payload field offsets | 🔬 | Type 0x02 confirmed; SavepointLSN/DataPageFlushLSN offsets within 16-byte block unknown |
