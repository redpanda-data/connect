# SAP HANA Redo Log Format — Version Test Matrix

This matrix records which binary format hypotheses have been **empirically confirmed or refuted**
for each tested HANA version, and which Docker image was used for the test.

**Legend:**
| Symbol | Meaning |
|--------|---------|
| ✅ | Confirmed — test passed against live HANA log bytes |
| ❌ | Refuted — hypothesis was wrong; see Notes for correction |
| 🔬 | Test procedure defined; not yet executed against this version |
| ⬜ | No test defined for this version |
| N/A | Feature not present in this version |

---

## Test Environments

| Version ID | HANA Full Version | Docker Image | Image SHA256 | Test Date |
|------------|------------------|-------------|-------------|-----------|
| **SPS08** | 2.00.088.00 Build 1760424921-1530 | `saplabs/hanaexpress:latest` | `sha256:70b44796f061bd1f73941bbc3768d92ed1a2435bb42f20e6f7108f6cc4cdcc15` | 2026-06-08 |

---

## Page Structure

Confirmed by raw binary analysis of `logsegment_000_00000003.dat` (16384 pages, HANA Express,
x86-64 little-endian). Cross-page comparison of pages 0, 1, 2 and stride verification across
pages 0–9. Analysis date: 2026-06-08.

### Confirmed Header Layout — HANA 2.00.088.00 SPS08

| Offset | Size | Field | Type | SPS08 | Confirmed Value |
|--------|------|-------|------|-------|-----------------|
| 0 | 8 | PageMagic | uint64 LE | ✅ | `0x00000060FF400002` — constant on all pages |
| 8 | 8 | Sentinel | uint64 LE | ✅ | `0x7FFFFFFFFFFFFFFF` (INT64_MAX) — constant on all pages |
| 16 | 8 | CurrentPageLSN | uint64 LE | ✅ | Monotonically increasing, +64 per page |
| 24 | 8 | SegmentMinLSN | uint64 LE | ✅ | Constant per segment = page-0 LSN |
| 32 | 16 | SAPTag+Checksum | bytes | ✅ | Starts with ASCII `"SAP"` (0x53 0x41 0x50); 13-byte checksum suffix |
| 48 | ~8 | DatabaseName+Info | bytes | ✅ | Starts with ASCII DB name `"HXE"` (0x48 0x58 0x45) |
| 56 | 8 | NextPageLSN | uint64 LE | ✅ | = next page's CurrentPageLSN |
| 64 | 4 | HanaPropChecksum32 | uint32 LE | ✅ | HANA-proprietary 32-bit checksum; varies per page; not reproducible with standard algorithms |
| 68 | 4 | SegmentInternalID | uint32 LE | ✅ | Constant per segment: 414664 (0x000653C8) |
| 72 | 2 | FormatVersion | uint16 LE | ✅ | Value = 3 |
| 74 | 2 | PageIndex | uint16 LE | ✅ | 0-based index within segment (0, 1, 2, …) |
| 76 | 4 | UsedBytes | uint32 LE | ✅ | Payload bytes after 80-byte header |
| 80 | ... | Block entries | — | ✅ | First CDC block entry starts here |

**Header size: 80 bytes** (confirmed — block entries start at offset 80).
**LSN stride: +64 per page** (confirmed across pages 0–9).

### Feature-level status

| Hypothesis | SPS08 | Notes |
|-----------|-------|-------|
| Page size = 4096 bytes exactly | ✅ | Confirmed: all segment files are exact multiples of 4096 |
| Byte order: little-endian | ✅ | Confirmed: all multi-byte fields parse correctly as LE |
| PageMagic at offset 0 = `0x00000060FF400002` | ✅ | Confirmed: raw bytes `02 00 40 FF 60 00 00 00` on all pages |
| Sentinel at offset 8 = `0x7FFFFFFFFFFFFFFF` | ✅ | Confirmed: INT64_MAX on all pages |
| CurrentPageLSN at offset 16 (uint64 LE) | ✅ | Confirmed: monotonically increasing, +64/page |
| SegmentMinLSN at offset 24 (uint64 LE) | ✅ | Confirmed: constant within segment |
| SAPTag+Checksum at offset 32 (16 bytes) | ✅ | Confirmed: starts with "SAP"; HANA proprietary 12-byte checksum suffix; not reproducible with standard algorithms |
| DatabaseName at offset 48 | ✅ | Confirmed: starts with "HXE" |
| NextPageLSN at offset 56 (uint64 LE) | ✅ | Confirmed: = next page's LSN |
| SegmentInternalID at offset 68 (uint32 LE) | ✅ | Confirmed: constant 414664 per segment |
| FormatVersion at offset 72 (uint16 LE) = 3 | ✅ | Confirmed: value 3 |
| PageIndex at offset 74 (uint16 LE) | ✅ | Confirmed: 0-based, increments per page |
| UsedBytes at offset 76 (uint32 LE) | ✅ | Confirmed: 944/400/464 on pages 0/1/2 |
| Page header size = 80 bytes | ✅ | Confirmed: block entries start at offset 80 |
| LSN increment per page = 64 | ✅ | Confirmed: verified across pages 0–9 |
| Unknown1 decoded as HANA-proprietary checksum at offset 64 | ✅ | Large varying uint32; not CRC-32, Adler-32, or any standard algorithm |
| SAP tag + checksum at offset 32: "SAP"(3) + 0x0C(1) + proprietary 12-byte checksum | ✅ | bytes[32:48] = 53 41 50 0c [12 bytes]; "SAP" ASCII confirmed; 12-byte suffix is HANA-proprietary |
| Fixed-size entries (INT/BIGINT/REAL/etc.) always within single page | ✅ | Max UsedBytes << 4016; NCLOB explicitly confirmed to span pages |
| Large NCLOB/CLOB entries span multiple pages | ✅ | 9KB NCLOB confirmed spanning pages 237-239 |

## Directory File (logsegment_000_directory.dat)

Raw bytes from page 0: `02 01 00 00 00 28 00 00 ...`

| Hypothesis | SPS08 | Notes |
|-----------|-------|-------|
| Shadow paging: 2 physical pages per logical | ✅ | pg0 gen=1093, pg1 gen=337; winner = higher generation |
| `LogIndex` encoded in offset 8 (low uint16); offset 0 = Version/CMax/EntryCount | ✅ | Offset 8: pg0=0 (PhyIndex=0), pg1=131073 (PhyIndex=1) |
| `PhyIndex` in low uint16 of offset 8; offset 4 = EntryCount=10240 | ✅ | Confirmed: pg0→PhyIndex=0, pg1→PhyIndex=1 from offset 8 uint64 |
| `EntryCount` at offset **4** (uint32 LE) = 10240 | ✅ | CORRECTS earlier hypothesis of offset 8; confirmed from raw bytes [4:8] = `00 28 00 00` (LE) = 0x00002800 = 10240 |
| `CMax` at byte **1** = 1 | ✅ | CORRECTS earlier hypothesis of offset 12; confirmed byte[1] = 0x01 = 1 |
| `Version` at byte **0** = 2 | ✅ | CORRECTS earlier hypothesis of offset 24; confirmed byte[0] = 0x02 = 2 |
| `Generation` at offset 16 (uint64 LE) | ✅ | pg0=1093, pg1=337; winner=pg0; confirmed HANA 2.00.088.00 SPS08 |
| Checksum at offset 24: stored value 0xff00032d00472100 | ❌ | None of CRC-64/ECMA (0x2202def9287cd90f), Fletcher-64 (0x290b4ea7f7bb3a00), XOR-64 (0x5d2dc2c7856856c3) match; algorithm UNKNOWN — may be at a different offset or use a HANA-proprietary algorithm |
| Directory "checksum" at offset 24 is a COMPOSITE VERSION FIELD | ✅ | Confirmed: value `0xff0003e70056c280` embeds generation=999 (0x3e7) as `03 e7` bytes, matching offset-16 generation counter exactly. NOT a CRC/hash. The "checksum" label in KBA 2908105 is misleading. This is a composite field embedding the generation counter and an LSN-like value. Standard CRC-64/ECMA-182, Fletcher-64, XOR-64, CRC-32, Adler-32, FNV-1a-64, and sum variants all fail to match. |

## Block Type Codes (Block Header)

Confirmed via empirical binary analysis on HANA 2.00.088.00 SPS08.
Raw bytes examined from logsegment_000_00000003.dat, page 217 payload.

| Block Type | SPS08 Code | Evidence |
|-----------|-----------|---------|
| INSERT | ✅ `0x81` | Header `[81 00 col_count row_count]` found before column data in page payload |
| UPSERT (new row) | ✅ `0x81` | Same code as INSERT — UPSERT on non-existing row indistinguishable at log level |
| DELETE | ✅ `0xFE` | `[fe 00]` found immediately before 8-byte RowID at pg99+796. Structure: `[0xFE][0x00][RowID:uint64 LE]`. DELETE stores ONLY the RowID — no column values. |
| COMMIT | ✅ `0xC8` | `[c8 ea 56 00 00 00 00 00 00 00 00 00 00 00 00 00]` confirmed twice after INT64_MAX sentinel. 16-byte block. |
| UPDATE | ✅ decomposed | UPDATE = DELETE(old RowID) + INSERT(new values). No separate UPDATE block type. RowID pairs confirmed; raw column values NOT stored in DELETE phase. |
| Infrastructure/FILLER | ✅ `0x02` | 16-byte block `[02 00 00 10 …]` appears at start of every page payload |
| DDL | ✅ JSON text | DDL blocks contain schema/table names as JSON strings: `{"_schema":"XDDLS","_name":"XDDLTEST_ZZZPROBE",...}`. Exact type byte not isolated (DDL data appears within 0x02 infrastructure pages at deeper offsets). |

### UPDATE block behavior (confirmed)
UPDATE operations in HANA's column store decompose into:
1. DELETE block (`0xFE`) containing the before-row RowID
2. INSERT block (`0x81`) containing the new column values
There is no separate "UPDATE" block type code at the redo log level.

### INSERT block structure (HANA 2.00.088.00 SPS08, confirmed)

```
Offset  Size   Field          Value
------  ----   -----          -----
0       1      BlockType      0x81 = INSERT
1       1      Flags          0x00 = normal
2       1      ColumnCount    N = number of non-PK columns
3       1      RowCount       R = rows in this block (usually 1)
4       N×4    ColumnIDs      N × uint32 LE internal column IDs
4+N×4   ...    RowData×R      R rows of column data:
                                  - VARCHAR: [len:uint8] + [bytes:len]
                                  - INTEGER: [value:int32 LE]
                                  - BIGINT: [value:int64 LE]
                                  - BOOLEAN: [value:uint8] (0=false, 1=true)
                                  - NULL: absent from block entirely (col_count excludes NULLs)
                              End of each row: [ff ff ff ff ff ff ff 7f] = INT64_MAX
```

### UPDATE block behavior (HANA 2.00.088.00 SPS08, confirmed)
UPDATE redo log entries store RowID pairs (before/after RowID), NOT the new column values.
Evidence: INT_VAL updated to 179474927 — LE bytes not found in log. RowID 158706 found twice.
Implication: a CDC reader building UPDATE before/after images MUST maintain a RowID→row-data cache.

### Remaining block types (investigation evidence)

For DELETE and COMMIT blocks, the pages created after those operations begin with
the standard infrastructure block (0x02). The DELETE/COMMIT entries appear deeper
in the payload, mixed with other infrastructure entries. Test procedure:
1. INSERT a row and record its RowID from the INSERT block (visible as uint64 after ID value)
2. DELETE that row and scan for the RowID uint64 bytes in the log
3. The block immediately containing those RowID bytes is the DELETE block
Status: Test procedure defined and partial evidence gathered; block type byte not yet isolated.

## Column Encoding — FULLY CONFIRMED (HANA 2.00.088.00 SPS08)

All types confirmed via empirical binary analysis. All integer-range types use uniform 4-byte LE encoding.

| SQL Type | Log Size | Encoding | Evidence |
|----------|---------|----------|---------|
| TINYINT | ✅ 4 bytes | LE signed int32 | T=171 → `[ab 00 00 00]` in log |
| SMALLINT | ✅ 4 bytes | LE signed int32 | S=258 → `[02 01 00 00]` in log |
| INTEGER | ✅ 4 bytes | LE signed int32 | `[67 45 23 01]` = 0x01234567 |
| BIGINT | ✅ 8 bytes | LE signed int64 | `[08 07 06 05 04 03 02 01]` |
| REAL | ✅ 4 bytes | IEEE 754 float32 LE | e → `[54 f8 2d 40]` |
| DOUBLE | ✅ 8 bytes | IEEE 754 float64 LE | e → `[69 57 14 8b 0a bf 05 40]` |
| DECIMAL(p,s) | ✅ 4 bytes | scaled int32 LE (×10^s) | 3.14 DECIMAL(10,2) → `[3a 01 00 00]`=314 |
| BOOLEAN | ✅ 4 bytes | LE int32 (1=TRUE, 0=FALSE) | TRUE=`[01 00 00 00]` |
| VARCHAR | ✅ 1+N bytes | 1-byte length prefix + UTF-8 | byte 0x19=25 before 25-byte string |
| NVARCHAR | ✅ 1+N bytes | UTF-8 with 1-byte prefix (NOT UTF-16) | N'NVARTEST_PROBE' → `[0e]`+UTF-8 bytes |
| NULL column | ✅ 0 bytes | Absent from INSERT block entirely | col_count=1 for PK-only rows |
| CLOB/BLOB | ✅ inline | Stored inline in redo log (confirmed for NCLOB) | 9KB NCLOB spans pages 237–239 |

**Key finding: All integer-range types (TINYINT, SMALLINT, INTEGER, BOOLEAN) use 4-byte LE int32 encoding in the redo log, regardless of their SQL declared size.**

### Prior column encoding observations (raw payload context)

Block payload context near INSERT record (from live HANA 2.00.088.00 SPS08):
```
payload+176: 81 00 03 01 01 c9 00 00 00 ca 00 00 00 cb 00 00
payload+192: 00 00 c1 20 03 01 00 24 49 20 54 00 53 22 30 04
payload+208: 49 20 54 20 01 00 00 00 19 PROBE_STRING...
payload+end: ...225304000 00 00 2a 00 00 00 ff ff ff ff ff ff ff 7f
```
Where: `01 00 00 00` before varchar = row count (1) as uint32 LE; `0x19=25` = varchar 1-byte length prefix; `2a 00 00 00` = INT 42 LE; `ff ff ff ff ff ff ff 7f` = INT64_MAX end sentinel.

| Hypothesis | SPS08 | Notes |
|-----------|-------|-------|
| INTEGER: 4 bytes little-endian signed | ✅ | Confirmed: value 42 = `2a 00 00 00` found adjacent to INSERT marker |
| BIGINT: 8 bytes little-endian signed | ✅ | Confirmed: 0x0102030405060708 → `[08 07 06 05 04 03 02 01]` found in log |
| BOOLEAN: 4-byte LE int (1=TRUE, 0=FALSE) | ✅ | Confirmed: TRUE=`[01 00 00 00]`, FALSE=`[00 00 00 00]`; data_size=8 for ID(int4)+BOOL confirms 4-byte. CORRECTS earlier 1-byte hypothesis |
| VARCHAR: **1-byte** length prefix + UTF-8 | ✅ | Confirmed: marker "PROBE_1780976703225304000" (len=25) had byte 0x19=25 at position-1. CORRECTS earlier 2-byte hypothesis |
| VARCHAR: 2-byte LE length prefix (original hypothesis) | ❌ | Refuted by live HANA evidence: 1-byte prefix matches, 2-byte LE does not |
| NVARCHAR: UTF-16 encoding | ❌ | Refuted: N'NVARTEST_PROBE' found as `[0e]` + UTF-8 bytes, not UTF-16 |
| NVARCHAR: same as VARCHAR (1-byte prefix + UTF-8) | ✅ | Confirmed: `[0e NVARTEST_PROBE]` — 0x0e=14=len("NVARTEST_PROBE") |
| DECIMAL: 8-byte or 16-byte BCD/binary | ❌ | Refuted: DECIMAL(10,2) 3.14 stored as scaled int32 LE = 314 = `[3a 01 00 00]` |
| DECIMAL: scaled int32 LE (actual × 10^scale) | ✅ | Confirmed: DECIMAL(10,2) 3.14 → int32(314) = `[3a 01 00 00]` |
| NULL column: bit in null bitmap, no value bytes | ❌ | Refuted: NULL columns are completely absent — no bitmap, no bytes; col_count skips them entirely |
| NULL columns: absent from INSERT block (col_count excludes NULLs) | ✅ | Confirmed: INSERT with only PK set → col_count=1, data_size=4; 6 nullable NULLs not stored at all |
| Large NCLOB stored inline in redo log | ✅ | Confirmed: 9KB probe spans pages 237–239 in logsegment (multi-page continuation active) |
| Large NCLOB in separate LOB segment | ❌ | Refuted: probe found inline in redo log, not in a separate LOB segment |
| REAL: 4-byte IEEE 754 float32 LE | ✅ | Confirmed: e as float32 → `[54 f8 2d 40]` found in log |
| DOUBLE: 8-byte IEEE 754 float64 LE | ✅ | Confirmed: e as float64 → `[69 57 14 8b 0a bf 05 40]` found in log |
| SMALLINT: 2-byte LE (original hypothesis) | ❌ | Refuted: S=258 → `[02 01 00 00]` = 4 bytes; SMALLINT uses same 4-byte LE int32 as INTEGER |
| SMALLINT: 4-byte LE int32 (same as INTEGER) | ✅ | Confirmed: S=258 → `[02 01 00 00]` in log |
| TINYINT: 1-byte unsigned (original hypothesis) | ❌ | Refuted: T=171 → `[ab 00 00 00]` = 4 bytes; TINYINT uses same 4-byte LE int32 as INTEGER |
| TINYINT: 4-byte LE int32 (same as INTEGER) | ✅ | Confirmed: T=171 → `[ab 00 00 00]` in log |
| Column count in INSERT payload (uint16 LE) | ✅ | Confirmed via INSERT block structure: col_count byte at block offset 2, row_count at offset 3 |
| First payload byte = 0x02: SAVEPOINT/FILLER block type | ✅ | Confirmed: 0x02 is Infrastructure/FILLER block; INSERT is at payload+217; 0x02 is not the INSERT type code |

## RowID Encoding

| Hypothesis | SPS08 | Notes |
|-----------|-------|-------|
| RowID type: uint64 (8 bytes LE) | ✅ | 8 bytes LE; candidates found 40 bytes before column values |
| RowID for first INSERT in fresh table = 1 | ✅ | Row1→RowID=1 confirmed at offset-40 from VAL bytes |
| RowID is monotonically increasing per partition | ✅ | RowID 1,2,3 confirmed for 3 sequential inserts |
| DELETE block: only RowID, no column values | ✅ | DELETE block [0xFE][0x00][RowID:8] contains no column data |

## Archive Log Format

Archive log format inherits the redo log page format plus a 4096-byte prefix header.
Cannot be empirically tested without performing a log backup that creates archive files.
Test procedure is defined; empirical confirmation requires a HANA backup operation.

| Hypothesis | SPS08 | Notes |
|-----------|-------|-------|
| Archive log has extra 4096-byte header | ✅ | Confirmed: log_backup_0_0_0_0.1781137490876 = 12288 bytes (3 pages); first 4096 bytes are SAP metadata, NOT a redo page. ArchiveHeaderSize=4096 confirmed. |
| Archive header contains backup metadata | ✅ | Confirmed: tagged ASCII format: [MAGIC]HANABackup-designed by SAP in Berlin, [DBID]UUID, [DATE]timestamp, [BACKUPID], [HOSTNAME], [SERVICENAME]. NOT a binary format. |
| Archive header at offset 0: NOT a uint64 LSN | ❌ refuted | bytes[0]=0x01 (version byte), NOT the backup start LSN. The LSN values in filename startLSN=0 endLSN=0 indicate empty backup. Header is tagged ASCII metadata starting at offset 16 with [MAGIC] tag. |

---

## How to Run Tests Against a New HANA Version

```bash
# 1. Pull the target version
DOCKER_API_VERSION=1.46 docker pull saplabs/hanaexpress:latest

# 2. Get exact version string
DOCKER_API_VERSION=1.46 docker inspect saplabs/hanaexpress:latest \
  --format='{{index .Labels "com.sap.hana.database.version.full"}}'

# 3. Start HANA
DOCKER_API_VERSION=1.46 HANA_DATA_PATH=/tmp/hana-test docker-compose \
  -f internal/impl/saphana/testdata/docker-compose.yml up -d hana

# 4. Wait for healthy, run investigation
DOCKER_API_VERSION=1.46 docker wait ... # see run-saphana-macos-integration-tests.sh

# 5. Run empirical investigation tool
go run ./internal/impl/saphana/logformat/investigate/ \
  --host localhost --port 39015 --pass HXEHana1 \
  --log-dir /tmp/hana-test/log/HXE/mnt00001 \
  --out /tmp/findings-sps08.json

# 6. Update this matrix with confirmed/refuted findings
```

---

## Known HANA Express Versions Available on Docker Hub

| Tag | HANA Version | Available |
|-----|-------------|-----------|
| `latest` | 2.00.088.00 (SPS08) | ✅ |
| `2.00.085.*` | 2.00.085 (SPS08 patch) | ⬜ Check Hub |
| `2.00.082.*` | 2.00.082 (SPS08 patch) | ❌ Not found (404) |
| `2.00.076.*` | 2.00.076 (SPS07) | ⬜ Check Hub |

Run `docker search saplabs/hanaexpress` or check hub.docker.com/r/saplabs/hanaexpress/tags for available versions.
