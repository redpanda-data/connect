# SAP HANA Redo Log Format — File Layout

> **Status: REVERSE-ENGINEERED** — Not an official SAP specification. See `00_overview.md` for sources.

---

## Online Redo Log Files

### Segment File Naming

```
logsegment_<partitionID>_<NNN>.dat
```

| Field | Type | Notes |
|-------|------|-------|
| `partitionID` | zero-padded decimal | Identifies the database partition (volume). Starts at `000`. |
| `NNN` | zero-padded decimal | Segment sequence number within the partition. |

**Examples:**
```
logsegment_000_000090.dat   — partition 0, segment 90
logsegment_001_000001.dat   — partition 1, segment 1
```

### Directory File Naming

```
logsegment_<partitionID>_directory.dat
```

One directory file per partition. Contains the segment index using shadow paging.
See `03_directory_file.md` for the binary layout of this file.

---

## Archive (Backup) Log Files

### Archive Log File Naming

```
log_backup_<VolumeID>_<PartitionID>_<FirstLSN>_<LastLSN>[.<BackupID>]
```

| Field | Type | Notes |
|-------|------|-------|
| `VolumeID` | decimal integer | Service volume identifier. Typically `1` for the index server. |
| `PartitionID` | decimal integer | Partition within the volume. Typically `0` for single-node. |
| `FirstLSN` | decimal uint64 | First Log Sequence Number in this segment. |
| `LastLSN` | decimal uint64 | Last Log Sequence Number in this segment. |
| `BackupID` | decimal int64 | Optional. Millisecond epoch timestamp of the backup. Absent for some formats. |

**Worked Example:**

```
log_backup_1_0_538414336_538623872.1415905765532
```

Parsed:
- VolumeID = 1
- PartitionID = 0
- FirstLSN = 538,414,336 (0x201D5780)
- LastLSN = 538,623,872 (0x20207B80)
- BackupID = 1,415,905,765,532 ms (Unix epoch ≈ 2014-11-13 UTC)

LSN range size: 538,623,872 − 538,414,336 = 209,536 bytes = ~204 KB (sub-segment granularity possible for compressed backups).

**LSN Note:** Archive filenames use decimal; HANA trace output uses hex with `0x` prefix.
The same LSN `0x70cb3180` = 1,892,532,608 decimal. LSNs are confirmed >32 bits: an observed
example `0x26524f683` = 10,286,855,811, which requires uint64.

---

## Online vs Archive Format

| Property | Online Segment | Archive Log |
|----------|---------------|-------------|
| Extra header | None — starts at byte 0 with a redo page | 4096-byte archive header prepended |
| Page content | Identical | Identical (after skipping the 4KB header) |
| Source | Empirical observation | Confirmed — HANA 2.00.088.00 SPS08 |

The archive header is **exactly 4096 bytes** (one page; confirmed — equals the page size,
`ArchiveHeaderSize = 4096`). The first 4096 bytes are a **tagged ASCII header** (NOT binary):
the header contains newline-separated tagged fields beginning with ASCII tags such as:
- `[MAGIC]HANABackup - designed by SAP in Berlin`
- `[DBID]<UUID>`
- `[DATE]<timestamp>`

Redo log pages begin at byte 4096. The internal byte layout of this header's binary portions
(VolumeID/PartitionID/FirstLSN/LastLSN/BackupID offsets and checksum) remains undecoded beyond
the ASCII tagged region.

---

## SYS.M_LOG_SEGMENTS — Runtime Metadata

When HANA is running, segment metadata is queryable via SQL:

```sql
SELECT
    VOLUME_ID,
    PARTITION_ID,
    SEGMENT_ID,
    FILE_NAME,
    MIN_POSITION,   -- FirstLSN (BIGINT)
    MAX_POSITION,   -- LastLSN  (BIGINT)
    STATE,          -- 'Open', 'Closed', 'Free', 'BackedUp', etc.
    USED_SIZE,
    TOTAL_SIZE
FROM SYS.M_LOG_SEGMENTS
ORDER BY VOLUME_ID, PARTITION_ID, SEGMENT_ID;
```

Source: https://help.sap.com/docs/SAP_HANA_PLATFORM/4fe29514fd584807ac9f2a04f6754767/20b65c1275191014b23ac1bfd7a95e19.html

The `MIN_POSITION` and `MAX_POSITION` columns are BIGINT (signed 64-bit), matching the uint64
LSN type. The `STATE` column values correspond to the `SegmentState` enum in `types.go`.

---

## Log Buffer Flush Lifecycle

1. HANA maintains 8 in-memory log buffers × 1024 KB each (configurable via `log_buffer_count` and `log_buffer_size_kb`).
2. Writes accumulate in the active buffer; the buffer is flushed to disk on COMMIT or when full.
3. Each flush writes one or more complete 4KB pages. Partial pages are padded with FILLER entries.
4. Segments are pre-allocated and reused in round-robin fashion after being freed (backed up + log position advanced past all open transactions).

The segment lifecycle state machine:
```
Preallocated → Formatting → Open → Closed → BackedUp → Free → RetainedFree
                                         ↘ ClosedIncomplete → FreeIncomplete
```

---

## Verified on HANA 2.00 SPS08

**Confirmed:**
- Online segment naming `logsegment_<partitionID>_<NNN>.dat`.
- Directory file naming `logsegment_<partitionID>_directory.dat`, one per partition.
- Archive log filename grammar and field types (VolumeID, PartitionID, FirstLSN, LastLSN, BackupID).
- LSNs are uint64 (observed `0x26524f683` = 10,286,855,811 exceeds 32 bits).
- Archive log prepends exactly 4096 bytes before the first redo page; online segments start at byte 0.
- `SYS.M_LOG_SEGMENTS` column set and BIGINT LSN columns.

**Confirmed (updated):**
- Archive log first 4096 bytes = tagged ASCII header (NOT binary). Contains `[MAGIC]HANABackup - designed by SAP in Berlin`, `[DBID]UUID`, `[DATE]timestamp`, etc. Redo pages start at byte 4096.

**Genuinely unknown:**
- Binary field offsets within the archive header's tagged ASCII region (VolumeID, PartitionID, FirstLSN, LastLSN, BackupID as binary integers).
