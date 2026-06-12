# SAP HANA Redo Log Format — SAVEPOINT Block

> **Status: REVERSE-ENGINEERED** — Not an official SAP specification. See `00_overview.md` for sources.

---

## Status

**PARTIALLY CONFIRMED** — `ALTER SYSTEM SAVEPOINT` produces pages starting with infrastructure
blocks of type `0x02`. The specific savepoint block type was not isolated to a distinct code
beyond `0x02`; pages with savepoints start with `02 00 00 10 a1 13 81 ea...` (16-byte
infrastructure blocks). Payload field offsets remain undecoded beyond this observation.

---

## Log Block Type Identifier

**Confirmed (HANA 2.00.088.00 SPS08):** `ALTER SYSTEM SAVEPOINT` produces infrastructure
blocks of type `0x02`. Pages with savepoints start with `02 00 00 10 a1 13 81 ea...`
(16-byte infrastructure blocks). Infrastructure type `0x02` is used for savepoint-related
entries; a savepoint record is embedded in the page infrastructure.

The `BlockTypeSavepoint = 0xF8` placeholder in `types.go` does not correspond to a confirmed
type code — the observed code for infrastructure/savepoint blocks is `0x02`.

---

## What Generates This Block

SAVEPOINT blocks are written by the HANA persistence layer, NOT by SQL statements. Two
categories:

1. **Internal system savepoints** — Written approximately every `savepoint_interval_s`
   seconds (default: 300 seconds = 5 minutes). These mark a consistent recovery point
   for crash recovery. At a savepoint, all dirty data pages have been flushed to the data
   area; the log only needs to be replayed from the savepoint LSN on restart.

2. **SQL savepoints** — Written when a `SAVEPOINT <name>` statement is executed within a
   transaction:
   ```sql
   SAVEPOINT my_savepoint;
   -- ... DML ...
   ROLLBACK TO SAVEPOINT my_savepoint;
   ```

---

## AFTER Triggers

**N/A** — SAVEPOINT is an internal persistence block. No application triggers are associated.

---

## Payload Structure

**Observed (HANA 2.00.088.00 SPS08):** Pages produced by `ALTER SYSTEM SAVEPOINT` start with:
```
02 00 00 10 a1 13 81 ea ...
```
This is a 16-byte infrastructure block (type `0x02`). The savepoint record is embedded within
this infrastructure block. The internal field offsets (SavepointLSN, DataPageFlushLSN,
SavepointID) within the 16-byte block payload have not been decoded.

**Inferred fields** (from crash-recovery requirements — offsets unknown):

| Field | Type | Size | Notes |
|-------|------|------|-------|
| SavepointLSN | uint64 | 8 bytes | The LSN at which this savepoint was written. LSNs are confirmed uint64. |
| DataPageFlushLSN | uint64 | 8 bytes | The LSN up to which data pages have been flushed. Recovery replays only from this LSN. |

The SavepointID (name for SQL savepoints) is not confirmed present in system savepoint blocks.

---

## Key CDC Implications

1. **Not a CDC event**: Savepoint blocks do not represent data changes and should be
   silently skipped by CDC readers.
2. **Recovery position marker**: For a CDC reader that checkpoints its own log position,
   system savepoint blocks are useful — they confirm that all prior log positions have
   their data permanently flushed. If the CDC reader's process crashes, it only needs to
   replay from the most recent savepoint LSN.
3. **SQL savepoint rollback**: If a `ROLLBACK TO SAVEPOINT` is later encountered, the CDC
   reader must discard all events buffered after the corresponding SAVEPOINT block's LSN
   (for the same transaction).

---

## Savepoint Interval Configuration

```sql
-- Query current savepoint interval
SELECT KEY, VALUE FROM SYS.M_INIFILE_CONTENTS
WHERE FILE_NAME = 'global.ini'
  AND SECTION = 'persistence'
  AND KEY = 'savepoint_interval_s';

-- Default: 300 (5 minutes)
```

Source: SAP HANA Administration Guide — https://help.sap.com/docs/SAP_HANA_PLATFORM/6b94445c94ae495c83a19646e7c3fd56/

---

## Verified on HANA 2.00 SPS08

**Confirmed (HANA 2.00.088.00 SPS08):**
- System savepoints written ~every `savepoint_interval_s` (default 300s); SQL savepoints on `SAVEPOINT <name>`.
- Savepoint blocks are not CDC data events (skip them).
- `ALTER SYSTEM SAVEPOINT` produces infrastructure blocks type `0x02`; pages start with `02 00 00 10 a1 13 81 ea...`.
- Infrastructure type `0x02` is the savepoint-related block type code.

**Genuinely unknown:**
- Exact offsets of SavepointLSN / DataPageFlushLSN within the 16-byte infrastructure block payload.
- Whether SQL `SAVEPOINT <name>` produces a distinct block or the same `0x02` infrastructure type.

---

## Sources

- SAP HANA Administration Guide (savepoint interval): https://help.sap.com/docs/SAP_HANA_PLATFORM/6b94445c94ae495c83a19646e7c3fd56/
