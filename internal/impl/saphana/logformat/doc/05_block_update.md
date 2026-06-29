# SAP HANA Redo Log Format — UPDATE Block

> **Status: REVERSE-ENGINEERED** — Not an official SAP specification. See `00_overview.md` for sources.

---

## Status

**CONFIRMED** (behavior) / **INFERRED** (exact encoding) — The batched before/after RowID
structure is empirically observed. The column bitmap and value encoding details
are inferred from the logical description.

---

## Log Block Type Identifier

**Confirmed (HANA 2.00.088.00 SPS08):** UPDATE is NOT a distinct block type in the redo log.
An UPDATE is decomposed into DELETE(0xFE) + INSERT(0x81) at the log level. There is no
separate UPDATE type code. The `BlockTypeUpdate = 0xFE` placeholder in `types.go` is
superseded by this finding — UPDATE should be parsed as DELETE followed by INSERT.

---

## What SQL Generates This Block

```sql
UPDATE schema.table SET col1 = val1, col2 = val2 WHERE ...;
```

Important: SAP HANA does NOT use supplemental logging for UPDATE. Only the **changed
columns** are written to the log — not all columns. A CDC reader needing the full before-
image must maintain a row cache populated from prior INSERT/UPDATE blocks.

---

## AFTER Triggers

**Yes** — `AFTER UPDATE` triggers fire for UPDATE operations.

---

## Batch Encoding — Key Discovery

**Confirmed (HANA 2.00.088.00 SPS08):** UPDATE decomposes into DELETE(0xFE) + INSERT(0x81)
in the redo log. There is no separate UPDATE block type. Each updated row produces:
1. A DELETE block carrying the old RowID (uint64 LE).
2. An INSERT block carrying the new RowID and all new column values.

There is no separate UPDATE batch count in the log — the DELETE+INSERT pair is the atomic
unit. Column bitmap is not present in the DELETE block; the full new row values are in the
INSERT block.

---

## Payload Structure

**Confirmed encoding (HANA 2.00.088.00 SPS08):**

UPDATE in the redo log is DELETE + INSERT — not a single block with a batch count. Each
pair covers one updated row:

| Block | Type | Payload |
|-------|------|---------|
| DELETE | 0xFE | `[0xFE][0x00][OldRowID:uint64 LE]` |
| INSERT | 0x81 | `[0x81][0x00][col_count][row_count]` + column descriptor `49 20 54 04` + new values |

There is no separate batch count field or column bitmap in the DELETE block. The INSERT block
carries all new column values (not just changed columns). Column bitmap position in the
log is not applicable — the full row is written in the INSERT block.

The prior "batch format" model (N before-RowIDs → 1 after-RowID + column bitmap) does NOT
match confirmed HANA 2.00.088.00 SPS08 behavior. CDC readers should parse UPDATE as
DELETE(RowID) + INSERT(full new row).

---

## MVCC and RowID Semantics for UPDATE

In HANA's columnar store, an UPDATE is implemented as a logical delete of the old row
version and a physical insert of the new row version (MVCC). This means:
- The "before RowID" is the RowID of the version being superseded.
- The "after RowID" is the RowID of the newly written version.
- The RowID space is NOT stable across updates — after-RowID ≠ before-RowID.
- A CDC reader must update its row cache: remove before-RowIDs, add after-RowID.

Source: Empirical observation (before/after RowID structure); SAP HANA internal analysis (MVCC implementation).

---

## Key CDC Implications

1. **No before-image in the log**: Only changed columns are written. To produce before/after
   full-row images, the CDC reader must maintain a row cache.
2. **Batch updates**: A single SQL UPDATE may produce multiple UPDATE blocks. The CDC reader
   must accumulate all blocks within a transaction before emitting change events, or
   emit partial updates and handle downstream deduplication.
3. **RowID instability**: After an UPDATE, the RowID changes. DELETE blocks for the updated
   row will reference the *after* RowID, not the before RowID.
4. **Schema dependency**: Parsing the column bitmap requires knowing the table's column count
   and ordering. The CDC reader needs a schema cache populated from DDL events or initial
   catalog scan.
5. **N:1 RowID ratio**: The N before-RowIDs to 1 after-RowID means a batch update of N rows
   in HANA's engine results in N new physical row versions but only one "new logical row."
   This is an internal HANA optimization detail; CDC readers should emit N individual UPDATE
   events (one per before-RowID → after-RowID pair).

---

## Verified on HANA 2.00 SPS08

**Confirmed (HANA 2.00.088.00 SPS08):**
- UPDATE decomposes into DELETE(0xFE) + INSERT(0x81) in the redo log; no separate UPDATE type code.
- `AFTER UPDATE` triggers fire.
- MVCC: after-RowID ≠ before-RowID (new physical version).
- No separate UPDATE batch count in log — DELETE+INSERT pair is the unit.
- Column bitmap is NOT present in the DELETE block; full new row values are in the INSERT block.

**Genuinely unknown:**
- Whether multiple-row UPDATEs produce multiple DELETE+INSERT pairs or a batched form.
- Exact byte cap per batch (if batching occurs).

---

## Sources

- SAP HANA SQL Reference: https://help.sap.com/docs/SAP_HANA_PLATFORM/4fe29514fd584807ac9f2a04f6754767/
- SAP SYS.M_LOG_SEGMENTS view: https://help.sap.com/docs/SAP_HANA_PLATFORM/4fe29514fd584807ac9f2a04f6754767/20b65c1275191014b23ac1bfd7a95e19.html
