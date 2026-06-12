# SAP HANA Redo Log Format — UPSERT Block

> **Status: REVERSE-ENGINEERED** — Not an official SAP specification. See `00_overview.md` for sources.

---

## Status

**CONFIRMED** (behavior) / **INFERRED** (payload structure) — Empirical analysis confirms
that UPSERT on a new row uses INSERT type 0x81 in the redo log. UPSERT on an existing row
is likely decomposed into DELETE(0xFE) + INSERT(0x81), consistent with UPDATE behavior.
Payload structure is inferred from confirmed INSERT encoding.

---

## Log Block Type Identifier

**Confirmed (HANA 2.00.088.00 SPS08):** UPSERT on a new row (INSERT path) uses block type
`0x81` — identical to INSERT. UPSERT on an existing row (UPDATE path) is consistent with
UPDATE behavior: likely DELETE(0xFE) + INSERT(0x81).

The `BlockTypeUpsert = 0xFC` placeholder in `types.go` does not match confirmed behavior.
CDC readers should handle UPSERT as:
- New row: INSERT(0x81)
- Existing row: DELETE(0xFE) + INSERT(0x81) (same as UPDATE)

---

## What SQL Generates This Block

```sql
UPSERT schema.table (col1, col2, ...) VALUES (...) WITH PRIMARY KEY;
-- or equivalently:
REPLACE schema.table (col1, col2, ...) VALUES (...);
```

SAP HANA's `UPSERT` / `REPLACE` statement performs an INSERT if the primary key does not
exist, or an UPDATE if it does. **In the redo log, a new-row UPSERT appears as INSERT (0x81);
an existing-row UPSERT appears as DELETE(0xFE) + INSERT(0x81)**, consistent with UPDATE behavior.

---

## AFTER Triggers

**Implementation-defined** — Whether AFTER INSERT, AFTER UPDATE, or a combined AFTER UPSERT
trigger fires depends on the HANA trigger system version and the actual execution path.
Source: SAP HANA SQL Reference (trigger behavior).

---

## Payload Structure

**Confirmed (INSERT path, HANA 2.00.088.00 SPS08):** UPSERT on a new row produces a block
with the same structure as INSERT (type 0x81):

| Field | Type | Size | Notes |
|-------|------|------|-------|
| Block header | bytes | 4 bytes | `[0x81][0x00][col_count][row_count]` |
| Column descriptor | bytes | 4 bytes | `49 20 54 04` preceding 4-byte column values |
| RowID | uint64 LE | 8 bytes | New RowID assigned for this row; located 40 bytes before column values. |
| ColumnValues | byte[] | variable | All column values (same encoding as INSERT). |

**UPSERT existing row (inferred from UPDATE behavior):** The existing-row path likely produces
DELETE(0xFE) + INSERT(0x81). OldRowID is carried in the DELETE block; no separate OldRowID
field in the INSERT block. This is consistent with confirmed UPDATE decomposition.

**Genuinely unknown:** Whether the existing-row UPSERT produces a structurally distinct block
or is byte-for-byte identical to DELETE + INSERT.

---

## Key CDC Implications

1. **Do not decompose at parse time**: A CDC reader should emit UPSERT as its own operation
   type and let the downstream consumer decide whether to implement it as INSERT-or-UPDATE.
2. **Row cache**: If the OldRowID is present, it should be used to update the row cache
   (remove old entry, add new entry). If not present, it is a new-insert UPSERT.
3. **Idempotency**: UPSERT semantics are naturally idempotent on the primary key, which is
   useful for CDC consumers implementing at-least-once delivery.

---

## Verified on HANA 2.00.088.00 SPS08

**Confirmed:**
- UPSERT new row = INSERT (block type 0x81); same structure as a regular INSERT.
- Block header: `[0x81][0x00][col_count][row_count]`.
- RowID uint64 LE located 40 bytes before column values.
- Column descriptor `49 20 54 04` precedes 4-byte column values.

**Inferred (consistent with confirmed UPDATE behavior):**
- UPSERT on existing row = DELETE(0xFE) + INSERT(0x81); no separate UPSERT type code.

**Genuinely unknown:**
- Whether existing-row UPSERT has any structural difference from plain UPDATE in the log.

---

## Sources

- SAP HANA SQL Reference (UPSERT/REPLACE statement): https://help.sap.com/docs/SAP_HANA_PLATFORM/4fe29514fd584807ac9f2a04f6754767/
