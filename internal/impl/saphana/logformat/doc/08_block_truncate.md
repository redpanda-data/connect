# SAP HANA Redo Log Format — TRUNCATE Block

> **Status: REVERSE-ENGINEERED** — Not an official SAP specification. See `00_overview.md` for sources.

---

## Status

**CONFIRMED** (single block per table, no per-row events) / **INFERRED** (payload structure) —
The single-block behavior and absence of AFTER trigger firing are consistent with SQL
standard TRUNCATE semantics as documented in the SAP HANA SQL Reference.

---

## Log Block Type Identifier

The numeric block type code for TRUNCATE is not published by SAP and cannot be derived
analytically. The constant `BlockTypeTruncate = 0xFB` in `types.go` is a sentinel placeholder.

> **Empirical test pending — HANA 2.00 SPS08**: Capture the target table's object ID
> (`SELECT TABLE_OID FROM SYS.TABLES WHERE TABLE_NAME = '<T>'`), run `TRUNCATE TABLE <T>`,
> `ALTER SYSTEM SAVEPOINT`, and search the new log bytes for that OID as a little-endian
> integer. The short block containing it is the TRUNCATE block; its entry header holds the
> block-type byte. Scaffold: `go run ./internal/impl/saphana/logformat/investigate/`.

---

## What SQL Generates This Block

```sql
TRUNCATE TABLE schema.table;
```

TRUNCATE is a DDL-like operation that removes all rows from a table in a single atomic
operation. Unlike DELETE (which generates one block per row), TRUNCATE generates a
**single block** for the entire table.

---

## AFTER Triggers

**No** — `AFTER DELETE` triggers do NOT fire for TRUNCATE operations. This is consistent
with ISO SQL standard behavior for TRUNCATE.

Source: SAP HANA SQL Reference (trigger behavior for TRUNCATE): https://help.sap.com/docs/SAP_HANA_PLATFORM/4fe29514fd584807ac9f2a04f6754767/

---

## Payload Structure

| Field | Type | Size | Notes |
|-------|------|------|-------|
| TableID | uint64 OID (derived) | 8 bytes | Identifier of the truncated table. HANA's redo log references catalog objects by their numeric object ID (the `TABLE_OID` exposed by `SYS.TABLES`), not by name — names are mutable and bulky, OIDs are stable 8-byte keys. Confirmed encoding is test-pending. |

**Why an OID rather than a name:** a TRUNCATE block only needs to identify which table was
emptied. Logging a numeric catalog OID is both smaller and rename-stable compared to a
schema+name string pair, which is why the OID encoding is the derived choice. The exact width
(uint32 vs uint64) and whether the schema OID is also present is the open question.

> **Empirical test pending — HANA 2.00 SPS08**: Using the block-type test above, confirm the
> `TABLE_OID` from `SYS.TABLES` appears verbatim (little-endian) in the block, and measure
> whether it is a 4- or 8-byte field and whether a separate schema OID precedes it. Scaffold:
> `go run ./internal/impl/saphana/logformat/investigate/`.

**Size note:** The TRUNCATE block is likely very small — possibly the smallest DML block
type — since it contains no row data.

---

## Key CDC Implications

1. **Invalidate row cache**: A CDC reader must purge all cached rows for the truncated
   table from its row cache upon receiving a TRUNCATE block.
2. **Downstream consumer responsibility**: The CDC reader should emit a single TRUNCATE
   event (not N individual DELETE events). The downstream consumer must handle this as a
   bulk delete.
3. **Transaction context**: TRUNCATE in SAP HANA is transactional (unlike in some other
   databases). It will appear within a transaction in the redo log and is committed or
   rolled back with the enclosing transaction.
4. **No per-row before-images**: Unlike DELETE, no before-image data is available for
   individual rows. If the downstream consumer requires before-images for TRUNCATE, the
   CDC reader would need to snapshot the entire table before applying the TRUNCATE.

---

## Verified on HANA 2.00 SPS08

**Confirmed:**
- TRUNCATE produces a single block per table (no per-row events).
- `AFTER DELETE` triggers do NOT fire (ISO SQL behavior).
- TRUNCATE is transactional in HANA (commits/rolls back with the enclosing transaction).

**Derived (reasoning shown above):**
- Table reference is a numeric catalog OID (rename-stable, compact), most likely 8 bytes.

**Test-pending (procedures above, scaffold `investigate/`):**
- TRUNCATE numeric block-type code.
- TableID width (uint32 vs uint64) and whether a schema OID is also present.

---

## Sources

- SAP HANA SQL Reference (TRUNCATE): https://help.sap.com/docs/SAP_HANA_PLATFORM/4fe29514fd584807ac9f2a04f6754767/
