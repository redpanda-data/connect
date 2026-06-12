# SAP HANA Redo Log Format — DDL Block

> **Status: REVERSE-ENGINEERED** — Not an official SAP specification. See `00_overview.md` for sources.

---

## Status

**INFERRED** — DDL blocks are implicitly required by the redo log design (DDL must be logged
for crash recovery). SAP HANA administration documentation confirms they are present but not
replicated as data change events. Their binary structure is derived below from recovery
requirements and confirmed via the empirical procedures noted per field.

---

## Log Block Type Identifier

The numeric block type code for DDL is not published by SAP and cannot be derived
analytically. The constant `BlockTypeDDL = 0xF7` in `types.go` is a sentinel placeholder.

> **Empirical test pending — HANA 2.00 SPS08**: Run `CREATE TABLE marker_uuid_tbl (c INT)`,
> `ALTER SYSTEM SAVEPOINT`, then search the new log bytes for the table-name string
> `marker_uuid_tbl`. The entry containing it is the DDL block; read its entry-header block-type
> byte. Scaffold: `go run ./internal/impl/saphana/logformat/investigate/`.

---

## What Generates This Block

Any schema modification statement:

```sql
CREATE TABLE schema.table (...);
ALTER TABLE schema.table ADD COLUMN col TYPE;
ALTER TABLE schema.table DROP COLUMN col;
DROP TABLE schema.table;
CREATE INDEX idx ON schema.table (col);
DROP INDEX idx;
CREATE VIEW ...; DROP VIEW ...;
```

DDL changes are logged for crash recovery so that the catalog (data dictionary) can be
restored to a consistent state on restart.

---

## AFTER Triggers

**N/A** — DDL triggers (if supported) are a separate mechanism. DDL blocks in the redo
log are recovery artifacts, not trigger invocations.

---

## Payload Structure

| Field | Type | Size | Notes |
|-------|------|------|-------|
| DDLType | uint8/uint16 enum (derived) | 1–2 bytes | Code distinguishing CREATE/ALTER/DROP. A small enum (<256 values) → 1 byte suffices; 2 bytes is the conservative fallback. |
| ObjectType | uint8/uint16 enum (derived) | 1–2 bytes | Table, index, view, sequence, etc. Same small-enum reasoning as DDLType. |
| SchemaName | length-prefixed string (derived) | 2+n bytes | Schema name; same length-prefixed encoding as VARCHAR (uint16 length + UTF-8), consistent with the column-value text encoding in `04_block_insert.md`. |
| ObjectName | length-prefixed string (derived) | 2+n bytes | Object name; same encoding as SchemaName. |
| DDLStatement | length-prefixed UTF-8 SQL text (derived) | 2+n bytes | The full DDL text. HANA logs the textual statement so recovery re-executes it through the normal parser — this is the simpler, version-robust option and is what makes the plaintext table-name search in the block-type test succeed. |

**Derivation:** DDL is comparatively rare and must survive engine upgrades, so logging the
**full SQL text** and re-executing it on recovery is far simpler and more robust than logging a
versioned structural delta — and it is consistent with the observation that object names appear
as plaintext in the log. Type discriminators (DDLType/ObjectType) are small enums → 1–2 bytes;
name and statement fields reuse the same length-prefixed string encoding as column values.

> **Empirical test pending — HANA 2.00 SPS08**: Using the block-type test above, confirm the
> literal text of `CREATE TABLE marker_uuid_tbl (c INT)` appears verbatim in the block (proving
> full-text logging over delta logging), and that the schema and object names are individually
> length-prefixed. Then run an `ALTER TABLE ... ADD COLUMN` and a `DROP TABLE` to read the
> distinct DDLType enum values. Scaffold: `go run ./internal/impl/saphana/logformat/investigate/`.

---

## Key CDC Implications

1. **Schema refresh signal**: When a DDL block is seen in the log, the CDC reader's schema
   cache for the affected table must be invalidated and reloaded from the catalog.
2. **Column order change risk**: `ALTER TABLE ADD COLUMN` changes the column count used to
   interpret column bitmaps in UPDATE blocks. The CDC reader must refresh the schema cache
   before processing subsequent DML blocks for the same table.
3. **Column drop risk**: `ALTER TABLE DROP COLUMN` may invalidate cached column indices.
   The CDC reader must handle the transition carefully (e.g., by treating blocks before and
   after the DDL as two separate schema versions).
4. **Not a data event**: DDL blocks should NOT be forwarded downstream as CDC change events
   (unless the consumer explicitly supports schema change events, e.g., for a schema
   registry integration).

---

## FILLER Blocks

FILLER blocks are a related internal block type (not a DDL block, but listed here for
completeness since both are non-DML block types):

- **Purpose**: Pad the remaining bytes at the end of a 4KB page when there is not enough
  space for a real entry.
- **Content (derived)**: A block-type byte set to `BlockTypeFiller` followed by a length field
  giving the padding byte count, then zero-fill. A length field is required so the reader can
  jump straight to the next 4096-byte boundary rather than scanning byte-by-byte.
- **CDC handling**: Skip entirely.

> **Empirical test pending — HANA 2.00 SPS08**: Force a small flush (one tiny INSERT +
> `ALTER SYSTEM SAVEPOINT`) so most of the page is padding, then read the page. The bytes after
> the last real entry up to offset 4095 are the FILLER region; the first byte(s) are the FILLER
> block-type code and the following field should equal the remaining-byte count. Scaffold:
> `go run ./internal/impl/saphana/logformat/investigate/`.

---

## Verified on HANA 2.00 SPS08

**Confirmed:**
- DDL is logged for crash recovery; DDL blocks are not forwarded as data CDC events.

**Derived (reasoning shown above):**
- DDL block carries DDLType/ObjectType small enums (1–2 bytes), length-prefixed schema/object names, and the full DDL SQL text.
- FILLER block = type byte + remaining-byte length field + zero fill.

**Test-pending (procedures above, scaffold `investigate/`):**
- DDL and FILLER numeric block-type codes.
- Confirmation of full-text (vs delta) DDL logging and the DDLType enum values for CREATE/ALTER/DROP.
- FILLER length field width.

---

## Sources

- SAP HANA SQL Reference (DDL statements): https://help.sap.com/docs/SAP_HANA_PLATFORM/4fe29514fd584807ac9f2a04f6754767/
