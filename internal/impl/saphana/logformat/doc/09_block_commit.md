# SAP HANA Redo Log Format — COMMIT Block

> **Status: REVERSE-ENGINEERED** — Not an official SAP specification. See `00_overview.md` for sources.

---

## Status

**CONFIRMED** — COMMIT blocks demarcate transaction boundaries and contain a commit
timestamp. The MVCC timestamp encoding is confirmed by internal analysis.

---

## Log Block Type Identifier

The numeric block type code for COMMIT is not published by SAP and cannot be derived
analytically. The constant `BlockTypeCommit = 0xFA` in `types.go` is a sentinel placeholder.

> **Empirical test pending — HANA 2.00 SPS08**: In an explicit transaction, INSERT one
> UUID-marked row, then `COMMIT`, then `ALTER SYSTEM SAVEPOINT`. The block immediately following
> the INSERT block (located via the marker) is the COMMIT block; read its entry-header
> block-type byte. Cross-check the COMMIT timestamp field against
> `SELECT COMMIT_ID FROM SYS.M_TRANSACTIONS` taken around the commit. Scaffold:
> `go run ./internal/impl/saphana/logformat/investigate/`.

---

## What SQL Generates This Block

```sql
COMMIT;
-- or implicit commit at end of auto-commit statement
```

The COMMIT block is written as the final entry of a committing transaction. It anchors the
transaction's commit timestamp in the log.

---

## AFTER Triggers

**N/A** — COMMIT is an internal transaction management block, not a DML row operation.
No application triggers are associated with COMMIT.

---

## CDC Tool Capture

**N/A** (boundary marker) — CDC tools use COMMIT blocks to:
1. Determine the end of a transaction.
2. Retrieve the final commit timestamp for ordering.
3. Flush buffered change events for the transaction downstream.

COMMIT is not itself a change event — it is the signal to emit all buffered events.

---

## Payload Structure

| Field | Type | Size | Notes |
|-------|------|------|-------|
| CommitTimestamp | MVCCTimestamp | 8 bytes | The committed MVCC timestamp. High bit is 1 (IsCommitted = true). Bits[62:0] are the 63-bit monotonic commit counter. See `13_rowid_encoding.md`. |
| TransactionID | uint64 (derived) | 8 bytes | Transaction identifier linking this COMMIT to its preceding DML blocks. The in-flight MVCC timestamp encodes a 31-bit TCBIndex (see `13_rowid_encoding.md`); the most consistent design is for the COMMIT to carry the same 8-byte transaction handle the DML blocks carried. Exact encoding test-pending. |

**TransactionID derivation:** every DML block in the transaction already carries an in-flight
MVCC timestamp whose bits[62:32] are the 31-bit TCBIndex (Transaction Control Block index).
The COMMIT block must reference that same transaction so the reader can swap the in-flight
timestamps for the committed one — so the TransactionID is **derived to be an 8-byte value
matching the DML blocks' transaction handle** (either the raw in-flight MVCC timestamp or its
TCBIndex). The remaining question is whether the field is the full 8-byte timestamp or a
narrower TCB index.

> **Empirical test pending — HANA 2.00 SPS08**: Run two interleaved transactions, each inserting
> a distinct UUID marker, and commit them in a known order. For each COMMIT block, confirm the
> TransactionID field equals the in-flight transaction handle observed in that transaction's DML
> blocks, and read its width. Scaffold: `go run ./internal/impl/saphana/logformat/investigate/`.

**CommitTimestamp note:** The MVCCTimestamp value written in the COMMIT block will always
have the high bit set (IsCommitted() == true). The 63-bit value is a monotonically
increasing counter used by HANA's MVCC system to determine row visibility.

---

## Key CDC Implications

1. **Transaction boundary**: A COMMIT block marks the end of a transaction. All DML blocks
   (INSERT/UPDATE/DELETE/UPSERT/TRUNCATE) between the preceding COMMIT (or BOT) and this
   COMMIT belong to the same transaction.
2. **Emit on COMMIT**: A CDC reader using transactional delivery MUST buffer all change
   events for a transaction and only emit them atomically when the COMMIT block is seen.
   This prevents partial-transaction visibility.
3. **Commit ordering**: The commit timestamp provides a total order of transactions. CDC
   readers can use this for replication lag measurement and consistent snapshot determination.
4. **LSN vs commit timestamp**: The LSN is the log position (write order), while the commit
   timestamp is the commit order. For multi-statement transactions, LSN and commit timestamp
   may differ in ordering relative to other concurrent transactions.

---

## Verified on HANA 2.00 SPS08

**Confirmed:**
- COMMIT marks transaction end and carries a committed MVCC timestamp (high bit = 1).
- Commit timestamp provides a total order of committed transactions (logical, not wall-clock).

**Derived (reasoning shown above):**
- TransactionID is an 8-byte value matching the DML blocks' in-flight transaction handle (TCBIndex).

**Test-pending (procedures above, scaffold `investigate/`):**
- COMMIT numeric block-type code.
- TransactionID field width and whether it is the full timestamp or a narrower TCB index.

---

## Sources

- SAP HANA SQL Reference: https://help.sap.com/docs/SAP_HANA_PLATFORM/4fe29514fd584807ac9f2a04f6754767/
- SAP SYS.M_LOG_SEGMENTS view: https://help.sap.com/docs/SAP_HANA_PLATFORM/4fe29514fd584807ac9f2a04f6754767/20b65c1275191014b23ac1bfd7a95e19.html
- SAP SYS.M_TRANSACTIONS view: https://help.sap.com/docs/SAP_HANA_PLATFORM/4fe29514fd584807ac9f2a04f6754767/
