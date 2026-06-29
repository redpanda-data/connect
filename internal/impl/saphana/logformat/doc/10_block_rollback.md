# SAP HANA Redo Log Format — ROLLBACK Block

> **Status: REVERSE-ENGINEERED** — Not an official SAP specification. See `00_overview.md` for sources.

---

## Status

**CONFIRMED** (block type exists, CDC readers must discard on ROLLBACK) / **INFERRED**
(payload structure) — The rollback discard requirement follows from SAP HANA transaction
semantics. Payload structure is inferred from minimal requirements.

---

## Log Block Type Identifier

The numeric block type code for ROLLBACK is not published by SAP and cannot be derived
analytically. The constant `BlockTypeRollback = 0xF9` in `types.go` is a sentinel placeholder.

> **Empirical test pending — HANA 2.00 SPS08**: In an explicit transaction, INSERT one
> UUID-marked row, then `ROLLBACK`, then `ALTER SYSTEM SAVEPOINT`. The block following the
> (now-rolled-back) INSERT block is the ROLLBACK block; read its entry-header block-type byte.
> Scaffold: `go run ./internal/impl/saphana/logformat/investigate/`.

---

## What SQL Generates This Block

```sql
ROLLBACK;
-- or implicit rollback on error / session disconnect
```

Also generated internally by HANA for:
- Statement-level rollbacks (single statement within a multi-statement transaction)
- System-initiated rollbacks on deadlock detection
- Session crash / timeout cleanup

---

## AFTER Triggers

**N/A** — ROLLBACK is a transaction management block. No application triggers fire on
ROLLBACK (the DML operations being rolled back had their trigger windows already evaluated;
the rollback undoes the data effects but trigger invocations are not reversed).

---

## CDC Tool Capture

**N/A** (discard marker) — CDC tools use ROLLBACK blocks to discard all buffered change
events for the rolling-back transaction. The ROLLBACK block is not itself a change event.

---

## Payload Structure

| Field | Type | Size | Notes |
|-------|------|------|-------|
| TransactionID | uint64 (derived) | 8 bytes | Transaction identifier matching the preceding DML blocks. Same handle as the COMMIT block's TransactionID (see `09_block_commit.md`) — the reader needs it to know which buffered events to discard. |
| SavepointLSN | uint64 or absent | 8 bytes if present | INFERRED: present only for `ROLLBACK TO SAVEPOINT`, giving the LSN target to roll back to. Absent for a full rollback. |

**TransactionID derivation:** the only mandatory content of a ROLLBACK block is the identity of
the transaction whose buffered changes must be discarded. By symmetry with the COMMIT block
(which carries the same transaction handle to group its DML blocks), the ROLLBACK TransactionID
is **derived to be the same 8-byte transaction handle** (the in-flight TCBIndex / MVCC
timestamp). A full rollback needs nothing more; a partial `ROLLBACK TO SAVEPOINT` additionally
needs a target LSN.

> **Empirical test pending — HANA 2.00 SPS08**: Run two interleaved transactions, roll one back,
> and confirm the ROLLBACK block's TransactionID matches that transaction's DML in-flight handle
> and read its width. Separately, set a `SAVEPOINT sp`, do more DML, `ROLLBACK TO SAVEPOINT sp`,
> savepoint, and check whether the block carries an extra 8-byte LSN equal to the savepoint's LSN
> (and whether it is the same block type or a distinct one). Scaffold:
> `go run ./internal/impl/saphana/logformat/investigate/`.

---

## Key CDC Implications

1. **Discard all buffered events**: Upon receiving a ROLLBACK block, the CDC reader MUST
   discard all buffered change events for the identified transaction. Failing to do so
   would replicate uncommitted data (a consistency violation).
2. **Savepoint rollbacks**: A partial ROLLBACK TO SAVEPOINT reverts changes to a named
   savepoint within a transaction but does not end the transaction. The CDC reader must
   handle this by discarding only the events after the savepoint LSN, not the entire
   transaction buffer. (Whether a distinct savepoint-rollback block type exists, or whether
   this is encoded as a ROLLBACK with a SavepointLSN reference, is resolved by the
   ROLLBACK TO SAVEPOINT step of the test procedure above.)
3. **Row cache cleanup**: Any row cache entries added by the rolling-back transaction
   (from UPDATE events) should be reverted to the pre-transaction state.
4. **Memory management**: For long-running transactions that are ultimately rolled back, the
   CDC reader's in-memory event buffer may have grown large. The reader should release this
   memory promptly on ROLLBACK.

---

## Verified on HANA 2.00 SPS08

**Confirmed:**
- ROLLBACK block exists; CDC readers must discard all buffered events for the transaction.
- No application triggers fire on ROLLBACK.

**Derived (reasoning shown above):**
- TransactionID is the same 8-byte transaction handle as the COMMIT block.
- Full rollback needs only the TransactionID; partial rollback additionally needs a target LSN.

**Test-pending (procedures above, scaffold `investigate/`):**
- ROLLBACK numeric block-type code.
- TransactionID width.
- Whether ROLLBACK TO SAVEPOINT is the same block type with a SavepointLSN, or a distinct type.

---

## Sources

- SAP HANA SQL Reference: https://help.sap.com/docs/SAP_HANA_PLATFORM/4fe29514fd584807ac9f2a04f6754767/
