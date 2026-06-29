# SAP HANA Redo Log Format — RowID and MVCC Timestamp Encoding

> **Status: REVERSE-ENGINEERED** — Not an official SAP specification. See `00_overview.md` for sources.
> The RowID triple structure is CONFIRMED from internal analysis and CDC vendor documentation.
> The MVCC timestamp bit layout is CONFIRMED from internal analysis.

---

## The RowID Triple

Source: SAP HANA internal analysis.

Each log entry references a row using a triple: **(RowID; (FragID; RowPos))**

### RowID

- **Type**: uint64
- **Assigned**: Once, at INSERT time.
- **Immutable**: Never changes through delta merges, column reorganizations, or partition migrations.
- **Purpose**: Stable row identifier for undo/redo log correlation.
- **Range**: Full uint64 (0 to 2^64-1). The space is partitioned per table partition.

### FragID

- **Type**: uint32 (derived). RowPos is uint32 and indexes within a fragment; a fragment count
  comfortably fits the same 32-bit space, and pairing two uint32s (FragID, RowPos) yields an
  aligned 8-byte locator. uint64 is the conservative fallback if fragment IDs prove to be global
  rather than per-table. Width is test-pending.
- **Assigned**: When the row is first written into a fragment (column store delta or main fragment).
- **Mutable**: Changes when data is migrated to a new fragment during a delta merge.
- **Purpose**: Together with RowPos, provides O(1) direct row lookup by array index.

### RowPos

- **Type**: uint32
- **Meaning**: The row's position (zero-based array offset) within its fragment's column arrays.
- **Mutable**: Changes after delta merges (rows are reordered during merge).
- **Maximum**: 2^31 rows per partition (INFERRED from uint32 type).

---

## Row Resolution Algorithm

Given a RowID triple (RowID, FragID, RowPos) from a redo log entry:

```
1. Look up FragID in the fragment directory.
2. If fragment exists:
   - Use (FragID, RowPos) as a direct array index → O(1) lookup
3. If fragment no longer exists (merged/dropped):
   - Use RowID to search the inverted index → O(log n) lookup
   - The inverted index maps RowID → current (FragID, RowPos) in the new fragment
```

This two-path resolution ensures that log replay is correct across delta merges without
requiring the log itself to be updated when fragments change.

Source: SAP HANA internal analysis.

---

## Partition Encoding in RowID

For **partitioned tables**, the RowID space is partitioned. INFERRED encoding:

```
RowID bit layout for partitioned tables:
  bits[63:48] = PartitionID (16-bit partition number) — INFERRED
  bits[47:0]  = Local row counter within partition    — INFERRED
```

**Why INFERRED:** The 16-bit partition prefix is a common RDBMS approach but has not been
confirmed for HANA. The RowID is assigned per partition but how partition information is
encoded in the 64-bit value has not been confirmed.

> **Empirical test pending — HANA 2.00 SPS08**: Create a 2-partition table
> (`PARTITION BY HASH(pk) PARTITIONS 2`), insert one row into each partition, and read
> `SELECT $rowid$, ... FROM t` for both. If the high 16 bits differ by the partition number while
> the low 48 bits are independent counters, the bits[63:48]=PartitionID layout is confirmed; if
> the RowIDs are simply globally monotonic, partitioning is not encoded in the RowID.
> Scaffold: `go run ./internal/impl/saphana/logformat/investigate/`.

---

## MVCC Timestamp Encoding

Source: SAP HANA internal analysis; see `../types.go` for the Go implementation.

The MVCC timestamp is a 64-bit value with two interpretations based on the high bit:

### In-Flight Transaction (High Bit = 0)

```
Bit layout:
  bit  63    = 0 (in-flight marker)
  bits 62:32 = TCBIndex (31 bits) — Transaction Control Block index
  bits 31:0  = SSN (32 bits)      — Statement Sequence Number
```

| Field | Bits | Width | Description |
|-------|------|-------|-------------|
| Marker | [63] | 1 | Always 0 for in-flight |
| TCBIndex | [62:32] | 31 | Index into the Transaction Control Block array. Identifies the active transaction. |
| SSN | [31:0] | 32 | Statement Sequence Number within the transaction. Increments for each statement. |

### Committed Transaction (High Bit = 1)

```
Bit layout:
  bit  63    = 1 (committed marker)
  bits 62:0  = CommitTimestamp (63 bits) — monotonically increasing commit counter
```

| Field | Bits | Width | Description |
|-------|------|-------|-------------|
| Marker | [63] | 1 | Always 1 for committed |
| CommitTimestamp | [62:0] | 63 | Monotonically increasing commit counter. Provides a total order of committed transactions. |

### Usage in Redo Log

- **DML entries** (INSERT, UPDATE, DELETE) written during an active transaction carry an
  in-flight timestamp: TCBIndex identifies which transaction owns the entry.
- **COMMIT entries** write the final committed timestamp (high bit = 1).
- When the redo log is replayed for crash recovery or CDC, the committed timestamp from
  the COMMIT block is used to replace the in-flight timestamps in the preceding DML blocks.

---

## Commit Timestamp Ordering

The 63-bit commit timestamp is a global, monotonically increasing counter maintained by
the HANA transaction manager. It provides:
1. A total order of all committed transactions.
2. The basis for MVCC snapshot reads (a read with timestamp T sees all commits with
   CommitTimestamp ≤ T).
3. Replication lag measurement in CDC tools.

The commit timestamp does NOT correspond to wall-clock time. It is a logical counter.
Mapping to wall-clock time requires querying SYS.M_TRANSACTIONS or using the COMMIT
block's LSN (which has a wall-clock correlation via the log segment timestamps).

---

## Go Type Reference

The `MVCCTimestamp` type in `../types.go` implements:
- `IsCommitted() bool` — checks high bit
- `CommitTimestamp() uint64` — returns bits[62:0] for committed timestamps
- `TCBIndex() uint32` — returns bits[62:32] for in-flight timestamps
- `SSN() uint32` — returns bits[31:0] for in-flight timestamps

---

## Verified on HANA 2.00 SPS08

**Confirmed (HANA 2.00.088.00 SPS08):**
- RowID triple `(RowID; (FragID; RowPos))`; RowID is uint64, immutable, assigned at INSERT.
- RowPos is uint32 (fragment array offset, mutable across merges).
- MVCC timestamp bit layout: high bit discriminates committed (1) vs in-flight (0); committed = 63-bit counter; in-flight = 31-bit TCBIndex + 32-bit SSN.
- Commit timestamp is a logical counter, not wall-clock.
- RowID is uint64 LE, 8 bytes — confirmed from INSERT block structure.
- First row in partition = RowID 1 (confirmed).
- Monotonically increasing: Row1=1, Row2=2, Row3=3 (confirmed).
- RowID located 40 bytes before column values in INSERT block (confirmed).

**Derived (reasoning shown above):**
- FragID is uint32 (pairs with uint32 RowPos into an aligned 8-byte locator).

**Genuinely unknown:**
- FragID width (uint32 vs uint64).
- Whether PartitionID is encoded in the RowID high bits, and the split point.

---

## Sources

- SAP HANA internal analysis (RowID triple, FragID, RowPos, MVCC timestamp bit layout)
- SAP SYS.M_TRANSACTIONS view: https://help.sap.com/docs/SAP_HANA_PLATFORM/4fe29514fd584807ac9f2a04f6754767/
