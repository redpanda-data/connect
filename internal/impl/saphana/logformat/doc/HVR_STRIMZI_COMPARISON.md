# SAP HANA CDC — HVR/Strimzi Feature Completeness Matrix

This document compares the `saphana_cdc` Redpanda Connect input connector against
the HVR 6 (Fivetran) and Strimzi/Debezium feature sets for SAP HANA CDC.

Sources:
- HVR 6 Capabilities for SAP HANA: https://fivetran.com/docs/hvr6/capabilities/610/capabilities-for-sap-hana
- Strimzi/Debezium: no official HANA connector; comparison based on Debezium's general CDC contract
- Qlik Replicate SAP HANA source: https://help.qlik.com/en-US/replicate/

Legend: ✅ Supported · ⚠️ Partial · ❌ Not supported · 🔬 Research/future

---

## Change Capture Operations

| Operation | HVR 6 | Debezium Pattern | `saphana_cdc` | Notes |
|-----------|-------|-----------------|----------------|-------|
| INSERT capture | ✅ | ✅ | ✅ | Via AFTER INSERT trigger → `_RPCN_CDC.CHANGES` |
| UPDATE with before-image | ✅ | ✅ | ✅ | Trigger captures `:old.*` in OLD_VALUES |
| DELETE capture | ✅ | ✅ | ✅ | Trigger captures `:old.*` before-image |
| UPSERT / REPLACE | ✅ | ⚠️ | ✅ | Trigger fires as I or U depending on row existence |
| TRUNCATE capture | ❌ | ⚠️ | ❌ | TRUNCATE does not fire row-level AFTER triggers in HANA |
| MERGE INTO | N/A | ⚠️ | ✅ | HANA lacks native MERGE; handled as UPSERT |
| Bulk INSERT (INSERT…SELECT) | ✅ | ✅ | ✅ | N individual trigger firings, N events emitted |

## Snapshot

| Feature | HVR 6 | Debezium Pattern | `saphana_cdc` | Notes |
|---------|-------|-----------------|----------------|-------|
| Initial snapshot | ✅ | ✅ | ✅ | `snapshot_mode: initial` |
| Snapshot watermark before rows | ✅ | ✅ | ✅ | MAX(ID) from change table captured before SELECT |
| Snapshot modes | ✅ | ✅ | ⚠️ | `initial` / `never` / `always` (no `when_needed`) |
| Snapshot parallelism | ✅ | ✅ | ❌ | Single table at a time; future work |

## Schema Handling

| Feature | HVR 6 | Debezium Pattern | `saphana_cdc` | Notes |
|---------|-------|-----------------|----------------|-------|
| Schema cache (SYS.TABLE_COLUMNS) | ✅ | ✅ | ✅ | Double-checked locking, RWMutex |
| Schema drift detection | ✅ | ✅ | ⚠️ | Cache invalidated on DDL; new columns seen after cache miss |
| DDL event capture | ✅ (AdaptDDL) | ✅ | ❌ | DDL blocks appear in redo log but not emitted as events |
| All HANA data types mapped | ✅ | N/A | ✅ | 25 types mapped in HANATypeKind() |
| LOB columns (CLOB/NCLOB/BLOB) | ✅ | ✅ | ⚠️ | Trigger captures length metadata; full content available via direct query |

## Message Format

| Feature | HVR 6 | Debezium Pattern | `saphana_cdc` | Notes |
|---------|-------|-----------------|----------------|-------|
| Debezium envelope (`before`/`after`/`source`/`op`) | N/A | ✅ | ✅ | Exact Debezium format |
| `op` codes (c/u/d/r/hb) | N/A | ✅ | ✅ | All 5 op codes implemented |
| Source metadata block | N/A | ✅ | ✅ | connector, name, schema, table, lsn, snapshot, ts_ms |
| Message metadata headers | N/A | ✅ | ✅ | saphana_schema, saphana_table, saphana_op, saphana_lsn, saphana_ts_ms |
| Heartbeat events | ✅ | ✅ | ✅ | Emitted after configurable idle interval |
| Schema Registry (Avro) | ✅ | ✅ | ✅ | Via benthos `schema_registry_encode` processor |

## Reliability & Checkpointing

| Feature | HVR 6 | Debezium Pattern | `saphana_cdc` | Notes |
|---------|-------|-----------------|----------------|-------|
| Durable LSN checkpoint | ✅ | ✅ | ✅ | HANA table `_RPCN_CDC.CHECKPOINT` |
| External cache (Redis/Memcached) | ✅ | N/A | ❌ | Future: use benthos cache resource |
| Exactly-once delivery | ✅ | ✅ | ⚠️ | At-least-once; LSN-keyed dedup possible downstream |
| Reconnect on DB failure | ✅ | ✅ | ⚠️ | Exponential backoff; full reconnect on next `Connect()` |
| Long transaction buffering | ✅ | ✅ | ✅ | Events buffered in channel; no in-memory transaction merge needed (trigger-based) |

## HANA-Specific

| Feature | HVR 6 | Qlik | `saphana_cdc` | Notes |
|---------|-------|------|----------------|-------|
| Column store tables | ✅ | ✅ | ✅ | Primary table type |
| Row store tables | ❌ | ❌ | ❌ | Unsupported by design (different log format) |
| HANA Cloud | ✅ | ✅ | ✅ | Connects via TLS; requires `TLSServerName` in DSN |
| HANA 2.0 SPS 03+ | ✅ | ✅ | ✅ | JSON_OBJECT requires SPS 03+; Express is SPS 08 |
| Multi-tenant (CDB/PDB) | ⚠️ | ⚠️ | ❌ | Not tested; future work |
| Scale-out clusters | ❌ | ❌ | ❌ | Each node has separate log; cross-node ordering not guaranteed |
| Log-based CDC (binary redo log) | ✅ | ✅ | 🔬 | Page header + block type codes confirmed (HANA 2.00.088.00 SPS08): INSERT=0x81, DELETE=0xFE, COMMIT=0xC8, UPDATE=decomposed DELETE+INSERT. ColumnType code bytes not yet decoded. |
| Trigger-based CDC | N/A | ✅ | ✅ | Primary mechanism |

## Deployment

| Feature | HVR 6 | Debezium Pattern | `saphana_cdc` | Notes |
|---------|-------|-----------------|----------------|-------|
| HANA JDBC driver required | ✅ | N/A | ✅ | `github.com/SAP/go-hdb` (pure Go, Apache 2.0) |
| No C library dependency | N/A | N/A | ✅ | Pure Go via go-hdb |
| Docker/Kubernetes native | N/A | ✅ | ✅ | Standard benthos input; no host filesystem access needed |
| Mac dev script | N/A | N/A | ✅ | `internal/impl/saphana/scripts/dev.sh` |

---

## Key Gaps vs HVR/Qlik

1. **Partial binary redo log parsing** — Page header and block type codes are empirically confirmed (HANA 2.00.088.00 SPS08): 4096-byte pages, 80-byte header, LSN at offset 16, INSERT=0x81, DELETE=0xFE, COMMIT=0xC8, UPDATE=DELETE+INSERT (no separate type), UPSERT new row=0x81. Archive log: 4096-byte tagged ASCII header (`[MAGIC]HANABackup...`), redo pages start at byte 4096. Remaining work: ColumnType code bytes in `49 20 54 04` descriptor, full entry-length framing, multi-page continuation. Once these are confirmed, log-based CDC can replace the trigger-based approach for lower latency and no trigger overhead. See `logformat/doc/VERSION_MATRIX.md` for confirmed findings.

2. **No TRUNCATE capture** — HANA row-level AFTER triggers do not fire on TRUNCATE. HVR detects this via a single `truncate` block type in the redo log. Mitigation: document that consumers should treat TRUNCATE as a gap signal.

3. **No DDL streaming** — HVR's `AdaptDDL` action updates schema enrollment on CREATE/ALTER/DROP TABLE. Our implementation relies on cache invalidation and schema re-fetch; DDL events are not emitted.

4. **LOB content** — Triggers capture LOB byte length, not content. Full LOB values are accessible via direct HANA query but not streamed in CDC events (matching HVR's behavior for disk LOBs: unchanged LOB columns appear as NULL in update records).
