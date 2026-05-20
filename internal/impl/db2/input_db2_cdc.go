// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/confx"
	"github.com/redpanda-data/connect/v4/internal/impl/db2/db2cli"
	"github.com/redpanda-data/connect/v4/internal/impl/db2/replication"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	db2CDCFieldDSN                      = "dsn"
	db2CDCFieldSchema                   = "schema"
	db2CDCFieldTables                   = "tables"
	db2CDCFieldTableIncludeRegex        = "table_include_regex"
	db2CDCFieldTableExcludeRegex        = "table_exclude_regex"
	db2CDCFieldCDCSchema                = "cdc_schema"
	db2CDCFieldSnapshotMode             = "snapshot_mode"
	db2CDCFieldSnapshotMaxBatchSize     = "snapshot_max_batch_size"
	db2CDCFieldSnapshotParallelism      = "snapshot_parallelism"
	db2CDCFieldSnapshotIsolationMode    = "snapshot_isolation_mode"
	db2CDCFieldCheckpointMode           = "checkpoint_mode"
	db2CDCFieldCheckpointCache          = "checkpoint_cache"
	db2CDCFieldCheckpointCacheKey       = "checkpoint_cache_key"
	db2CDCFieldCheckpointCacheTableName = "checkpoint_cache_table_name"
	db2CDCFieldCheckpointLimit          = "checkpoint_limit"
	db2CDCFieldStreamBackoffInterval    = "stream_backoff_interval"
	db2CDCFieldPollBatchSize            = "poll_batch_size"
	db2CDCFieldHeartbeatInterval        = "heartbeat_interval"
	db2CDCFieldEmitSchemaChanges        = "emit_schema_changes"
	db2CDCFieldSignalTable              = "signal_table"
)

// snapshotMode is Go's equivalent of an enum: a named string type with an
// exhaustive const block. The YAML config field accepts the string values below.
// Default: snapshotModeInitial ("initial").
type snapshotMode string

const (
	snapshotModeInitial     snapshotMode = "initial"      // snapshot on first start only
	snapshotModeAlways      snapshotMode = "always"       // snapshot on every start
	snapshotModeNever       snapshotMode = "never"        // skip snapshot, stream only
	snapshotModeInitialOnly snapshotMode = "initial_only" // snapshot then stop (no streaming)
	snapshotModeWhenNeeded  snapshotMode = "when_needed"  // snapshot if no checkpoint exists
	snapshotModeNoData      snapshotMode = "no_data"      // Debezium alias for "never"
)

const (
	// signalFetchLimit is the max rows returned per signal-table poll.
	// Signals are rare control messages; 100 is generous and bounds memory.
	signalFetchLimit = 100
	// schemaChangeFetchLimit caps the schema-change watermark scan to bound
	// memory when many tables are added to replication in a single cycle.
	schemaChangeFetchLimit = 1000
)

// errSignalCleanupRetry is returned by runIncrementalSnapshotWithContext when
// signal DELETE fails after a successful snapshot. It signals the caller to
// preserve pendingIncrementalCtx so the next processSignals tick retries
// deletion via the SignalIDs stored in the checkpoint rather than triggering
// a duplicate full snapshot via the un-deleted execute-snapshot signal row.
var errSignalCleanupRetry = errors.New("signal cleanup needs retry")

// shouldTakeSnapshot returns true if a full snapshot should be performed given
// the connector's snapshot mode and the current checkpoint CSN.
func shouldTakeSnapshot(mode snapshotMode, checkpoint replication.CSN) bool {
	switch mode {
	case snapshotModeInitial, snapshotModeWhenNeeded:
		// Snapshot only when there is no prior checkpoint (first start or wiped state).
		return checkpoint.IsNull()
	case snapshotModeAlways, snapshotModeInitialOnly:
		return true
	default: // snapshotModeNever, snapshotModeNoData
		return false
	}
}

func db2CDCConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("4.92.0").
		Categories("Services").
		Summary("Consumes change data capture (CDC) events from IBM DB2 using SQL Replication.").
		Description(`Streams change events from IBM DB2 tables using SQL Replication (the ASNCDC schema).

This connector uses *pure Go* with `+"`github.com/ebitengine/purego`"+` for DB2 CLI bindings — no CGO or C toolchain required at build time.

== How it works

DB2 SQL Replication maintains a set of *change tables* (also called CD tables) alongside your source tables. A capture daemon reads the DB2 transaction log and writes INSERT, UPDATE, and DELETE operations into those tables. This input polls those change tables and converts each row into a Redpanda Connect message.

The connector operates in two phases:

1. *Snapshot* (when `+"`snapshot_mode: initial`"+` or `+"`always`"+`): reads all existing rows from each monitored table inside a single `+"`REPEATABLE READ`"+` read-only transaction. The CDC position (CSN) is captured *before* the first row is read, so no changes are missed while the snapshot is in progress.
2. *Streaming*: polls each change table for new rows with `+"`IBMSNAP_COMMITSEQ`"+` greater than the last checkpoint. The maximum `+"`SYNCHPOINT`"+` from `+"`ASNCDC.IBMSNAP_REGISTER`"+` is used as an upper bound on every poll to avoid reading uncommitted rows.

== UPDATE representation

DB2 LUW SQL Replication encodes UPDATE operations as a DELETE record followed by an INSERT record that share the same `+"`IBMSNAP_COMMITSEQ`"+` value. This connector detects these pairs using LEAD/LAG window functions (ported from the Debezium DB2 connector) and merges them into a single `+"`op: u`"+` (update) event. The `+"`before`"+` field contains the old row data and `+"`after`"+` contains the new row data.

== Message format (Debezium-compatible)

The message body matches the https://debezium.io/documentation/reference/stable/connectors/db2.html[Debezium DB2 connector^] envelope format, making this connector a drop-in replacement for existing Debezium consumers.

`+"```json"+`
{
  "before": null,
  "after":  { "ID": 42, "NAME": "Alice", "SALARY": 75000 },
  "source": {
    "connector":  "redpanda.db2",
    "name":       "redpanda.db2_cdc",
    "schema":     "DB2ADMIN",
    "table":      "EMPLOYEES",
    "commit_lsn": "CSN:000000000000C350",
    "change_lsn": null,
    "snapshot":   "false",
    "ts_ms":      1705315845000
  },
  "op":    "c",
  "ts_ms": 1705315845000
}
`+"```"+`

`+"**`op` codes**"+` match Debezium: `+"`c`"+` = create/insert, `+"`u`"+` = update, `+"`d`"+` = delete, `+"`r`"+` = read (snapshot), `+"`hb`"+` = heartbeat, `+"`schema_change`"+` = schema change.

`+"**`before` field**"+`: Populated per Debezium semantics. For `+"`op: u`"+` (update) events, `+"`before`"+` contains the old row data and `+"`after`"+` contains the new row data. For `+"`op: d`"+` (delete) events, `+"`before`"+` contains the deleted row and `+"`after`"+` is `+"`null`"+`. For `+"`op: c`"+` (insert) and `+"`op: r`"+` (snapshot read) events, `+"`before`"+` is `+"`null`"+`.

`+"**`change_lsn`**"+`: DB2 LUW SQL Replication exposes only `+"`IBMSNAP_COMMITSEQ`"+` (the commit LSN). The intra-transaction LSN (`+"`change_lsn`"+` in Debezium) is not available and is always `+"`null`"+`.

The following metadata keys are set on every message:

- `+"`db2_schema`"+`: source table schema
- `+"`db2_table`"+`: source table name
- `+"`db2_operation`"+`: human-readable op — `+"`read`"+`, `+"`insert`"+`, `+"`update`"+`, `+"`delete`"+`, `+"`heartbeat`"+`, or `+"`schema_change`"+`
- `+"`db2_op`"+`: Debezium op code — `+"`r`"+`, `+"`c`"+`, `+"`u`"+`, `+"`d`"+`, `+"`hb`"+`, or `+"`schema_change`"+`
- `+"`db2_csn`"+`: commit sequence number string (empty for snapshot events; backward-compat alias for `+"`db2_commit_lsn`"+`)
- `+"`db2_commit_lsn`"+`: same as `+"`db2_csn`"+` (Debezium field name)
- `+"`db2_connector`"+`: always `+"`redpanda.db2`"+`
- `+"`db2_snapshot`"+`: `+"`true`"+` for snapshot rows, `+"`false`"+` for streaming rows
- `+"`db2_timestamp`"+`: `+"`IBMSNAP_LOGMARKER`"+` timestamp from the change table (RFC3339Nano; omitted for snapshot events)

== Prerequisites

1. IBM DB2 10.1 or later with SQL Replication installed.
2. The DB2 CLI shared library must be present at runtime:
   - Linux: `+"`libdb2.so.1`"+` (from the IBM Data Server Driver package)
   - macOS: `+"`libdb2.dylib`"+`
   - Windows: `+"`db2cli.dll`"+`

=== Installing on Debian/Ubuntu

Download the IBM Data Server Driver Package (dsdriver) from the IBM support site and run:

`+"```sh"+`
tar xzf ibm_data_server_driver_package_linuxx64.tar.gz
cd dsdriver && bash installDSDriver
export LD_LIBRARY_PATH=/opt/ibm/dsdriver/lib:$LD_LIBRARY_PATH
`+"```"+`

=== Installing on RHEL/CentOS

`+"```sh"+`
rpm -ivh ibm-datasrvrmgr-*.rpm
export LD_LIBRARY_PATH=/opt/ibm/dsdriver/lib:$LD_LIBRARY_PATH
`+"```"+`

=== Kubernetes init container

`+"```yaml"+`
initContainers:
  - name: install-db2-driver
    image: ibmcom/db2:11.5.9.0
    command: ["/bin/sh", "-c", "cp /opt/ibm/db2/V11.5/lib64/libdb2.so.1 /shared/lib/"]
    volumeMounts:
      - name: db2-lib
        mountPath: /shared/lib
volumes:
  - name: db2-lib
    emptyDir: {}
`+"```"+`

3. SQL Replication must be configured and the capture daemon running:
`+"```sql"+`
-- Start the CDC capture daemon
CALL ASNCDC.ASNCDCSERVICES('start', 'asncdc');

-- Register each table you want to capture (repeat for each table)
CALL ASNCDC.ADDTABLE('MYSCHEMA', 'EMPLOYEES');
CALL ASNCDC.ADDTABLE('MYSCHEMA', 'ORDERS');
`+"```"+`

== Minimal configuration

`+"```yaml"+`
input:
  db2_cdc:
    dsn: "DATABASE=SAMPLE;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"
    schema: "DB2ADMIN"
    tables: ["EMPLOYEES", "ORDERS"]
    # snapshot_mode defaults to "initial": snapshot on first start, stream continuously thereafter.
    # Set to "never" to skip the snapshot and stream from the current log position only.
`+"```"+`

== Checkpoint persistence

The connector tracks the highest processed `+"`IBMSNAP_COMMITSEQ`"+` so it can resume after a restart without replaying already-delivered events. By default a `+"`RPCN.CDC_CHECKPOINT`"+` table is created in DB2 (the `+"`RPCN`"+` schema must already exist). Set `+"`checkpoint_cache`"+` to use an external https://www.docs.redpanda.com/redpanda-connect/components/caches/about[cache resource^] instead.
`).
		Fields(
			service.NewStringField(db2CDCFieldDSN).
				Description("DB2 connection string in the keyword=value format accepted by SQLDriverConnect. "+
					"Common keywords: `DATABASE` (database alias), `HOSTNAME`, `PORT` (default 50000), "+
					"`PROTOCOL` (`TCPIP`), `UID`, `PWD`. The DB2 CLI shared library must be installed "+
					"separately — this connector does not bundle it.").
				Secret().
				Example("DATABASE=SAMPLE;HOSTNAME=db2host;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"),

			service.NewStringField(db2CDCFieldSchema).
				Description("DB2 schema (TABSCHEMA) that owns the monitored tables. "+
					"Maps to SOURCE_OWNER in ASNCDC.IBMSNAP_REGISTER. "+
					"Case-insensitive — automatically normalized to uppercase.").
				Example("DB2ADMIN"),

			service.NewStringListField(db2CDCFieldTables).
				Description("List of table names (without schema prefix) to capture changes from. "+
					"Each table must already be registered with SQL Replication via ASNCDC.ADDTABLE before the connector starts. "+
					"The connector will fail at startup if any listed table is missing from ASNCDC.IBMSNAP_REGISTER. "+
					"When empty, all CDC-registered tables in the schema are discovered dynamically from ASNCDC.IBMSNAP_REGISTER.").
				Example([]string{"EMPLOYEES", "ORDERS"}).
				Optional(),

			service.NewStringListField(db2CDCFieldTableIncludeRegex).
				Description("Optional list of regular expressions; only tables whose names match at least one pattern are captured. "+
					"Applied after `tables`. When `tables` is empty, all CDC-registered tables in the schema are discovered first and this filter narrows the set. "+
					"Patterns are matched against the bare table name (without schema prefix).").
				Example([]string{"^EMP", "^ORDER"}).
				Optional().
				Advanced(),

			service.NewStringListField(db2CDCFieldTableExcludeRegex).
				Description("Optional list of regular expressions; tables whose names match any pattern are excluded from capture. "+
					"Applied after `table_include_regex`.").
				Example([]string{"_TEST$", "_STAGING$"}).
				Optional().
				Advanced(),

			service.NewStringField(db2CDCFieldCDCSchema).
				Description("Schema that owns the SQL Replication control tables (IBMSNAP_REGISTER and the generated change tables). "+
					"Defaults to `ASNCDC`, which is the standard installation schema. "+
					"Change this only if SQL Replication was installed under a custom schema.").
				Default("ASNCDC").
				Advanced(),

			service.NewStringEnumField(db2CDCFieldSnapshotMode,
				string(snapshotModeInitial),
				string(snapshotModeAlways),
				string(snapshotModeNever),
				string(snapshotModeInitialOnly),
				string(snapshotModeWhenNeeded),
				string(snapshotModeNoData)).
				Description("Controls when an initial full-table snapshot is performed:\n\n"+
					"- `initial` (default): snapshot only on first start (when no checkpoint exists). Skipped on restart.\n"+
					"- `always`: snapshot on every start, regardless of existing checkpoint.\n"+
					"- `never`: skip snapshot; start streaming from the current log position.\n"+
					"- `initial_only`: snapshot then stop (no streaming phase).\n"+
					"- `when_needed`: snapshot if and only if no prior checkpoint exists; equivalent to `initial` for fresh deployments but useful when the checkpoint table may have been wiped.\n"+
					"- `no_data`: Debezium-compatible alias for `never`; no rows are read during snapshot.").
				Default(string(snapshotModeInitial)),

			service.NewIntField(db2CDCFieldSnapshotMaxBatchSize).
				Description("Number of rows fetched per round-trip during the initial snapshot. "+
					"Rows are retrieved using keyset pagination (ordered by primary key), so reducing this value "+
					"lowers memory usage at the cost of more DB2 round-trips. "+
					"Increase for large tables with small rows to improve snapshot throughput.").
				Default(1024).
				Advanced(),

			service.NewIntField(db2CDCFieldSnapshotParallelism).
				Description("Number of tables to snapshot concurrently during the initial snapshot phase. "+
					"Currently fixed at 1 (sequential): all workers share a single read-only transaction "+
					"and the underlying db2cli driver uses a 1-connection pool, so concurrency > 1 would "+
					"serialize anyway and risk statement-handle data races. "+
					"Values greater than 1 are accepted and stored but have no effect until "+
					"per-worker connection support is added in a future release.").
				Default(1).
				Advanced(),

			service.NewStringEnumField(db2CDCFieldSnapshotIsolationMode,
				"repeatable_read", "read_committed").
				Description("SQL transaction isolation level used during the snapshot phase. "+
					"`repeatable_read` (default) uses DB2 Repeatable Read (RR) — no phantom reads, strongest consistency. "+
					"`read_committed` uses DB2 Cursor Stability (CS) — lower lock overhead but snapshot consistency is not guaranteed across tables.").
				Default("repeatable_read").
				Advanced(),

			service.NewStringField(db2CDCFieldCheckpointMode).
				Description("Checkpoint tracking mode. `auto` (default) detects the DB2 version and "+
					"selects CSN-based checkpointing on DB2 10.1+. Set to `csn` to require CSN "+
					"mode explicitly and fail fast on older DB2 versions.").
				Default("auto").
				Advanced(),

			service.NewStringField(db2CDCFieldCheckpointCache).
				Description("Name of a https://www.docs.redpanda.com/redpanda-connect/components/caches/about[cache resource^] to use for storing the checkpoint CSN. "+
					"If not set, the connector automatically creates and manages a `"+db2CDCFieldCheckpointCacheTableName+"` table in DB2. "+
					"Use an external cache (e.g. Redis) when you want checkpoint state to survive a DB2 rebuild "+
					"or when the connector user lacks DDL privileges.").
				Example("my_redis_cache").
				Optional().
				Advanced(),

			service.NewStringField(db2CDCFieldCheckpointCacheKey).
				Description("Key used to store and retrieve the checkpoint in the cache or DB2 checkpoint table. "+
					"Change this value if you run multiple DB2 CDC connectors that share the same cache resource "+
					"or the same DB2 instance, to avoid checkpoint collisions.").
				Default("db2_cdc_checkpoint").
				Advanced(),

			service.NewStringField(db2CDCFieldCheckpointCacheTableName).
				Description("Fully-qualified DB2 table name for the built-in checkpoint store. "+
					"The table is created automatically if it does not exist (SQLSTATE 42710 is silently ignored). "+
					"The connector will attempt to CREATE the schema if it does not already exist; "+
					"if the connector user lacks CREATE SCHEMA privileges, the schema must be created manually beforehand. "+
					"Only used when `"+db2CDCFieldCheckpointCache+"` is not set.").
				Default("RPCN.CDC_CHECKPOINT").
				Advanced(),

			service.NewIntField(db2CDCFieldCheckpointLimit).
				Description("Maximum number of messages that can be in-flight (sent but not yet acknowledged) at any one time. "+
					"The CSN checkpoint is only advanced once all messages at a given position are acknowledged, "+
					"preserving at-least-once delivery guarantees. "+
					"Increase this value to allow larger output batches and higher throughput; "+
					"decrease it to reduce the number of re-delivered messages after a restart.").
				Default(4096).
				Advanced(),

			service.NewDurationField(db2CDCFieldStreamBackoffInterval).
				Description("How long to wait between change-table polls when no new rows are found. "+
					"Note that the DB2 capture daemon itself introduces latency — reducing this below the capture "+
					"daemon's commit interval has no benefit. "+
					"Increase for low-traffic tables to reduce unnecessary DB2 queries.").
				Default("1s").
				Advanced(),

			service.NewIntField(db2CDCFieldPollBatchSize).
				Description("Maximum number of change rows fetched per change-table query. "+
					"Each poll issues one query per monitored table. "+
					"Reduce this value if individual change events are large (e.g. wide rows). "+
					"Increase it to improve throughput for high-velocity tables.").
				Default(2048).
				Advanced(),

			service.NewDurationField(db2CDCFieldHeartbeatInterval).
				Description("Interval at which heartbeat messages (`op=hb`) are emitted when no CDC changes are available. "+
					"Heartbeats keep downstream consumers alive on low-traffic tables and advance the checkpoint on idle connectors. "+
					"The default of 30s matches the Debezium DB2 connector recommended setting. "+
					"Set to 0s to disable.").
				Default("30s").
				Advanced(),

			service.NewBoolField(db2CDCFieldEmitSchemaChanges).
				Description("When true, emit a schema change event (`op=schema_change`) whenever a new table is added to SQL Replication "+
					"(i.e., when `ASNCDC.ADDTABLE` is called while the connector is running). "+
					"The connector polls `ASNCDC.IBMSNAP_REGISTER` for new entries on each backoff interval. "+
					"Defaults to true to match Debezium behaviour; set to false to suppress schema change events.").
				Default(true).
				Advanced(),

			service.NewStringField(db2CDCFieldSignalTable).
				Description("Fully-qualified DB2 table name for the signal channel (e.g. `DB2INST1.CDC_SIGNALS`). "+
					"When set, the connector polls this table for `execute-snapshot` signals. "+
					"An `execute-snapshot` signal triggers an ad-hoc snapshot of the specified tables without restarting the connector. "+
					"Create the table with: "+
					"`CREATE TABLE <schema>.<table> (ID VARCHAR(255) NOT NULL, TYPE VARCHAR(64) NOT NULL, DATA VARCHAR(2048), PRIMARY KEY (ID))`").
				Optional().
				Advanced(),

			service.NewAutoRetryNacksToggleField(),
			service.NewBatchPolicyField("batching"),
		)
}

func init() {
	service.MustRegisterBatchInput("db2_cdc", db2CDCConfigSpec(), newDB2CDCInput)
}

//------------------------------------------------------------------------------

// db2CDCInput streams change data capture events from IBM DB2 tables via SQL Replication.
//
// Lifecycle:
//
//	Connect() → runCDC (goroutine) → runSnapshot + runStreaming → ReadBatch (polling loop)
//	Close()   → shutSig.TriggerSoftStop() → wg.Wait() → db.Close()
//
// See db2CDCConfigSpec() for the full list of user-visible config fields and their defaults.
type db2CDCInput struct {
	// ── DB connections ────────────────────────────────────────────────────────
	// db is the primary *sql.DB (single-connection pool). All streaming reads
	// and snapshot queries go through this connection. Nil after Close() returns.
	// Guarded by mu.
	db *sql.DB
	// auxDB is a dedicated single-connection pool for control-path operations:
	// signal polling, schema-change queries, and incremental snapshot queries.
	// Keeping it separate from db prevents control-path DDL from starving the
	// streaming poll and allows signal processing to run concurrently with acks.
	// Nil after Close() returns. Guarded by mu.
	auxDB *sql.DB
	// streamer is the SQL Replication poller, initialized by runStreaming.
	// processSignals reads streamer (under mu) to get the current stream CSN for
	// open-window deduplication during incremental snapshots.
	streamer *replication.Streamer

	// ── Static configuration (set in newDB2CDCInput, immutable thereafter) ────
	dsn            string              // DB2 SQLDriverConnect keyword=value connection string
	schema         string              // source table schema (upper-cased, alphanumeric+underscore)
	tables         []string            // explicit table list; empty = auto-discover from IBMSNAP_REGISTER
	tableFilter    *confx.RegexpFilter // include/exclude regex filter applied after tables list
	asnCDCSchema   string              // SQL Replication control schema, default "ASNCDC"
	snapshotMode   snapshotMode        // when to run the initial full-table snapshot
	checkpointMode string              // "auto" (detect DB2 version) or "csn" (require 10.1+)

	// ── Checkpoint store ──────────────────────────────────────────────────────
	checkpointCacheKey string // map key for the streaming-CSN checkpoint entry
	checkpointLimit    int    // max unacked messages before ReadBatch blocks (backpressure)
	// cpCacheName is the name of an external service.Cache resource. When empty,
	// the connector creates and manages its own checkpoint table in DB2 (cpCacheTableName).
	cpCacheName      string
	cpCacheTableName string // fully-qualified table, e.g. "RPCN.CDC_CHECKPOINT"
	// Pre-computed DML for the checkpoint table.
	//
	// Same rationale as the sigSelect* fields: each query embeds cpCacheTableName
	// (e.g. "RPCN.CDC_CHECKPOINT"), so they cannot be package-level constants.
	// The format strings live in constants_sql.go as cpMergeSQLFmt, cpSelectSQLFmt,
	// cpDeleteSQLFmt. These are the fully-instantiated versions, computed once in
	// newDB2CDCInput so the hot-path saveCheckpointKV / loadCheckpointKV methods
	// never call fmt.Sprintf on every ack or poll tick.
	cpMergeSQL  string // MERGE INTO … WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT
	cpSelectSQL string // SELECT CACHE_VAL FROM … WHERE CACHE_KEY = ?
	cpDeleteSQL string // DELETE FROM … WHERE CACHE_KEY = ?

	// ── Streaming behaviour ───────────────────────────────────────────────────
	heartbeatInterval time.Duration // interval between synthetic heartbeat events; 0 = disabled
	emitSchemaChanges bool          // true = emit schema_change events on table re-registration
	// lastSeenSynchCSN is updated on every poll from ASNCDC.IBMSNAP_REGISTER and
	// used as the upper-bound watermark for CDC streaming. This prevents reading
	// uncommitted rows by bounding queries to CSNs that are fully captured.
	// Guarded by mu.
	lastSeenSynchCSN replication.CSN
	signalTable      string // fully-qualified table for execute-snapshot / stop-snapshot signals

	// Pre-computed query strings for signal-table operations.
	//
	// Why instance fields rather than package-level constants?
	// Each query embeds the user-configured signal_table name (e.g.
	// "DB2INST1.CDC_SIGNALS"), so they cannot be compile-time constants.
	// The corresponding format strings (with %[1]s/%[2]d placeholders) live in
	// constants_sql.go as sigSelectStopSQLFmt, sigSelectPauseSQLFmt, etc.
	// Here we store the fully-instantiated versions, computed once in
	// newDB2CDCInput from the immutable signalTable field, so processSignals
	// (invoked on every BackoffInterval tick) never calls fmt.Sprintf.
	//
	// Signal table schema (auto-created if missing):
	//   CREATE TABLE <signalTable> (
	//     ID   VARCHAR(255) NOT NULL,
	//     TYPE VARCHAR(64)  NOT NULL,
	//     DATA VARCHAR(2048),
	//     PRIMARY KEY (ID)
	//   )
	sigSelectStopSQL   string // SELECT … WHERE TYPE = 'stop-snapshot'   FETCH FIRST N ROWS ONLY
	sigSelectPauseSQL  string // SELECT … WHERE TYPE = 'pause-snapshot'
	sigSelectResumeSQL string // SELECT … WHERE TYPE = 'resume-snapshot'
	sigSelectExecSQL   string // SELECT … WHERE TYPE = 'execute-snapshot'
	sigDeleteSQL       string // DELETE all processed signal rows
	sigDeleteExecSQL   string // DELETE WHERE ID = ? AND TYPE = 'execute-snapshot'
	sigAbortPollSQL    string // poll for 'stop-snapshot' while a snapshot is running

	// schemaChangeQueryTemplate is pre-computed with a single %s placeholder for
	// the hex-encoded last-seen CSN. All other query parts (schema name, FETCH
	// FIRST N) are baked in at construction to avoid rebuilding the multi-line
	// string on every BackoffInterval tick.
	schemaChangeQueryTemplate string

	// ── Replication engine config ─────────────────────────────────────────────
	snapshotConfig replication.SnapshotConfig
	streamConfig   replication.StreamConfig

	// ── Internal queues ───────────────────────────────────────────────────────
	capped    *checkpoint.Capped[replication.CSN] // tracks in-flight message watermarks for acking
	eventChan chan replication.ChangeEvent        // CDC goroutine → ReadBatch delivery channel
	errChan   chan error                          // fatal errors from CDC goroutines → ReadBatch

	// ── Resources and observability ───────────────────────────────────────────
	res      *service.Resources
	lagGauge *service.MetricGauge // db2_cdc_lag_ms gauge: time.Since(event.Timestamp) in ms
	version  replication.Version  // detected DB2 server version (set in Connect)
	log      *service.Logger

	// ── Synchronization primitives ────────────────────────────────────────────
	// Invariants:
	//   mu           – primary short-held lock for state updates.
	//                  Guards: db, auxDB, closed, streamer, lastSeenSynchCSN,
	//                          pendingIncrementalCtx, cdcCancel.
	//   shutSig      – two-phase shutdown (soft-stop → drain, hard-stop → abort).
	//                  Reset to a fresh signaller at the top of each Connect() call
	//                  so goroutines from a prior Close() don't bleed into the next
	//                  connect cycle.
	//   cdcCancel    – cancels the CDC context on hard-stop (Close timeout path).
	//                  Written under mu; safe to read outside mu after goroutines exit.
	//   wg           – counts goroutines launched in Connect(). Close() calls
	//                  wg.Wait() before closing any DB handle to prevent use-after-close.
	//   stopSnapshotMu / stopSnapshotCh – abort-snapshot channel and its guard.
	//                  Kept separate from mu to prevent a deadlock where the snapshot
	//                  goroutine holds mu while the signal-poller goroutine tries to
	//                  close stopSnapshotCh; the two goroutines can make independent progress.
	shutSig   *shutdown.Signaller
	cdcCancel context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.Mutex
	closed    bool

	// pendingIncrementalCtx is loaded from the checkpoint store in Connect() when
	// a partial incremental snapshot was interrupted by a prior restart. The first
	// processSignals tick resumes from this context instead of starting fresh.
	// Nil when no incomplete snapshot is pending. Guarded by mu.
	pendingIncrementalCtx *replication.IncrementalSnapshotContext

	// stopSnapshotCh is closed by the signal-poller goroutine when a 'stop-snapshot'
	// signal arrives. runIncrementalSnapshotWithContext selects on this channel to
	// abort the in-progress snapshot. A new channel is allocated immediately after
	// closing so the next snapshot starts with a fresh, un-closed channel.
	// Guarded by stopSnapshotMu.
	stopSnapshotMu sync.Mutex
	stopSnapshotCh chan struct{}
}

func newDB2CDCInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	dsn, err := conf.FieldString(db2CDCFieldDSN)
	if err != nil {
		return nil, err
	}

	schema, err := conf.FieldString(db2CDCFieldSchema)
	if err != nil {
		return nil, err
	}
	schema = strings.ToUpper(schema)
	if !isValidDB2Identifier(schema) {
		return nil, fmt.Errorf("schema %q contains invalid characters: only uppercase letters, digits, and underscores are allowed", schema)
	}

	var tables []string
	if conf.Contains(db2CDCFieldTables) {
		if tables, err = conf.FieldStringList(db2CDCFieldTables); err != nil {
			return nil, err
		}
	}
	for i, t := range tables {
		tables[i] = strings.ToUpper(t)
		if !isValidDB2Identifier(tables[i]) {
			return nil, fmt.Errorf("tables[%d] %q contains invalid characters: only uppercase letters, digits, and underscores are allowed", i, t)
		}
	}

	var includePatterns, excludePatterns []string
	if conf.Contains(db2CDCFieldTableIncludeRegex) {
		if includePatterns, err = conf.FieldStringList(db2CDCFieldTableIncludeRegex); err != nil {
			return nil, err
		}
	}
	if conf.Contains(db2CDCFieldTableExcludeRegex) {
		if excludePatterns, err = conf.FieldStringList(db2CDCFieldTableExcludeRegex); err != nil {
			return nil, err
		}
	}

	tableIncludes, err := confx.ParseRegexpPatterns(includePatterns)
	if err != nil {
		return nil, fmt.Errorf("table_include_regex: %w", err)
	}
	tableExcludes, err := confx.ParseRegexpPatterns(excludePatterns)
	if err != nil {
		return nil, fmt.Errorf("table_exclude_regex: %w", err)
	}

	tableFilter := &confx.RegexpFilter{Include: tableIncludes, Exclude: tableExcludes}

	asnCDCSchema, err := conf.FieldString(db2CDCFieldCDCSchema)
	if err != nil {
		return nil, err
	}
	asnCDCSchema = strings.ToUpper(asnCDCSchema)
	if !isValidDB2Identifier(asnCDCSchema) {
		return nil, fmt.Errorf("cdc_schema %q contains invalid characters: only uppercase letters, digits, and underscores are allowed", asnCDCSchema)
	}

	snapshotModeStr, err := conf.FieldString(db2CDCFieldSnapshotMode)
	if err != nil {
		return nil, err
	}
	mode := snapshotMode(snapshotModeStr)

	snapshotMaxBatchSize, err := conf.FieldInt(db2CDCFieldSnapshotMaxBatchSize)
	if err != nil {
		return nil, err
	}
	if snapshotMaxBatchSize < 1 {
		return nil, errors.New("snapshot_max_batch_size must be at least 1")
	}

	snapshotParallelism, err := conf.FieldInt(db2CDCFieldSnapshotParallelism)
	if err != nil {
		return nil, err
	}

	snapshotIsolationModeStr, err := conf.FieldString(db2CDCFieldSnapshotIsolationMode)
	if err != nil {
		return nil, err
	}
	var snapshotIsolationLevel sql.IsolationLevel
	switch snapshotIsolationModeStr {
	case "repeatable_read":
		snapshotIsolationLevel = sql.LevelSerializable // DB2 RR = sql Serializable
	case "read_committed":
		snapshotIsolationLevel = sql.LevelReadCommitted
	default:
		return nil, fmt.Errorf("snapshot_isolation_mode %q: must be 'repeatable_read' or 'read_committed'", snapshotIsolationModeStr)
	}

	checkpointMode, err := conf.FieldString(db2CDCFieldCheckpointMode)
	if err != nil {
		return nil, err
	}

	var cpCacheName string
	if conf.Contains(db2CDCFieldCheckpointCache) {
		if cpCacheName, err = conf.FieldString(db2CDCFieldCheckpointCache); err != nil {
			return nil, err
		}
	}

	checkpointCacheKey, err := conf.FieldString(db2CDCFieldCheckpointCacheKey)
	if err != nil {
		return nil, err
	}
	if len(checkpointCacheKey) == 0 {
		return nil, errors.New("checkpoint_cache_key must not be empty")
	}
	if len(checkpointCacheKey) > 255 {
		return nil, fmt.Errorf("checkpoint_cache_key length %d exceeds maximum 255 characters", len(checkpointCacheKey))
	}

	cpCacheTableName, err := conf.FieldString(db2CDCFieldCheckpointCacheTableName)
	if err != nil {
		return nil, err
	}
	cpCacheTableName = strings.ToUpper(cpCacheTableName)
	if err := validateQualifiedIdentifier(cpCacheTableName); err != nil {
		return nil, fmt.Errorf("checkpoint_cache_table_name %q: %w", cpCacheTableName, err)
	}

	checkpointLimit, err := conf.FieldInt(db2CDCFieldCheckpointLimit)
	if err != nil {
		return nil, err
	}

	streamBackoffInterval, err := conf.FieldDuration(db2CDCFieldStreamBackoffInterval)
	if err != nil {
		return nil, err
	}
	if streamBackoffInterval <= 0 {
		return nil, fmt.Errorf("stream_backoff_interval must be positive, got %v", streamBackoffInterval)
	}

	pollBatchSize, err := conf.FieldInt(db2CDCFieldPollBatchSize)
	if err != nil {
		return nil, err
	}
	if pollBatchSize < 1 {
		return nil, errors.New("poll_batch_size must be at least 1")
	}

	heartbeatInterval, err := conf.FieldDuration(db2CDCFieldHeartbeatInterval)
	if err != nil {
		return nil, err
	}

	emitSchemaChanges, err := conf.FieldBool(db2CDCFieldEmitSchemaChanges)
	if err != nil {
		return nil, err
	}

	var signalTable string
	if conf.Contains(db2CDCFieldSignalTable) {
		if signalTable, err = conf.FieldString(db2CDCFieldSignalTable); err != nil {
			return nil, err
		}
		signalTable = strings.ToUpper(signalTable)
		if err := validateQualifiedIdentifier(signalTable); err != nil {
			return nil, fmt.Errorf("signal_table %q: %w", signalTable, err)
		}
	}

	tableFilterFn := tableFilter.Matches

	d := &db2CDCInput{
		dsn:                dsn,
		schema:             schema,
		tables:             tables,
		tableFilter:        tableFilter,
		heartbeatInterval:  heartbeatInterval,
		emitSchemaChanges:  emitSchemaChanges,
		lastSeenSynchCSN:   replication.NullCSN(),
		signalTable:        signalTable,
		asnCDCSchema:       asnCDCSchema,
		snapshotMode:       mode,
		checkpointMode:     checkpointMode,
		checkpointCacheKey: checkpointCacheKey,
		checkpointLimit:    checkpointLimit,
		cpCacheName:        cpCacheName,
		cpCacheTableName:   cpCacheTableName,
		snapshotConfig: replication.SnapshotConfig{
			Schemas:        []string{schema},
			Tables:         tables,
			AsnCDCSchema:   asnCDCSchema,
			BatchSize:      snapshotMaxBatchSize,
			TableFilter:    tableFilterFn,
			Parallelism:    snapshotParallelism,
			IsolationLevel: snapshotIsolationLevel,
		},
		streamConfig: replication.StreamConfig{
			Schemas:         []string{schema},
			Tables:          tables,
			AsnCDCSchema:    asnCDCSchema,
			BackoffInterval: streamBackoffInterval,
			PollBatchSize:   pollBatchSize,
			StartingCSN:     replication.NullCSN(),
			TableFilter:     tableFilterFn,
		},
		capped:         checkpoint.NewCapped[replication.CSN](int64(checkpointLimit)),
		eventChan:      make(chan replication.ChangeEvent, 2*pollBatchSize), // 2× so a full batch never stalls the streamer
		errChan:        make(chan error, 2),
		shutSig:        shutdown.NewSignaller(),
		res:            mgr,
		lagGauge:       mgr.Metrics().NewGauge("db2_cdc_lag_ms"),
		log:            mgr.Logger(),
		stopSnapshotCh: make(chan struct{}),
	}

	// Pre-compute invariant SQL strings at construction time. cpCacheTableName and
	// signalTable are immutable after this point, so these strings never change.
	// Computing them here eliminates the data race between initCheckpointTable /
	// initSignalTable (called from Connect) and ack funcs that read them concurrently.
	d.cpMergeSQL = fmt.Sprintf(cpMergeSQLFmt, cpCacheTableName)
	d.cpSelectSQL = fmt.Sprintf(cpSelectSQLFmt, cpCacheTableName)
	d.cpDeleteSQL = fmt.Sprintf(cpDeleteSQLFmt, cpCacheTableName)
	if signalTable != "" {
		d.sigSelectStopSQL = fmt.Sprintf(sigSelectStopSQLFmt, signalTable, signalFetchLimit)
		d.sigSelectPauseSQL = fmt.Sprintf(sigSelectPauseSQLFmt, signalTable, signalFetchLimit)
		d.sigSelectResumeSQL = fmt.Sprintf(sigSelectResumeSQLFmt, signalTable, signalFetchLimit)
		d.sigSelectExecSQL = fmt.Sprintf(sigSelectExecSQLFmt, signalTable, signalFetchLimit)
		d.sigDeleteSQL = fmt.Sprintf(sigDeleteSQLFmt, signalTable)
		d.sigDeleteExecSQL = fmt.Sprintf(sigDeleteExecSQLFmt, signalTable)
		d.sigAbortPollSQL = fmt.Sprintf(sigAbortPollSQLFmt, signalTable)
	}
	// Pre-compute the schema-change poll query template. Only the lastHex watermark
	// (hex-only, safe to embed) varies per call; everything else is construction-time constant.
	d.schemaChangeQueryTemplate = fmt.Sprintf(schemaChangeQueryFmt, asnCDCSchema, schemaChangeFetchLimit)

	batchInput, err := service.AutoRetryNacksBatchedToggled(conf, d)
	if err != nil {
		return nil, err
	}

	return conf.WrapBatchInputExtractTracingSpanMapping("db2_cdc", batchInput)
}

func (d *db2CDCInput) Connect(ctx context.Context) error {
	// Phase 1: hold d.mu only long enough to check/reset the connection state and
	// register the initial non-nil d.db pointer.  Releasing the lock here is
	// required because loadCheckpoint (called in Phase 2) calls loadCheckpointKV,
	// which itself acquires d.mu to read d.db — keeping the lock across that call
	// would deadlock the calling goroutine.
	d.mu.Lock()
	if d.db != nil {
		d.mu.Unlock()
		return nil
	}

	// Reset shutdown state so goroutines launched below start with a fresh
	// signaller. Without this, a prior Close() (which triggers shutSig) would
	// leave the new goroutine's context already cancelled, causing it to exit
	// immediately and making reconnection silently fail.
	d.shutSig = shutdown.NewSignaller()
	d.closed = false
	d.mu.Unlock()

	// Drain any leftover errors from a previous session so ReadBatch does not
	// see stale fatal errors on reconnect. errChan has capacity 2, so loop.
	for {
		select {
		case <-d.errChan:
		default:
			goto drainedErrChan
		}
	}
drainedErrChan:

	// Drain stale buffered events from the previous session.  Without this,
	// ReadBatch would deliver pre-reconnect events that the new CDC loop will
	// also re-produce from the saved checkpoint, causing duplicates.
	for {
		select {
		case <-d.eventChan:
		default:
			goto eventChanDrained
		}
	}
eventChanDrained:

	// Reset the checkpoint tracker so a high pending-count from the old session
	// does not immediately block new events from being tracked.
	d.mu.Lock()
	d.capped = checkpoint.NewCapped[replication.CSN](int64(d.checkpointLimit))
	d.mu.Unlock()

	// Helper: tear down everything that was opened and return the given error.
	// Called on any failure path after d.db is set.
	cleanup := func(err error) error {
		d.mu.Lock()
		if d.auxDB != nil {
			d.auxDB.Close()
			d.auxDB = nil
		}
		if d.db != nil {
			d.db.Close()
			d.db = nil
		}
		d.mu.Unlock()
		return err
	}

	// "db2-cli" is the Redpanda-internal driver name for our purego DB2 CLI
	// adapter (internal/impl/db2/db2cli). We use purego instead of
	// github.com/ibmdb/go_ibm_db because go_ibm_db requires CGO and the IBM
	// C++ runtime at build time, making cross-compilation and pre-built binary
	// distribution impossible. purego loads libdb2.so / libdb2.dylib at
	// runtime, keeping the binary pure Go.
	db, err := sql.Open("db2-cli", d.dsn)
	if err != nil {
		return fmt.Errorf("opening DB2 connection: %w", sanitizeConnErr(err))
	}

	// Limit to a single connection — CDC logic assumes stable connection state.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("pinging DB2: %w", sanitizeConnErr(err))
	}

	// Phase 1b: register d.db under the lock so other goroutines (e.g. ReadBatch,
	// Close) see a consistent non-nil pointer.  From here on, use the cleanup
	// helper on every error path.
	d.mu.Lock()
	d.db = db
	d.mu.Unlock()

	version, err := d.detectVersion(ctx)
	if err != nil {
		return cleanup(fmt.Errorf("detecting DB2 version: %w", err))
	}
	d.mu.Lock()
	d.version = version
	d.mu.Unlock()
	d.log.Infof("Connected to DB2 %s", version)

	actualMode, err := d.determineCheckpointMode()
	if err != nil {
		return cleanup(err)
	}
	d.mu.Lock()
	d.checkpointMode = actualMode
	d.mu.Unlock()

	// Initialize checkpoint persistence.
	if d.cpCacheName == "" {
		if err := d.initCheckpointTable(ctx); err != nil {
			return cleanup(fmt.Errorf("initializing checkpoint table: %w", err))
		}
		d.log.Infof("Using DB2 table %q for checkpoint persistence", d.cpCacheTableName)
	} else {
		d.log.Infof("Using external cache %q for checkpoint persistence", d.cpCacheName)
	}

	// Open a dedicated auxiliary connection for out-of-band operations (signal
	// polling, schema change detection, incremental snapshots). These run
	// concurrently with the main CDC streaming loop, so they need their own
	// connection pool to avoid starving d.db's single slot.
	if d.signalTable != "" || d.emitSchemaChanges {
		auxDB, err := sql.Open("db2-cli", d.dsn)
		if err != nil {
			return cleanup(fmt.Errorf("opening auxiliary DB2 connection: %w", sanitizeConnErr(err)))
		}
		auxDB.SetMaxOpenConns(1)
		auxDB.SetMaxIdleConns(1)
		if err := auxDB.PingContext(ctx); err != nil {
			auxDB.Close()
			return cleanup(fmt.Errorf("pinging auxiliary DB2 connection: %w", sanitizeConnErr(err)))
		}
		d.mu.Lock()
		d.auxDB = auxDB
		d.mu.Unlock()
	}

	// Initialize signal table if configured.
	if d.signalTable != "" {
		if err := d.initSignalTable(ctx); err != nil {
			return cleanup(fmt.Errorf("initializing signal table: %w", err))
		}
		d.log.Infof("Using DB2 table %q for incremental snapshot signals", d.signalTable)
	}

	// Phase 2: load checkpoint / incremental context — these call loadCheckpointKV
	// internally, which acquires d.mu itself (safe because we released it above).
	startingCSN, err := d.loadCheckpoint(ctx)
	if err != nil {
		return cleanup(fmt.Errorf("loading checkpoint: %w", err))
	}

	// Always assign startingCSN so runCDC sees a NullCSN when there is no
	// checkpoint.  The zero-value CSN{} has isNull=false, so leaving
	// StartingCSN unset when no checkpoint exists would cause the snapshot
	// condition (StartingCSN.IsNull()) to be false and the snapshot phase
	// to be silently skipped.
	// Hold d.mu for the write: a concurrent reconnect from ReadBatch could
	// otherwise race against a runCDC goroutine reading streamConfig fields.
	d.mu.Lock()
	d.streamConfig.StartingCSN = startingCSN
	d.mu.Unlock()
	if !startingCSN.IsNull() {
		d.log.Infof("Resuming from checkpoint CSN: %s", startingCSN)
		// The stream poll query uses IBMSNAP_COMMITSEQ > X'<afterCSN>', so
		// setting StartingCSN = savedCSN resumes correctly from the next event.
		// Calling .Next() here was a double-increment that silently dropped
		// events at exactly checkpoint+1.
	} else {
		d.log.Info("No checkpoint found, starting from the beginning")
	}

	loadedIncrCtx, err := d.loadIncrementalContext(ctx)
	if err != nil {
		d.log.Warnf("Failed to load incremental snapshot context, will restart snapshot on next signal: %v", err)
		loadedIncrCtx = nil
	}
	d.mu.Lock()
	d.pendingIncrementalCtx = loadedIncrCtx
	d.mu.Unlock()
	if loadedIncrCtx != nil {
		ct := loadedIncrCtx.CurrentTable()
		if ct != nil {
			d.log.Infof("Resuming incremental snapshot: tables=%d currentTable=%s.%s",
				len(loadedIncrCtx.Tables), safeLogID(ct.Schema), safeLogID(ct.Name))
		} else {
			// All tables were already snapshotted before the crash; the context has
			// an empty Tables queue. Discard it so we don't attempt a no-op resume.
			d.log.Infof("Incremental snapshot context found but all tables already complete; clearing stale checkpoint")
			d.mu.Lock()
			d.pendingIncrementalCtx = nil
			d.mu.Unlock()
			if err := d.clearIncrementalContext(ctx); err != nil {
				d.log.Warnf("Failed to clear stale incremental context: %v", err)
			}
		}
	}

	// Seed the schema-changes watermark so we do not emit spurious schema_change
	// events for tables that were registered before this connector started.
	if d.emitSchemaChanges {
		if err := d.seedSchemaChangesWatermark(ctx); err != nil {
			// Non-fatal: lastSeenSynchCSN remains NullCSN (all-zeros). The first
			// checkForSchemaChanges poll will match every registered table, producing
			// a burst of spurious schema_change events at startup. Those events are
			// noisy but not incorrect — downstream consumers must tolerate them.
			d.log.Warnf("Failed to seed schema-changes watermark: %v", err)
		}
	}

	// Phase 3: derive the goroutine context and launch the CDC loop.
	// Derive a long-lived context from the shutdown signaller, not from the Connect
	// ctx which callers may cancel once Connect returns.
	cdcCtx, cdcCancel := d.shutSig.SoftStopCtx(context.Background())
	d.mu.Lock()
	d.cdcCancel = cdcCancel
	d.mu.Unlock()
	// Add before launching so Close()'s wg.Wait() cannot return between the
	// unlock and the goroutine start.
	d.wg.Add(1)
	go d.runCDC(cdcCtx)

	return nil
}

func (d *db2CDCInput) determineCheckpointMode() (string, error) {
	switch d.checkpointMode {
	case "csn":
		if !d.version.SupportsCDC() {
			return "", fmt.Errorf(
				"checkpoint_mode='csn' requires DB2 10.1+, found version %s; upgrade or use checkpoint_mode='auto'",
				d.version,
			)
		}
		d.log.Info("Using CSN checkpoint mode [EXPLICIT]")
		return "csn", nil

	case "auto":
		if d.version.AtLeast(10, 1) {
			d.log.Infof("Auto-detected CSN support (DB2 %s) — using CSN checkpoint mode", d.version)
			return "csn", nil
		}
		return "", fmt.Errorf("DB2 version %s does not support CSN (requires 10.1+); upgrade DB2", d.version)

	default:
		return "", fmt.Errorf("invalid checkpoint_mode %q: must be 'csn' or 'auto'", d.checkpointMode)
	}
}

func (d *db2CDCInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	// Snapshot shutSig under d.mu to avoid a data race with Connect(), which
	// writes d.shutSig under d.mu. The channel returned by SoftStopChan() is
	// stable for the lifetime of the signaller, so holding the channel reference
	// past the lock is safe.
	d.mu.Lock()
	softStop := d.shutSig.SoftStopChan()
	d.mu.Unlock()

	// Block until at least one event is available.
	var firstEvent replication.ChangeEvent
	select {
	case firstEvent = <-d.eventChan:
	case err := <-d.errChan:
		return nil, nil, err
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-softStop:
		return nil, nil, service.ErrEndOfInput
	}

	// Snapshot PollBatchSize and capped under lock. Connect() replaces d.capped under
	// d.mu on reconnect; reading it without the lock is a data race.
	d.mu.Lock()
	pollBatchSize := d.streamConfig.PollBatchSize
	capped := d.capped
	d.mu.Unlock()

	// Drain additional events non-blocking up to pollBatchSize.
	rawEvents := make([]replication.ChangeEvent, 0, pollBatchSize)
	rawEvents = append(rawEvents, firstEvent)
drainLoop:
	for len(rawEvents) < pollBatchSize {
		select {
		case ev := <-d.eventChan:
			rawEvents = append(rawEvents, ev)
		default:
			break drainLoop
		}
	}

	// Convert events to messages and track each CSN.
	// resolveFns are always called (even on nack) to release Capped capacity slots.
	// The on-disk checkpoint is only advanced when ackErr == nil, so the watermark
	// never moves past unprocessed messages. AutoRetryNacks redelivers by calling
	// the same ackFunc with nil on retry.
	batch := make(service.MessageBatch, 0, len(rawEvents))
	resolveFns := make([]func() *replication.CSN, 0, len(rawEvents))

	// Set lag gauge once per batch using the first (oldest) event, which has the
	// maximum lag. Calling Set on every event would overwrite with decreasing lag
	// values, masking the worst-case latency that matters for alerting.
	for i, event := range rawEvents {
		if i == 0 && !event.Timestamp.IsZero() {
			d.lagGauge.Set(max(time.Since(event.Timestamp).Milliseconds(), 0))
		}
		msg, err := d.eventToMessage(event)
		if err != nil {
			return nil, nil, err
		}
		var resolveFn func() *replication.CSN
		if !event.CSN.IsNull() {
			if resolveFn, err = capped.Track(ctx, event.CSN, 1); err != nil {
				return nil, nil, fmt.Errorf("tracking CSN: %w", err)
			}
		}
		batch = append(batch, msg)
		resolveFns = append(resolveFns, resolveFn)
	}

	ackFunc := func(ctx context.Context, ackErr error) error {
		// Always call resolveFns to release Capped slots even on nack, so the
		// checkpoint capacity does not leak when auto_replay_nacks is false.
		var highest *replication.CSN
		for _, fn := range resolveFns {
			if fn == nil {
				continue
			}
			if h := fn(); h != nil {
				highest = h
			}
		}
		if ackErr != nil {
			// Slots released above; do not advance the checkpoint on batch failure.
			// With auto_replay_nacks: true the batch will be retried and the same
			// ackFunc called again with nil; with auto_replay_nacks: false the
			// in-memory watermark advances but the on-disk checkpoint is NOT saved,
			// so a restart will re-read from the last committed position.
			return nil
		}
		if highest != nil {
			if err := d.saveCheckpoint(ctx, *highest); err != nil {
				d.log.Warnf("failed to save checkpoint: %v", err)
			}
		}
		return nil
	}

	return batch, ackFunc, nil
}

func (d *db2CDCInput) Close(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	sig := d.shutSig // snapshot under lock before releasing
	d.mu.Unlock()

	sig.TriggerSoftStop()

	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		d.log.Warnf("context cancelled while waiting for CDC goroutines to stop: %v", ctx.Err())
		sig.TriggerHardStop()
		d.mu.Lock()
		cancel := d.cdcCancel
		d.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		// Wait for goroutines to drain before closing DB connections. The context
		// is already cancelled so goroutines exit promptly; this prevents
		// use-after-close on auxDB/db when goroutines are mid-query.
		<-done
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.auxDB != nil {
		if err := d.auxDB.Close(); err != nil {
			d.log.Errorf("error closing auxiliary DB2 connection: %v", err)
		}
		d.auxDB = nil
	}

	if d.db != nil {
		if err := d.db.Close(); err != nil {
			d.log.Errorf("error closing DB2 connection: %v", err)
		}
		d.db = nil
	}

	d.streamer = nil

	return nil
}

// runCDC executes the two-phase CDC loop: initial snapshot (optional) → streaming.
func (d *db2CDCInput) runCDC(ctx context.Context) {
	defer d.wg.Done()

	d.mu.Lock()
	shutSig := d.shutSig
	doSnapshot := shouldTakeSnapshot(d.snapshotMode, d.streamConfig.StartingCSN)
	d.mu.Unlock()

	if doSnapshot {
		d.log.Info("Starting snapshot phase")
		if err := d.runSnapshot(ctx); err != nil {
			if ctx.Err() == nil && !shutSig.IsSoftStopSignalled() && !errors.Is(err, service.ErrEndOfInput) {
				select {
				case d.errChan <- fmt.Errorf("snapshot failed: %w", err):
				default:
					d.log.Warnf("Snapshot error dropped (channel full): %v", err)
				}
			}
			return
		}
		d.log.Info("Snapshot complete")
		if d.snapshotMode == snapshotModeInitialOnly {
			d.log.Info("snapshot_mode=initial_only: stopping after snapshot")
			// Signal ReadBatch to return ErrEndOfInput so the pipeline terminates
			// cleanly. Without this, ReadBatch blocks forever on d.eventChan because
			// runCDC exiting does not wake up the ReadBatch select.
			shutSig.TriggerSoftStop()
			return
		}
	}

	d.log.Info("Starting streaming phase")
	if err := d.runStreaming(ctx); err != nil {
		if ctx.Err() == nil && !shutSig.IsSoftStopSignalled() {
			select {
			case d.errChan <- fmt.Errorf("streaming failed: %w", err):
			default:
				d.log.Warnf("Streaming error dropped (channel full): %v", err)
			}
		}
	}
}

func (d *db2CDCInput) runSnapshot(ctx context.Context) error {
	d.mu.Lock()
	db := d.db
	version := d.version
	snapshotConfig := d.snapshotConfig
	d.mu.Unlock()
	if db == nil {
		return nil
	}
	snapshotter := replication.NewSnapshotter(db, snapshotConfig, version)

	handler := func(event replication.ChangeEvent) error {
		select {
		case d.eventChan <- event:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	startingCSN, err := snapshotter.Snapshot(ctx, handler)
	if err != nil {
		return err
	}

	d.mu.Lock()
	d.streamConfig.StartingCSN = startingCSN
	d.mu.Unlock()
	d.log.Infof("Snapshot captured at CSN: %s", startingCSN)

	return nil
}

func (d *db2CDCInput) runStreaming(ctx context.Context) error {
	d.mu.Lock()
	db := d.db
	// Snapshot streamConfig under the lock: the Go memory model does not guarantee
	// visibility of concurrent struct field writes without synchronization, even on
	// different fields. ReadBatch reads streamConfig.PollBatchSize under d.mu, so all
	// reads of streamConfig fields from this goroutine must also hold d.mu.
	streamConfig := d.streamConfig
	version := d.version
	d.mu.Unlock()
	if db == nil {
		return nil
	}
	streamer := replication.NewStreamer(db, streamConfig, version, replication.WithStreamerLogger(d.log))

	if err := streamer.Initialize(ctx); err != nil {
		return fmt.Errorf("initializing streamer: %w", err)
	}

	// Make the Streamer accessible to pollSignals so the incremental snapshot engine
	// can install deduplication windows. Must be set before any goroutine reads it.
	d.mu.Lock()
	d.streamer = streamer
	d.mu.Unlock()

	// Read BackoffInterval from the already-locked local snapshot (captured at the
	// top of runStreaming under d.mu). A second lock acquire would be a race-detector
	// hit: both accesses land on the same struct with different goroutines writing
	// adjacent fields, which the detector treats as a potential race even when the
	// exact field has not been concurrently written.
	backoffInterval := streamConfig.BackoffInterval

	// streamCtx is cancelled when streamer.Stream returns (success, error, or
	// context cancellation). This guarantees sub-goroutines (heartbeat/signals/
	// schema) exit promptly after a stream failure rather than leaking until Close.
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()

	// Pre-register all sub-goroutine counts before launching any. This prevents
	// a race where Close()'s wg.Wait() completes between individual wg.Add+go pairs.
	addCount := 0
	if d.heartbeatInterval > 0 {
		addCount++
	}
	if d.emitSchemaChanges {
		addCount++
	}
	if d.signalTable != "" {
		addCount++
	}
	if addCount > 0 {
		d.wg.Add(addCount)
	}

	if d.heartbeatInterval > 0 {
		go d.runHeartbeat(streamCtx)
	}

	if d.emitSchemaChanges {
		go d.pollSchemaChanges(streamCtx, backoffInterval)
	}

	if d.signalTable != "" {
		go d.pollSignals(streamCtx, backoffInterval)
	}

	handler := func(event replication.ChangeEvent) error {
		select {
		case d.eventChan <- event:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return streamer.Stream(ctx, handler)
}

func (d *db2CDCInput) runHeartbeat(ctx context.Context) {
	defer d.wg.Done()
	ticker := time.NewTicker(d.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			hb := replication.ChangeEvent{
				Operation: replication.OpTypeHeartbeat,
				Timestamp: time.Now().UTC(),
			}
			select {
			case d.eventChan <- hb:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (d *db2CDCInput) pollSchemaChanges(ctx context.Context, interval time.Duration) {
	defer d.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := d.checkForSchemaChanges(ctx); err != nil {
				d.log.Warnf("schema change poll: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// seedSchemaChangesWatermark seeds lastSeenSynchCSN to the current maximum
// CD_NEW_SYNCHPOINT in IBMSNAP_REGISTER so that the first checkForSchemaChanges
// poll does not emit spurious schema_change events for tables that were already
// registered before this connector started.
func (d *db2CDCInput) seedSchemaChangesWatermark(ctx context.Context) error {
	d.mu.Lock()
	auxDB := d.auxDB
	d.mu.Unlock()
	if auxDB == nil {
		return nil
	}
	var maxBytes []byte
	err := auxDB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT MAX(CD_NEW_SYNCHPOINT) FROM %s.IBMSNAP_REGISTER WHERE SOURCE_OWNER = ?", d.asnCDCSchema),
		d.schema,
	).Scan(&maxBytes)
	if errors.Is(err, sql.ErrNoRows) || len(maxBytes) == 0 {
		return nil
	}
	if err != nil {
		return err
	}
	csn := replication.NewCSNFromDBValue(maxBytes)
	d.mu.Lock()
	d.lastSeenSynchCSN = csn
	d.mu.Unlock()
	return nil
}

func (d *db2CDCInput) checkForSchemaChanges(ctx context.Context) error {
	// Read auxDB, streamer, and lastSeenSynchCSN under a single lock acquisition.
	d.mu.Lock()
	auxDB := d.auxDB
	streamer := d.streamer
	lastSynch := d.lastSeenSynchCSN
	d.mu.Unlock()
	if auxDB == nil {
		return nil
	}

	// CommitSeqByteLen is 10 for DB2 ≤11.x and 16 for DB2 12.1+. The Streamer
	// detects the actual length during Initialize() and stores it on its own
	// config copy — read from the Streamer rather than d.streamConfig (which is
	// never updated after the Streamer is constructed).
	var byteLen int
	if streamer != nil {
		byteLen = streamer.CommitSeqByteLen()
	}
	if byteLen <= 0 {
		byteLen = 10
	}
	lastHex := lastSynch.SQLHex(byteLen)

	// schemaChangeQueryTemplate is pre-computed at construction; only lastHex varies.
	// lastHex is from CSN.SQLHex() (hex digits only, enforced by the encoder) so it
	// is safe to embed. schemaChangeFetchLimit caps memory use when many tables are
	// added at once; the watermark advances per-row so subsequent polls catch overflow.
	query := fmt.Sprintf(d.schemaChangeQueryTemplate, lastHex)

	rows, err := auxDB.QueryContext(ctx, query, d.schema)
	if err != nil {
		return fmt.Errorf("querying schema changes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var srcOwner, srcTable string
		var synchBytes []byte
		if err := rows.Scan(&srcOwner, &srcTable, &synchBytes); err != nil {
			d.log.Warnf("scanning schema change row: %v", err)
			continue
		}
		cleanOwner := strings.TrimSpace(srcOwner)
		cleanTable := strings.TrimSpace(srcTable)
		if !isValidDB2Identifier(cleanOwner) || !isValidDB2Identifier(cleanTable) {
			d.log.Warnf("schema change: skipping row with invalid identifier owner=%q table=%q", safeLogID(cleanOwner), safeLogID(cleanTable))
			continue
		}
		csn := replication.NewCSNFromDBValue(synchBytes)
		event := replication.ChangeEvent{
			Schema:    cleanOwner,
			Table:     cleanTable,
			Operation: replication.OpTypeSchemaChange,
			CSN:       csn,
			Timestamp: time.Now().UTC(),
		}
		// Invalidate the column metadata cache for this source table so that the
		// next poll re-derives column indices from the updated change table schema
		// (e.g., after ASNCDC.ADDCOLUMN). Safe when the table is not yet tracked
		// (InvalidateColumnMetaForSource is a no-op for unknown source keys).
		if streamer != nil {
			streamer.InvalidateColumnMetaForSource(cleanOwner, cleanTable)
		}
		// Note: s.changeTables is not updated here. Tables added via ASNCDC.ADDTABLE
		// are reported as schema_change events but not polled until the connector
		// restarts. A future Streamer.AddTable() API would enable hot-add without restart.
		// Advance the watermark only after the event is delivered so a crash/restart
		// does not skip the event.
		select {
		case d.eventChan <- event:
			d.mu.Lock()
			d.lastSeenSynchCSN = csn
			d.mu.Unlock()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return rows.Err()
}

// initSignalTable creates the signal table in DB2 if it does not exist.
func (d *db2CDCInput) initSignalTable(ctx context.Context) error {
	d.mu.Lock()
	auxDB := d.auxDB
	d.mu.Unlock()
	if auxDB == nil {
		return errors.New("not connected")
	}
	if parts := strings.SplitN(d.signalTable, ".", 2); len(parts) == 2 {
		// Double-quote the schema name for defense-in-depth; isValidDB2Identifier
		// already guarantees [A-Z0-9_] so no embedded quotes can exist, but quoting
		// makes the SQL safe by structure rather than by validation guarantee.
		schemaName := replication.QuoteDB2Identifier(parts[0])
		if _, err := auxDB.ExecContext(ctx, "CREATE SCHEMA "+schemaName); err != nil && !isAlreadyExistsError(err) {
			return fmt.Errorf("creating schema %q: %w", parts[0], err)
		}
	}
	_, err := auxDB.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			ID   VARCHAR(255) NOT NULL,
			TYPE VARCHAR(64)  NOT NULL,
			DATA VARCHAR(2048),
			PRIMARY KEY (ID)
		)`, d.signalTable))
	if err != nil && !isAlreadyExistsError(err) {
		return fmt.Errorf("create signal table %s: %w", d.signalTable, err)
	}
	return nil
}

func (d *db2CDCInput) pollSignals(ctx context.Context, interval time.Duration) {
	defer d.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := d.processSignals(ctx); err != nil {
				d.log.Warnf("signal poll: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// processSignals polls the signal table for execute-snapshot requests and drives
// the incremental snapshot engine.
//
//	Signal table ──► parseSnapshotSignalTables ──► dedup ──► filterPendingTables
//	                                                               │
//	                                                               ▼
//	                                           IncrementalSnapshotEngine.RunTable()
//	                                                               │
//	                                           ┌──────────────────┤
//	                                           │  Per-chunk:      │
//	                                           │  checkpoint      │
//	                                           └──────────────────┘
//	                                                               │
//	                                           clearIncrementalContext (all tables done)
//
// On Connect(), any saved IncrementalSnapshotContext is loaded into
// pendingIncrementalCtx and resumed here before new signals are processed.
func (d *db2CDCInput) processSignals(ctx context.Context) error {
	d.mu.Lock()
	auxDB := d.auxDB
	d.mu.Unlock()
	if auxDB == nil {
		return nil
	}

	// Handle stop-snapshot signals before anything else so they abort any pending resume.
	if d.signalTable != "" {
		stopRows, stopErr := auxDB.QueryContext(ctx, d.sigSelectStopSQL)
		if stopErr != nil {
			d.log.Warnf("querying stop-snapshot signals: %v", stopErr)
		} else {
			var stopIDs []string
			for stopRows.Next() {
				var id string
				if e := stopRows.Scan(&id); e != nil {
					d.log.Warnf("scanning stop-snapshot signal row: %v", e)
					continue
				}
				stopIDs = append(stopIDs, id)
			}
			if e := stopRows.Err(); e != nil {
				d.log.Warnf("iterating stop-snapshot signals: %v", e)
			}
			stopRows.Close()
			if len(stopIDs) > 0 {
				// Process only the first stop signal; close the channel once.
				id := stopIDs[0]
				d.log.Infof("stop-snapshot signal id=%q: aborting incremental snapshot", safeLogID(id))
				d.stopSnapshotMu.Lock()
				close(d.stopSnapshotCh)
				d.stopSnapshotCh = make(chan struct{})
				d.stopSnapshotMu.Unlock()
				if err := d.clearIncrementalContext(ctx); err != nil {
					d.log.Warnf("stop-snapshot: failed to clear incremental context checkpoint: %v", err)
				}
				d.mu.Lock()
				d.pendingIncrementalCtx = nil
				d.mu.Unlock()
				for _, delID := range stopIDs {
					if _, err := auxDB.ExecContext(ctx, d.sigDeleteSQL, delID); err != nil {
						d.log.Warnf("stop-snapshot: deleting signal id=%q: %v", safeLogID(delID), err)
					}
				}
			}
			// Skip execute-snapshot processing this cycle — stop signals take precedence.
			if len(stopIDs) > 0 {
				return nil
			}
		}
	}

	// Handle pause-snapshot signals.
	if d.signalTable != "" {
		pauseRows, pauseErr := auxDB.QueryContext(ctx, d.sigSelectPauseSQL)
		if pauseErr != nil {
			d.log.Warnf("querying pause-snapshot signals: %v", pauseErr)
		} else {
			var pauseIDs []string
			for pauseRows.Next() {
				var id string
				if e := pauseRows.Scan(&id); e != nil {
					d.log.Warnf("scanning pause-snapshot signal row: %v", e)
					continue
				}
				pauseIDs = append(pauseIDs, id)
			}
			if e := pauseRows.Err(); e != nil {
				d.log.Warnf("iterating pause-snapshot signals: %v", e)
			}
			pauseRows.Close()
			if len(pauseIDs) > 0 {
				// Process only the first pause signal; close the channel once.
				id := pauseIDs[0]
				d.log.Infof("pause-snapshot signal id=%q: pausing incremental snapshot", safeLogID(id))
				d.stopSnapshotMu.Lock()
				close(d.stopSnapshotCh)
				d.stopSnapshotCh = make(chan struct{})
				d.stopSnapshotMu.Unlock()
				// Clear the in-memory resume pointer so the next processSignals call
				// does not auto-resume the snapshot before a resume-snapshot signal
				// arrives. The DB checkpoint is intentionally preserved so that
				// resume-snapshot can reload it and continue from the same position.
				d.mu.Lock()
				pendingForPause := d.pendingIncrementalCtx
				d.pendingIncrementalCtx = nil
				d.mu.Unlock()
				// Mark only the active snapshot's execute-snapshot signal rows as paused so
				// unrelated pending snapshots (different signal IDs) are not accidentally paused.
				if pendingForPause != nil && len(pendingForPause.SignalIDs) > 0 {
					pausePlaceholders := make([]string, len(pendingForPause.SignalIDs))
					pauseArgs := make([]any, len(pendingForPause.SignalIDs))
					for i, sigID := range pendingForPause.SignalIDs {
						pausePlaceholders[i] = "?"
						pauseArgs[i] = sigID
					}
					scopedPauseSQL := fmt.Sprintf(
						"UPDATE %s SET TYPE = 'paused-snapshot' WHERE ID IN (%s) AND TYPE = 'execute-snapshot'",
						d.signalTable, strings.Join(pausePlaceholders, ","),
					)
					if _, err := auxDB.ExecContext(ctx, scopedPauseSQL, pauseArgs...); err != nil {
						d.log.Warnf("pause-snapshot: marking execute-snapshot signals as paused: %v", err)
					}
				} else {
					// No active snapshot context — nothing to pause. A blanket UPDATE here
					// would incorrectly pause unrelated execute-snapshot signals that have
					// not yet been processed.
					d.log.Infof("pause-snapshot signal id=%q: no active snapshot context; nothing to pause", safeLogID(id))
				}
				for _, delID := range pauseIDs {
					if _, err := auxDB.ExecContext(ctx, d.sigDeleteSQL, delID); err != nil {
						d.log.Warnf("pause-snapshot: deleting pause signal id=%q: %v", safeLogID(delID), err)
					}
				}
				// Skip resume/execute processing this tick — pause takes precedence.
				// Without this return, a co-existing resume-snapshot row would flip the
				// paused signals back to execute-snapshot in the same tick, defeating the pause.
				return nil
			}
		}
	}

	// Handle resume-snapshot signals — flip paused-snapshot back to execute-snapshot.
	if d.signalTable != "" {
		resumeRows, resumeErr := auxDB.QueryContext(ctx, d.sigSelectResumeSQL)
		if resumeErr != nil {
			d.log.Warnf("querying resume-snapshot signals: %v", resumeErr)
		} else {
			var resumeIDs []string
			for resumeRows.Next() {
				var id string
				if e := resumeRows.Scan(&id); e != nil {
					d.log.Warnf("scanning resume-snapshot signal row: %v", e)
					continue
				}
				resumeIDs = append(resumeIDs, id)
			}
			if e := resumeRows.Err(); e != nil {
				d.log.Warnf("iterating resume-snapshot signals: %v", e)
			}
			resumeRows.Close()
			if len(resumeIDs) > 0 {
				// Load the paused context first so we can scope the UPDATE to its
				// SignalIDs, avoiding an inadvertent blanket-unpausing of any other
				// concurrently-paused snapshots that belong to a different session.
				inc, incErr := d.loadIncrementalContext(ctx)
				if incErr != nil {
					d.log.Warnf("resume-snapshot: loading incremental context: %v", incErr)
				}

				if inc != nil && len(inc.SignalIDs) > 0 {
					// Scoped UPDATE: only flip the signal rows that belong to this
					// checkpoint's execute-snapshot batch.
					placeholders := strings.TrimSuffix(strings.Repeat("?,", len(inc.SignalIDs)), ",")
					scopedSQL := fmt.Sprintf(
						"UPDATE %s SET TYPE = 'execute-snapshot' WHERE ID IN (%s) AND TYPE = 'paused-snapshot'",
						d.signalTable, placeholders,
					)
					args := make([]any, len(inc.SignalIDs))
					for i, id := range inc.SignalIDs {
						args[i] = id
					}
					if _, err := auxDB.ExecContext(ctx, scopedSQL, args...); err != nil {
						d.log.Warnf("resume-snapshot: restoring execute-snapshot signals (scoped): %v", err)
					}
				} else {
					// No SignalIDs in persisted checkpoint (either the checkpoint is
					// absent or predates SignalID tracking). Executing an unscoped
					// UPDATE would inadvertently un-pause unrelated concurrent snapshots
					// in a multi-signal environment. Skip the UPDATE and log so the
					// operator knows they may need to re-send the resume-snapshot signal.
					d.log.Warnf("resume-snapshot: no SignalIDs in incremental checkpoint; skipping execute-snapshot restore — re-send resume-snapshot signal if snapshot does not resume")
				}

				for _, id := range resumeIDs {
					d.log.Infof("resume-snapshot signal id=%q: resuming incremental snapshot", safeLogID(id))
					if _, err := auxDB.ExecContext(ctx, d.sigDeleteSQL, id); err != nil {
						d.log.Warnf("resume-snapshot: deleting resume signal id=%q: %v", safeLogID(id), err)
					}
				}

				if inc != nil {
					d.mu.Lock()
					d.pendingIncrementalCtx = inc
					d.mu.Unlock()
					d.log.Infof("resume-snapshot: reloaded incremental context (%d tables)", len(inc.Tables))
				}
			}
		}
	}

	// Resume a snapshot that was interrupted by a previous restart before polling
	// for new signals — this ensures we pick up where we left off.
	d.mu.Lock()
	pendingCtx := d.pendingIncrementalCtx
	s := d.streamer // read under lock: d.streamer may be nil if Close() ran before we reach here
	d.mu.Unlock()
	if pendingCtx != nil && s != nil {
		// Clear AFTER a successful run only. If runIncrementalSnapshotWithContext
		// returns an error (e.g. context cancelled), keep pendingIncrementalCtx
		// intact so the next poll can resume from the same position.
		if err := d.runIncrementalSnapshotWithContext(ctx, s, pendingCtx); err != nil {
			// context.Canceled with parent ctx still alive means a stop/pause signal
			// aborted the snapshot intentionally — not a fatal error.
			if errors.Is(err, context.Canceled) && ctx.Err() == nil {
				d.log.Infof("db2 cdc: incremental snapshot resume aborted by stop/pause signal")
				return nil
			}
			// errSignalCleanupRetry: snapshot done but signal DELETE failed.
			// Leave pendingIncrementalCtx intact; next tick retries the delete
			// via ictx.SignalIDs without re-running the snapshot.
			if errors.Is(err, errSignalCleanupRetry) {
				d.log.Infof("db2 cdc: signal cleanup will retry on next poll (transient DELETE failure)")
				return nil
			}
			return fmt.Errorf("resuming incremental snapshot: %w", err)
		}
		d.mu.Lock()
		d.pendingIncrementalCtx = nil
		d.mu.Unlock()
	}

	// Collect all pending signals into memory first, then close the result set
	// before calling runIncrementalSnapshot. This avoids a single-connection
	// deadlock: the SELECT would hold the auxDB connection open while the
	// snapshot transaction needs that same connection.
	type pendingSignal struct {
		id, data string
	}
	var pending []pendingSignal

	rows, err := auxDB.QueryContext(ctx, d.sigSelectExecSQL)
	if err != nil {
		return fmt.Errorf("querying signals: %w", err)
	}
	for rows.Next() {
		var id string
		var data sql.NullString
		if err := rows.Scan(&id, &data); err != nil {
			d.log.Warnf("scanning signal row: %v", err)
			continue
		}
		pending = append(pending, pendingSignal{id: id, data: data.String})
	}
	scanErr := rows.Err()
	rows.Close() // release auxDB connection before snapshot
	if scanErr != nil {
		return scanErr
	}

	for _, sig := range pending {
		tables := parseSnapshotSignalTables(sig.data, d.schema, func(sigSchema, table string) {
			d.log.Warnf("execute-snapshot signal id=%q: schema prefix %q on table %q does not match connector schema %q; snapshotting %s.%s",
				safeLogID(sig.id), safeLogID(sigSchema), safeLogID(table), d.schema, d.schema, safeLogID(table))
		})
		tables = d.filterPendingTables(tables)
		if len(tables) == 0 {
			d.log.Infof("execute-snapshot signal id=%q: all tables already in pending resume context; skipping", safeLogID(sig.id))
			if _, err := auxDB.ExecContext(ctx, d.sigDeleteExecSQL, sig.id); err != nil {
				d.log.Warnf("execute-snapshot: deleting skipped signal id=%q: %v", safeLogID(sig.id), err)
			}
			continue
		}
		safeTables := make([]string, len(tables))
		for i, t := range tables {
			safeTables[i] = safeLogID(t)
		}
		d.log.Infof("received execute-snapshot signal id=%q tables=%q", safeLogID(sig.id), safeTables)
		if err := d.runIncrementalSnapshot(ctx, sig.id, tables); err != nil {
			d.log.Warnf("incremental snapshot for signal %q: %v", safeLogID(sig.id), err)
			if errors.Is(err, errSignalCleanupRetry) {
				// pendingIncrementalCtx is still set from the retry path; break so
				// filterPendingTables on subsequent iterations doesn't use stale state.
				break
			}
		}
		// Signal deletion is handled inside runIncrementalSnapshotWithContext after
		// clearIncrementalContext to ensure atomicity (no duplicate delivery on crash).
	}
	return nil
}

func (d *db2CDCInput) runIncrementalSnapshot(ctx context.Context, signalID string, tables []string) error {
	// Validate and filter table names from the (untrusted) signal DATA field
	// before they reach any SQL interpolation — applies to BOTH code paths below.
	validated := make([]string, 0, len(tables))
	for _, t := range tables {
		if !isValidDB2Identifier(t) {
			d.log.Warnf("Incremental snapshot: ignoring table %q from signal — fails identifier validation (possible injection attempt)", safeLogID(t))
			continue
		}
		validated = append(validated, t)
	}
	filtered := make([]string, 0, len(validated))
	for _, t := range validated {
		// Enforce the configured tables allowlist so signal-triggered snapshots
		// cannot target tables the user did not explicitly configure.
		if len(d.tables) > 0 && !slices.Contains(d.tables, t) {
			d.log.Warnf("Incremental snapshot: ignoring table %q from signal — not in configured tables list", safeLogID(t))
			continue
		}
		if d.tableFilter == nil || d.tableFilter.Matches(t) {
			filtered = append(filtered, t)
		}
	}
	if len(filtered) == 0 {
		d.log.Warnf("Incremental snapshot: all requested tables excluded by table filter or identifier validation; skipping")
		return nil
	}

	d.mu.Lock()
	s := d.streamer
	auxDB := d.auxDB
	version := d.version
	d.mu.Unlock()

	if s == nil {
		// Streaming not yet initialised; fall back to non-coordinated snapshot.
		// This only happens if a signal arrives before the Streamer is ready.
		d.log.Warnf("Incremental snapshot requested before Streamer is ready; using full snapshot (no dedup window)")
		if auxDB == nil {
			return nil
		}
		cfg := replication.SnapshotConfig{
			Schemas:      []string{d.schema},
			Tables:       filtered,
			AsnCDCSchema: d.asnCDCSchema,
			BatchSize:    d.snapshotConfig.BatchSize,
		}
		snapshotter := replication.NewSnapshotter(auxDB, cfg, version)
		handler := func(event replication.ChangeEvent) error {
			select {
			case d.eventChan <- event:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		_, snapshotErr := snapshotter.Snapshot(ctx, handler)
		if snapshotErr == nil && signalID != "" {
			if _, delErr := auxDB.ExecContext(ctx, d.sigDeleteSQL, signalID); delErr != nil {
				d.log.Warnf("fallback snapshot completed but could not delete signal id=%q: %v", safeLogID(signalID), delErr)
			}
		}
		return snapshotErr
	}

	iTables := make([]replication.IncrementalTable, len(filtered))
	for i, t := range filtered {
		iTables[i] = replication.IncrementalTable{Schema: d.schema, Name: t, SignalID: signalID}
	}

	ictx := &replication.IncrementalSnapshotContext{
		Tables:        iTables,
		LastEmittedPK: map[string][]any{},
		MaxPK:         map[string][]any{},
	}

	return d.runIncrementalSnapshotWithContext(ctx, s, ictx)
}

func (d *db2CDCInput) runIncrementalSnapshotWithContext(ctx context.Context, s *replication.Streamer, ictx *replication.IncrementalSnapshotContext) error {
	d.mu.Lock()
	auxDB := d.auxDB
	d.mu.Unlock()
	if auxDB == nil {
		// auxDB is nil when the connector has been closed or Connect() has not yet
		// been called. A nil here means the snapshot signal is silently dropped.
		// Log so operators know rather than wonder why no snapshot rows arrived.
		d.log.Warnf("db2 cdc: incremental snapshot requested but auxiliary DB connection is nil (connector closing?); snapshot dropped")
		return nil
	}

	// Derive a cancellable context so stop-snapshot signals can abort mid-snapshot.
	d.stopSnapshotMu.Lock()
	stopCh := d.stopSnapshotCh
	d.stopSnapshotMu.Unlock()

	snapshotCtx, snapshotCancel := context.WithCancel(ctx)
	stopWatchDone := make(chan struct{})
	pollerDone := make(chan struct{})
	// Cancel before waiting: snapshotCancel() must run first so the watcher and
	// poller goroutines see snapshotCtx.Done() and exit. Defer is LIFO, so a
	// single closure guarantees the correct order.
	defer func() {
		snapshotCancel()
		<-stopWatchDone
		<-pollerDone
	}()
	go func() {
		defer close(stopWatchDone)
		select {
		case <-stopCh:
			snapshotCancel()
		case <-snapshotCtx.Done():
		}
	}()
	if d.signalTable != "" {
		// Poll for stop/pause signals every 5 s so they can abort a running snapshot.
		// processSignals is blocked while the snapshot runs and cannot do this itself.
		//
		// Note: this goroutine shares auxDB (MaxOpenConns(1)) with the ChunkReader.
		// QueryRowContext here blocks until the current chunk SELECT finishes and
		// rows.Close() returns the connection. Abort reaction is therefore bounded by
		// the duration of one chunk read (~snapshot_max_batch_size rows). For default
		// settings (1024 rows) this is typically sub-second; for very wide tables or
		// slow storage it may take longer but the abort is guaranteed to fire after
		// the chunk completes.
		go func() {
			defer close(pollerDone)
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-snapshotCtx.Done():
					return
				case <-ticker.C:
					var signalType string
					if err := auxDB.QueryRowContext(snapshotCtx, d.sigAbortPollSQL).Scan(&signalType); err != nil {
						continue // no rows or transient error — keep polling
					}
					d.log.Infof("db2 cdc: %q signal received while snapshot is running; aborting snapshot", safeLogID(signalType))
					d.stopSnapshotMu.Lock()
					close(d.stopSnapshotCh)
					d.stopSnapshotCh = make(chan struct{})
					d.stopSnapshotMu.Unlock()
					// Cancel immediately rather than waiting for the stopWatcher goroutine
					// to observe the closed channel — eliminates one goroutine scheduling
					// round-trip that could allow an extra chunk to start before abort.
					snapshotCancel()
					return
				}
			}
		}()
	} else {
		close(pollerDone) // no signal table — poller not needed
	}

	engine := replication.NewIncrementalSnapshotEngine(
		auxDB,
		d.asnCDCSchema,
		d.snapshotConfig.BatchSize,
		s,
		func(ctx context.Context, inc *replication.IncrementalSnapshotContext) error {
			return d.saveIncrementalContext(ctx, inc)
		},
		d.log,
	)

	// Collect signal IDs before the loop: AdvanceTable removes entries from
	// ictx.Tables, so by the time the loop exits ictx.Tables is empty and we
	// cannot recover the signal IDs after the fact.
	//
	// On the cleanup-only path (Running()==false after restart), Tables is already
	// empty so we recover IDs from ictx.SignalIDs instead.
	signalIDs := make(map[string]struct{})
	for _, tbl := range ictx.Tables {
		if tbl.SignalID != "" {
			signalIDs[tbl.SignalID] = struct{}{}
		}
	}
	// Populate from ictx.SignalIDs on the cleanup-only path (Tables already empty).
	for _, sigID := range ictx.SignalIDs {
		signalIDs[sigID] = struct{}{}
	}

	// Persist signal IDs into the context checkpoint before the snapshot loop so
	// that a cross-session retry can recover them even if the session crashes after
	// all tables are snapshotted but before signal DELETE succeeds.
	if len(signalIDs) > 0 && len(ictx.SignalIDs) == 0 {
		for sigID := range signalIDs {
			ictx.SignalIDs = append(ictx.SignalIDs, sigID)
		}
		if err := d.saveIncrementalContext(ctx, ictx); err != nil {
			d.log.Warnf("failed to persist signal IDs in incremental context: %v", err)
		}
	}

	for ictx.Running() {
		if err := snapshotCtx.Err(); err != nil {
			return err
		}
		table := *ictx.CurrentTable()
		streamCSN := s.CurrentCSN()

		d.log.Infof("Starting incremental snapshot for %s.%s (from CSN %s)", safeLogID(table.Schema), safeLogID(table.Name), streamCSN)
		if err := engine.RunTable(snapshotCtx, ictx, table, streamCSN); err != nil {
			return fmt.Errorf("incremental snapshot %s.%s: %w", table.Schema, table.Name, err)
		}
		d.log.Infof("Completed incremental snapshot for %s.%s", safeLogID(table.Schema), safeLogID(table.Name))
		ictx.AdvanceTable()
	}

	// Delete signal rows BEFORE clearing the context checkpoint so that on a
	// crash between these two operations the context is still present and the
	// snapshot can resume from the last checkpoint rather than restarting from
	// scratch and delivering duplicate rows.
	//
	// If any DELETE fails, return errSignalCleanupRetry so the caller preserves
	// pendingIncrementalCtx. The next processSignals tick re-enters this function
	// with Running()==false and populates signalIDs from ictx.SignalIDs to retry.
	var anyDeleteFailed bool
	for sigID := range signalIDs {
		if _, err := auxDB.ExecContext(ctx, d.sigDeleteSQL, sigID); err != nil {
			d.log.Warnf("failed to delete execute-snapshot signal %q after completion: %v", safeLogID(sigID), err)
			anyDeleteFailed = true
		}
	}
	if anyDeleteFailed {
		// Preserve ictx so the next processSignals tick resumes from the last
		// checkpoint rather than re-running the full snapshot (duplicate delivery).
		d.mu.Lock()
		d.pendingIncrementalCtx = ictx
		d.mu.Unlock()
		return errSignalCleanupRetry
	}

	if err := d.clearIncrementalContext(ctx); err != nil {
		d.log.Warnf("Failed to clear incremental context checkpoint: %v", err)
	}
	return nil
}

// parseSnapshotSignalTables parses the DATA field of an execute-snapshot signal.
// Accepts JSON {"data-collections":["SCHEMA.TABLE",...]} or bare table names.
// Schema prefixes are stripped; duplicate table names are removed; the caller
// sets Schema separately in SnapshotConfig.
// warnFn is called (when non-nil) for each entry whose schema prefix does not
// match expectedSchema, so the caller can warn the operator.
func parseSnapshotSignalTables(data, expectedSchema string, warnFn func(signalSchema, table string)) []string {
	// Use a pointer so we can distinguish an explicit empty array [] (key present,
	// len 0) from an absent data-collections key (pointer nil). When the key is
	// absent we fall through to the comma-split path for backward compatibility.
	type signalData struct {
		DataCollections *[]string `json:"data-collections"`
	}
	var sd signalData
	var raw []string
	jsonParsed := json.Unmarshal([]byte(data), &sd) == nil
	if jsonParsed && sd.DataCollections != nil && len(*sd.DataCollections) == 0 {
		// Explicit empty list means "no tables" — return nil rather than falling
		// through to the comma-split path, which would treat the raw JSON string
		// as a table name and emit a misleading isValidDB2Identifier warning.
		return nil
	}
	stripSchema := func(t string) string {
		t = strings.ToUpper(t)
		if parts := strings.SplitN(t, ".", 2); len(parts) == 2 {
			if warnFn != nil && parts[0] != expectedSchema {
				warnFn(parts[0], parts[1])
			}
			return parts[1]
		}
		return t
	}
	if jsonParsed && sd.DataCollections != nil && len(*sd.DataCollections) > 0 {
		raw = make([]string, 0, len(*sd.DataCollections))
		for _, t := range *sd.DataCollections {
			raw = append(raw, stripSchema(t))
		}
	} else {
		// Fallback: comma-separated table names.
		if data == "" {
			return nil
		}
		parts := strings.Split(data, ",")
		raw = make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			raw = append(raw, stripSchema(p))
		}
	}
	// Deduplicate while preserving order. Allocate a fresh slice rather than
	// raw[:0] to avoid sharing the backing array with the caller's input.
	seen := make(map[string]struct{}, len(raw))
	out := make([]string, 0, len(raw))
	for _, t := range raw {
		if _, ok := seen[t]; !ok {
			seen[t] = struct{}{}
			out = append(out, t)
		}
	}
	return out
}

// filterPendingTables returns tables with any entries already queued in
// pendingIncrementalCtx removed — those will be handled on resume.
// Allocates a fresh output slice to avoid aliasing the input backing array.
func (d *db2CDCInput) filterPendingTables(tables []string) []string {
	d.mu.Lock()
	pendingCtx := d.pendingIncrementalCtx
	d.mu.Unlock()
	if pendingCtx == nil {
		return tables
	}
	pending := make(map[string]struct{}, len(pendingCtx.Tables))
	for _, t := range pendingCtx.Tables {
		pending[t.Schema+"."+t.Name] = struct{}{}
	}
	out := make([]string, 0, len(tables))
	for _, t := range tables {
		if _, skip := pending[d.schema+"."+t]; !skip {
			out = append(out, t)
		}
	}
	return out
}

// debeziumOp maps a replication.OpType to the Debezium operation code used in the
// "op" field of the Debezium envelope and in the db2_op metadata key.
//
// Codes: c=insert, u=update, d=delete, r=snapshot read, hb=heartbeat.
// schema_change and any unrecognised type fall through to the string value of op.
func debeziumOp(op replication.OpType) string {
	switch op {
	case replication.OpTypeInsert:
		return "c"
	case replication.OpTypeUpdate:
		return "u"
	case replication.OpTypeDelete:
		return "d"
	case replication.OpTypeRead:
		return "r"
	case replication.OpTypeHeartbeat:
		return "hb"
	default:
		return string(op)
	}
}

// idempotencyKey returns a deterministic string key for the given event.
// CDC events use schema.table.CSN.intentseq; snapshot events use PK column values only.
// Using only PK columns prevents non-PK columns (which may contain sensitive data)
// from appearing verbatim in message metadata.
// Consumers can use this key to deduplicate at-least-once delivery.
func idempotencyKey(event replication.ChangeEvent) string {
	if !event.CSN.IsNull() {
		var sb strings.Builder
		// 36 = max CSN string length: "CSN:" + 32 hex digits (DB2 12.1 16-byte CSN).
		// DB2 ≤11.x uses "CSN:" + 20 hex = 24; over-estimating by 12 avoids realloc.
		sb.Grow(len(event.Schema) + 1 + len(event.Table) + 1 + 36 + 1 + 20)
		sb.WriteString(event.Schema)
		sb.WriteByte('.')
		sb.WriteString(event.Table)
		sb.WriteByte('.')
		sb.WriteString(event.CSN.String())
		sb.WriteByte('.')
		sb.WriteString(strconv.FormatInt(event.IntentSeq, 10))
		return sb.String()
	}
	// Snapshot event: build a stable key from PK column values only.
	// PKColumns is always set by the snapshot and incremental snapshot code paths.
	// PKColumns should always be set by snapshot/incremental code paths. An empty
	// set indicates a programming error — log once per event so it is visible.
	cols := event.PKColumns
	if len(cols) == 0 {
		allKeys := make([]string, 0, len(event.Data))
		for k := range event.Data {
			allKeys = append(allKeys, k)
		}
		slices.Sort(allKeys)
		h := fnv.New64a()
		for _, k := range allKeys {
			_, _ = fmt.Fprintf(h, "%s=%v\n", k, event.Data[k])
		}
		return fmt.Sprintf("%s.%s.snapshot.nopk.%016x", event.Schema, event.Table, h.Sum64())
	}
	// Use \x00 as key=value pair separator to avoid collisions when column
	// values contain commas or equals signs (e.g. COL_A="x,y" vs COL_A="x").
	var sb strings.Builder
	sb.Grow(len(event.Schema) + 1 + len(event.Table) + 9 + len(cols)*20)
	sb.WriteString(event.Schema)
	sb.WriteByte('.')
	sb.WriteString(event.Table)
	sb.WriteString(".snapshot.")
	for i, k := range cols {
		if i > 0 {
			sb.WriteByte(0)
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		switch v := event.Data[k].(type) {
		case int64:
			sb.WriteString(strconv.FormatInt(v, 10))
		case string:
			sb.WriteString(v)
		case json.Number:
			sb.WriteString(v.String())
		case float64:
			sb.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
		case nil:
			sb.WriteString("<nil>")
		default:
			fmt.Fprintf(&sb, "%v", v)
		}
	}
	return sb.String()
}

// debeziumSnapshotValue returns the Debezium snapshot field value for the
// given event. Debezium uses "true" during a snapshot, "last" for the final
// snapshot event, and "false" (or absent) during streaming. Because this
// connector does not track which snapshot row is the last, all snapshot rows
// emit "true"; streaming rows emit "false".
func debeziumSnapshotValue(op replication.OpType) string {
	if op == replication.OpTypeRead {
		return "true"
	}
	return "false"
}

// eventToMessage converts a ChangeEvent to a Redpanda Connect message whose
// body matches the Debezium DB2 connector envelope format for drop-in
// compatibility with existing Debezium consumers.
//
// Envelope structure:
//
//	{
//	  "before":  null | { <column>: <value>, ... },
//	  "after":   null | { <column>: <value>, ... },
//	  "source":  { "connector": "redpanda.db2", "name": "redpanda.db2_cdc", "schema": "...",
//	               "table": "...", "commit_lsn": "CSN:...", "change_lsn": null,
//	               "snapshot": "true"|"false", "ts_ms": <epoch-ms>, ... },
//	  "op":      "c"|"u"|"d"|"r",
//	  "ts_ms":   <epoch-ms>
//	}
//
// The before/after fields follow Debezium semantics:
//   - INSERT (op="c"): before=null, after=new row
//   - UPDATE (op="u"): before=old row data, after=new row data
//   - DELETE (op="d"): before=deleted row data, after=null
//   - READ   (op="r"): before=null, after=snapshot row
//
// Metadata keys set on the message:
//
//   - db2_schema        — source table schema (TABSCHEMA)
//   - db2_table         — source table name
//   - db2_operation     — human-readable op: "read", "insert", "delete", "update"
//   - db2_op            — Debezium op code: "r", "c", "d", "u"
//   - db2_csn           — commit sequence number string (backward-compat alias)
//   - db2_commit_lsn    — same as db2_csn (Debezium naming)
//   - db2_connector        — always "redpanda.db2"
//   - db2_snapshot         — "true" for snapshot rows, "false" for streaming rows
//   - db2_idempotency_key  — deterministic key for deduplication: "SCHEMA.TABLE.CSN.intentseq" for CDC events, "SCHEMA.TABLE.snapshot.<pk-fields>" for snapshot rows
//   - db2_timestamp        — IBMSNAP_LOGMARKER timestamp (RFC3339Nano; omitted if zero)
func (*db2CDCInput) eventToMessage(event replication.ChangeEvent) (*service.Message, error) {
	if event.Operation == replication.OpTypeHeartbeat {
		tsMs := event.Timestamp.UnixMilli()
		envelope := map[string]any{
			"op":    "hb",
			"ts_ms": tsMs,
		}
		msg := service.NewMessage(nil)
		msg.SetStructuredMut(envelope)
		msg.MetaSetMut("db2_operation", "heartbeat")
		msg.MetaSetMut("db2_op", "hb")
		msg.MetaSetMut("db2_schema", "")
		msg.MetaSetMut("db2_table", "")
		msg.MetaSetMut("db2_csn", "")
		msg.MetaSetMut("db2_commit_lsn", "")
		msg.MetaSetMut("db2_connector", "redpanda.db2")
		msg.MetaSetMut("db2_snapshot", "false")
		msg.MetaSetMut("db2_idempotency_key", fmt.Sprintf("heartbeat.%d", event.Timestamp.UnixNano()))
		if !event.Timestamp.IsZero() {
			msg.MetaSetMut("db2_timestamp", event.Timestamp.Format(time.RFC3339Nano))
		}
		return msg, nil
	}

	if event.Operation == replication.OpTypeSchemaChange {
		tsMs := event.Timestamp.UnixMilli()
		envelope := map[string]any{
			"op": "schema_change",
			"source": map[string]any{
				"schema": event.Schema,
				"table":  event.Table,
				"csn":    event.CSN.String(),
				"ts_ms":  tsMs,
			},
			"ts_ms": tsMs,
		}
		msg := service.NewMessage(nil)
		msg.SetStructuredMut(envelope)
		msg.MetaSetMut("db2_operation", "schema_change")
		msg.MetaSetMut("db2_schema", event.Schema)
		msg.MetaSetMut("db2_table", event.Table)
		msg.MetaSetMut("db2_op", "schema_change")
		csnStr := event.CSN.String()
		msg.MetaSetMut("db2_csn", csnStr)
		msg.MetaSetMut("db2_commit_lsn", csnStr)
		msg.MetaSetMut("db2_connector", "redpanda.db2")
		msg.MetaSetMut("db2_snapshot", "false")
		msg.MetaSetMut("db2_idempotency_key", fmt.Sprintf("%s.%s.schema_change.%s", event.Schema, event.Table, csnStr))
		if !event.Timestamp.IsZero() {
			msg.MetaSetMut("db2_timestamp", event.Timestamp.Format(time.RFC3339Nano))
		}
		return msg, nil
	}

	// Determine source timestamp in epoch-milliseconds.
	var tsMs int64
	if !event.Timestamp.IsZero() {
		tsMs = event.Timestamp.UnixMilli()
	}

	csnStr := event.CSN.String() // "" for snapshot rows (NullCSN); computed once and reused below

	// Build the source block to match Debezium's SourceInfo struct.
	// change_lsn is not tracked separately by DB2 LUW SQL Replication
	// (it only exposes IBMSNAP_COMMITSEQ = commit_lsn), so it is null.
	source := map[string]any{
		"connector":  "redpanda.db2",
		"name":       "redpanda.db2_cdc",
		"schema":     event.Schema,
		"table":      event.Table,
		"commit_lsn": csnStr,
		"change_lsn": nil, // not available from DB2 LUW SQL Replication
		"snapshot":   debeziumSnapshotValue(event.Operation),
		"ts_ms":      tsMs,
	}

	// Populate before/after fields per Debezium semantics:
	//   INSERT (c): before=null,            after=new row
	//   UPDATE (u): before=old row data,    after=new row data
	//   DELETE (d): before=deleted row data, after=null
	//   READ   (r): before=null,            after=snapshot row
	var before any
	var after any
	switch event.Operation {
	case replication.OpTypeDelete:
		before = event.Data
		after = nil
	case replication.OpTypeUpdate:
		before = event.BeforeData
		after = event.Data
	default:
		before = nil
		after = event.Data
	}

	opCode := debeziumOp(event.Operation)
	envelope := map[string]any{
		"before": before,
		"after":  after,
		"source": source,
		"op":     opCode,
		"ts_ms":  tsMs,
	}

	msg := service.NewMessage(nil)
	msg.SetStructuredMut(envelope)

	// Metadata — standard Debezium-named keys plus backward-compat aliases.
	msg.MetaSetMut("db2_schema", event.Schema)
	msg.MetaSetMut("db2_table", event.Table)
	msg.MetaSetMut("db2_operation", string(event.Operation))
	msg.MetaSetMut("db2_op", opCode)
	msg.MetaSetMut("db2_csn", csnStr)        // backward-compat alias
	msg.MetaSetMut("db2_commit_lsn", csnStr) // Debezium naming
	msg.MetaSetMut("db2_connector", "redpanda.db2")
	msg.MetaSetMut("db2_snapshot", debeziumSnapshotValue(event.Operation))
	msg.MetaSetMut("db2_idempotency_key", idempotencyKey(event))
	if !event.Timestamp.IsZero() {
		msg.MetaSetMut("db2_timestamp", event.Timestamp.Format(time.RFC3339Nano))
	}

	return msg, nil
}

// detectVersion queries DB2 for its version string.
func (d *db2CDCInput) detectVersion(ctx context.Context) (replication.Version, error) {
	d.mu.Lock()
	db := d.db
	d.mu.Unlock()
	if db == nil {
		return replication.Version{}, errors.New("not connected")
	}
	var versionStr string
	err := db.QueryRowContext(ctx,
		"SELECT SERVICE_LEVEL FROM SYSIBMADM.ENV_INST_INFO FETCH FIRST 1 ROW ONLY",
	).Scan(&versionStr)
	if err != nil {
		// Fallback for older DB2 versions.
		if err2 := db.QueryRowContext(ctx, "VALUES (PROD_RELEASE)").Scan(&versionStr); err2 != nil {
			return replication.Version{}, fmt.Errorf("detecting DB2 version (ENV_INST_INFO: %w; PROD_RELEASE: %v)", err, err2)
		}
	}

	return replication.ParseVersion(versionStr)
}

// initCheckpointTable creates the checkpoint table in DB2 if it does not exist.
func (d *db2CDCInput) initCheckpointTable(ctx context.Context) error {
	d.mu.Lock()
	db := d.db
	d.mu.Unlock()
	if db == nil {
		return errors.New("not connected")
	}
	// Create the schema for the checkpoint table if it does not already exist.
	// Only ignore the "already exists" error (SQLSTATE 42710); propagate
	// permission errors, connection errors, etc. so the operator sees the real cause.
	if parts := strings.SplitN(d.cpCacheTableName, ".", 2); len(parts) == 2 {
		schemaName := replication.QuoteDB2Identifier(parts[0])
		if _, err := db.ExecContext(ctx, "CREATE SCHEMA "+schemaName); err != nil && !isAlreadyExistsError(err) {
			return fmt.Errorf("create schema %s for checkpoint table: %w", parts[0], err)
		}
	}

	createSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			CACHE_KEY VARCHAR(255) NOT NULL,
			CACHE_VAL VARCHAR(32672) NOT NULL,
			PRIMARY KEY (CACHE_KEY)
		)`, d.cpCacheTableName)

	_, err := db.ExecContext(ctx, createSQL)
	if err != nil && !isAlreadyExistsError(err) {
		return fmt.Errorf("create checkpoint table %s: %w", d.cpCacheTableName, err)
	}

	return nil
}

// saveCheckpointKV upserts an arbitrary key→value pair into the checkpoint store.
func (d *db2CDCInput) saveCheckpointKV(ctx context.Context, key, val string) error {
	if d.cpCacheName != "" {
		var cacheErr error
		if err := d.res.AccessCache(ctx, d.cpCacheName, func(c service.Cache) {
			cacheErr = c.Set(ctx, key, []byte(val), nil)
		}); err != nil {
			return fmt.Errorf("accessing checkpoint cache: %w", err)
		}
		return cacheErr
	}

	d.mu.Lock()
	db := d.db
	d.mu.Unlock()
	if db == nil {
		// Connector has been closed: the checkpoint cannot be persisted.
		// Return an error so the engine propagates the failure rather than
		// silently treating the checkpoint as saved (crash would replay all
		// already-processed rows on restart).
		return errors.New("checkpoint not saved: DB connection is nil (connector closed)")
	}

	mergeSQL := d.cpMergeSQL
	if mergeSQL == "" {
		mergeSQL = fmt.Sprintf(`
			MERGE INTO %s AS T
			USING (VALUES (?, ?)) AS S(CACHE_KEY, CACHE_VAL)
			ON T.CACHE_KEY = S.CACHE_KEY
			WHEN MATCHED THEN UPDATE SET T.CACHE_VAL = S.CACHE_VAL
			WHEN NOT MATCHED THEN INSERT (CACHE_KEY, CACHE_VAL) VALUES (S.CACHE_KEY, S.CACHE_VAL)
		`, d.cpCacheTableName)
	}
	_, err := db.ExecContext(ctx, mergeSQL, key, val)
	if err != nil {
		return fmt.Errorf("saving checkpoint KV to DB2 table: %w", err)
	}
	return nil
}

// loadCheckpointKV returns the stored value for key, or "" if not found.
func (d *db2CDCInput) loadCheckpointKV(ctx context.Context, key string) (string, error) {
	if d.cpCacheName != "" {
		var val []byte
		var cacheErr error
		if err := d.res.AccessCache(ctx, d.cpCacheName, func(c service.Cache) {
			val, cacheErr = c.Get(ctx, key)
		}); err != nil {
			return "", fmt.Errorf("accessing checkpoint cache: %w", err)
		}
		if errors.Is(cacheErr, service.ErrKeyNotFound) {
			return "", nil
		}
		if cacheErr != nil {
			return "", fmt.Errorf("reading checkpoint KV: %w", cacheErr)
		}
		return string(val), nil
	}

	d.mu.Lock()
	db := d.db
	d.mu.Unlock()
	if db == nil {
		return "", nil
	}

	selectSQL := d.cpSelectSQL
	if selectSQL == "" {
		selectSQL = fmt.Sprintf("SELECT CACHE_VAL FROM %s WHERE CACHE_KEY = ?", d.cpCacheTableName)
	}
	var val string
	err := db.QueryRowContext(ctx, selectSQL, key).Scan(&val)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("loading checkpoint KV from DB2 table: %w", err)
	}
	return val, nil
}

// deleteCheckpointKV removes a key from the checkpoint store.
func (d *db2CDCInput) deleteCheckpointKV(ctx context.Context, key string) error {
	if d.cpCacheName != "" {
		var cacheErr error
		if err := d.res.AccessCache(ctx, d.cpCacheName, func(c service.Cache) {
			cacheErr = c.Delete(ctx, key)
		}); err != nil {
			return fmt.Errorf("accessing checkpoint cache: %w", err)
		}
		if errors.Is(cacheErr, service.ErrKeyNotFound) {
			return nil
		}
		return cacheErr
	}

	d.mu.Lock()
	db := d.db
	d.mu.Unlock()
	if db == nil {
		return nil
	}
	deleteSQL := d.cpDeleteSQL
	if deleteSQL == "" {
		deleteSQL = fmt.Sprintf("DELETE FROM %s WHERE CACHE_KEY = ?", d.cpCacheTableName)
	}
	_, err := db.ExecContext(ctx, deleteSQL, key)
	return err
}

// loadCheckpoint reads the last saved CSN from the checkpoint store.
func (d *db2CDCInput) loadCheckpoint(ctx context.Context) (replication.CSN, error) {
	val, err := d.loadCheckpointKV(ctx, d.checkpointCacheKey)
	if err != nil || val == "" {
		return replication.NullCSN(), err
	}
	return replication.ParseCSN(val)
}

// saveCheckpoint persists the highest fully-processed CSN to the checkpoint store.
func (d *db2CDCInput) saveCheckpoint(ctx context.Context, csn replication.CSN) error {
	return d.saveCheckpointKV(ctx, d.checkpointCacheKey, csn.String())
}

// incrementalContextKey returns the checkpoint store key for the incremental
// snapshot context, namespaced under d.checkpointCacheKey so that multiple
// connectors sharing the same external cache (checkpoint_cache) do not
// overwrite each other's in-progress snapshots.
func (d *db2CDCInput) incrementalContextKey() string {
	return d.checkpointCacheKey + "_incremental_ctx"
}

func (d *db2CDCInput) saveIncrementalContext(ctx context.Context, inc *replication.IncrementalSnapshotContext) error {
	b, err := json.Marshal(inc)
	if err != nil {
		return fmt.Errorf("marshaling incremental context: %w", err)
	}
	return d.saveCheckpointKV(ctx, d.incrementalContextKey(), string(b))
}

func (d *db2CDCInput) loadIncrementalContext(ctx context.Context) (*replication.IncrementalSnapshotContext, error) {
	val, err := d.loadCheckpointKV(ctx, d.incrementalContextKey())
	if err != nil || val == "" {
		return nil, err
	}
	// Guard against a tampered checkpoint store sending a huge payload that
	// would exhaust heap before the JSON decoder can reject it.
	const maxIncrementalContextBytes = 1 << 20 // 1 MiB
	if len(val) > maxIncrementalContextBytes {
		return nil, fmt.Errorf("incremental context checkpoint too large (%d bytes, max %d) — discarding; snapshot will restart from beginning", len(val), maxIncrementalContextBytes)
	}
	// Use json.Decoder with UseNumber so BIGINT primary keys > 2^53 are preserved
	// as json.Number rather than being truncated to float64 (which would corrupt
	// the keyset-pagination cursor for large integer PKs on resume).
	var inc replication.IncrementalSnapshotContext
	dec := json.NewDecoder(strings.NewReader(val))
	dec.UseNumber()
	if err := dec.Decode(&inc); err != nil {
		return nil, fmt.Errorf("unmarshaling incremental context: %w", err)
	}
	if inc.LastEmittedPK == nil {
		inc.LastEmittedPK = map[string][]any{}
	}
	if inc.MaxPK == nil {
		inc.MaxPK = map[string][]any{}
	}
	// Validate deserialized table identifiers before they reach SQL interpolation.
	// A tampered checkpoint store must not be a SQL injection vector.
	for _, tbl := range inc.Tables {
		if !isValidDB2Identifier(tbl.Schema) || !isValidDB2Identifier(tbl.Name) {
			return nil, fmt.Errorf("checkpoint contains invalid table identifier %q.%q — discarding resume context", tbl.Schema, tbl.Name)
		}
	}
	return &inc, nil
}

func (d *db2CDCInput) clearIncrementalContext(ctx context.Context) error {
	return d.deleteCheckpointKV(ctx, d.incrementalContextKey())
}

// isAlreadyExistsError returns true when the error indicates an object already
// exists. The IBM DB2 CLI driver may return two different SQLSTATE codes for
// this condition:
//   - "42S01": ODBC standard code for "base table or view already exists"
//     (returned by the CLI driver in practice for CREATE TABLE on an existing table)
//   - "42710": IBM-specific DB2 SQLSTATE for "duplicate name"
//
// Uses the structured *db2cli.DB2Error type when available so the check is an
// exact string comparison rather than a substring scan, avoiding false positives.
func isAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	if db2Err, ok := errors.AsType[*db2cli.DB2Error](err); ok {
		// 42S01 = ODBC "base table or view already exists" (CLI driver returns this)
		// 42710 = IBM DB2 "duplicate name"
		return db2Err.SQLState == "42710" || db2Err.SQLState == "42S01"
	}
	// Fallback for wrapped or non-DB2Error errors (e.g. wrapped fmt.Errorf).
	msg := err.Error()
	return strings.Contains(msg, "SQLSTATE=42710") || strings.Contains(msg, "SQLSTATE 42710") ||
		strings.Contains(msg, "SQLSTATE=42S01") || strings.Contains(msg, "[42S01]")
}

// isValidDB2Identifier returns true if s is a valid unquoted DB2 identifier:
// starts with a letter or underscore, then contains only uppercase letters,
// digits, or underscores. This matches DB2's rules for ordinary identifiers and
// is sufficient to ensure the string is safe to embed directly in SQL strings
// without quoting or parameter binding.
func isValidDB2Identifier(s string) bool {
	if s == "" {
		return false
	}
	r0 := rune(s[0])
	if (r0 < 'A' || r0 > 'Z') && r0 != '_' {
		return false
	}
	for _, r := range s[1:] {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}

// validateQualifiedIdentifier validates a possibly-qualified identifier of the form
// "TABLE" or "SCHEMA.TABLE". Each part must satisfy isValidDB2Identifier.
func validateQualifiedIdentifier(s string) error {
	parts := strings.SplitN(s, ".", 2)
	for _, part := range parts {
		if !isValidDB2Identifier(part) {
			return fmt.Errorf("identifier part %q contains invalid characters: only uppercase letters, digits, and underscores are allowed", part)
		}
	}
	return nil
}

// safeLogID truncates a signal-table ID value to at most maxLen printable ASCII
// characters before it appears in log output. Signal IDs are VARCHAR(255) values
// written by operators, but a maliciously crafted value could inject control
// characters into structured log output. Truncation is capped at 64 chars which
// is well above any legitimate signal ID length.
func safeLogID(id string) string {
	const maxLen = 64
	// Strip non-printable ASCII to prevent log injection.
	clean := strings.Map(func(r rune) rune {
		if r >= 0x20 && r < 0x7F {
			return r
		}
		return -1
	}, id)
	if len(clean) > maxLen {
		return clean[:maxLen] + "…"
	}
	return clean
}
