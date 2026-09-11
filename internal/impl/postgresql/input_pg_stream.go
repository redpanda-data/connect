// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pgstream

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
	incsnapshot "github.com/redpanda-data/connect/v4/internal/impl/postgresql/incrementalsnapshot"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/redpanda-data/connect/v4/internal/replication"
	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

const (
	fieldDSN                       = "dsn"
	fieldIncludeTxnMarkers         = "include_transaction_markers"
	fieldStreamSnapshot            = "stream_snapshot"
	fieldSnapshotMemSafetyFactor   = "snapshot_memory_safety_factor"
	fieldSnapshotBatchSize         = "snapshot_batch_size"
	fieldSchema                    = "schema"
	fieldTables                    = "tables"
	fieldCheckpointLimit           = "checkpoint_limit"
	fieldTemporarySlot             = "temporary_slot"
	fieldPgStandbyTimeout          = "pg_standby_timeout"
	fieldWalMonitorInterval        = "pg_wal_monitor_interval"
	fieldSlotName                  = "slot_name"
	fieldBatching                  = "batching"
	fieldMaxParallelSnapshotTables = "max_parallel_snapshot_tables"
	fieldUnchangedToastValue       = "unchanged_toast_value"
	fieldHeartbeatInterval         = "heartbeat_interval"
	fieldSignalTableName           = "signal_table_name"
	fieldAWSIAMAuth                = "aws"
	// FieldAWSIAMAuthEnabled enabled field.
	FieldAWSIAMAuthEnabled = "enabled"
	shutdownTimeout        = 5 * time.Second

	fieldIncSnapshot                   = "incremental_snapshot"
	fieldIncSnapshotEnabled            = "enabled"
	fieldIncrementalSnapshotChunkSize  = "chunk_size"
	fieldIncSnapshotCheckpointCache    = "checkpoint_cache"
	fieldIncSnapshotCheckpointCacheKey = "checkpoint_cache_key"
	fieldIncSnapshotHeartbeatInterval  = "heartbeat_interval"
)

func notImportedAWSOptFn(_ context.Context, awsConf *service.ParsedConfig, _ *pgconn.Config, _ *service.Logger) (TokenBuilder, error) {
	if enabled, _ := awsConf.FieldBool(FieldAWSIAMAuthEnabled); !enabled {
		return nil, nil
	}
	return nil, errors.New("unable to configure AWS authentication as this binary does not import components/aws")
}

// AWSOptFn is populated with the child `aws` package when imported.
var AWSOptFn = notImportedAWSOptFn

// TokenBuilder can be used for fetching passwords at runtime during connection (ie. IAM auth tokens)
type TokenBuilder func(context.Context) error

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

func newPostgresCDCConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Version("4.39.0").
		Summary(`Streams changes from a PostgreSQL database using logical replication.`).
		Description(`Streams changes from a PostgreSQL database for Change Data Capture (CDC).
Additionally, if ` + "`" + fieldStreamSnapshot + "`" + ` is set to true, then the existing data in the database is also streamed too.

== Metadata

This input adds the following metadata fields to each message:
- table: Name of the table that the message originated from
- operation: Type of operation that generated the message: "read", "insert", "update", or "delete". "read" is from messages that are read in the initial snapshot phase. This will also be "begin" and "commit" if ` + "`" + fieldIncludeTxnMarkers + "`" + ` is enabled
- lsn: the log sequence number in postgres
- schema: The table schema in benthos common schema format, compatible with processors like parquet_encode
- commit_ts_ms: The commit timestamp of the transaction as a Unix millisecond timestamp. Not set for snapshot reads.
- before: The pre-change state of the row for update and delete operations, in benthos common schema format. For updates, availability depends on the table's REPLICA IDENTITY setting - with the default identity only key columns are present, with REPLICA IDENTITY FULL all columns are present.

== Unserializable rows

A row whose decoded WAL data cannot be marshalled to JSON (in practice non-finite floating point values such as NaN or Infinity) is published with its error set and a plain-text rendering of the row as the payload, rather than stalling the stream or silently dropping the row. Such messages can be inspected with the ` + "`errored()`" + ` Bloblang function and routed with error-handling components (for example a ` + "`switch`" + ` output with ` + "`reject_errored`" + `, or a dead-letter queue); if not handled they flow through the pipeline like any other message. The replication checkpoint advances past them normally once acknowledged.
		`).
		Field(service.NewStringField(fieldDSN).
			Description("The Data Source Name for the PostgreSQL database in the form of `postgres://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]`. Please note that Postgres enforces SSL by default, you can override this with the parameter `sslmode=disable` if required.").
			ShortDescription("The Data Source Name for the PostgreSQL database, in postgres:// URL form.").
			Example("postgres://foouser:foopass@localhost:5432/foodb?sslmode=disable")).
		Field(service.NewBoolField(fieldIncludeTxnMarkers).
			Description(`When set to true, empty messages with operation types BEGIN and COMMIT are generated for the beginning and end of each transaction. Messages with operation metadata set to "begin" or "commit" will have null message payloads.`).
			ShortDescription("Emit empty BEGIN and COMMIT messages at the start and end of each transaction.").
			Default(false)).
		Field(service.NewBoolField(fieldStreamSnapshot).
			Description("When set to true, the plugin will first stream a snapshot of all existing data in the database before streaming changes. In order to use this the tables that are being snapshot MUST have a primary key set so that reading from the table can be parallelized. Note that this has no effect if `" + fieldTables + "` is left empty, since the snapshot is only planned for tables listed there.").
			ShortDescription("Stream a snapshot of all existing data before streaming changes. Snapshot tables must have a primary key.").
			Example(true).
			Default(false)).
		Field(service.NewFloatField(fieldSnapshotMemSafetyFactor).
			Description("Determines the fraction of available memory that can be used for streaming the snapshot. Values between 0 and 1 represent the percentage of memory to use. Lower values make initial streaming slower but help prevent out-of-memory errors.").
			ShortDescription("Fraction of available memory, between 0 and 1, that may be used for streaming the snapshot.").
			Example(0.2).
			Default(1).
			Deprecated()).
		Field(service.NewIntField(fieldSnapshotBatchSize).
			Description("The number of rows to fetch in each batch when querying the snapshot.").
			Example(10000).
			Default(1000)).
		Field(service.NewStringField(fieldSchema).
			Description("The PostgreSQL schema from which to replicate data.").
			Examples("public", `"MyCaseSensitiveSchemaNeedingQuotes"`),
		).
		Field(service.NewStringListField(fieldTables).
			Description(`A list of table names to include in the logical replication. Each table should be specified as a separate item.

If left empty, the underlying PostgreSQL publication is created ` + "`FOR ALL TABLES`" + `, which replicates every table in every schema of the database, ignoring ` + "`" + fieldSchema + "`" + `. This also disables ` + "`" + fieldStreamSnapshot + "`" + `, since the initial snapshot is only planned for tables listed here.`).
			Example([]string{"my_table_1", `"MyCaseSensitiveTableNeedingQuotes"`})).
		Field(service.NewIntField(fieldCheckpointLimit).
			Description("The maximum number of messages that can be processed at a given time. Increasing this limit enables parallel processing and batching at the output level. Any given LSN will not be acknowledged unless all messages under that offset are delivered in order to preserve at least once delivery guarantees.").
			ShortDescription("The maximum number of messages that can be processed at a given time.").
			Default(1024)).
		Field(service.NewBoolField(fieldTemporarySlot).
			Description("If set to true, creates a temporary replication slot that is automatically dropped when the connection is closed.").
			Default(false)).
		Field(service.NewStringField(fieldSlotName).
			Description(`The name of the PostgreSQL logical replication slot to use. If not provided, a random name will be generated. You can create this slot manually before starting replication if desired.

Note: To avoid needing to grant the replication user permission to create publications, you can manually create the publications ahead of time.
This connector uses the naming pattern ` + "`pglog_stream_<replication_slot_name>`" + `, so be sure to create them using this convention.
			`).
			ShortDescription("The name of the PostgreSQL logical replication slot to use. A random name is generated if not provided.").
			Example("my_test_slot")).
		Field(service.NewDurationField(fieldPgStandbyTimeout).
			Description("Specify the standby timeout before refreshing an idle connection.").
			Example("30s").
			Default("10s")).
		Field(service.NewDurationField(fieldWalMonitorInterval).
			Description("How often to report changes to the replication lag.").
			Example("6s").
			Default("3s")).
		Field(service.NewIntField(fieldMaxParallelSnapshotTables).
			Description("Int specifies a number of tables that will be processed in parallel during the initial snapshot processing stage.").
			ShortDescription("Number of tables to snapshot in parallel.").
			Default(1)).
		Field(service.NewAnyField(fieldUnchangedToastValue).
			Description("The value to emit when there are unchanged TOAST values in the stream. This occurs for updates and deletes where REPLICA IDENTITY is not FULL.").
			ShortDescription("The value to emit when TOAST values are unchanged in the stream.").
			Default(nil).
			Example("__redpanda_connect_unchanged_toast_value__").
			Optional().
			Advanced()).
		Field(service.NewDurationField(fieldHeartbeatInterval).
			Description("The interval at which to write heartbeat messages. Heartbeat messages are needed in scenarios when the subscribed tables are low frequency, but there are other high frequency tables writing. Due to the checkpointing mechanism for replication slots, not having new messages to acknowledge will prevent postgres from reclaiming the write ahead log, which can exhaust the local disk. Having heartbeats allows Redpanda Connect to safely acknowledge data periodically and move forward the committed point in the log so it can be reclaimed. Setting the duration to 0s will disable heartbeats entirely. Heartbeats are created by periodically writing logical messages to the write ahead log using `pg_logical_emit_message`.\n\nHeartbeats also pace `incremental_snapshot`: on a quiet table they are the only thing advancing the snapshot. This interval just keeps the slot current, so `" + fieldIncSnapshot + "." + fieldIncSnapshotHeartbeatInterval + "` applies alongside it and the more frequent wins for the life of the input. A non-zero value is still required here.").
			ShortDescription("Interval at which to write heartbeat messages, keeping the replication slot current on low-traffic tables.").
			Default("1h").
			Example("0s").
			Example("24h").
			Advanced()).
		Field(service.NewTLSField("tls")).
		Description("Using this field overrides the SSL/TLS settings in the environment and DSN.").
		Field(service.NewObjectField(fieldAWSIAMAuth,
			service.NewBoolField(FieldAWSIAMAuthEnabled).
				Description("Enable AWS IAM authentication for PostgreSQL. When enabled, an IAM authentication token is generated and used as the password.").
				ShortDescription("Enable AWS IAM authentication, generating a temporary token to use as the password.").
				Default(false),
			service.NewStringField("region").
				Description("The AWS region where the PostgreSQL instance is located. If no region is specified then the environment default will be used.").
				ShortDescription("The AWS region where the PostgreSQL instance is located. Defaults to the environment region.").
				Optional(),
			service.NewStringField("endpoint").
				Description("The PostgreSQL endpoint hostname (e.g., mydb.abc123.us-east-1.rds.amazonaws.com)."),
			service.NewStringField("id").
				Description("The ID of credentials to use.").
				Optional().Advanced(),
			service.NewStringField("secret").
				Description("The secret for the credentials being used.").
				Optional().Advanced().Secret(),
			service.NewStringField("token").
				Description("The token for the credentials being used, required when using short term credentials.").
				Optional().Advanced(),
			service.NewStringField("role").
				Description("Optional AWS IAM role ARN to assume for authentication. Alternatively, use `roles` array for role chaining instead.").
				ShortDescription("Optional AWS IAM role ARN to assume for authentication.").
				Optional(),
			service.NewStringField("role_external_id").
				Description("Optional external ID for the role assumption. Only used with the `role` field. Alternatively, use `roles` array for role chaining instead.").
				ShortDescription("Optional external ID for the role assumption. Only used alongside the role field.").
				Optional(),
			service.NewObjectListField("roles",
				service.NewStringField("role").
					Default("").
					Description("AWS IAM role ARN to assume."),
				service.NewStringField("role_external_id").
					Description("Optional external ID for the role assumption.").
					Default("").
					Optional(),
			).
				Description("Optional array of AWS IAM roles to assume for authentication. Roles can be assumed in sequence, enabling chaining for purposes such as cross-account access. Each role can optionally specify an external ID.").
				ShortDescription("AWS IAM roles to assume for authentication. Assumed in sequence to allow role chaining.").
				Optional(),
		).
			Description("AWS IAM authentication configuration for PostgreSQL instances. When enabled, IAM credentials are used to generate temporary authentication tokens instead of a static password.").
			ShortDescription("AWS IAM authentication configuration for PostgreSQL instances.").
			Advanced().
			Optional()).
		Field(service.NewStringField(fieldSignalTableName).
			Description(`The name of the table used to send control signals to the connector, excluding the schema. The table must
exist in the schema configured via the ` + "`schema`" + ` field, and must not also appear in ` + "`" + fieldTables + "`" + `
— the signal table is implicitly added to the publication and excluded from snapshot scans, so listing
it in both places is rejected at startup. It must have at least these columns — startup validation checks
column names only, not types, so a wrong column type (e.g. ` + "`data JSONB`" + ` instead of ` + "`TEXT`" + `)
is only caught at runtime, on the first signal row read:

- **id** — any type representable as a string (e.g. ` + "`SERIAL`" + `, ` + "`BIGSERIAL`" + `, ` + "`UUID`" + `, ` + "`VARCHAR`" + `)
- **type** — should be ` + "`VARCHAR`" + ` or another string type — the signal type (see supported signals below)
- **data** — should be ` + "`TEXT`" + ` — a JSON object containing signal parameters

Create the table with:

` + "```sql" + `
CREATE TABLE <schema>.<signal_table_name> (
    id   SERIAL PRIMARY KEY,
    type VARCHAR(32),
    data TEXT
);
` + "```" + `

Signal rows are published as regular output messages (` + "`operation=insert`" + `, ` + "`table=<signal_table_name>`" + `).
To exclude them from downstream processing, filter on the ` + "`table`" + ` metadata field using a
` + "`mapping`" + ` processor:

` + "```yaml" + `
pipeline:
  processors:
    - mapping: |
        root = if @table == "rpcn_signal_table" { deleted() } else { this }
` + "```" + `

**Supported signals**

**` + "`log`" + `** — recognized and logged when received. The ` + "`data`" + ` column must contain
a JSON object with a ` + "`message`" + ` key, whose value is written to the connector's log output.

` + "```sql" + `
INSERT INTO <schema>.<signal_table_name> (type, data) VALUES ('log', '{"message": "Signal message"}');
` + "```" + `

**` + "`snapshot`" + `** — backfills the named tables incrementally, alongside streaming. Requires
` + "`" + fieldIncSnapshot + "." + fieldIncSnapshotEnabled + "`" + `. The ` + "`data`" + ` column must contain a JSON object
with a ` + "`tables`" + ` key listing table names in the configured ` + "`schema`" + `, excluding the schema itself:

` + "```sql" + `
INSERT INTO <schema>.<signal_table_name> (type, data) VALUES ('snapshot', '{"tables": ["orders", "customers"]}');
` + "```" + `

Each table must appear in ` + "`" + fieldTables + "`" + ` (or that list must be empty, replicating everything):
an unreplicated table has no live changes to deduplicate its backfill against, so a write landing
after its chunk is read would be lost. Each must also have a primary key, which the backfill pages
by — a table replicated under ` + "`REPLICA IDENTITY FULL`" + ` without one cannot be snapshotted. A signal
naming a table that fails either check is rejected and logged dropped from the snapshot queue. If a check cannot be run at all - a
connection reset, say - the stream restarts and the signal is read again, so the request is not lost.

Each table joins the back of the backfill queue. A table this run already covers is skipped and
logged, so a repeated signal does not re-read it. To read one again, point
` + "`" + fieldIncSnapshot + "." + fieldIncSnapshotCheckpointCacheKey + "`" + ` at a fresh key.`).
			Example("rpcn_signal_table").
			Default("").
			Advanced()).
		// incremental snapshot config
		Field(service.NewObjectField(fieldIncSnapshot,
			service.NewBoolField(fieldIncSnapshotEnabled).
				Description("Backfills tables in chunks alongside replication, on request. Tables are not configured here: insert a `"+replication.SnapshotSignalType+"` row into `"+fieldSignalTableName+"` to ask for one, so a backfill can be started at any time without a config change. A signal table is therefore required. Unlike `"+fieldStreamSnapshot+"` this needs no up-front snapshot phase and does not delay replication. The two are mutually exclusive: both read the same rows, so enabling either alongside the other would deliver everything twice.\n\nProgress is driven by the replication stream: each streamed transaction releases a buffered chunk, and several more follow immediately if the database was idle during the read. Quiet tables therefore advance in bursts on each heartbeat, paced by `"+fieldIncSnapshotHeartbeatInterval+"`.\n\nA row can arrive twice, once from replication and once from the backfill: when a primary key reuses or fills a gap below the table's current maximum, or -- whatever the key type -- when a row is inserted after replication starts but before the snapshot reaches its table. Treat rows as idempotent upserts keyed by primary key, as is standard CDC practice.").
				ShortDescription("Backfill signalled tables in chunks, alongside replication streaming.").
				Default(incsnapshot.DefaultIncSnapshotEnabled),
			service.NewIntField(fieldIncrementalSnapshotChunkSize).
				Description("The number of rows to read per chunk while incrementally snapshotting a table.").
				Default(incsnapshot.DefaultIncSnapshotChunkSize),
			service.NewDurationField(fieldIncSnapshotHeartbeatInterval).
				Description("How often to heartbeat while `"+fieldIncSnapshotEnabled+"` is `true`. The snapshot only advances on a streamed transaction, so on quiet tables this paces it. Raise it to reduce write load at the cost of a slower backfill.\n\nWhichever of this and the top-level `"+fieldHeartbeatInterval+"` is more frequent wins, and applies for the life of the input: it is fixed at startup, and stays in force between backfills as well as during them. Heartbeats are transactional only while a backfill is in progress, since that is the only time the snapshot needs a transaction id from one; between backfills they cost nothing extra.").
				ShortDescription("How often to heartbeat while incremental snapshotting is enabled, which paces it on quiet tables.").
				Default(incsnapshot.DefaultIncSnapshotHeartbeatInterval.String()),
			service.NewStringField(fieldIncSnapshotCheckpointCache).
				Description("A https://www.docs.redpanda.com/redpanda-connect/components/caches/about[cache resource^] storing the snapshot's progress, so a restart resumes instead of starting over. Required when `"+fieldIncSnapshotEnabled+"` is `true`.").
				ShortDescription("Cache resource storing incremental snapshot progress, so restarts resume instead of starting over. Required when enabled.").
				Optional(),
			service.NewStringField(fieldIncSnapshotCheckpointCacheKey).
				Description("The key used to store the incremental snapshot progress in `"+fieldIncSnapshotCheckpointCache+"`. Use a different key if multiple incremental snapshots share the same cache.\n\nChanging or clearing this key discards the record of which tables have been backfilled, so a `"+replication.SnapshotSignalType+"` signal reads a table again.").
				Default(incsnapshot.DefaultIncSnapshotCheckpointKey),
		).
			Description("Configures chunked snapshotting that runs alongside replication streaming.").
			ShortDescription("Configures chunked snapshotting that runs alongside replication streaming.").
			Advanced().
			Optional()).
		Field(service.NewAutoRetryNacksToggleField()).
		Field(service.NewBatchPolicyField(fieldBatching))
}

func newPgStreamInput(conf *service.ParsedConfig, mgr *service.Resources) (s service.BatchInput, err error) {
	var (
		dsn                       string
		dbSlotName                string
		temporarySlot             bool
		schema                    string
		tables                    []string
		streamSnapshot            bool
		includeTxnMarkers         bool
		snapshotBatchSize         int
		checkpointLimit           int
		walMonitorInterval        time.Duration
		maxParallelSnapshotTables int
		pgStandbyTimeout          time.Duration
		batching                  service.BatchPolicy
		unchangedToastValue       any
		heartbeatInterval         time.Duration
		iamAuthEnabled            bool
		iamAuthTokenBuilder       TokenBuilder
		signalTableName           string
	)

	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	if dsn, err = conf.FieldString(fieldDSN); err != nil {
		return nil, err
	}
	if dbSlotName, err = conf.FieldString(fieldSlotName); err != nil {
		return nil, err
	}
	if dbSlotName == "" {
		return nil, errors.New("slot_name is required")
	}

	if err := validateSimpleString(dbSlotName); err != nil {
		return nil, fmt.Errorf("invalid slot_name: %w", err)
	}

	if temporarySlot, err = conf.FieldBool(fieldTemporarySlot); err != nil {
		return nil, err
	}

	if includeTxnMarkers, err = conf.FieldBool(fieldIncludeTxnMarkers); err != nil {
		return nil, err
	}

	if schema, err = conf.FieldString(fieldSchema); err != nil {
		return nil, err
	}

	if tables, err = conf.FieldStringList(fieldTables); err != nil {
		return nil, err
	}

	if checkpointLimit, err = conf.FieldInt(fieldCheckpointLimit); err != nil {
		return nil, err
	}

	if streamSnapshot, err = conf.FieldBool(fieldStreamSnapshot); err != nil {
		return nil, err
	}

	if snapshotBatchSize, err = conf.FieldInt(fieldSnapshotBatchSize); err != nil {
		return nil, err
	}

	if batching, err = conf.FieldBatchPolicy(fieldBatching); err != nil {
		return nil, err
	} else if batching.IsNoop() {
		batching.Count = 1
	}

	if pgStandbyTimeout, err = conf.FieldDuration(fieldPgStandbyTimeout); err != nil {
		return nil, err
	}

	if walMonitorInterval, err = conf.FieldDuration(fieldWalMonitorInterval); err != nil {
		return nil, err
	}

	if maxParallelSnapshotTables, err = conf.FieldInt(fieldMaxParallelSnapshotTables); err != nil {
		return nil, err
	}

	if unchangedToastValue, err = conf.FieldAny(fieldUnchangedToastValue); err != nil {
		return nil, err
	}

	if heartbeatInterval, err = conf.FieldDuration(fieldHeartbeatInterval); err != nil {
		return nil, err
	}

	if signalTableName, err = conf.FieldString(fieldSignalTableName); err != nil {
		return nil, err
	}

	if signalTableName != "" {
		normalizedSignalTable, err := sanitize.NormalizePostgresIdentifier(signalTableName)
		if err != nil {
			return nil, fmt.Errorf("invalid %s %q: %w", fieldSignalTableName, signalTableName, err)
		}
		for _, table := range tables {
			normalizedTable, err := sanitize.NormalizePostgresIdentifier(table)
			if err != nil {
				return nil, fmt.Errorf("invalid table name %q: %w", table, err)
			}
			if normalizedTable == normalizedSignalTable {
				return nil, fmt.Errorf("%s %q must not also appear in %s - the signal table is implicitly added to the publication and excluded from snapshot scans", fieldSignalTableName, signalTableName, fieldTables)
			}
		}
	}

	awsConf := conf.Namespace(fieldAWSIAMAuth)
	iamAuthEnabled, _ = awsConf.FieldBool(FieldAWSIAMAuthEnabled)

	incSnapshot, err := parseIncrementalSnapshotCfg(conf, heartbeatInterval, signalTableName, streamSnapshot)
	if err != nil {
		return nil, err
	}

	pgConnConfig, err := pgconn.ParseConfigWithOptions(dsn, pgconn.ParseConfigOptions{
		// Don't support dynamic reading of password
		GetSSLPassword: func(context.Context) string { return "" },
	})
	if err != nil {
		return nil, err
	}

	logger := mgr.Logger()

	if iamAuthTokenBuilder, err = AWSOptFn(context.Background(), awsConf, pgConnConfig, logger); err != nil {
		return nil, err
	}
	var tlsConf *tls.Config
	if tlsConf, err = conf.FieldTLS("tls"); err != nil {
		return nil, err
	}
	if tlsConf != nil {
		pgConnConfig.TLSConfig = tlsConf
		pgConnConfig.TLSConfig.ServerName = pgConnConfig.Host
	}
	// This is required for postgres to understand we're interested in replication.
	// https://github.com/jackc/pglogrepl/issues/6
	pgConnConfig.RuntimeParams["replication"] = "database"

	snapshotMetrics := mgr.Metrics().NewGauge("postgres_snapshot_progress", "table")
	replicationLag := mgr.Metrics().NewGauge("postgres_replication_lag_bytes")

	i := &pgStreamInput{
		streamConfig: &pglogicalstream.Config{
			DBConfig:         pgConnConfig,
			TLSConfig:        pgConnConfig.TLSConfig,
			DBRawDSN:         dsn,
			DBSchema:         schema,
			DBTables:         tables,
			RefreshAuthToken: iamAuthTokenBuilder,

			IncludeTxnMarkers:        includeTxnMarkers,
			ReplicationSlotName:      dbSlotName,
			BatchSize:                snapshotBatchSize,
			StreamOldData:            streamSnapshot,
			TemporaryReplicationSlot: temporarySlot,
			PgStandbyTimeout:         pgStandbyTimeout,
			WalMonitorInterval:       walMonitorInterval,
			MaxSnapshotWorkers:       maxParallelSnapshotTables,
			Logger:                   logger,
			UnchangedToastValue:      unchangedToastValue,
			HeartbeatInterval:        heartbeatInterval,
			SignalTableName:          signalTableName,
			IncrementalSnapshot:      incSnapshot.cfg,
		},
		batching:        batching,
		checkpointLimit: checkpointLimit,
		msgChan:         make(chan asyncMessage),

		mgr:             mgr,
		logger:          mgr.Logger(),
		snapshotMetrics: snapshotMetrics,
		replicationLag:  replicationLag,
		stopSig:         shutdown.NewSignaller(),

		iamAuthEnabled: iamAuthEnabled,

		incSnapshotCheckpointCache:    incSnapshot.cache,
		incSnapshotCheckpointCacheKey: incSnapshot.cacheKey,
	}

	if i.controlSig, err = newControlSignaller(schema, signalTableName, logger); err != nil {
		return nil, err
	}

	// Has stopped is how we notify that we're not connected. This will get reset at connection time.
	i.stopSig.TriggerHasStopped()

	r, err := service.AutoRetryNacksBatchedToggled(conf, i)
	if err != nil {
		return nil, err
	}

	return conf.WrapBatchInputExtractTracingSpanMapping("postgres_cdc", r)
}

// validateSimpleString ensures we aren't vuln to SQL injection.
func validateSimpleString(s string) error {
	for _, b := range []byte(s) {
		isDigit := b >= '0' && b <= '9'
		isLower := b >= 'a' && b <= 'z'
		isUpper := b >= 'A' && b <= 'Z'
		isDelimiter := b == '_'
		if !isDigit && !isLower && !isUpper && !isDelimiter {
			return fmt.Errorf("invalid postgres identifier %q", s)
		}
	}
	return nil
}

func init() {
	service.MustRegisterBatchInput("postgres_cdc", newPostgresCDCConfig(), newPgStreamInput)
	// Legacy naming
	service.MustRegisterBatchInput("pg_stream", newPostgresCDCConfig().Deprecated(), newPgStreamInput)
}

type pgStreamInput struct {
	streamConfig    *pglogicalstream.Config
	logger          *service.Logger
	mgr             *service.Resources
	msgChan         chan asyncMessage
	batching        service.BatchPolicy
	checkpointLimit int

	snapshotMetrics *service.MetricGauge
	replicationLag  *service.MetricGauge
	controlSig      controlSignaller
	stopSig         *shutdown.Signaller

	// snapshotAckWG tracks in-flight snapshot batches: incremented when a
	// snapshot batch (nil LSN) is enqueued and decremented when it is
	// acknowledged. The snapshot->stream handoff blocks until it drains so the
	// replication slot is not promoted before snapshot rows are durable.
	snapshotAckWG sync.WaitGroup

	// IAM authentication fields
	iamAuthEnabled bool

	// only applies to incremental snapshot when enabled
	incSnapshotCheckpointCache    string
	incSnapshotCheckpointCacheKey string
	checkpointSeq                 atomic.Uint64

	// lastPersistedMu protects the lastPersisted fields below, which
	// commitCheckpoint touches from concurrent acknowledgements.
	lastPersistedMu sync.Mutex
	// lastPersistedIncSnapshotState avoids needless cache writes.
	// checkpointTracker copies the last state onto every later checkpoint,
	// so most acknowledgements carry an unchanged value.
	lastPersistedIncSnapshotState []byte
	// lastPersistedIncSnapshotSeq is the Seq of the state in the cache.
	// persistIncSnapshotState uses it to reject an older state.
	lastPersistedIncSnapshotSeq uint64
}

func (p *pgStreamInput) Connect(ctx context.Context) error {
	// If IAM authentication is enabled, generate a new token
	if p.iamAuthEnabled && p.streamConfig.RefreshAuthToken != nil {
		if err := p.streamConfig.RefreshAuthToken(ctx); err != nil {
			return fmt.Errorf("unable to generate IAM auth token: %w", err)
		}
	}

	if p.streamConfig.IncrementalSnapshotCfg().IsEnabled() {
		state, err := p.loadCachedIncSnapshotState(ctx)
		if err != nil {
			return fmt.Errorf("unable to load incremental snapshot checkpoint: %w", err)
		}
		p.streamConfig.IncrementalSnapshot.ResumeState = state
		if state == nil {
			p.logger.Debugf("Incremental snapshot: no checkpoint found, will start fresh")
		} else {
			p.logger.Debugf("Incremental snapshot: loaded checkpoint (current_table=%v, remaining=%d, tables=%d)", state.CurrentTable, len(state.RemainingTables), len(state.Tables))
		}
	}

	pgStream, err := pglogicalstream.NewPgStream(ctx, p.streamConfig)
	if err != nil {
		return fmt.Errorf("unable to create replication stream: %w", err)
	}
	batcher, err := p.batching.NewBatcher(p.mgr)
	if err != nil {
		return err
	}
	// Reset our stop signal
	p.stopSig = shutdown.NewSignaller()
	go p.processStream(pgStream, batcher)
	return err
}

func (p *pgStreamInput) processStream(pgStream *pglogicalstream.Stream, batcher *service.Batcher) {
	monitorLoop := asyncroutine.NewPeriodic(p.streamConfig.WalMonitorInterval, func() {
		// Periodically collect stats
		report := pgStream.GetProgress()
		for name, progress := range report.TableProgress {
			p.snapshotMetrics.SetFloat64(progress, name.String())
		}
		p.replicationLag.Set(report.WalLagInBytes)
	})
	monitorLoop.Start()
	defer monitorLoop.Stop()
	ctx, cancel := p.stopSig.SoftStopCtx(context.Background())
	defer cancel()
	defer func() {
		ctx, cancel := p.stopSig.HardStopCtx(context.Background())
		defer cancel()
		if err := batcher.Close(ctx); err != nil {
			p.logger.Errorf("unable to close batcher: %s", err)
		}
		// TODO(rockwood): We should wait for outstanding acks to be completed (best effort)
		if err := pgStream.Stop(ctx); err != nil {
			p.logger.Errorf("unable to stop replication stream: %s", err)
		}
		p.stopSig.TriggerHasStopped()
	}()

	var nextTimedBatchChan <-chan time.Time

	// offsets are nilable since we don't provide offset tracking during the snapshot phase
	cp := newCheckpointTracker(int64(p.checkpointLimit), &p.checkpointSeq)

	// blockingSnapshotComplete gates the isSnapshot/snapshotAckWG barrier to
	// the one-shot stream_snapshot phase, never to incremental snapshot's
	// nil-LSN batches. See Stream.BlockingSnapshot.
	blockingSnapshotComplete := !pgStream.BlockingSnapshot

	// pendingIncrementalState holds the newest checkpoint state until the
	// next flush sends it. The "lsn" metadata of a message moves in the same
	// way.
	var pendingIncrementalState []byte

	// batcherBuffered is the number of messages in the batcher that the
	// input has not given to checkpointTracker. The tracker cannot order
	// those rows. Therefore a checkpoint with no rows must not resolve while
	// this number is not zero. Refer to the
	// IncrementalSnapshotCheckpointOpType case.
	batcherBuffered := 0

	for !p.stopSig.IsSoftStopSignalled() {
		select {
		case <-nextTimedBatchChan:
			nextTimedBatchChan = nil
			flushedBatch, err := batcher.Flush(ctx)
			if err != nil {
				p.logger.Debugf("timed flush batch error: %s", err)
				break
			}
			batcherBuffered = 0
			if err := p.flushBatch(ctx, pgStream, cp, flushedBatch, pendingIncrementalState, blockingSnapshotComplete); err != nil {
				p.logger.Debugf("failed to flush batch: %s", err)
				break
			}
			// Clear the state only when flushBatch tracked a batch. An empty
			// flush tracks nothing, and a clear here loses the checkpoint.
			if len(flushedBatch) > 0 {
				pendingIncrementalState = nil
			}
		case batch := <-pgStream.Messages():
			if len(batch) == 1 && batch[0].Operation == pglogicalstream.SnapshotCompleteOpType {
				// Snapshot fully emitted. Flush any buffered rows, then block
				// until every snapshot batch is acknowledged downstream before
				// signalling the stream to promote the replication slot. Blocks
				// until acks drain or soft-stop (no timeout, by design).
				nextTimedBatchChan = nil
				flushedBatch, err := batcher.Flush(ctx)
				if err != nil {
					p.logger.Debugf("error flushing snapshot completion batch: %s", err)
					// The sentinel is a one-shot signal; if we bail here without
					// acking, the barrier's snapshot goroutine blocks on
					// snapshotAcked forever. Trigger a restart instead of stalling.
					p.stopSig.TriggerSoftStop()
					break
				}
				batcherBuffered = 0
				if err := p.flushBatch(ctx, pgStream, cp, flushedBatch, pendingIncrementalState, blockingSnapshotComplete); err != nil {
					p.logger.Debugf("failed to flush snapshot completion batch: %s", err)
					p.stopSig.TriggerSoftStop()
					break
				}
				// Clear the state only when flushBatch tracked a batch. An
				// empty flush tracks nothing, and a clear here loses the
				// checkpoint.
				if len(flushedBatch) > 0 {
					pendingIncrementalState = nil
				}
				drained := make(chan struct{})
				go func() {
					// May outlive the select below if soft-stop fires while the
					// downstream is stalled; bounded by process lifetime.
					p.snapshotAckWG.Wait()
					close(drained)
				}()
				select {
				case <-drained:
					pgStream.MarkSnapshotAcknowledged()
					blockingSnapshotComplete = true
				case <-p.stopSig.SoftStopChan():
				}
				break
			}
			if len(batch) == 1 && batch[0].Operation == pglogicalstream.IncrementalSnapshotCheckpointOpType {
				// State advanced with no rows emitted, so no message can
				// carry it. Hold it pending -- state only advances, so the
				// newest wins and anything left pending rides out on the
				// next flush.
				pendingIncrementalState = batch[0].IncrementalSnapshotState
				if batcherBuffered > 0 {
					// The tracker cannot see the rows still in the batcher,
					// so committing now would checkpoint past them.
					break
				}
				// Nothing untracked is buffered, so this still resolves
				// behind any unresolved earlier batch.
				if err := p.commitIncrementalSnapshotCheckpoint(ctx, pgStream, cp, batch[0].IncrementalSnapshotState); err != nil {
					// Stays pending for the next flush. Until one succeeds
					// the stored checkpoint is stale.
					p.logger.Warnf("unable to commit incremental snapshot checkpoint, retrying on the next flush: %s", err)
					break
				}
				pendingIncrementalState = nil
				break
			}
			var (
				flush bool
				mb    []byte
			)
			for _, msg := range batch {
				// noop if not configured
				if _, err := p.controlSig.listen(&msg); err != nil {
					// Log it and fall through to the normal emit path below.
					p.logger.Errorf("failed to detect control signal in change event: %s", err)
				}

				if msg.IncrementalSnapshotState != nil {
					pendingIncrementalState = msg.IncrementalSnapshotState
				}
				if msg.Operation == pglogicalstream.IncrementalSnapshotCheckpointOpType {
					// Defensive: this sentinel never shares a batch with other
					// messages today, but never forward it if that changes.
					continue
				}

				var marshalErr error
				if mb, marshalErr = json.Marshal(msg.Data); marshalErr != nil {
					// A marshal failure is deterministic (in practice
					// non-finite floats), so neither skipping the row (silent
					// loss) nor restarting (the same row fails on every
					// reconnect, and the stalled slot blocks WAL retention on
					// the server) can make progress. Publish the row with its
					// error set instead: the stream keeps moving,
					// at-least-once holds (the row IS delivered, flagged),
					// and operators can inspect or route it with
					// error-handling components.
					rowLSN := "unknown"
					if msg.LSN != nil {
						rowLSN = *msg.LSN
					}
					p.logger.Warnf("Publishing unmarshalable row from table %s (LSN %s) with its error set for error-routing: %v", msg.Table, rowLSN, marshalErr)
					mb = fmt.Appendf(nil, "%+v", msg.Data)
				}
				batchMsg := service.NewMessage(mb)
				if marshalErr != nil {
					batchMsg.SetError(fmt.Errorf("marshalling WAL row from table %s: %w", msg.Table, marshalErr))
				}
				batchMsg.MetaSet("table", msg.Table)
				batchMsg.MetaSet("operation", string(msg.Operation))
				if msg.LSN != nil {
					batchMsg.MetaSet("lsn", *msg.LSN)
				}
				if !msg.CommitTime.IsZero() {
					batchMsg.MetaSet("commit_ts_ms", strconv.FormatInt(msg.CommitTime.UnixMilli(), 10))
				}
				if msg.ColumnSchema != nil {
					batchMsg.MetaSetImmut("schema", service.ImmutableAny{V: msg.ColumnSchema})
				}
				if msg.BeforeData != nil {
					batchMsg.MetaSetImmut("before", service.ImmutableAny{V: msg.BeforeData})
				}
				batcherBuffered++
				if batcher.Add(batchMsg) {
					flush = true
				}
			}
			if flush {
				nextTimedBatchChan = nil
				flushedBatch, err := batcher.Flush(ctx)
				if err != nil {
					p.logger.Debugf("error flushing batch: %s", err)
					break
				}
				batcherBuffered = 0
				if err := p.flushBatch(ctx, pgStream, cp, flushedBatch, pendingIncrementalState, blockingSnapshotComplete); err != nil {
					p.logger.Debugf("failed to flush batch: %s", err)
					break
				}
				// Clear the state only when flushBatch tracked a batch. An
				// empty flush tracks nothing, and a clear here loses the
				// checkpoint.
				if len(flushedBatch) > 0 {
					pendingIncrementalState = nil
				}
			} else {
				d, ok := batcher.UntilNext()
				if ok {
					nextTimedBatchChan = time.After(d)
				}
			}
		case err := <-pgStream.Errors():
			p.logger.Warnf("logical replication stream error: %s", err)
			// If the stream has internally errored then we should stop and restart processing
			p.stopSig.TriggerSoftStop()
		case <-p.stopSig.SoftStopChan():
			p.logger.Debug("soft stop triggered, stopping logical replication stream")
		}
	}
}

// checkpointTracker tracks checkpointOffset values. It resolves
// acknowledgements in order, also when they arrive out of order. It merges
// each offset with the last offset before it tracks the offset. A
// resolution with a nil lsn or a nil incSnapshotState therefore does not
// remove the value of the field that it does not hold.
//
// Call Track only from the processStream goroutine. The functions that Track
// returns are safe to call from other goroutines at the same time. The
// acknowledgement functions do this.
type checkpointTracker struct {
	cp   *checkpoint.Capped[checkpointOffset]
	last checkpointOffset
	// seq is owned by the input, not this tracker, so it survives a
	// reconnect: a replacement tracker keeps counting where the last one
	// stopped. See pgStreamInput.checkpointSeq.
	seq *atomic.Uint64
}

func newCheckpointTracker(limit int64, seq *atomic.Uint64) *checkpointTracker {
	return &checkpointTracker{
		cp:  checkpoint.NewCapped[checkpointOffset](limit),
		seq: seq,
	}
}

func (t *checkpointTracker) Track(ctx context.Context, offset checkpointOffset, batchSize int64) (func() *checkpointOffset, error) {
	// Orders the offsets for commitCheckpoint, monotonic for the life of the
	// input across any number of trackers.
	offset.seq = t.seq.Add(1)
	t.last = t.last.merge(offset)
	return t.cp.Track(ctx, t.last, batchSize)
}

// commitCheckpoint applies a resolved checkpointOffset: the snapshot state
// first, then the LSN, and the LSN only if that write succeeded.
//
// Either order alone is wrong. Acknowledging first leaves an acknowledged
// position with no state; acknowledging anyway advances the slot past rows
// the state was meant to account for, a snapshot signal among them, which
// never streams again. Holding the acknowledgement back only retains WAL
// until the next attempt, which is the recoverable direction.
//
// A failed AckLSN leaves the state written: it records what was delivered,
// which stays true, and the position is retried.
func (p *pgStreamInput) commitCheckpoint(ctx context.Context, pgStream *pglogicalstream.Stream, offset checkpointOffset) error {
	if offset.incSnapshotState != nil {
		if err := p.persistIncSnapshotState(ctx, offset); err != nil {
			return err
		}
	}
	if offset.lsn != nil {
		if err := pgStream.AckLSN(ctx, *offset.lsn); err != nil {
			return fmt.Errorf("unable to ack LSN to postgres: %w", err)
		}
	}
	return nil
}

// persistIncSnapshotState writes offset's snapshot state to the cache.
//
// Acknowledgements run concurrently, so it holds lastPersistedMu across the
// test and the write to keep them atomic. The lock alone is not enough --
// the calls can take it in either order -- so it also rejects an offset no
// newer than the one already written.
func (p *pgStreamInput) persistIncSnapshotState(ctx context.Context, offset checkpointOffset) error {
	p.lastPersistedMu.Lock()
	defer p.lastPersistedMu.Unlock()

	if offset.seq <= p.lastPersistedIncSnapshotSeq {
		// A newer state is already in the cache.
		return nil
	}

	// Unchanged state: record the seq, skip the write.
	if bytes.Equal(offset.incSnapshotState, p.lastPersistedIncSnapshotState) {
		p.lastPersistedIncSnapshotSeq = offset.seq
		return nil
	}

	if err := p.saveIncrementalSnapshotState(ctx, offset.incSnapshotState); err != nil {
		return fmt.Errorf("unable to persist incremental snapshot checkpoint: %w", err)
	}
	p.lastPersistedIncSnapshotState = offset.incSnapshotState
	p.lastPersistedIncSnapshotSeq = offset.seq
	return nil
}

// commitIncrementalSnapshotCheckpoint tracks a row-less checkpoint and
// resolves it. Tracking rather than writing it directly keeps it behind
// every earlier batch, so it cannot pass rows the pipeline has not
// acknowledged.
//
// Only call it when the batcher holds no rows the tracker does not have.
// The tracker cannot see rows still in the batcher, so it would resolve
// past them.
func (p *pgStreamInput) commitIncrementalSnapshotCheckpoint(ctx context.Context, pgStream *pglogicalstream.Stream, checkpointer *checkpointTracker, state []byte) error {
	resolveFn, err := checkpointer.Track(ctx, checkpointOffset{incSnapshotState: state}, 0)
	if err != nil {
		return fmt.Errorf("unable to checkpoint incremental snapshot state: %w", err)
	}
	maxOffset := resolveFn()
	if maxOffset == nil {
		return nil
	}
	return p.commitCheckpoint(ctx, pgStream, *maxOffset)
}

func (p *pgStreamInput) flushBatch(
	ctx context.Context,
	pgStream *pglogicalstream.Stream,
	checkpointer *checkpointTracker,
	batch service.MessageBatch,
	incSnapshotState []byte,
	blockingSnapshotComplete bool,
) error {
	if len(batch) == 0 {
		return nil
	}

	// Snapshot rows have no LSN, and they share this batcher with change
	// rows that have an LSN. Therefore a batch can end with a snapshot row.
	// Search backwards for the last message that has an LSN. Do not use the
	// last message of the batch.
	var lsn *string
	for i := len(batch) - 1; i >= 0; i-- {
		if lsnStr, ok := batch[i].MetaGet("lsn"); ok {
			lsn = &lsnStr
			break
		}
	}
	offset := checkpointOffset{lsn: lsn, incSnapshotState: incSnapshotState}
	resolveFn, err := checkpointer.Track(ctx, offset, int64(len(batch)))
	if err != nil {
		return fmt.Errorf("unable to checkpoint: %w", err)
	}

	// The single stream_snapshot phase also has no LSN. Track those batches,
	// and the change from snapshot to stream can then wait for their
	// acknowledgement. Refer to the message handling in the read loop.
	//
	// Incremental snapshot batches also have no LSN, but they continue for
	// the life of the stream. Therefore blockingSnapshotComplete removes
	// them from that wait after the first phase is complete.
	isSnapshot := lsn == nil && !blockingSnapshotComplete

	ackFn := func(ctx context.Context, _ error) error {
		if isSnapshot {
			defer p.snapshotAckWG.Done()
		}
		maxOffset := resolveFn()
		if maxOffset == nil {
			return nil
		}
		return p.commitCheckpoint(ctx, pgStream, *maxOffset)
	}
	if isSnapshot {
		p.snapshotAckWG.Add(1)
	}
	select {
	case p.msgChan <- asyncMessage{msg: batch, ackFn: ackFn}:
	case <-ctx.Done():
		if isSnapshot {
			p.snapshotAckWG.Done()
		}
		return ctx.Err()
	}
	return nil
}

// loadCachedIncSnapshotState reads the incremental snapshot checkpoint from
// the cache. A key that does not exist shows that there is no checkpoint and
// that the snapshot starts new. This result is not an error.
func (p *pgStreamInput) loadCachedIncSnapshotState(ctx context.Context) (*incrementalsnapshot.State, error) {
	var (
		cacheVal []byte
		cErr     error
	)
	if err := p.mgr.AccessCache(ctx, p.incSnapshotCheckpointCache, func(c service.Cache) {
		cacheVal, cErr = c.Get(ctx, p.incSnapshotCheckpointCacheKey)
	}); err != nil {
		return nil, fmt.Errorf("unable to access cache for reading: %w", err)
	}
	if errors.Is(cErr, service.ErrKeyNotFound) {
		return nil, nil
	} else if cErr != nil {
		return nil, fmt.Errorf("unable to read checkpoint from cache: %w", cErr)
	} else if cacheVal == nil {
		return nil, nil
	}
	state := new(incrementalsnapshot.State)
	if err := json.Unmarshal(cacheVal, state); err != nil {
		if errors.Is(err, incrementalsnapshot.ErrUnsupportedStateVersion) {
			return nil, fmt.Errorf("%w: change %s.%s, or clear that key, to start a fresh snapshot", err, fieldIncSnapshot, fieldIncSnapshotCheckpointCacheKey)
		}
		return nil, fmt.Errorf("unable to unmarshal incremental snapshot checkpoint: %w", err)
	}
	return state, nil
}

// saveIncrementalSnapshotState writes an incremental snapshot checkpoint to
// the cache. The caller supplies the checkpoint as bytes.
func (p *pgStreamInput) saveIncrementalSnapshotState(ctx context.Context, state []byte) error {
	var cErr error
	if err := p.mgr.AccessCache(ctx, p.incSnapshotCheckpointCache, func(c service.Cache) {
		cErr = c.Set(ctx, p.incSnapshotCheckpointCacheKey, state, nil)
	}); err != nil {
		return fmt.Errorf("unable to access cache for writing: %w", err)
	}
	if cErr != nil {
		return fmt.Errorf("unable to persist checkpoint to cache: %w", cErr)
	}
	return nil
}

func (p *pgStreamInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case m := <-p.msgChan:
		return m.msg, m.ackFn, nil
	case <-p.stopSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (p *pgStreamInput) Close(ctx context.Context) error {
	p.stopSig.TriggerSoftStop()
	select {
	case <-ctx.Done():
	case <-time.After(shutdownTimeout):
	case <-p.stopSig.HasStoppedChan():
	}
	p.stopSig.TriggerHardStop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(shutdownTimeout):
	case <-p.stopSig.HasStoppedChan():
	}
	return nil
}
