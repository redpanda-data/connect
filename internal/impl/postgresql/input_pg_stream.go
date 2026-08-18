// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pgstream

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
	"github.com/redpanda-data/connect/v4/internal/license"
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

	fieldIncSnapshot                        = "incremental_snapshot"
	fieldIncSnapshotEnabled                 = "enabled"
	fieldIncrementalSnapshotTables          = "tables"
	fieldIncrementalSnapshotChunkSize       = "chunk_size"
	fieldIncSnapshotCheckpointCache         = "checkpoint_cache"
	fieldIncSnapshotCheckpointCacheKey      = "checkpoint_cache_key"
	defaultIncrementalSnapshotCheckpointKey = "postgres_cdc_incremental_snapshot"
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
			Description("Int specifies a number of tables that will be processed in parallel during the snapshot processing stage").
			Default(1)).
		Field(service.NewAnyField(fieldUnchangedToastValue).
			Description("The value to emit when there are unchanged TOAST values in the stream. This occurs for updates and deletes where REPLICA IDENTITY is not FULL.").
			ShortDescription("The value to emit when TOAST values are unchanged in the stream.").
			Default(nil).
			Example("__redpanda_connect_unchanged_toast_value__").
			Optional().
			Advanced()).
		Field(service.NewDurationField(fieldHeartbeatInterval).
			Description("The interval at which to write heartbeat messages. Heartbeat messages are needed in scenarios when the subscribed tables are low frequency, but there are other high frequency tables writing. Due to the checkpointing mechanism for replication slots, not having new messages to acknowledge will prevent postgres from reclaiming the write ahead log, which can exhaust the local disk. Having heartbeats allows Redpanda Connect to safely acknowledge data periodically and move forward the committed point in the log so it can be reclaimed. Setting the duration to 0s will disable heartbeats entirely. Heartbeats are created by periodically writing logical messages to the write ahead log using `pg_logical_emit_message`.").
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
` + "```").
			Example("rpcn_signal_table").
			Default("").
			Advanced()).
		Field(service.NewObjectField(fieldIncSnapshot,
			service.NewBoolField(fieldIncSnapshotEnabled).
				Description("When set to true, the connector performs an incremental (chunked) snapshot of the configured tables automatically, starting as soon as logical replication streaming begins. Unlike `"+fieldStreamSnapshot+"`, this requires no dedicated up-front snapshot phase, does not block replication from starting, and needs no signal table or external trigger. It is independent of `"+fieldStreamSnapshot+"`, so the two can be enabled together or used separately.\n\nCorrectness (no duplicate rows) is only guaranteed for tables whose primary key is monotonically increasing for new rows (for example a serial/identity column, or a UUIDv7-style key) during the snapshot. Rows inserted with a primary key that reuses or fills a gap below the table's current maximum key while the snapshot is in progress may be delivered twice: once from replication, once from the backfill. Consumers should treat incoming rows as idempotent upserts keyed by primary key, as is standard CDC practice.").
				ShortDescription("Automatically and continuously snapshot the configured tables in chunks, concurrently with replication streaming.").
				Default(false),
			service.NewStringListField(fieldIncrementalSnapshotTables).
				Description("The tables to incrementally snapshot. If omitted, the tables configured in `"+fieldTables+"` are used instead.").
				Optional(),
			service.NewIntField(fieldIncrementalSnapshotChunkSize).
				Description("The number of rows to read per chunk while incrementally snapshotting a table.").
				Default(1024),
			service.NewStringField(fieldIncSnapshotCheckpointCache).
				Description("A https://www.docs.redpanda.com/redpanda-connect/components/caches/about[cache resource^] used to durably store the incremental snapshot's progress, allowing it to resume from where it left off after a restart instead of starting over. Required when `"+fieldIncSnapshotEnabled+"` is `true`.").
				ShortDescription("Cache resource storing incremental snapshot progress, so restarts resume instead of starting over. Required when enabled.").
				Optional(),
			service.NewStringField(fieldIncSnapshotCheckpointCacheKey).
				Description("The key used to store the incremental snapshot progress in `"+fieldIncSnapshotCheckpointCache+"`. An alternative key can be provided if multiple incremental snapshots share the same cache.").
				Default(defaultIncrementalSnapshotCheckpointKey),
		).
			Description("Configures incremental snapshotting of one or more tables, which runs automatically and concurrently with logical replication streaming once enabled.").
			ShortDescription("Configures automatic, chunked incremental snapshotting that runs concurrently with replication streaming.").
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

		incSnapshotEnabled            bool
		incSnapshotTables             []string
		incSnapshotChunkSize          int
		incSnapshotCheckpointCache    string
		incSnapshotCheckpointCacheKey string
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

	incSnapshotConf := conf.Namespace(fieldIncSnapshot)
	if incSnapshotEnabled, err = incSnapshotConf.FieldBool(fieldIncSnapshotEnabled); err != nil {
		return nil, err
	}
	if incSnapshotConf.Contains(fieldIncrementalSnapshotTables) {
		if incSnapshotTables, err = incSnapshotConf.FieldStringList(fieldIncrementalSnapshotTables); err != nil {
			return nil, err
		}
	}
	if incSnapshotChunkSize, err = incSnapshotConf.FieldInt(fieldIncrementalSnapshotChunkSize); err != nil {
		return nil, err
	}
	if incSnapshotConf.Contains(fieldIncSnapshotCheckpointCache) {
		if incSnapshotCheckpointCache, err = incSnapshotConf.FieldString(fieldIncSnapshotCheckpointCache); err != nil {
			return nil, err
		}
	}
	if incSnapshotCheckpointCacheKey, err = incSnapshotConf.FieldString(fieldIncSnapshotCheckpointCacheKey); err != nil {
		return nil, err
	}
	if incSnapshotEnabled && incSnapshotCheckpointCache == "" {
		return nil, fmt.Errorf("%s.%s is required when %s.%s is true", fieldIncSnapshot, fieldIncSnapshotCheckpointCache, fieldIncSnapshot, fieldIncSnapshotEnabled)
	}
	if incSnapshotEnabled && !conf.Resources().HasCache(incSnapshotCheckpointCache) {
		return nil, fmt.Errorf("unknown cache resource: %s", incSnapshotCheckpointCache)
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

	var incSnapshotCfg *pglogicalstream.IncrementalSnapshotCfg
	if incSnapshotEnabled {
		incSnapshotCfg = &pglogicalstream.IncrementalSnapshotCfg{
			Enabled:   true,
			Tables:    incSnapshotTables,
			ChunkSize: incSnapshotChunkSize,
		}
	}

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
			IncrementalSnapshot:      incSnapshotCfg,
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

		checkpointCache:    incSnapshotCheckpointCache,
		checkpointCacheKey: incSnapshotCheckpointCacheKey,
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

	// checkpointCache/checkpointCacheKey matter only when IncrementalSnapshot
	// is enabled; otherwise checkpointCache is empty and unused.
	checkpointCache    string
	checkpointCacheKey string
}

func (p *pgStreamInput) Connect(ctx context.Context) error {
	// If IAM authentication is enabled, generate a new token
	if p.iamAuthEnabled && p.streamConfig.RefreshAuthToken != nil {
		if err := p.streamConfig.RefreshAuthToken(ctx); err != nil {
			return fmt.Errorf("unable to generate IAM auth token: %w", err)
		}
	}

	if p.streamConfig.IncrementalSnapshot != nil {
		state, err := p.loadIncrementalSnapshotState(ctx)
		if err != nil {
			return fmt.Errorf("unable to load incremental snapshot checkpoint: %w", err)
		}
		p.streamConfig.IncrementalSnapshot.ResumeState = state
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
	cp := checkpoint.NewCapped[checkpointOffset](int64(p.checkpointLimit))

	// blockingSnapshotComplete restricts the isSnapshot/snapshotAckWG barrier to
	// the one-shot stream_snapshot phase, not the also-nil-LSN but continuous
	// incremental snapshot batches. Starts true unless this session will
	// actually run that phase (see WillEmitBlockingSnapshot), then flips true
	// once its completion sentinel is handled.
	blockingSnapshotComplete := !pgStream.WillEmitBlockingSnapshot()

	// pendingIncrementalState carries the most recent checkpoint state until
	// it rides along with the next downstream flush, like a message's own
	// "lsn" metadata does.
	var pendingIncrementalState []byte

	for !p.stopSig.IsSoftStopSignalled() {
		select {
		case <-nextTimedBatchChan:
			nextTimedBatchChan = nil
			flushedBatch, err := batcher.Flush(ctx)
			if err != nil {
				p.logger.Debugf("timed flush batch error: %s", err)
				break
			}
			if err := p.flushBatch(ctx, pgStream, cp, flushedBatch, pendingIncrementalState, blockingSnapshotComplete); err != nil {
				p.logger.Debugf("failed to flush batch: %s", err)
				break
			}
			pendingIncrementalState = nil
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
				if err := p.flushBatch(ctx, pgStream, cp, flushedBatch, pendingIncrementalState, blockingSnapshotComplete); err != nil {
					p.logger.Debugf("failed to flush snapshot completion batch: %s", err)
					p.stopSig.TriggerSoftStop()
					break
				}
				pendingIncrementalState = nil
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
				// State advanced with no rows emitted (e.g. every buffered row
				// was deduplicated), so there's no message to ride along with
				// -- track and resolve immediately, still ordered behind any
				// earlier unresolved batch.
				if err := p.commitIncrementalSnapshotCheckpoint(ctx, pgStream, cp, batch[0].IncrementalSnapshotState); err != nil {
					p.logger.Debugf("failed to commit incremental snapshot checkpoint: %s", err)
				}
				break
			}
			var (
				flush bool
				mb    []byte
				err   error
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

				if mb, err = json.Marshal(msg.Data); err != nil {
					p.logger.Errorf("failure to marshal message: %s", err)
					break
				}
				batchMsg := service.NewMessage(mb)
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
				if err := p.flushBatch(ctx, pgStream, cp, flushedBatch, pendingIncrementalState, blockingSnapshotComplete); err != nil {
					p.logger.Debugf("failed to flush batch: %s", err)
					break
				}
				pendingIncrementalState = nil
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

// checkpointOffset is the per-batch payload tracked by the LSN checkpointer.
// IncrementalSnapshotState is non-nil when a batch (or the phantom row-less
// one from commitIncrementalSnapshotCheckpoint) carries a checkpoint;
// bundling it with LSN gives it the same ack-ordering discipline.
type checkpointOffset struct {
	lsn                      *string
	incrementalSnapshotState []byte
}

// commitCheckpoint applies a resolved checkpointOffset: acks the LSN first,
// then persists the incremental snapshot state, so a crash never leaves
// acknowledged-but-unpersisted progress.
func (p *pgStreamInput) commitCheckpoint(ctx context.Context, pgStream *pglogicalstream.Stream, offset checkpointOffset) error {
	if offset.lsn != nil {
		if err := pgStream.AckLSN(ctx, *offset.lsn); err != nil {
			return fmt.Errorf("unable to ack LSN to postgres: %w", err)
		}
	}
	if offset.incrementalSnapshotState != nil {
		if err := p.saveIncrementalSnapshotState(ctx, offset.incrementalSnapshotState); err != nil {
			return fmt.Errorf("unable to persist incremental snapshot checkpoint: %w", err)
		}
	}
	return nil
}

// commitIncrementalSnapshotCheckpoint tracks and immediately resolves a
// row-less checkpoint. Tracking it (rather than persisting directly) still
// gates it behind every earlier tracked batch, so it can't surface ahead of
// unacknowledged rows earlier in the stream.
func (p *pgStreamInput) commitIncrementalSnapshotCheckpoint(
	ctx context.Context,
	pgStream *pglogicalstream.Stream,
	checkpointer *checkpoint.Capped[checkpointOffset],
	state []byte,
) error {
	resolveFn, err := checkpointer.Track(ctx, checkpointOffset{incrementalSnapshotState: state}, 0)
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
	checkpointer *checkpoint.Capped[checkpointOffset],
	batch service.MessageBatch,
	incrementalSnapshotState []byte,
	blockingSnapshotComplete bool,
) error {
	if len(batch) == 0 {
		return nil
	}

	var lsn *string
	lastMsg := batch[len(batch)-1]
	lsnStr, ok := lastMsg.MetaGet("lsn")
	if ok {
		lsn = &lsnStr
	}
	offset := checkpointOffset{lsn: lsn, incrementalSnapshotState: incrementalSnapshotState}
	resolveFn, err := checkpointer.Track(ctx, offset, int64(len(batch)))
	if err != nil {
		return fmt.Errorf("unable to checkpoint: %w", err)
	}

	// The one-shot stream_snapshot phase also carries no LSN; track those
	// batches so the snapshot->stream handoff can block on their ack (see the
	// sentinel handling in the read loop). Incremental snapshot batches are
	// also nil-LSN but run continuously, so blockingSnapshotComplete excludes
	// them from that one-shot barrier once the upfront phase has completed.
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

// loadIncrementalSnapshotState reads the persisted incremental snapshot
// checkpoint from checkpointCache, if any. A missing key means there's no
// checkpoint yet (fresh start), not an error.
func (p *pgStreamInput) loadIncrementalSnapshotState(ctx context.Context) (*incrementalsnapshot.State, error) {
	var (
		cacheVal []byte
		cErr     error
	)
	if err := p.mgr.AccessCache(ctx, p.checkpointCache, func(c service.Cache) {
		cacheVal, cErr = c.Get(ctx, p.checkpointCacheKey)
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
		return nil, fmt.Errorf("unable to unmarshal incremental snapshot checkpoint: %w", err)
	}
	return state, nil
}

// saveIncrementalSnapshotState persists an already-serialized incremental
// snapshot checkpoint to checkpointCache.
func (p *pgStreamInput) saveIncrementalSnapshotState(ctx context.Context, state []byte) error {
	var cErr error
	if err := p.mgr.AccessCache(ctx, p.checkpointCache, func(c service.Cache) {
		cErr = c.Set(ctx, p.checkpointCacheKey, state, nil)
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
