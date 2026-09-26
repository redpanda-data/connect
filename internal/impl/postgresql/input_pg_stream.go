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
			Description("The data source name (DSN) of the PostgreSQL database from which you want to stream updates. Use the format `postgres://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]`. PostgreSQL enforces SSL by default. To disable SSL, for example in a secure environment, add `sslmode=disable` to the connection string.").
			ShortDescription("The Data Source Name for the PostgreSQL database, in postgres:// URL form.").
			Example("postgres://foouser:foopass@localhost:5432/foodb?sslmode=disable")).
		Field(service.NewBoolField(fieldIncludeTxnMarkers).
			Description(`When set to ` + "`" + `true` + "`" + `, creates empty messages for the ` + "`" + `BEGIN` + "`" + ` and ` + "`" + `COMMIT` + "`" + ` operations that start and complete each transaction. Messages with the ` + "`" + `operation` + "`" + ` metadata field set to ` + "`" + `begin` + "`" + ` or ` + "`" + `commit` + "`" + ` have null message payloads.`).
			ShortDescription("Emit empty BEGIN and COMMIT messages at the start and end of each transaction.").
			Default(false)).
		Field(service.NewBoolField(fieldStreamSnapshot).
			Description("When set to `true`, this input streams a snapshot of all existing data in the source database before streaming data changes. To use this setting, all database tables that you want to replicate _must_ have a primary key, which allows the input to read each table in parallel. This setting has no effect if `" + fieldTables + "` is left empty, since the snapshot is only planned for tables listed there.").
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
			Description(`The number of table rows to fetch in each batch when querying the snapshot.

This option is only available when ` + "`" + `stream_snapshot` + "`" + ` is set to ` + "`" + `true` + "`" + `.`).
			Example(10000).
			Default(1000)).
		Field(service.NewStringField(fieldSchema).
			Description("The PostgreSQL schema from which to replicate data.").
			Examples("public", `"MyCaseSensitiveSchemaNeedingQuotes"`),
		).
		Field(service.NewStringListField(fieldTables).
			Description(`A list of database table names to include in the snapshot and logical replication. Specify each table name as a separate item.

If left empty, the underlying PostgreSQL publication is created ` + "`FOR ALL TABLES`" + `, which replicates every table in every schema of the database, ignoring ` + "`" + fieldSchema + "`" + `. This also disables ` + "`" + fieldStreamSnapshot + "`" + `, since the initial snapshot is only planned for tables listed here.`).
			Example([]string{"my_table_1", `"MyCaseSensitiveTableNeedingQuotes"`})).
		Field(service.NewIntField(fieldCheckpointLimit).
			Description("The maximum number of messages that this input can process at a given time. Increasing this limit enables parallel processing, and batching at the output level. To preserve at-least-once guarantees, any given log sequence number (LSN) is not acknowledged until all messages under that offset are delivered.").
			ShortDescription("The maximum number of messages that can be processed at a given time.").
			Default(1024)).
		Field(service.NewBoolField(fieldTemporarySlot).
			Description(`If set to ` + "`" + `true` + "`" + `, the input creates a temporary replication slot that is automatically dropped when the connection to your source database is closed. You might use this option to:

- Avoid data accumulating in the replication slot when a pipeline is paused or stopped.
- Test the connector.

If the pipeline is restarted and ` + "`" + `stream_snapshot` + "`" + ` is enabled, another data snapshot is taken before data updates are streamed.`).
			Default(false)).
		Field(service.NewStringField(fieldSlotName).
			Description(`The name of the PostgreSQL logical replication slot to use. If the slot does not exist, the input creates it. You can also create the slot manually before starting replication.

To avoid granting the replication user permission to create publications, you can create the publications manually ahead of time. This input uses the naming pattern ` + "`" + `pglog_stream_<replication_slot_name>` + "`" + `, so create publications using this convention.`).
			ShortDescription("The name of the PostgreSQL logical replication slot to use. The input creates the slot if it does not exist.").
			Example("my_test_slot")).
		Field(service.NewDurationField(fieldPgStandbyTimeout).
			Description("Specify the standby timeout after which an idle connection is refreshed to keep the connection alive.").
			Example("30s").
			Default("10s")).
		Field(service.NewDurationField(fieldWalMonitorInterval).
			Description("How often to report changes to the replication lag and write them to Redpanda Connect metrics.").
			Example("6s").
			Default("3s")).
		Field(service.NewIntField(fieldMaxParallelSnapshotTables).
			Description("Specify the maximum number of tables that are processed in parallel when the initial snapshot of the source database is taken.").
			Default(1)).
		Field(service.NewAnyField(fieldUnchangedToastValue).
			Description("Specify the value to emit when unchanged TOAST values appear in the message stream. Unchanged values occur for data updates and deletes when `REPLICA IDENTITY` is not set to `FULL`.").
			ShortDescription("The value to emit when TOAST values are unchanged in the stream.").
			Default(nil).
			Example("__redpanda_connect_unchanged_toast_value__").
			Optional().
			Advanced()).
		Field(service.NewDurationField(fieldHeartbeatInterval).
			Description(`The interval between heartbeat messages, which Redpanda Connect writes to the write-ahead log (WAL) using the ` + "`" + `pg_logical_emit_message` + "`" + ` function.

Heartbeat messages are useful when you subscribe to data changes from tables with low activity, while other tables in the database have higher-frequency updates. Without new messages to acknowledge, PostgreSQL cannot reclaim the WAL, which can exhaust the local disk. Heartbeat messages allow Redpanda Connect to periodically acknowledge new messages even when no data updates occur. Each acknowledgement advances the committed point in the WAL, which ensures that PostgreSQL can safely reclaim older log segments.

Set ` + "`" + `heartbeat_interval` + "`" + ` to ` + "`" + `0s` + "`" + ` to disable heartbeats.`).
			ShortDescription("Interval at which to write heartbeat messages, keeping the replication slot current on low-traffic tables.").
			Default("1h").
			Example("0s").
			Example("24h").
			Advanced()).
		Field(service.NewTLSField("tls").
			Description("Using this field overrides the SSL/TLS settings in the environment and DSN.")).
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
				Description("The PostgreSQL endpoint hostname (for example, mydb.abc123.us-east-1.rds.amazonaws.com)."),
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
			Description(`AWS IAM authentication configuration for PostgreSQL instances. When enabled, IAM credentials are used to generate temporary authentication tokens instead of a static password.

This is useful for connecting to Amazon RDS or Aurora PostgreSQL instances with IAM database authentication enabled. The generated tokens are valid for 15 minutes and are automatically refreshed.

For more information about AWS credentials configuration, see the xref:guides:cloud/aws.adoc[credentials for AWS] guide.`).
			ShortDescription("AWS IAM authentication configuration for PostgreSQL instances.").
			Advanced().
			Optional()).
		Field(service.NewStringField(fieldSignalTableName).
			Description(`The name of the table used to send control signals to the connector, excluding the schema. The table must
exist in the schema configured via the ` + "`schema`" + ` field, and must not also appear in ` + "`" + fieldTables + "`" + `,
since the signal table is implicitly added to the publication and excluded from snapshot scans, so listing
it in both places is rejected at startup. It must have at least these columns; startup validation checks
column names only, not types, so a wrong column type (for example ` + "`data JSONB`" + ` instead of ` + "`TEXT`" + `)
is only caught at runtime, on the first signal row read:

- **id**: any type representable as a string (for example ` + "`SERIAL`" + `, ` + "`BIGSERIAL`" + `, ` + "`UUID`" + `, ` + "`VARCHAR`" + `)
- **type**: should be ` + "`VARCHAR`" + ` or another string type: the signal type (see supported signals below)
- **data**: should be ` + "`TEXT`" + `: a JSON object containing signal parameters

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

**` + "`log`" + `**: recognized and logged when received. The ` + "`data`" + ` column must contain
a JSON object with a ` + "`message`" + ` key, whose value is written to the connector's log output.

` + "```sql" + `
INSERT INTO <schema>.<signal_table_name> (type, data) VALUES ('log', '{"message": "Signal message"}');
` + "```").
			Example("rpcn_signal_table").
			Default("").
			Advanced()).
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
}

func (p *pgStreamInput) Connect(ctx context.Context) error {
	// If IAM authentication is enabled, generate a new token
	if p.iamAuthEnabled && p.streamConfig.RefreshAuthToken != nil {
		if err := p.streamConfig.RefreshAuthToken(ctx); err != nil {
			return fmt.Errorf("unable to generate IAM auth token: %w", err)
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
	cp := checkpoint.NewCapped[*string](int64(p.checkpointLimit))
	for !p.stopSig.IsSoftStopSignalled() {
		select {
		case <-nextTimedBatchChan:
			nextTimedBatchChan = nil
			flushedBatch, err := batcher.Flush(ctx)
			if err != nil {
				p.logger.Debugf("timed flush batch error: %s", err)
				break
			}
			if err := p.flushBatch(ctx, pgStream, cp, flushedBatch); err != nil {
				p.logger.Debugf("failed to flush batch: %s", err)
				break
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
				if err := p.flushBatch(ctx, pgStream, cp, flushedBatch); err != nil {
					p.logger.Debugf("failed to flush snapshot completion batch: %s", err)
					p.stopSig.TriggerSoftStop()
					break
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
				case <-p.stopSig.SoftStopChan():
				}
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
				if err := p.flushBatch(ctx, pgStream, cp, flushedBatch); err != nil {
					p.logger.Debugf("failed to flush batch: %s", err)
					break
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

func (p *pgStreamInput) flushBatch(
	ctx context.Context,
	pgStream *pglogicalstream.Stream,
	checkpointer *checkpoint.Capped[*string],
	batch service.MessageBatch,
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
	resolveFn, err := checkpointer.Track(ctx, lsn, int64(len(batch)))
	if err != nil {
		return fmt.Errorf("unable to checkpoint: %w", err)
	}

	// Snapshot batches carry no LSN. Track them so the snapshot->stream handoff
	// can block until they are acknowledged downstream (see the sentinel handling
	// in the read loop).
	isSnapshot := lsn == nil

	ackFn := func(ctx context.Context, _ error) error {
		if isSnapshot {
			defer p.snapshotAckWG.Done()
		}
		maxOffset := resolveFn()
		if maxOffset == nil {
			return nil
		}
		maxLSN := *maxOffset
		if maxLSN == nil {
			return nil
		}
		if err = pgStream.AckLSN(ctx, *maxLSN); err != nil {
			return fmt.Errorf("unable to ack LSN to postgres: %w", err)
		}
		return nil
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
