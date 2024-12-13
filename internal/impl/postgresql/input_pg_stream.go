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
	"encoding/json"
	"fmt"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"
	"github.com/jackc/pgx/v5/pgconn"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream"
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

	shutdownTimeout = 5 * time.Second
)

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

func newPostgresCDCConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.39.0").
		Summary(`Streams changes from a PostgreSQL database using logical replication.`).
		Description(`Streams changes from a PostgreSQL database for Change Data Capture (CDC).
Additionally, if ` + "`" + fieldStreamSnapshot + "`" + ` is set to true, then the existing data in the database is also streamed too.

== Metadata

This input adds the following metadata fields to each message:
- table (Name of the table that the message originated from)
- operation (Type of operation that generated the message: "read", "insert", "update", or "delete". "read" is from messages that are read in the initial snapshot phase. This will also be "begin" and "commit" if ` + "`" + fieldIncludeTxnMarkers + "`" + ` is enabled)
- lsn (the log sequence number in postgres)
		`).
		Field(service.NewStringField(fieldDSN).
			Description("The Data Source Name for the PostgreSQL database in the form of `postgres://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]`. Please note that Postgres enforces SSL by default, you can override this with the parameter `sslmode=disable` if required.").
			Example("postgres://foouser:foopass@localhost:5432/foodb?sslmode=disable")).
		Field(service.NewBoolField(fieldIncludeTxnMarkers).
			Description(`When set to true, empty messages with operation types BEGIN and COMMIT are generated for the beginning and end of each transaction. Messages with operation metadata set to "begin" or "commit" will have null message payloads.`).
			Default(false)).
		Field(service.NewBoolField(fieldStreamSnapshot).
			Description("When set to true, the plugin will first stream a snapshot of all existing data in the database before streaming changes. In order to use this the tables that are being snapshot MUST have a primary key set so that reading from the table can be parallelized.").
			Example(true).
			Default(false)).
		Field(service.NewFloatField(fieldSnapshotMemSafetyFactor).
			Description("Determines the fraction of available memory that can be used for streaming the snapshot. Values between 0 and 1 represent the percentage of memory to use. Lower values make initial streaming slower but help prevent out-of-memory errors.").
			Example(0.2).
			Default(1)).
		Field(service.NewIntField(fieldSnapshotBatchSize).
			Description("The number of rows to fetch in each batch when querying the snapshot. A value of 0 lets the plugin determine the batch size based on `snapshot_memory_safety_factor` property.").
			Example(10000).
			Default(0)).
		Field(service.NewStringField(fieldSchema).
			Description("The PostgreSQL schema from which to replicate data.").
			Examples("public", `"MyCaseSensitiveSchemaNeedingQuotes"`),
		).
		Field(service.NewStringListField(fieldTables).
			Description("A list of table names to include in the logical replication. Each table should be specified as a separate item.").
			Example([]string{"my_table_1", `"MyCaseSensitiveTableNeedingQuotes"`})).
		Field(service.NewIntField(fieldCheckpointLimit).
			Description("The maximum number of messages that can be processed at a given time. Increasing this limit enables parallel processing and batching at the output level. Any given LSN will not be acknowledged unless all messages under that offset are delivered in order to preserve at least once delivery guarantees.").
			Default(1024)).
		Field(service.NewBoolField(fieldTemporarySlot).
			Description("If set to true, creates a temporary replication slot that is automatically dropped when the connection is closed.").
			Default(false)).
		Field(service.NewStringField(fieldSlotName).
			Description("The name of the PostgreSQL logical replication slot to use. If not provided, a random name will be generated. You can create this slot manually before starting replication if desired.").
			Example("my_test_slot").
			Default("")).
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
		snapshotMemSafetyFactor   float64
		snapshotBatchSize         int
		checkpointLimit           int
		walMonitorInterval        time.Duration
		maxParallelSnapshotTables int
		pgStandbyTimeout          time.Duration
		batching                  service.BatchPolicy
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
	// Set the default to be a random string
	if dbSlotName == "" {
		dbSlotName, err = gonanoid.Generate("0123456789ABCDEFGHJKMNPQRSTVWXYZ", 32)
		if err != nil {
			return nil, err
		}
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

	if snapshotMemSafetyFactor, err = conf.FieldFloat(fieldSnapshotMemSafetyFactor); err != nil {
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

	pgConnConfig, err := pgconn.ParseConfigWithOptions(dsn, pgconn.ParseConfigOptions{
		// Don't support dynamic reading of password
		GetSSLPassword: func(context.Context) string { return "" },
	})
	if err != nil {
		return nil, err
	}
	// This is required for postgres to understand we're interested in replication.
	// https://github.com/jackc/pglogrepl/issues/6
	pgConnConfig.RuntimeParams["replication"] = "database"

	snapshotMetrics := mgr.Metrics().NewGauge("postgres_snapshot_progress", "table")
	replicationLag := mgr.Metrics().NewGauge("postgres_replication_lag_bytes")

	i := &pgStreamInput{
		streamConfig: &pglogicalstream.Config{
			DBConfig: pgConnConfig,
			DBRawDSN: dsn,
			DBSchema: schema,
			DBTables: tables,

			IncludeTxnMarkers:          includeTxnMarkers,
			ReplicationSlotName:        "rs_" + dbSlotName,
			BatchSize:                  snapshotBatchSize,
			StreamOldData:              streamSnapshot,
			TemporaryReplicationSlot:   temporarySlot,
			SnapshotMemorySafetyFactor: snapshotMemSafetyFactor,
			PgStandbyTimeout:           pgStandbyTimeout,
			WalMonitorInterval:         walMonitorInterval,
			MaxParallelSnapshotTables:  maxParallelSnapshotTables,
			Logger:                     mgr.Logger(),
		},
		batching:        batching,
		checkpointLimit: checkpointLimit,
		msgChan:         make(chan asyncMessage),

		mgr:             mgr,
		logger:          mgr.Logger(),
		snapshotMetrics: snapshotMetrics,
		replicationLag:  replicationLag,
		stopSig:         shutdown.NewSignaller(),
	}

	// Has stopped is how we notify that we're not connected. This will get reset at connection time.
	i.stopSig.TriggerHasStopped()

	r, err := service.AutoRetryNacksBatchedToggled(conf, i)
	if err != nil {
		return nil, err
	}

	return conf.WrapBatchInputExtractTracingSpanMapping("postgres_cdc", r)
}

// validateSimpleString ensures we aren't vuln to SQL injection
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
	err := service.RegisterBatchInput("postgres_cdc", newPostgresCDCConfig(), newPgStreamInput)
	if err != nil {
		panic(err)
	}
	// Legacy naming
	err = service.RegisterBatchInput("pg_stream", newPostgresCDCConfig().Deprecated(), newPgStreamInput)
	if err != nil {
		panic(err)
	}
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
	stopSig         *shutdown.Signaller
}

func (p *pgStreamInput) Connect(ctx context.Context) error {
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
	ctx, _ := p.stopSig.SoftStopCtx(context.Background())
	defer func() {
		ctx, _ := p.stopSig.HardStopCtx(context.Background())
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
		case message := <-pgStream.Messages():
			var (
				mb  []byte
				err error
			)
			if mb, err = json.Marshal(message.Data); err != nil {
				p.logger.Errorf("failure to marshal message: %s", err)
				break
			}
			batchMsg := service.NewMessage(mb)
			batchMsg.MetaSet("table", message.Table)
			batchMsg.MetaSet("operation", string(message.Operation))
			if message.LSN != nil {
				batchMsg.MetaSet("lsn", *message.LSN)
			}
			if batcher.Add(batchMsg) {
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

	ackFn := func(ctx context.Context, res error) error {
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
	select {
	case p.msgChan <- asyncMessage{msg: batch, ackFn: ackFn}:
	case <-ctx.Done():
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
