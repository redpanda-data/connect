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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream"
)

const (
	fieldDSN                     = "dsn"
	fieldStreamUncommitted       = "stream_uncommitted"
	fieldStreamSnapshot          = "stream_snapshot"
	fieldSnapshotMemSafetyFactor = "snapshot_memory_safety_factor"
	fieldSnapshotBatchSize       = "snapshot_batch_size"
	fieldDecodingPlugin          = "decoding_plugin"
	fieldSchema                  = "schema"
	fieldTables                  = "tables"
	fieldCheckpointLimit         = "checkpoint_limit"
	fieldTemporarySlot           = "temporary_slot"
	fieldPgStandbyTimeout        = "pg_standby_timeout_sec"
	fieldWalMonitorIntervalSec   = "pg_wal_monitor_interval_sec"
	fieldSlotName                = "slot_name"
	fieldBatching                = "batching"
)

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

var pgStreamConfigSpec = service.NewConfigSpec().
	Beta().
	Categories("Services").
	Version("4.39.0").
	Summary(`Streams changes from a PostgreSQL database using logical replication.`).
	Description(`Streams changes from a PostgreSQL database for Change Data Capture (CDC).
Additionally, if ` + "`" + fieldStreamSnapshot + "`" + ` is set to true, then the existing data in the database is also streamed too.

== Metadata

This input adds the following metadata fields to each message:
- mode (Either "streaming" or "snapshot" indicating whether the message is part of a streaming operation or snapshot processing)
- table (Name of the table that the message originated from)
- operation (Type of operation that generated the message, such as INSERT, UPDATE, or DELETE)
		`).
	Field(service.NewStringField(fieldDSN).
		Description("The Data Source Name for the PostgreSQL database in the form of `postgres://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]`. Please note that Postgres enforces SSL by default, you can override this with the parameter `sslmode=disable` if required.").
		Example("postgres://foouser:foopass@localhost:5432/foodb?sslmode=disable")).
	Field(service.NewBoolField(fieldStreamUncommitted).
		Description("If set to true, the plugin will stream uncommitted transactions before receiving a commit message from PostgreSQL. This may result in duplicate records if the connector is restarted.").
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
	Field(service.NewStringEnumField(fieldDecodingPlugin, "pgoutput", "wal2json").
		Description(`Specifies the logical decoding plugin to use for streaming changes from PostgreSQL. 'pgoutput' is the native logical replication protocol, while 'wal2json' provides change data as JSON.
		Important: No matter which plugin you choose, the data will be converted to JSON before sending it to Connect.
		`).
		Example("pgoutput").
		Default("pgoutput")).
	Field(service.NewStringField(fieldSchema).
		Description("The PostgreSQL schema from which to replicate data.").
		Example("public")).
	Field(service.NewStringListField(fieldTables).
		Description("A list of table names to include in the logical replication. Each table should be specified as a separate item.").
		Example(`
			- my_table
			- my_table_2
		`)).
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
	Field(service.NewIntField(fieldPgStandbyTimeout).
		Description("Int field that specifies default standby timeout for PostgreSQL replication connection").
		Example(10).
		Default(10)).
	Field(service.NewIntField(fieldWalMonitorIntervalSec).
		Description("Int field stat specifies ticker interval for WAL monitoring. Used to fetch replication slot lag").
		Example(3).
		Default(3)).
	Field(service.NewAutoRetryNacksToggleField()).
	Field(service.NewBatchPolicyField(fieldBatching))

func newPgStreamInput(conf *service.ParsedConfig, mgr *service.Resources) (s service.BatchInput, err error) {
	var (
		dsn                     string
		dbSlotName              string
		temporarySlot           bool
		schema                  string
		tables                  []string
		streamSnapshot          bool
		snapshotMemSafetyFactor float64
		decodingPlugin          string
		streamUncommitted       bool
		snapshotBatchSize       int
		checkpointLimit         int
		walMonitorIntervalSec   int
		pgStandbyTimeoutSec     int
		batching                service.BatchPolicy
	)

	dsn, err = conf.FieldString(fieldDSN)
	if err != nil {
		return nil, err
	}

	dbSlotName, err = conf.FieldString(fieldSlotName)
	if err != nil {
		return nil, err
	}
	// Set the default to be a random string
	if dbSlotName == "" {
		dbSlotName = uuid.NewString()
	}

	if err := validateSimpleString(dbSlotName); err != nil {
		return nil, fmt.Errorf("invalid slot_name: %w", err)
	}

	temporarySlot, err = conf.FieldBool(fieldTemporarySlot)
	if err != nil {
		return nil, err
	}

	schema, err = conf.FieldString(fieldSchema)
	if err != nil {
		return nil, err
	}

	tables, err = conf.FieldStringList(fieldTables)
	if err != nil {
		return nil, err
	}

	if checkpointLimit, err = conf.FieldInt(fieldCheckpointLimit); err != nil {
		return nil, err
	}

	streamSnapshot, err = conf.FieldBool(fieldStreamSnapshot)
	if err != nil {
		return nil, err
	}

	streamUncommitted, err = conf.FieldBool(fieldStreamUncommitted)
	if err != nil {
		return nil, err
	}

	decodingPlugin, err = conf.FieldString(fieldDecodingPlugin)
	if err != nil {
		return nil, err
	}

	snapshotMemSafetyFactor, err = conf.FieldFloat(fieldSnapshotMemSafetyFactor)
	if err != nil {
		return nil, err
	}

	snapshotBatchSize, err = conf.FieldInt(fieldSnapshotBatchSize)
	if err != nil {
		return nil, err
	}

	if batching, err = conf.FieldBatchPolicy(fieldBatching); err != nil {
		return nil, err
	} else if batching.IsNoop() {
		batching.Count = 1
	}

	pgStandbyTimeoutSec, err = conf.FieldInt(fieldPgStandbyTimeout)
	if err != nil {
		return nil, err
	}

	walMonitorIntervalSec, err = conf.FieldInt(fieldWalMonitorIntervalSec)
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
	// This is required for postgres to understand we're interested in replication.
	// https://github.com/jackc/pglogrepl/issues/6
	pgConnConfig.RuntimeParams["replication"] = "database"

	snapshotMetrics := mgr.Metrics().NewGauge("snapshot_progress", "table")
	replicationLag := mgr.Metrics().NewGauge("replication_lag_bytes")

	i := &pgStreamInput{
		dbConfig: pgConnConfig,
		// dbRawDSN is used for creating golang PG Connection
		// as using pgconn.Config for golang doesn't support multiple queries in the prepared statement for Postgres Version <= 14
		dbRawDSN:                dsn,
		streamSnapshot:          streamSnapshot,
		snapshotMemSafetyFactor: snapshotMemSafetyFactor,
		slotName:                dbSlotName,
		schema:                  schema,
		tables:                  tables,
		decodingPlugin:          decodingPlugin,
		streamUncommitted:       streamUncommitted,
		temporarySlot:           temporarySlot,
		snapshotBatchSize:       snapshotBatchSize,
		batching:                batching,
		checkpointLimit:         checkpointLimit,
		pgStandbyTimeoutSec:     pgStandbyTimeoutSec,
		walMonitorIntervalSec:   walMonitorIntervalSec,
		cMut:                    sync.Mutex{},
		msgChan:                 make(chan asyncMessage),

		mgr:             mgr,
		logger:          mgr.Logger(),
		snapshotMetrics: snapshotMetrics,
		replicationLag:  replicationLag,
		inTxState:       atomic.Bool{},
		releaseTrxChan:  make(chan bool),
	}

	r, err := service.AutoRetryNacksBatchedToggled(conf, i)
	if err != nil {
		return nil, err
	}

	return conf.WrapBatchInputExtractTracingSpanMapping("pg_stream", r)
}

// validateSimpleString ensures we aren't vuln to SQL injection
func validateSimpleString(s string) error {
	for _, b := range []byte(s) {
		isDigit := b >= '0' && b <= '9'
		isLower := b >= 'a' && b <= 'z'
		isUpper := b >= 'A' && b <= 'Z'
		isDelimiter := b == '_' || b == '-'
		if !isDigit && !isLower && !isUpper && !isDelimiter {
			return fmt.Errorf("invalid postgres identifier %q", s)
		}
	}
	// See: https://github.com/jackc/pgx/security/advisories/GHSA-m7wr-2xf7-cm9p
	if strings.Contains(s, "--") {
		return fmt.Errorf("invalid postgres identifier %q", s)
	}
	return nil
}

func init() {
	err := service.RegisterBatchInput(
		"pg_stream", pgStreamConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newPgStreamInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type pgStreamInput struct {
	dbConfig                *pgconn.Config
	dbRawDSN                string
	pgLogicalStream         *pglogicalstream.Stream
	slotName                string
	pgStandbyTimeoutSec     int
	walMonitorIntervalSec   int
	temporarySlot           bool
	schema                  string
	tables                  []string
	decodingPlugin          string
	streamSnapshot          bool
	snapshotMemSafetyFactor float64
	snapshotBatchSize       int
	streamUncommitted       bool
	logger                  *service.Logger
	mgr                     *service.Resources
	cMut                    sync.Mutex
	msgChan                 chan asyncMessage
	batching                service.BatchPolicy
	checkpointLimit         int

	snapshotMetrics *service.MetricGauge
	replicationLag  *service.MetricGauge

	releaseTrxChan chan bool
	inTxState      atomic.Bool
}

func (p *pgStreamInput) Connect(ctx context.Context) error {
	pgStream, err := pglogicalstream.NewPgStream(ctx, &pglogicalstream.Config{
		DBConfig: p.dbConfig,
		DBRawDSN: p.dbRawDSN,
		DBSchema: p.schema,
		DBTables: p.tables,

		ReplicationSlotName:        "rs_" + p.slotName,
		BatchSize:                  p.snapshotBatchSize,
		StreamOldData:              p.streamSnapshot,
		TemporaryReplicationSlot:   p.temporarySlot,
		StreamUncommitted:          p.streamUncommitted,
		DecodingPlugin:             p.decodingPlugin,
		SnapshotMemorySafetyFactor: p.snapshotMemSafetyFactor,
		PgStandbyTimeoutSec:        p.pgStandbyTimeoutSec,
		WalMonitorIntervalSec:      p.walMonitorIntervalSec,
		Logger:                     p.logger,
	})
	if err != nil {
		return fmt.Errorf("unable to create replication stream: %w", err)
	}

	p.pgLogicalStream = pgStream
	batchPolicy, err := p.batching.NewBatcher(p.mgr)
	if err != nil {
		return err
	}
	go func() {
		defer func() {
			batchPolicy.Close(context.Background())
		}()

		var nextTimedBatchChan <-chan time.Time

		// offsets are nilable since we don't provide offset tracking during the snapshot phase
		var latestOffset *int64
		cp := checkpoint.NewCapped[*int64](int64(p.checkpointLimit))
		for {
			select {
			case <-nextTimedBatchChan:
				nextTimedBatchChan = nil
				flushedBatch, err := batchPolicy.Flush(ctx)
				if err != nil {
					p.logger.Debugf("Timed flush batch error: %w", err)
					break
				}

				if err := p.flushBatch(ctx, cp, flushedBatch, latestOffset, false); err != nil {
					break
				}

				// TrxCommit LSN must be acked when all the messages in the batch are processed
			case trxCommitLsn, open := <-p.pgLogicalStream.AckTxChan():
				if !open {
					break
				}

				flushedBatch, err := batchPolicy.Flush(ctx)
				if err != nil {
					p.logger.Debugf("Flush batch error: %w", err)
					break
				}

				if err = p.flushBatch(ctx, cp, flushedBatch, latestOffset, true); err != nil {
					break
				}

				if err = p.pgLogicalStream.AckLSN(trxCommitLsn); err != nil {
					p.logger.Errorf("Failed to ack LSN: %v", err)
					break
				}

				p.pgLogicalStream.ConsumedCallback() <- true

			case message, open := <-p.pgLogicalStream.Messages():
				if !open {
					break
				}
				var (
					mb  []byte
					err error
				)
				if message.Lsn != nil {
					parsedLSN, err := LSNToInt64(*message.Lsn)
					if err != nil {
						p.logger.Errorf("Failed to parse LSN: %v", err)
						break
					}
					latestOffset = &parsedLSN
				}

				if len(message.Changes) == 0 {
					p.logger.Debugf("Received empty message on LSN: %v", message.Lsn)
					continue
				}

				if mb, err = json.Marshal(message); err != nil {
					break
				}

				batchMsg := service.NewMessage(mb)

				batchMsg.MetaSet("mode", string(message.Mode))
				batchMsg.MetaSet("table", message.Changes[0].Table)
				batchMsg.MetaSet("operation", message.Changes[0].Operation)
				if message.Changes[0].TableSnapshotProgress != nil {
					p.snapshotMetrics.SetFloat64(*message.Changes[0].TableSnapshotProgress, message.Changes[0].Table)
				}
				if message.WALLagBytes != nil {
					p.replicationLag.Set(*message.WALLagBytes)
				}

				if batchPolicy.Add(batchMsg) {
					nextTimedBatchChan = nil
					flushedBatch, err := batchPolicy.Flush(ctx)
					if err != nil {
						p.logger.Debugf("Flush batch error: %w", err)
						break
					}
					waitForCommit := message.Mode == pglogicalstream.StreamModeStreaming
					if err := p.flushBatch(ctx, cp, flushedBatch, latestOffset, waitForCommit); err != nil {
						break
					}
				}
			case <-ctx.Done():
				if err = p.pgLogicalStream.Stop(); err != nil {
					p.logger.Errorf("Failed to stop pglogical stream: %v", err)
				}
				return
			}
		}
	}()

	return err
}

func (p *pgStreamInput) flushBatch(ctx context.Context, checkpointer *checkpoint.Capped[*int64], msg service.MessageBatch, lsn *int64, waitForCommit bool) error {
	if msg == nil {
		return nil
	}

	resolveFn, err := checkpointer.Track(ctx, lsn, int64(len(msg)))
	if err != nil {
		if ctx.Err() == nil {
			p.mgr.Logger().Errorf("Failed to checkpoint offset: %v\n", err)
		}
		return err
	}

	var wg sync.WaitGroup
	if waitForCommit {
		wg.Add(1)
	}
	ackFn := func(ctx context.Context, res error) error {
		if waitForCommit {
			defer wg.Done()
		}
		maxOffset := resolveFn()
		if maxOffset == nil {
			return nil
		}
		p.cMut.Lock()
		defer p.cMut.Unlock()
		if lsn == nil {
			return nil
		}
		if err = p.pgLogicalStream.AckLSN(Int64ToLSN(*lsn)); err != nil {
			return err
		}
		return nil
	}
	select {
	case p.msgChan <- asyncMessage{msg: msg, ackFn: ackFn}:
	case <-ctx.Done():
		return ctx.Err()
	}
	wg.Wait() // Noop if !waitForCommit
	return nil
}

func (p *pgStreamInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	p.cMut.Lock()
	msgChan := p.msgChan
	p.cMut.Unlock()
	if msgChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case m, open := <-msgChan:
		if !open {
			return nil, nil, service.ErrNotConnected
		}
		return m.msg, m.ackFn, nil
	case <-ctx.Done():

	}

	return nil, nil, ctx.Err()
}

func (p *pgStreamInput) Close(ctx context.Context) error {
	if p.pgLogicalStream != nil {
		return p.pgLogicalStream.Stop()
	}
	return nil
}
