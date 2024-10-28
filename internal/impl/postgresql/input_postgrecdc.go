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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lucasepe/codename"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream"
)

var randomSlotName string

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

var pgStreamConfigSpec = service.NewConfigSpec().
	Beta().
	Categories("Services").
	Version("0.0.1").
	Summary(`Creates a PostgreSQL replication slot for Change Data Capture (CDC)
		== Metadata

This input adds the following metadata fields to each message:
- streaming (Indicates whether the message is part of a streaming operation or snapshot processing)
- table (Name of the table that the message originated from)
- operation (Type of operation that generated the message, such as INSERT, UPDATE, or DELETE)
		`).
	Field(service.NewStringField("host").
		Description("The hostname or IP address of the PostgreSQL instance.").
		Example("123.0.0.1")).
	Field(service.NewIntField("port").
		Description("The port number on which the PostgreSQL instance is listening.").
		Example(5432).
		Default(5432)).
	Field(service.NewStringField("user").
		Description("Username of a user with replication permissions. For AWS RDS, this typically requires superuser privileges.").
		Example("postgres"),
	).
	Field(service.NewStringField("password").
		Description("Password for the specified PostgreSQL user.")).
	Field(service.NewStringField("schema").
		Description("The PostgreSQL schema from which to replicate data.")).
	Field(service.NewStringField("database").
		Description("The name of the PostgreSQL database to connect to.")).
	Field(service.NewStringEnumField("tls", "require", "none").
		Description("Specifies whether to use TLS for the database connection. Set to 'require' to enforce TLS, or 'none' to disable it.").
		Example("none").
		Default("none")).
	Field(service.NewBoolField("stream_uncomited").
		Description("If set to true, the plugin will stream uncommitted transactions before receiving a commit message from PostgreSQL. This may result in duplicate records if the connector is restarted.").
		Default(false)).
	Field(service.NewStringField("pg_conn_options").
		Description("Additional PostgreSQL connection options as a string. Refer to PostgreSQL documentation for available options.").
		Default(""),
	).
	Field(service.NewBoolField("stream_snapshot").
		Description("When set to true, the plugin will first stream a snapshot of all existing data in the database before streaming changes.").
		Example(true).
		Default(false)).
	Field(service.NewFloatField("snapshot_memory_safety_factor").
		Description("Determines the fraction of available memory that can be used for streaming the snapshot. Values between 0 and 1 represent the percentage of memory to use. Lower values make initial streaming slower but help prevent out-of-memory errors.").
		Example(0.2).
		Default(1)).
	Field(service.NewIntField("snapshot_batch_size").
		Description("The number of rows to fetch in each batch when querying the snapshot. A value of 0 lets the plugin determine the batch size based on `snapshot_memory_safety_factor` property.").
		Example(10000).
		Default(0)).
	Field(service.NewStringEnumField("decoding_plugin", "pgoutput", "wal2json").
		Description("Specifies the logical decoding plugin to use for streaming changes from PostgreSQL. 'pgoutput' is the native logical replication protocol, while 'wal2json' provides change data as JSON.").
		Example("pgoutput").
		Default("pgoutput")).
	Field(service.NewStringListField("tables").
		Description("A list of table names to include in the logical replication. Each table should be specified as a separate item.").
		Example(`
			- my_table
			- my_table_2
		`)).
	Field(service.NewIntField("checkpoint_limit").
		Description("The maximum number of messages of the same topic and partition that can be processed at a given time. Increasing this limit enables parallel processing and batching at the output level to work on individual partitions. Any given offset will not be committed unless all messages under that offset are delivered in order to preserve at least once delivery guarantees.").
		Version("3.33.0").Default(1024)).
	Field(service.NewBoolField("temporary_slot").
		Description("If set to true, creates a temporary replication slot that is automatically dropped when the connection is closed.").
		Default(false)).
	Field(service.NewStringField("slot_name").
		Description("The name of the PostgreSQL logical replication slot to use. If not provided, a random name will be generated. You can create this slot manually before starting replication if desired.").
		Example("my_test_slot").
		Default(randomSlotName)).
	Field(service.NewAutoRetryNacksToggleField()).
	Field(service.NewBatchPolicyField("batching").Advanced())

func newPgStreamInput(conf *service.ParsedConfig, mgr *service.Resources) (s service.BatchInput, err error) {
	var (
		dbName                  string
		dbPort                  int
		dbHost                  string
		dbSchema                string
		dbUser                  string
		dbPassword              string
		dbSlotName              string
		temporarySlot           bool
		tlsSetting              string
		tables                  []string
		streamSnapshot          bool
		snapshotMemSafetyFactor float64
		decodingPlugin          string
		pgConnOptions           string
		streamUncomited         bool
		snapshotBatchSize       int
		checkpointLimit         int
		batching                service.BatchPolicy
	)

	dbSchema, err = conf.FieldString("schema")
	if err != nil {
		return nil, err
	}

	dbSlotName, err = conf.FieldString("slot_name")
	if err != nil {
		return nil, err
	}

	temporarySlot, err = conf.FieldBool("temporary_slot")
	if err != nil {
		return nil, err
	}

	if dbSlotName == "" {
		dbSlotName = randomSlotName
	}

	dbPassword, err = conf.FieldString("password")
	if err != nil {
		return nil, err
	}

	dbUser, err = conf.FieldString("user")
	if err != nil {
		return nil, err
	}

	tlsSetting, err = conf.FieldString("tls")
	if err != nil {
		return nil, err
	}

	dbName, err = conf.FieldString("database")
	if err != nil {
		return nil, err
	}

	dbHost, err = conf.FieldString("host")
	if err != nil {
		return nil, err
	}

	dbPort, err = conf.FieldInt("port")
	if err != nil {
		return nil, err
	}

	tables, err = conf.FieldStringList("tables")
	if err != nil {
		return nil, err
	}

	if checkpointLimit, err = conf.FieldInt("checkpoint_limit"); err != nil {
		return nil, err
	}

	streamSnapshot, err = conf.FieldBool("stream_snapshot")
	if err != nil {
		return nil, err
	}

	streamUncomited, err = conf.FieldBool("stream_uncomited")
	if err != nil {
		return nil, err
	}

	decodingPlugin, err = conf.FieldString("decoding_plugin")
	if err != nil {
		return nil, err
	}

	snapshotMemSafetyFactor, err = conf.FieldFloat("snapshot_memory_safety_factor")
	if err != nil {
		return nil, err
	}

	snapshotBatchSize, err = conf.FieldInt("snapshot_batch_size")
	if err != nil {
		return nil, err
	}

	if pgConnOptions, err = conf.FieldString("pg_conn_options"); err != nil {
		return nil, err
	}

	if pgConnOptions != "" {
		pgConnOptions = "options=" + pgConnOptions
	}

	if batching, err = conf.FieldBatchPolicy("batching"); err != nil {
		return nil, err
	} else if batching.IsNoop() {
		batching.Count = 1
	}

	pgconnConfig := pgconn.Config{
		Host:     dbHost,
		Port:     uint16(dbPort),
		Database: dbName,
		User:     dbUser,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		Password: dbPassword,
	}

	if tlsSetting == "none" {
		pgconnConfig.TLSConfig = nil
	}

	snapsotMetrics := mgr.Metrics().NewGauge("snapshot_progress", "table")
	replicationLag := mgr.Metrics().NewGauge("replication_lag_bytes")
	snapshotMessageRate := mgr.Metrics().NewGauge("snapshot_message_rate")
	snapshotRateCounter := NewRateCounter()

	i := &pgStreamInput{
		dbConfig:                pgconnConfig,
		streamSnapshot:          streamSnapshot,
		snapshotMemSafetyFactor: snapshotMemSafetyFactor,
		slotName:                dbSlotName,
		schema:                  dbSchema,
		pgConnRuntimeParam:      pgConnOptions,
		tls:                     pglogicalstream.TLSVerify(tlsSetting),
		tables:                  tables,
		decodingPlugin:          decodingPlugin,
		streamUncomited:         streamUncomited,
		temporarySlot:           temporarySlot,
		snapshotBatchSize:       snapshotBatchSize,
		snapshotMessageRate:     snapshotMessageRate,
		snapshotRateCounter:     snapshotRateCounter,
		batching:                batching,
		checkpointLimit:         checkpointLimit,
		cMut:                    sync.Mutex{},
		msgChan:                 make(chan asyncMessage),

		mgr:             mgr,
		logger:          mgr.Logger(),
		metrics:         mgr.Metrics(),
		snapshotMetrics: snapsotMetrics,
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

func init() {
	rng, _ := codename.DefaultRNG()
	randomSlotName = strings.ReplaceAll(codename.Generate(rng, 5), "-", "_")

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
	dbConfig                pgconn.Config
	tls                     pglogicalstream.TLSVerify
	pglogicalStream         *pglogicalstream.Stream
	pgConnRuntimeParam      string
	slotName                string
	temporarySlot           bool
	schema                  string
	tables                  []string
	decodingPlugin          string
	streamSnapshot          bool
	snapshotMemSafetyFactor float64
	snapshotBatchSize       int
	streamUncomited         bool
	logger                  *service.Logger
	mgr                     *service.Resources
	metrics                 *service.Metrics
	cMut                    sync.Mutex
	msgChan                 chan asyncMessage
	batching                service.BatchPolicy
	checkpointLimit         int

	snapshotRateCounter *RateCounter
	snapshotMessageRate *service.MetricGauge
	snapshotMetrics     *service.MetricGauge
	replicationLag      *service.MetricGauge

	pendingTrx     *string
	releaseTrxChan chan bool
	inTxState      atomic.Bool
}

func (p *pgStreamInput) Connect(ctx context.Context) error {
	pgStream, err := pglogicalstream.NewPgStream(pglogicalstream.Config{
		PgConnRuntimeParam:       p.pgConnRuntimeParam,
		DBHost:                   p.dbConfig.Host,
		DBPassword:               p.dbConfig.Password,
		DBUser:                   p.dbConfig.User,
		DBPort:                   int(p.dbConfig.Port),
		DBTables:                 p.tables,
		DBName:                   p.dbConfig.Database,
		DBSchema:                 p.schema,
		ReplicationSlotName:      "rs_" + p.slotName,
		TLSVerify:                p.tls,
		BatchSize:                p.snapshotBatchSize,
		StreamOldData:            p.streamSnapshot,
		TemporaryReplicationSlot: p.temporarySlot,
		StreamUncomited:          p.streamUncomited,
		DecodingPlugin:           p.decodingPlugin,

		SnapshotMemorySafetyFactor: p.snapshotMemSafetyFactor,
	})
	if err != nil {
		return err
	}

	p.pglogicalStream = pgStream

	go func() {
		batchPolicy, err := p.batching.NewBatcher(p.mgr)
		if err != nil {
			p.logger.Errorf("Failed to initialise batch policy: %v, falling back to no policy.\n", err)
			conf := service.BatchPolicy{Count: 1}
			if batchPolicy, err = conf.NewBatcher(p.mgr); err != nil {
				panic(err)
			}
		}

		defer func() {
			batchPolicy.Close(context.Background())
		}()

		var nextTimedBatchChan <-chan time.Time
		var flushBatch func(context.Context, chan<- asyncMessage, service.MessageBatch, *int64, *chan bool) bool
		flushBatch = p.asyncCheckpointer()

		// offsets are nilable since we don't provide offset tracking during the snapshot phase
		var latestOffset *int64

		for {
			select {
			case <-nextTimedBatchChan:
				nextTimedBatchChan = nil
				flushedBatch, err := batchPolicy.Flush(ctx)
				if err != nil {
					p.mgr.Logger().Debugf("Timed flush batch error: %w", err)
					break
				}

				if !flushBatch(ctx, p.msgChan, flushedBatch, latestOffset, nil) {
					break
				}
			case _, open := <-p.pglogicalStream.TxBeginChan():
				if !open {
					p.logger.Debugf("TxBeginChan closed, exiting...")
					break
				}

				p.logger.Debugf("Entering transaction state. Stop messages from ack until we receive commit message...")
				p.inTxState.Store(true)

				// TrxCommit LSN must be acked when all the bessages in the batch are processed
			case trxCommitLsn, open := <-p.pglogicalStream.AckTxChan():
				if !open {
					break
				}

				p.cMut.Lock()
				p.cMut.Unlock()

				flushedBatch, err := batchPolicy.Flush(ctx)
				if err != nil {
					p.mgr.Logger().Debugf("Flush batch error: %w", err)
					break
				}

				callbackChan := make(chan bool)
				if !flushBatch(ctx, p.msgChan, flushedBatch, latestOffset, &callbackChan) {
					break
				}

				<-callbackChan
				p.pglogicalStream.AckLSN(trxCommitLsn)
				p.pglogicalStream.ConsumedCallback() <- true

			case message, open := <-p.pglogicalStream.Messages():
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
				if mb, err = json.Marshal(message); err != nil {
					break
				}

				batchMsg := service.NewMessage(mb)

				streaming := strconv.FormatBool(message.IsStreaming)
				batchMsg.MetaSet("streaming", streaming)
				batchMsg.MetaSet("table", message.Changes[0].Table)
				batchMsg.MetaSet("operation", message.Changes[0].Operation)
				if message.Changes[0].TableSnapshotProgress != nil {
					p.snapshotMetrics.SetFloat64(*message.Changes[0].TableSnapshotProgress, message.Changes[0].Table)
				}
				if message.WALLagBytes != nil {
					p.replicationLag.Set(*message.WALLagBytes)
				}

				p.snapshotRateCounter.Increment()
				p.snapshotMessageRate.SetFloat64(p.snapshotRateCounter.Rate())

				if batchPolicy.Add(batchMsg) {
					nextTimedBatchChan = nil
					flushedBatch, err := batchPolicy.Flush(ctx)
					if err != nil {
						p.mgr.Logger().Debugf("Flush batch error: %w", err)
						break
					}
					if message.IsStreaming {
						callbackChan := make(chan bool)
						if !flushBatch(ctx, p.msgChan, flushedBatch, latestOffset, &callbackChan) {
							break
						}
						<-callbackChan
					} else {
						if !flushBatch(ctx, p.msgChan, flushedBatch, latestOffset, nil) {
							break
						}
					}

				}
			case <-ctx.Done():
				p.pglogicalStream.Stop()
			}
		}
	}()

	return err
}

func (p *pgStreamInput) asyncCheckpointer() func(context.Context, chan<- asyncMessage, service.MessageBatch, *int64, *chan bool) bool {
	cp := checkpoint.NewCapped[*int64](int64(p.checkpointLimit))
	return func(ctx context.Context, c chan<- asyncMessage, msg service.MessageBatch, lsn *int64, txCommitConfirmChan *chan bool) bool {
		if msg == nil {
			if txCommitConfirmChan != nil {
				go func() {
					*txCommitConfirmChan <- true
				}()
			}
			return true
		}

		resolveFn, err := cp.Track(ctx, lsn, int64(len(msg)))
		if err != nil {
			if ctx.Err() == nil {
				p.mgr.Logger().Errorf("Failed to checkpoint offset: %v\n", err)
			}
			return false
		}

		select {
		case c <- asyncMessage{
			msg: msg,
			ackFn: func(ctx context.Context, res error) error {
				maxOffset := resolveFn()
				if maxOffset == nil {
					return nil
				}
				p.cMut.Lock()
				if lsn != nil {
					p.pglogicalStream.AckLSN(Int64ToLSN(*lsn))
					if txCommitConfirmChan != nil {
						*txCommitConfirmChan <- true
					}
				}
				p.cMut.Unlock()
				return nil
			},
		}:
		case <-ctx.Done():
			return false
		}

		return true
	}
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
	if p.pglogicalStream != nil {
		return p.pglogicalStream.Stop()
	}
	return nil
}
