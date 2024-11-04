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
	fieldHost                    = "host"
	fieldPort                    = "port"
	fieldUser                    = "user"
	fieldPass                    = "password"
	fieldSchema                  = "schema"
	fieldDatabase                = "database"
	fieldTLS                     = "tls"
	fieldStreamUncommitted       = "stream_uncomitted"
	fieldPgConnOptions           = "pg_conn_options"
	fieldStreamSnapshot          = "stream_snapshot"
	fieldSnapshotMemSafetyFactor = "snapshot_memory_safety_factor"
	fieldSnapshotBatchSize       = "snapshot_batch_size"
	fieldDecodingPlugin          = "decoding_plugin"
	fieldTables                  = "tables"
	fieldCheckpointLimit         = "checkpoint_limit"
	fieldTemporarySlot           = "temporary_slot"
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
- streaming (Boolean indicating whether the message is part of a streaming operation or snapshot processing)
- table (Name of the table that the message originated from)
- operation (Type of operation that generated the message, such as INSERT, UPDATE, or DELETE)
		`).
	Field(service.NewStringField(fieldHost).
		Description("The hostname or IP address of the PostgreSQL instance.").
		Example("123.0.0.1")).
	Field(service.NewIntField(fieldPort).
		Description("The port number on which the PostgreSQL instance is listening.").
		Example(5432).
		Default(5432)).
	Field(service.NewStringField(fieldUser).
		Description("Username of a user with replication permissions. For AWS RDS, this typically requires superuser privileges.").
		Example("postgres"),
	).
	Field(service.NewStringField(fieldPass).
		Description("Password for the specified PostgreSQL user.")).
	Field(service.NewStringField(fieldSchema).
		Description("The PostgreSQL schema from which to replicate data.")).
	Field(service.NewStringField(fieldDatabase).
		Description("The name of the PostgreSQL database to connect to.")).
	Field(service.NewTLSToggledField(fieldTLS).
		Description("Specifies whether to use TLS for the database connection. Set to 'require' to enforce TLS, or 'none' to disable it.").
		Default(nil)).
	Field(service.NewBoolField(fieldStreamUncommitted).
		Description("If set to true, the plugin will stream uncommitted transactions before receiving a commit message from PostgreSQL. This may result in duplicate records if the connector is restarted.").
		Default(false)).
	Field(service.NewStringField(fieldPgConnOptions).
		Description("Additional PostgreSQL connection options as a string. Refer to PostgreSQL documentation for available options.").
		Default(""),
	).
	Field(service.NewBoolField(fieldStreamSnapshot).
		Description("When set to true, the plugin will first stream a snapshot of all existing data in the database before streaming changes.").
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
		Important: No matter which plugin you choose, the data will be converted to JSON before sending it to Benthos.
		`).
		Example("pgoutput").
		Default("pgoutput")).
	Field(service.NewStringListField(fieldTables).
		Description("A list of table names to include in the logical replication. Each table should be specified as a separate item.").
		Example(`
			- my_table
			- my_table_2
		`)).
	Field(service.NewIntField(fieldCheckpointLimit).
		Description("The maximum number of messages that can be processed at a given time. Increasing this limit enables parallel processing and batching at the output level. Any given LSN will not be acknowledged unless all messages under that offset are delivered in order to preserve at least once delivery guarantees.").
		Version("3.33.0").Default(1024)).
	Field(service.NewBoolField(fieldTemporarySlot).
		Description("If set to true, creates a temporary replication slot that is automatically dropped when the connection is closed.").
		Default(false)).
	Field(service.NewStringField(fieldSlotName).
		Description("The name of the PostgreSQL logical replication slot to use. If not provided, a random name will be generated. You can create this slot manually before starting replication if desired.").
		Example("my_test_slot").
		Default("")).
	Field(service.NewAutoRetryNacksToggleField()).
	Field(service.NewBatchPolicyField(fieldBatching))

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

	dbSchema, err = conf.FieldString(fieldSchema)
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

	temporarySlot, err = conf.FieldBool(fieldTemporarySlot)
	if err != nil {
		return nil, err
	}

	dbPassword, err = conf.FieldString(fieldPass)
	if err != nil {
		return nil, err
	}

	dbUser, err = conf.FieldString(fieldUser)
	if err != nil {
		return nil, err
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled(fieldTLS)
	if err != nil {
		return nil, err
	}

	dbName, err = conf.FieldString(fieldDatabase)
	if err != nil {
		return nil, err
	}

	dbHost, err = conf.FieldString(fieldHost)
	if err != nil {
		return nil, err
	}

	dbPort, err = conf.FieldInt(fieldPort)
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

	streamUncomited, err = conf.FieldBool(fieldStreamUncommitted)
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

	if pgConnOptions, err = conf.FieldString(fieldPgConnOptions); err != nil {
		return nil, err
	}

	if pgConnOptions != "" {
		pgConnOptions = "options=" + pgConnOptions
	}

	if batching, err = conf.FieldBatchPolicy(fieldBatching); err != nil {
		return nil, err
	} else if batching.IsNoop() {
		batching.Count = 1
	}

	pgconnConfig := pgconn.Config{
		Host:      dbHost,
		Port:      uint16(dbPort),
		Database:  dbName,
		User:      dbUser,
		TLSConfig: tlsConf,
		Password:  dbPassword,
	}

	if !tlsEnabled {
		pgconnConfig.TLSConfig = nil
		tlsConf = nil
	}

	snapsotMetrics := mgr.Metrics().NewGauge("snapshot_progress", "table")
	replicationLag := mgr.Metrics().NewGauge("replication_lag_bytes")

	i := &pgStreamInput{
		dbConfig:                pgconnConfig,
		streamSnapshot:          streamSnapshot,
		snapshotMemSafetyFactor: snapshotMemSafetyFactor,
		slotName:                dbSlotName,
		schema:                  dbSchema,
		pgConnRuntimeParam:      pgConnOptions,
		tls:                     tlsConf,
		tables:                  tables,
		decodingPlugin:          decodingPlugin,
		streamUncomited:         streamUncomited,
		temporarySlot:           temporarySlot,
		snapshotBatchSize:       snapshotBatchSize,
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
	tls                     *tls.Config
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

	releaseTrxChan chan bool
	inTxState      atomic.Bool
}

func (p *pgStreamInput) Connect(ctx context.Context) error {
	pgStream, err := pglogicalstream.NewPgStream(ctx, &pglogicalstream.Config{
		PgConnRuntimeParam:       p.pgConnRuntimeParam,
		DBHost:                   p.dbConfig.Host,
		DBPassword:               p.dbConfig.Password,
		DBUser:                   p.dbConfig.User,
		DBPort:                   int(p.dbConfig.Port),
		DBTables:                 p.tables,
		DBName:                   p.dbConfig.Database,
		DBSchema:                 p.schema,
		ReplicationSlotName:      "rs_" + p.slotName,
		TLSConfig:                p.tls,
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
		flushBatch := p.asyncCheckpointer()

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

				// TrxCommit LSN must be acked when all the messages in the batch are processed
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
				if err = p.pglogicalStream.AckLSN(trxCommitLsn); err != nil {
					p.mgr.Logger().Errorf("Failed to ack LSN: %v", err)
					break
				}

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
				if err = p.pglogicalStream.Stop(); err != nil {
					p.logger.Errorf("Failed to stop pglogical stream: %v", err)
				}
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
				defer p.cMut.Unlock()
				if lsn != nil {
					if err = p.pglogicalStream.AckLSN(Int64ToLSN(*lsn)); err != nil {
						return err
					}
					if txCommitConfirmChan != nil {
						*txCommitConfirmChan <- true
					}
				}
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
