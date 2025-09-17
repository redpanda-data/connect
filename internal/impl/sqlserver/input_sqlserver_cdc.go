// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"
	"golang.org/x/sync/errgroup"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/sqlserver/replication"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	fieldConnectionString     = "connection_string"
	fieldStreamSnapshot       = "stream_snapshot"
	fieldSnapshotMaxBatchSize = "snapshot_max_batch_size"
	fieldCheckpointLimit      = "checkpoint_limit"
	fieldTables               = "tables"
	fieldCheckpointCache      = "checkpoint_cache"
	fieldCheckpointKey        = "checkpoint_key"
	fieldBatching             = "batching"
)

func init() {
	service.MustRegisterBatchInput("sql_server_cdc", mssqlStreamConfigSpec, newSqlServerCDCInput)
}

var mssqlStreamConfigSpec = service.NewConfigSpec().
	Beta().
	Categories("Services").
	Version("4.45.0").
	Summary("Creates an input that consumes from a Microsoft SQL Server's change tables.").
	Description(``).
	Field(service.NewStringField(fieldConnectionString).
		Description("The connection string of the Microsoft SQL Server database to connect to.").
		Example("sqlserver://username:password@host/instance?param1=value&param2=value"),
	).
	Field(service.NewBoolField(fieldStreamSnapshot).
		Description("If set to true, the connector will query all the existing data as a part of snapshot process. Otherwise, it will start from the current Log Sequence Number position."),
	).
	Field(service.NewIntField(fieldSnapshotMaxBatchSize).
		Description("The maximum number of rows to be streamed in a single batch when taking a snapshot.").
		Default(1000),
	).
	Field(service.NewStringListField(fieldTables).
		Description("A list of tables to stream from the database.").
		Example([]string{"table1", "table2"}).
		LintRule("root = if this.length() == 0 { [ \"field 'tables' must contain at least one table\" ] }"),
	).
	Field(service.NewStringField(fieldCheckpointCache).
		Description("A https://www.docs.redpanda.com/redpanda-connect/components/caches/about[cache resource^] to use for storing the current latest Log Sequence Number (LSN) that has been successfully delivered, this allows Redpanda Connect to continue from that Log Sequence Number (LSN) upon restart, rather than consume the entire state of the change table."),
	).
	Field(service.NewStringField(fieldCheckpointKey).
		Description("The key to use to store the snapshot position in `" + fieldCheckpointCache + "`. An alternative key can be provided if multiple CDC inputs share the same cache.").
		Default("sql_server_cdc_position"),
	).
	Field(service.NewIntField(fieldCheckpointLimit).
		Description("The maximum number of messages that can be processed at a given time. Increasing this limit enables parallel processing and batching at the output level. Any given Log Sequence Number (LSN) will not be acknowledged unless all messages under that offset are delivered in order to preserve at least once delivery guarantees.").
		Default(1024),
	).
	Field(service.NewAutoRetryNacksToggleField()).
	Field(service.NewBatchPolicyField(fieldBatching))

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

type config struct {
	connectionString     string
	streamSnapshot       bool
	snapshotMaxBatchSize int
	tables               []string
	lsnCache             string
	lsnCacheKey          string
	batcher              *service.Batcher
}

type sqlServerCDCInput struct {
	config *config
	db     *sql.DB

	res        *service.Resources
	checkpoint *checkpoint.Capped[replication.LSN]

	msgChan          chan asyncMessage
	rawMessageEvents chan replication.MessageEvent
	stopSig          *shutdown.Signaller
	logger           *service.Logger
}

func newSqlServerCDCInput(conf *service.ParsedConfig, res *service.Resources) (s service.BatchInput, err error) {
	var (
		connectionString     string
		streamSnapshot       bool
		snapshotMaxBatchSize int
		tables               []string
		lsnCache             string
		lsnCacheKey          string
		batcher              *service.Batcher
		cp                   *checkpoint.Capped[replication.LSN]
	)

	if err := license.CheckRunningEnterprise(res); err != nil {
		return nil, err
	}
	if connectionString, err = conf.FieldString(fieldConnectionString); err != nil {
		return nil, err
	}
	if streamSnapshot, err = conf.FieldBool(fieldStreamSnapshot); err != nil {
		return nil, err
	}
	if snapshotMaxBatchSize, err = conf.FieldInt(fieldSnapshotMaxBatchSize); err != nil {
		return nil, err
	}
	var checkpointLimit int
	if checkpointLimit, err = conf.FieldInt(fieldCheckpointLimit); err != nil {
		return nil, err
	}
	cp = checkpoint.NewCapped[replication.LSN](int64(checkpointLimit))

	// TODO: support regular expression on tablenames
	if tables, err = conf.FieldStringList(fieldTables); err != nil {
		return nil, err
	}
	if lsnCache, err = conf.FieldString(fieldCheckpointCache); err != nil {
		return nil, err
	}
	if !conf.Resources().HasCache(lsnCache) {
		return nil, fmt.Errorf("unknown cache resource: %s", lsnCache)
	}
	if lsnCacheKey, err = conf.FieldString(fieldCheckpointKey); err != nil {
		return nil, err
	}

	var policy service.BatchPolicy
	if policy, err = conf.FieldBatchPolicy(fieldBatching); err != nil {
		return nil, err
	} else if policy.IsNoop() {
		policy.Count = 1
	}
	if batcher, err = policy.NewBatcher(res); err != nil {
		return nil, err
	}

	i := sqlServerCDCInput{
		config: &config{
			connectionString:     connectionString,
			streamSnapshot:       streamSnapshot,
			snapshotMaxBatchSize: snapshotMaxBatchSize,
			tables:               tables,
			lsnCache:             lsnCache,
			lsnCacheKey:          lsnCacheKey,
			batcher:              batcher,
		},
		checkpoint:       cp,
		res:              res,
		msgChan:          make(chan asyncMessage),
		rawMessageEvents: make(chan replication.MessageEvent),
		stopSig:          shutdown.NewSignaller(),
		logger:           res.Logger(),
	}

	batchInput, err := service.AutoRetryNacksBatchedToggled(conf, &i)
	if err != nil {
		return nil, err
	}

	return conf.WrapBatchInputExtractTracingSpanMapping("sql_server_cdc", batchInput)
}

func (i *sqlServerCDCInput) Connect(ctx context.Context) error {
	db, err := sql.Open("mssql", i.config.connectionString)
	if err != nil {
		return fmt.Errorf("failed to connect to sql server: %s", err)
	}

	i.db = db

	cachedLSN, err := i.getCachedLSN(ctx)
	if err != nil {
		return fmt.Errorf("unable to get cached LSN: %s", err)
	}

	var snapshotter *replication.Snapshot
	// no cached LSN means we're not recovering from a restart
	if i.config.streamSnapshot && len(cachedLSN) == 0 {
		db, err := sql.Open("mssql", i.config.connectionString)
		if err != nil {
			return fmt.Errorf("connecting to sql server for snapshotting: %s", err)
		}
		snapshotter = replication.NewSnapshot(db, i.config.tables, i.logger)
	}

	i.stopSig = shutdown.NewSignaller()
	ctx, done := i.stopSig.SoftStopCtx(context.Background())
	defer done()

	streamer := replication.NewChangeTableStream(i.config.tables, i.logger)
	if err := streamer.VerifyChangeTables(ctx, i.db, i.config.tables); err != nil {
		return fmt.Errorf("verifying sql server change tables: %s", err)
	}

	go func() {
		ctx, _ = i.stopSig.SoftStopCtx(context.Background())
		wg, softCtx := errgroup.WithContext(ctx)

		wg.Go(func() error {
			var (
				err    error
				maxLSN = cachedLSN
			)

			// snapshot if no LSN exists then store checkpoint once complete
			if snapshotter != nil {
				if maxLSN, err = i.processSnapshot(ctx, snapshotter); err != nil {
					return fmt.Errorf("processing snapshotting: %w", err)
				}
				if err := i.cacheLSN(ctx, maxLSN); err != nil {
					return fmt.Errorf("caching LSN after snapshotting: %w", err)
				}
			}
			// start streaming changes
			if err = i.streamChanges(ctx, streamer, maxLSN); err != nil {
				return fmt.Errorf("streaming changes: %w", err)
			}
			return nil
		})
		wg.Go(func() error { return i.consume(softCtx) })

		if err := wg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			i.logger.Errorf("Error during SQL Server CDC: %s", err)
		} else {
			i.logger.Info("Successfully shutdown SQL Server CDC stream")
		}
		i.stopSig.TriggerHasStopped()
	}()

	return nil
}

func (i *sqlServerCDCInput) consume(ctx context.Context) error {
	var nextTimedBatchChan <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-nextTimedBatchChan:
			nextTimedBatchChan = nil
			flushedBatch, err := i.config.batcher.Flush(ctx)
			if err != nil {
				return fmt.Errorf("timed flush batch error: %w", err)
			}
			if err := i.flushBatch(ctx, i.checkpoint, flushedBatch); err != nil {
				return fmt.Errorf("failed to flush periodic batch: %w", err)
			}
		case c := <-i.rawMessageEvents:
			data, err := json.Marshal(c.Data)
			if err != nil {
				return fmt.Errorf("failure to marshal message: %w", err)
			}
			msg := service.NewMessage(data)
			msg.MetaSet("table", c.Table)
			if c.LSN != nil {
				msg.MetaSet("start_lsn", string(c.LSN))
			}

			if i.config.batcher.Add(msg) {
				nextTimedBatchChan = nil
				flushedBatch, err := i.config.batcher.Flush(ctx)
				if err != nil {
					return fmt.Errorf("flush batch error: %w", err)
				}
				if err := i.flushBatch(ctx, i.checkpoint, flushedBatch); err != nil {
					return fmt.Errorf("failed to flush batch: %w", err)
				}
			} else {
				if d, ok := i.config.batcher.UntilNext(); ok {
					if nextTimedBatchChan == nil {
						nextTimedBatchChan = time.After(d)
					}
				}
			}
		}
	}
}

func (i *sqlServerCDCInput) publish() replication.Handler {
	return func(ctx context.Context, msg replication.MessageEvent) error {
		// r.logger.Debugf("LSN=%x, CommandID=%d, SeqVal=%x, op=%d table=%s cols=%d\n", c.startLSN, c.commandID, c.seqVal, c.operation, c.table, len(c.columns))
		select {
		case i.rawMessageEvents <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}
}

func (i *sqlServerCDCInput) streamChanges(ctx context.Context, stream *replication.ChangeTableStream, startPos replication.LSN) error {
	i.logger.Infof("Starting streaming %d change table(s)", len(i.config.tables))
	if err := stream.ReadChangeTables(ctx, i.db, startPos, i.publish()); err != nil {
		return fmt.Errorf("streaming from change tables: %w", err)
	}
	return nil
}

func (i *sqlServerCDCInput) flushBatch(ctx context.Context, checkpointer *checkpoint.Capped[replication.LSN], batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}

	lastMsg := batch[len(batch)-1]
	var checkpointLSN []byte
	// snapshot records don't have a start_lsn as we don't track those
	if lsn, ok := lastMsg.MetaGet("start_lsn"); ok {
		checkpointLSN = replication.LSN(lsn)
	}

	resolveFn, err := checkpointer.Track(ctx, checkpointLSN, int64(len(batch)))
	if err != nil {
		return fmt.Errorf("failed to track LSN checkpoint for batch: %w", err)
	}
	msg := asyncMessage{
		msg: batch,
		ackFn: func(ctx context.Context, _ error) error {
			lsn := resolveFn()
			if lsn != nil && len(*lsn) != 0 {
				return i.cacheLSN(ctx, *lsn)
			}
			return nil
		},
	}
	select {
	case i.msgChan <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (i *sqlServerCDCInput) getCachedLSN(ctx context.Context) (replication.LSN, error) {
	var (
		cacheVal []byte
		cErr     error
	)
	if err := i.res.AccessCache(ctx, i.config.lsnCache, func(c service.Cache) {
		cacheVal, cErr = c.Get(ctx, i.config.lsnCacheKey)
	}); err != nil {
		return nil, fmt.Errorf("unable to access cache for reading: %w", err)
	}
	if errors.Is(cErr, service.ErrKeyNotFound) {
		return nil, nil
	} else if cErr != nil {
		return nil, fmt.Errorf("unable read checkpoint from cache: %w", cErr)
	} else if len(cacheVal) == 0 {
		return nil, nil
	}
	return replication.LSN(cacheVal), nil
}

func (i *sqlServerCDCInput) cacheLSN(ctx context.Context, lsn replication.LSN) error {
	if len(lsn) == 0 {
		return errors.New("lsn for caching is empty")
	}

	var cErr error
	if err := i.res.AccessCache(ctx, i.config.lsnCache, func(c service.Cache) {
		cErr = c.Set(ctx, i.config.lsnCacheKey, lsn, nil)
	}); err != nil {
		return fmt.Errorf("unable to access cache for writing: %w", err)
	}
	if cErr != nil {
		return fmt.Errorf("unable persist checkpoint to cache: %w", cErr)
	}
	return nil
}

func (i *sqlServerCDCInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case m := <-i.msgChan:
		return m.msg, m.ackFn, nil
	case <-i.stopSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (i *sqlServerCDCInput) processSnapshot(ctx context.Context, snapshot *replication.Snapshot) (replication.LSN, error) {
	var (
		lsn replication.LSN
		err error
	)
	i.logger.Infof("Starting snapshot of %d table(s)", len(snapshot.Tables))
	if lsn, err = snapshot.Prepare(ctx); err != nil {
		_ = snapshot.Close()
		return nil, fmt.Errorf("preparing snapshot: %w", err)
	}
	if err = snapshot.Read(ctx, i.config.snapshotMaxBatchSize, i.publish()); err != nil {
		_ = snapshot.Close()
		return nil, fmt.Errorf("reading snapshot: %w", err)
	}
	if err = snapshot.Close(); err != nil {
		return nil, fmt.Errorf("closing snapshot connections: %w", err)
	}
	i.logger.Infof("Completed running snapshot process")

	return lsn, nil
}

func (i *sqlServerCDCInput) Close(_ context.Context) error {
	if i.db != nil {
		return i.db.Close()
	}
	return nil
}
