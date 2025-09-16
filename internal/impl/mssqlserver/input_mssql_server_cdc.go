// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlserver

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
	service.MustRegisterBatchInput("mssql_server_cdc", mssqlStreamConfigSpec, newMssqlCDCReader)
}

var mssqlStreamConfigSpec = service.NewConfigSpec().
	Beta().
	Categories("Services").
	Version("4.45.0").
	Summary("Creates an input that consumes from a Microsoft SQL Server's change log.").
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
		Default("mssql_cdc_position"),
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

type msSqlServerCDCInput struct {
	connectionString     string
	streamSnapshot       bool
	snapshotMaxBatchSize int
	tables               []string
	lsnCache             string
	lsnCacheKey          string
	batching             service.BatchPolicy
	batchPolicy          *service.Batcher
	checkpoint           *checkpoint.Capped[LSN]

	logger *service.Logger
	res    *service.Resources

	db               *sql.DB
	msgChan          chan asyncMessage
	rawMessageEvents chan MessageEvent
	stopSig          *shutdown.Signaller
}

func newMssqlCDCReader(conf *service.ParsedConfig, res *service.Resources) (s service.BatchInput, err error) {
	if err := license.CheckRunningEnterprise(res); err != nil {
		return nil, err
	}

	i := msSqlServerCDCInput{
		logger:           res.Logger(),
		res:              res,
		msgChan:          make(chan asyncMessage),
		rawMessageEvents: make(chan MessageEvent),
		stopSig:          shutdown.NewSignaller(),
	}

	if i.connectionString, err = conf.FieldString(fieldConnectionString); err != nil {
		return nil, err
	}
	if i.streamSnapshot, err = conf.FieldBool(fieldStreamSnapshot); err != nil {
		return nil, err
	}
	if i.snapshotMaxBatchSize, err = conf.FieldInt(fieldSnapshotMaxBatchSize); err != nil {
		return nil, err
	}
	var checkpointLimit int
	if checkpointLimit, err = conf.FieldInt(fieldCheckpointLimit); err != nil {
		return nil, err
	}
	i.checkpoint = checkpoint.NewCapped[LSN](int64(checkpointLimit))

	// TODO: support regular expression on tablenames
	if i.tables, err = conf.FieldStringList(fieldTables); err != nil {
		return nil, err
	}
	if i.lsnCache, err = conf.FieldString(fieldCheckpointCache); err != nil {
		return nil, err
	}
	if !conf.Resources().HasCache(i.lsnCache) {
		return nil, fmt.Errorf("unknown cache resource: %s", i.lsnCache)
	}
	if i.lsnCacheKey, err = conf.FieldString(fieldCheckpointKey); err != nil {
		return nil, err
	}

	var batching service.BatchPolicy
	if batching, err = conf.FieldBatchPolicy(fieldBatching); err != nil {
		return nil, err
	} else if batching.IsNoop() {
		batching.Count = 1
	}
	i.batching = batching

	if i.batchPolicy, err = i.batching.NewBatcher(res); err != nil {
		return nil, err
	} else if batching.IsNoop() {
		batching.Count = 1
	}

	batchInput, err := service.AutoRetryNacksBatchedToggled(conf, &i)
	if err != nil {
		return nil, err
	}

	return conf.WrapBatchInputExtractTracingSpanMapping("mssql_server_cdc", batchInput)
}

func (i *msSqlServerCDCInput) Connect(ctx context.Context) error {
	db, err := sql.Open("mssql", i.connectionString)
	if err != nil {
		return fmt.Errorf("failed to connect to Microsoft SQL Server: %s", err)
	}

	i.db = db
	// TODO:
	// - Refactor snapshot functions on this input into snapshot package
	// - Refactor errgroup

	cachedLSN, err := i.getCachedLSN(ctx)
	if err != nil {
		return fmt.Errorf("unable to get cached LSN: %s", err)
	}

	var snapshot *snapshot
	// no cached LSN means we're not recovering from a restart
	if i.streamSnapshot && len(cachedLSN) == 0 {
		db, err := sql.Open("mssql", i.connectionString)
		if err != nil {
			return fmt.Errorf("connecting to Microsoft SQL Server for snapshotting: %s", err)
		}
		snapshot = NewSnapshot(db, i.tables, i.logger)
	}

	i.stopSig = shutdown.NewSignaller()
	ctx, done := i.stopSig.SoftStopCtx(context.Background())
	defer done()

	stream := &changeTableStream{
		trackedTables: make(map[string]changeTable, len(i.tables)),
		maxLSN:        cachedLSN,
		logger:        i.logger,
	}

	if err := stream.verifyChangeTables(ctx, i.db, i.tables); err != nil {
		return fmt.Errorf("verifying MS MSQL Server change tables: %s", err)
	}

	go func() {
		ctx, _ = i.stopSig.SoftStopCtx(context.Background())
		wg, softCtx := errgroup.WithContext(ctx)

		wg.Go(func() error {
			var err error
			if stream.maxLSN, err = i.processSnapshot(softCtx, snapshot); err != nil {
				return fmt.Errorf("running snapshotting process: %w", err)
			}
			if err = i.changeTables(softCtx, stream); err != nil {
				return fmt.Errorf("streaming CDC changes: %w", err)
			}
			return nil
		})
		// wg.Go(func() error { return r.readMessages(ctx, ctStream) })
		wg.Go(func() error { return i.batchMessages(softCtx) })

		if err := wg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			i.logger.Errorf("Error during Microsoft SQL Server CDC: %s", err)
		} else {
			i.logger.Info("Successfully shutdown Microsoft SQL Server CDC stream")
		}
		i.stopSig.TriggerHasStopped()
	}()

	return nil
}

func (i *msSqlServerCDCInput) batchMessages(ctx context.Context) error {
	var nextTimedBatchChan <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-nextTimedBatchChan:
			nextTimedBatchChan = nil
			flushedBatch, err := i.batchPolicy.Flush(ctx)
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

			if i.batchPolicy.Add(msg) {
				nextTimedBatchChan = nil
				flushedBatch, err := i.batchPolicy.Flush(ctx)
				if err != nil {
					return fmt.Errorf("flush batch error: %w", err)
				}
				if err := i.flushBatch(ctx, i.checkpoint, flushedBatch); err != nil {
					return fmt.Errorf("failed to flush batch: %w", err)
				}
			} else {
				if d, ok := i.batchPolicy.UntilNext(); ok {
					if nextTimedBatchChan == nil {
						nextTimedBatchChan = time.After(d)
					}
				}
			}
		}
	}
}

func (i *msSqlServerCDCInput) changeTables(ctx context.Context, stream *changeTableStream) error {
	// TODO: Improve this
	h := func(c *change) (LSN, error) {
		// r.logger.Debugf("LSN=%x, CommandID=%d, SeqVal=%x, op=%d table=%s cols=%d\n", c.startLSN, c.commandID, c.seqVal, c.operation, c.table, len(c.columns))
		select {
		case i.rawMessageEvents <- MessageEvent{
			Table:     c.table,
			Data:      c.columns,
			Operation: c.operation,
			LSN:       c.startLSN,
		}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return c.startLSN, nil
	}
	err := stream.readChangeTables(ctx, i.db, h)

	// if err != nil { //nolint:staticcheck
	return fmt.Errorf("streaming from change tables: %w", err)
	// }

	// return nil
}

func (i *msSqlServerCDCInput) flushBatch(ctx context.Context, checkpointer *checkpoint.Capped[LSN], batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}

	lastMsg := batch[len(batch)-1]
	var startLSN []byte
	if lsn, ok := lastMsg.MetaGet("start_lsn"); ok {
		startLSN = []byte(lsn)
	}

	resolveFn, err := checkpointer.Track(ctx, startLSN, int64(len(batch)))
	if err != nil {
		return fmt.Errorf("failed to track LSN checkpoint for batch: %w", err)
	}
	msg := asyncMessage{
		msg: batch,
		ackFn: func(ctx context.Context, _ error) error {
			if lsn := resolveFn(); lsn != nil {
				return i.setCachedLSN(ctx, *lsn)
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

func (i *msSqlServerCDCInput) getCachedLSN(ctx context.Context) (LSN, error) {
	var (
		cacheVal []byte
		cErr     error
	)
	if err := i.res.AccessCache(ctx, i.lsnCache, func(c service.Cache) {
		cacheVal, cErr = c.Get(ctx, i.lsnCacheKey)
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
	return LSN(cacheVal), nil
}

func (i *msSqlServerCDCInput) setCachedLSN(ctx context.Context, lsn LSN) error {
	var cErr error
	if err := i.res.AccessCache(ctx, i.lsnCache, func(c service.Cache) {
		cErr = c.Set(ctx, i.lsnCacheKey, lsn, nil)
	}); err != nil {
		return fmt.Errorf("unable to access cache for writing: %w", err)
	}
	if cErr != nil {
		return fmt.Errorf("unable persist checkpoint to cache: %w", cErr)
	}
	return nil
}

func (i *msSqlServerCDCInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case m := <-i.msgChan:
		return m.msg, m.ackFn, nil
	case <-i.stopSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (i *msSqlServerCDCInput) processSnapshot(ctx context.Context, snapshot *snapshot) (LSN, error) {
	var (
		lsn LSN
		err error
	)
	// If we are given a snapshot, then we need to read it.
	if snapshot != nil {
		i.logger.Infof("Starting snapshot of %d tables", len(snapshot.tables))

		if lsn, err = snapshot.prepare(ctx); err != nil {
			_ = snapshot.close()
			return nil, fmt.Errorf("preparing snapshot: %w", err)
		}
		// TODO: Improve this
		h := func(c MessageEvent) error {
			select {
			case i.rawMessageEvents <- c:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}
		if err = snapshot.read(ctx, i.snapshotMaxBatchSize, h); err != nil {
			_ = snapshot.close()
			return nil, fmt.Errorf("reading snapshot: %w", err)
		}
		if err = snapshot.close(); err != nil {
			return nil, fmt.Errorf("closing snapshot connections: %w", err)
		}

		i.logger.Infof("Completed running snapshot process")
	}
	return lsn, nil
}

func (i *msSqlServerCDCInput) Close(_ context.Context) error {
	if i.db != nil {
		return i.db.Close()
	}
	return nil
}
