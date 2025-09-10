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
	"strconv"
	"sync"
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

type msSqlServerCDCReader struct {
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

	dbMu             sync.Mutex
	db               *sql.DB
	msgChan          chan asyncMessage
	rawMessageEvents chan MessageEvent
	stopSig          *shutdown.Signaller
}

func newMssqlCDCReader(conf *service.ParsedConfig, res *service.Resources) (s service.BatchInput, err error) {
	if err := license.CheckRunningEnterprise(res); err != nil {
		return nil, err
	}

	reader := msSqlServerCDCReader{
		logger:           res.Logger(),
		res:              res,
		msgChan:          make(chan asyncMessage),
		rawMessageEvents: make(chan MessageEvent),
		stopSig:          shutdown.NewSignaller(),
	}

	if reader.connectionString, err = conf.FieldString(fieldConnectionString); err != nil {
		return nil, err
	}
	if reader.streamSnapshot, err = conf.FieldBool(fieldStreamSnapshot); err != nil {
		return nil, err
	}
	if reader.snapshotMaxBatchSize, err = conf.FieldInt(fieldSnapshotMaxBatchSize); err != nil {
		return nil, err
	}
	var checkpointLimit int
	if checkpointLimit, err = conf.FieldInt(fieldCheckpointLimit); err != nil {
		return nil, err
	}
	reader.checkpoint = checkpoint.NewCapped[LSN](int64(checkpointLimit))

	// TODO: support regular expression on tablenames
	if reader.tables, err = conf.FieldStringList(fieldTables); err != nil {
		return nil, err
	}
	if reader.lsnCache, err = conf.FieldString(fieldCheckpointCache); err != nil {
		return nil, err
	}
	if !conf.Resources().HasCache(reader.lsnCache) {
		return nil, fmt.Errorf("unknown cache resource: %s", reader.lsnCache)
	}
	if reader.lsnCacheKey, err = conf.FieldString(fieldCheckpointKey); err != nil {
		return nil, err
	}

	var batching service.BatchPolicy
	if batching, err = conf.FieldBatchPolicy(fieldBatching); err != nil {
		return nil, err
	} else if batching.IsNoop() {
		batching.Count = 1
	}
	reader.batching = batching

	if reader.batchPolicy, err = reader.batching.NewBatcher(res); err != nil {
		return nil, err
	} else if batching.IsNoop() {
		batching.Count = 1
	}

	batchInput, err := service.AutoRetryNacksBatchedToggled(conf, &reader)
	if err != nil {
		return nil, err
	}

	return conf.WrapBatchInputExtractTracingSpanMapping("mssql_server_cdc", batchInput)
}

func (r *msSqlServerCDCReader) Connect(ctx context.Context) error {
	db, err := sql.Open("mssql", r.connectionString)
	if err != nil {
		return fmt.Errorf("failed to connect to Microsoft SQL Server: %s", err)
	}

	r.dbMu.Lock()
	r.db = db
	r.dbMu.Unlock()

	cachedLSN, err := r.getCachedLSN(ctx)
	if err != nil {
		return fmt.Errorf("unable to get cached LSN: %s", err)
	}

	var snapshot *snapshot
	if r.streamSnapshot && cachedLSN == nil {
		snapshot = NewSnapshot(r.logger, r.db)
	}

	r.stopSig = shutdown.NewSignaller()
	ctx, done := r.stopSig.SoftStopCtx(context.Background())
	defer done()

	if snapshot != nil {
		if err := snapshot.prepare(ctx, r.tables); err != nil {
			return fmt.Errorf("processing snapshot: %s", err)
		}
	}

	ctStream := &changeTableStream{
		logger:        r.logger,
		trackedTables: make(map[string]changeTable, len(r.tables)),
		cachedLSN:     cachedLSN,
	}

	if err := ctStream.verifyChangeTables(ctx, r.db, r.tables); err != nil {
		return fmt.Errorf("verifying MS MSQL Server change tables: %s", err)
	}

	go func() {
		ctx, _ = r.stopSig.SoftStopCtx(context.Background())
		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error { return r.readMessages(ctx, ctStream) })
		wg.Go(func() error { return r.batchMessages(ctx) })

		if err := wg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			r.logger.Errorf("Error during Microsoft SQL Server CDC: %s", err)
		} else {
			r.logger.Info("Successfully shutdown Microsoft SQL Server CDC stream")
		}
		r.stopSig.TriggerHasStopped()
	}()

	return nil
}

func (r *msSqlServerCDCReader) batchMessages(ctx context.Context) error {
	var nextTimedBatchChan <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-nextTimedBatchChan:
			nextTimedBatchChan = nil
			flushedBatch, err := r.batchPolicy.Flush(ctx)
			if err != nil {
				return fmt.Errorf("timed flush batch error: %w", err)
			}
			if err := r.flushBatch(ctx, r.checkpoint, flushedBatch); err != nil {
				return fmt.Errorf("failed to flush periodic batch: %w", err)
			}
		case c := <-r.rawMessageEvents:
			data, err := json.Marshal(c.Data)
			if err != nil {
				return fmt.Errorf("failure to marshal message: %w", err)
			}
			msg := service.NewMessage(data)
			msg.MetaSet("table", c.Table)
			if c.StartLSN != nil {
				msg.MetaSet("start_lsn", string(c.StartLSN))
			}
			if c.EndLSN != nil {
				msg.MetaSet("end_lsn", string(c.EndLSN))
			}
			msg.MetaSet("sequence_value", string(c.SequenceValue))
			msg.MetaSet("operation", fmt.Sprint(c.Operation))
			msg.MetaSet("update_mask", string(c.UpdateMask))
			msg.MetaSet("command_id", strconv.Itoa(c.CommandID))

			if r.batchPolicy.Add(msg) {
				nextTimedBatchChan = nil
				flushedBatch, err := r.batchPolicy.Flush(ctx)
				if err != nil {
					return fmt.Errorf("flush batch error: %w", err)
				}
				if err := r.flushBatch(ctx, r.checkpoint, flushedBatch); err != nil {
					return fmt.Errorf("failed to flush batch: %w", err)
				}
			} else {
				if d, ok := r.batchPolicy.UntilNext(); ok {
					if nextTimedBatchChan == nil {
						nextTimedBatchChan = time.After(d)
					}
				}
			}
		}
	}
}

func (r *msSqlServerCDCReader) readMessages(ctx context.Context, stream *changeTableStream) error {
	err := stream.readChangeTables(ctx, r.db, func(c *change) (LSN, error) {
		// fmt.Printf("LSN=%x, CommandID=%d, SeqVal=%x, op=%d table=%s cols=%d\n", c.startLSN, c.commandID, c.seqVal, c.operation, c.table, len(c.columns))
		select {
		case r.rawMessageEvents <- MessageEvent{
			Table:         c.table,
			Data:          c.columns,
			Operation:     OpType(c.operation),
			SequenceValue: c.seqVal,
			CommandID:     c.commandID,
			StartLSN:      c.startLSN,
		}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return c.startLSN, nil
	})

	// if err != nil { //nolint:staticcheck
	return fmt.Errorf("streaming from change tables: %w", err)
	// }

	// return nil
}

func (r *msSqlServerCDCReader) flushBatch(ctx context.Context, checkpointer *checkpoint.Capped[LSN], batch service.MessageBatch) error {
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
				return r.setCachedLSN(ctx, *lsn)
			}
			return nil
		},
	}
	select {
	case r.msgChan <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *msSqlServerCDCReader) getCachedLSN(ctx context.Context) (LSN, error) {
	var (
		cacheVal []byte
		cErr     error
	)
	if err := r.res.AccessCache(ctx, r.lsnCache, func(c service.Cache) {
		cacheVal, cErr = c.Get(ctx, r.lsnCacheKey)
	}); err != nil {
		return nil, fmt.Errorf("unable to access cache for reading: %w", err)
	}
	if errors.Is(cErr, service.ErrKeyNotFound) {
		return nil, nil
	} else if cErr != nil {
		return nil, fmt.Errorf("unable read checkpoint from cache: %w", cErr)
	} else if cacheVal == nil {
		return nil, nil
	}
	return LSN(cacheVal), nil
}

func (r *msSqlServerCDCReader) setCachedLSN(ctx context.Context, lsn LSN) error {
	var cErr error
	if err := r.res.AccessCache(ctx, r.lsnCache, func(c service.Cache) {
		cErr = c.Set(ctx, r.lsnCacheKey, lsn, nil)
	}); err != nil {
		return fmt.Errorf("unable to access cache for writing: %w", err)
	}
	if cErr != nil {
		return fmt.Errorf("unable persist checkpoint to cache: %w", cErr)
	}
	return nil
}

func (r *msSqlServerCDCReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case m := <-r.msgChan:
		return m.msg, m.ackFn, nil
	case <-r.stopSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case <-ctx.Done():
	}
	return nil, nil, ctx.Err()
}

func (r *msSqlServerCDCReader) Close(_ context.Context) error {
	r.dbMu.Lock()
	defer r.dbMu.Unlock()
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}
