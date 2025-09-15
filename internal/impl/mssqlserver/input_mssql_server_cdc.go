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

func (r *msSqlServerCDCInput) Connect(ctx context.Context) error {
	db, err := sql.Open("mssql", r.connectionString)
	if err != nil {
		return fmt.Errorf("failed to connect to Microsoft SQL Server: %s", err)
	}

	r.db = db
	// TODO:
	// - Fix connection sharing, one connection for snapshot and one for reading CDC tables
	// - Refactor snapshot functions on this input into snapshot package
	// - Refactor errgroup

	cachedLSN, err := r.getCachedLSN(ctx)
	if err != nil {
		return fmt.Errorf("unable to get cached LSN: %s", err)
	}

	var snapshot *snapshot
	// no cached LSN means we're not recovering from a restart
	if r.streamSnapshot && len(cachedLSN) == 0 {
		db, err := sql.Open("mssql", r.connectionString)
		if err != nil {
			return fmt.Errorf("connecting to Microsoft SQL Server for snapshotting: %s", err)
		}
		snapshot = NewSnapshot(r.logger, db)
	}

	r.stopSig = shutdown.NewSignaller()
	ctx, done := r.stopSig.SoftStopCtx(context.Background())
	defer done()

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
		wg.Go(func() error {
			var err error
			if ctStream.cachedLSN, err = r.snapshot(ctx, snapshot); err != nil {
				return fmt.Errorf("running snapshotting process: %w", err)
			}
			if err = r.readMessages(ctx, ctStream); err != nil {
				return fmt.Errorf("streaming CDC changes: %w", err)
			}
			return nil
		})
		// wg.Go(func() error { return r.readMessages(ctx, ctStream) })
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

func (r *msSqlServerCDCInput) batchMessages(ctx context.Context) error {
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
			if c.LSN != nil {
				msg.MetaSet("start_lsn", string(c.LSN))
			}

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

func (r *msSqlServerCDCInput) readMessages(ctx context.Context, stream *changeTableStream) error {
	err := stream.readChangeTables(ctx, r.db, func(c *change) (LSN, error) {
		// fmt.Printf("LSN=%x, CommandID=%d, SeqVal=%x, op=%d table=%s cols=%d\n", c.startLSN, c.commandID, c.seqVal, c.operation, c.table, len(c.columns))
		select {
		case r.rawMessageEvents <- MessageEvent{
			Table:     c.table,
			Data:      c.columns,
			Operation: c.operation,
			LSN:       c.startLSN,
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

func (r *msSqlServerCDCInput) flushBatch(ctx context.Context, checkpointer *checkpoint.Capped[LSN], batch service.MessageBatch) error {
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

func (r *msSqlServerCDCInput) getCachedLSN(ctx context.Context) (LSN, error) {
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
	} else if len(cacheVal) == 0 {
		return nil, nil
	}
	return LSN(cacheVal), nil
}

func (r *msSqlServerCDCInput) setCachedLSN(ctx context.Context, lsn LSN) error {
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

func (r *msSqlServerCDCInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case m := <-r.msgChan:
		return m.msg, m.ackFn, nil
	case <-r.stopSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (i *msSqlServerCDCInput) snapshot(ctx context.Context, snapshot *snapshot) (LSN, error) {
	var (
		lsn LSN
		err error
	)
	// If we are given a snapshot, then we need to read it.
	if snapshot != nil {
		if lsn, err = snapshot.prepare(ctx, i.tables); err != nil {
			_ = snapshot.close()
			return nil, fmt.Errorf("preparing snapshot: %w", err)
		}
		if err = i.readSnapshot(ctx, snapshot); err != nil {
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

func (i *msSqlServerCDCInput) readSnapshot(ctx context.Context, snapshot *snapshot) error {
	// TODO: Process tables in parallel
	for _, table := range i.tables {
		tablePks, err := snapshot.getTablePrimaryKeys(ctx, table)
		if err != nil {
			return err
		}
		i.logger.Tracef("primary keys for table %s: %v", table, tablePks)
		lastSeenPksValues := map[string]any{}
		for _, pk := range tablePks {
			lastSeenPksValues[pk] = nil
		}

		var numRowsProcessed int

		i.logger.Infof("Beginning snapshot process for table '%s'", table)
		for {
			var batchRows *sql.Rows
			if numRowsProcessed == 0 {
				batchRows, err = snapshot.querySnapshotTable(ctx, table, tablePks, nil, i.snapshotMaxBatchSize)
			} else {
				batchRows, err = snapshot.querySnapshotTable(ctx, table, tablePks, &lastSeenPksValues, i.snapshotMaxBatchSize)
			}
			if err != nil {
				return fmt.Errorf("failed to execute snapshot table query: %s", err)
			}

			types, err := batchRows.ColumnTypes()
			if err != nil {
				return fmt.Errorf("failed to fetch column types: %s", err)
			}

			values, mappers := prepSnapshotScannerAndMappers(types)

			columns, err := batchRows.Columns()
			if err != nil {
				return fmt.Errorf("failed to fetch columns: %s", err)
			}

			var batchRowsCount int
			for batchRows.Next() {
				numRowsProcessed++
				batchRowsCount++

				if err := batchRows.Scan(values...); err != nil {
					return err
				}

				row := map[string]any{}
				for idx, value := range values {
					v, err := mappers[idx](value)
					if err != nil {
						return err
					}
					row[columns[idx]] = v
					if _, ok := lastSeenPksValues[columns[idx]]; ok {
						lastSeenPksValues[columns[idx]] = value
					}
				}

				select {
				case i.rawMessageEvents <- MessageEvent{
					LSN:       nil,
					Operation: int(MessageOperationRead),
					Table:     table,
					Data:      row,
				}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			if err := batchRows.Err(); err != nil {
				return fmt.Errorf("failed to iterate snapshot table: %s", err)
			}

			if batchRowsCount < i.snapshotMaxBatchSize {
				break
			}
		}
		i.logger.Infof("Completed snapshot process for table '%s'", table)
	}
	return nil
}

func (r *msSqlServerCDCInput) Close(_ context.Context) error {
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}
