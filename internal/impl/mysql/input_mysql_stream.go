// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"
	"github.com/go-mysql-org/go-mysql/canal"
	mysqlcdc "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-sql-driver/mysql"
	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/sync/errgroup"
)

const (
	fieldMySQLDSN                  = "dsn"
	fieldMySQLTables               = "tables"
	fieldStreamSnapshot            = "stream_snapshot"
	fieldMaxSnapshotParallelTables = "max_snapshot_parallel_tables"
	fieldSnapshotMaxBatchSize      = "snapshot_max_batch_size"
	fieldBatching                  = "batching"
	fieldCheckpointKey             = "checkpoint_key"
	fieldCheckpointCache           = "checkpoint_cache"
	fieldCheckpointLimit           = "checkpoint_limit"

	shutdownTimeout = 5 * time.Second
)

var mysqlStreamConfigSpec = service.NewConfigSpec().
	Summary("Enables MySQL streaming for RedPanda Connect.").
	Fields(
		service.NewStringField(fieldMySQLDSN).
			Description("The DSN of the MySQL database to connect to.").
			Example("user:password@tcp(localhost:3306)/database"),
		service.NewStringListField(fieldMySQLTables).
			Description("A list of tables to stream from the database.").
			Example([]string{"table1", "table2"}),
		service.NewStringField(fieldCheckpointCache).
			Description("A https://www.docs.redpanda.com/redpanda-connect/components/caches/about[cache resource^] to use for storing the current latest BinLog Position that has been successfully delivered, this allows Redpanda Connect to continue from that BinLog Position upon restart, rather than consume the entire state of the table.\""),
		service.NewStringField(fieldCheckpointKey).
			Description("The key to use to store the snapshot position in `"+fieldCheckpointCache+"`. An alternative key can be provided if multiple CDC inputs share the same cache.").
			Default("mysql_binlog_position"),
		service.NewIntField(fieldMaxSnapshotParallelTables).
			Description("Int specifies a number of tables to be streamed in parallel when taking a snapshot. If set to true, the connector will stream all tables in parallel. Otherwise, it will stream tables one by one.").
			Default(1),
		service.NewIntField(fieldSnapshotMaxBatchSize).
			Description("The maximum number of rows to be streamed in a single batch when taking a snapshot.").
			Default(1000),
		service.NewBoolField(fieldStreamSnapshot).
			Description("If set to true, the connector will query all the existing data as a part of snapshot process. Otherwise, it will start from the current binlog position."),
		service.NewAutoRetryNacksToggleField(),
		service.NewIntField(fieldCheckpointLimit).
			Description("The maximum number of messages that can be processed at a given time. Increasing this limit enables parallel processing and batching at the output level. Any given BinLog Position will not be acknowledged unless all messages under that offset are delivered in order to preserve at least once delivery guarantees.").
			Default(1024),
		service.NewBatchPolicyField(fieldBatching),
	)

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

type mysqlStreamInput struct {
	canal.DummyEventHandler

	mutex sync.Mutex
	// canal stands for mysql binlog listener connection
	canal               *canal.Canal
	mysqlConfig         *mysql.Config
	startBinLogPosition *mysqlcdc.Position
	currentLogPosition  *mysqlcdc.Position
	binLogCache         string
	binLogCacheKey      string

	dsn            string
	tables         []string
	streamSnapshot bool

	rawMessageEvents      chan MessageEvent
	snapshotMessageEvents chan MessageEvent
	msgChan               chan asyncMessage
	batching              service.BatchPolicy
	batchPolicy           *service.Batcher
	tablesFilterMap       map[string]bool
	checkPointLimit       int

	logger *service.Logger
	res    *service.Resources

	streamCtx context.Context
	errors    chan error
	cp        *checkpoint.Capped[*int64]

	snapshot                  *Snapshot
	shutSig                   *shutdown.Signaller
	snapshotMaxParallelTables int
	fieldSnapshotMaxBatchSize int
}

func newMySQLStreamInput(conf *service.ParsedConfig, res *service.Resources) (s service.BatchInput, err error) {
	streamInput := mysqlStreamInput{
		logger:                res.Logger(),
		rawMessageEvents:      make(chan MessageEvent),
		snapshotMessageEvents: make(chan MessageEvent),
		msgChan:               make(chan asyncMessage),
		res:                   res,

		errors:  make(chan error, 1),
		shutSig: shutdown.NewSignaller(),
	}

	var batching service.BatchPolicy

	if streamInput.dsn, err = conf.FieldString(fieldMySQLDSN); err != nil {
		return nil, err
	}

	streamInput.mysqlConfig, err = mysql.ParseDSN(streamInput.dsn)
	if err != nil {
		return nil, fmt.Errorf("error parsing mysql DSN: %v", err)
	}

	if streamInput.tables, err = conf.FieldStringList(fieldMySQLTables); err != nil {
		return nil, err
	}

	if streamInput.streamSnapshot, err = conf.FieldBool(fieldStreamSnapshot); err != nil {
		return nil, err
	}

	if streamInput.snapshotMaxParallelTables, err = conf.FieldInt(fieldMaxSnapshotParallelTables); err != nil {
		return nil, err
	}

	if streamInput.fieldSnapshotMaxBatchSize, err = conf.FieldInt(fieldSnapshotMaxBatchSize); err != nil {
		return nil, err
	}

	if streamInput.checkPointLimit, err = conf.FieldInt(fieldCheckpointLimit); err != nil {
		return nil, err
	}

	if streamInput.binLogCache, err = conf.FieldString(fieldCheckpointCache); err != nil {
		return nil, err
	}
	if !conf.Resources().HasCache(streamInput.binLogCache) {
		return nil, fmt.Errorf("unknown cache resource: %s", streamInput.binLogCache)
	}
	if streamInput.binLogCacheKey, err = conf.FieldString(fieldCheckpointKey); err != nil {
		return nil, err
	}

	i := &streamInput
	i.cp = checkpoint.NewCapped[*int64](int64(i.checkPointLimit))

	i.tablesFilterMap = map[string]bool{}
	for _, table := range i.tables {
		if err = validateTableName(table); err != nil {
			return nil, err
		}
		i.tablesFilterMap[table] = true
	}

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

	i.streamCtx, _ = i.shutSig.SoftStopCtx(context.Background())

	r, err := service.AutoRetryNacksBatchedToggled(conf, i)
	if err != nil {
		return nil, err
	}

	return conf.WrapBatchInputExtractTracingSpanMapping("mysql_cdc", r)
}

func init() {
	err := service.RegisterBatchInput("mysql_cdc", mysqlStreamConfigSpec, newMySQLStreamInput)
	if err != nil {
		panic(err)
	}
}

// ---- Redpanda Connect specific methods----

func (i *mysqlStreamInput) Connect(ctx context.Context) error {
	canalConfig := canal.NewDefaultConfig()
	canalConfig.Flavor = mysqlcdc.DEFAULT_FLAVOR
	canalConfig.Addr = i.mysqlConfig.Addr
	canalConfig.User = i.mysqlConfig.User
	canalConfig.Password = i.mysqlConfig.Passwd
	// resetting dump path since we are doing snapshot manually
	// this is required since canal will try to prepare dumper on init stage
	canalConfig.Dump.ExecutionPath = ""

	// Parse and set additional parameters
	canalConfig.Charset = i.mysqlConfig.Collation
	if i.mysqlConfig.TLS != nil {
		canalConfig.TLSConfig = i.mysqlConfig.TLS
	}
	canalConfig.ParseTime = true
	canalConfig.IncludeTableRegex = i.tables

	if err := i.res.AccessCache(ctx, i.binLogCache, func(c service.Cache) {
		binLogPositionBytes, cErr := c.Get(ctx, i.binLogCacheKey)
		if cErr != nil {
			if !errors.Is(cErr, service.ErrKeyNotFound) {
				i.logger.Errorf("failed to obtain cursor cache item. %v", cErr)
			}
			return
		}

		var storedMySQLBinLogPosition mysqlcdc.Position
		if err := json.Unmarshal(binLogPositionBytes, &storedMySQLBinLogPosition); err != nil {
			i.logger.With("error", err.Error()).Error("Failed to unmarshal stored binlog position.")
			return
		}

		i.startBinLogPosition = &storedMySQLBinLogPosition
	}); err != nil {
		i.logger.With("error", err.Error()).Error("Failed to access cursor cache.")
	}

	c, err := canal.NewCanal(canalConfig)
	if err != nil {
		return err
	}
	c.AddDumpTables(i.mysqlConfig.DBName, i.tables...)

	i.canal = c

	db, err := sql.Open("mysql", i.dsn)
	if err != nil {
		return err
	}

	// create snapshot instance
	snapshot := NewSnapshot(i.logger, db)
	i.snapshot = snapshot

	go i.startMySQLSync()
	return nil
}

func (i *mysqlStreamInput) readMessages(ctx context.Context) {
	var nextTimedBatchChan <-chan time.Time

	for !i.shutSig.IsSoftStopSignalled() {
		select {
		case <-ctx.Done():
			return
		case <-nextTimedBatchChan:
			nextTimedBatchChan = nil
			flushedBatch, err := i.batchPolicy.Flush(ctx)
			if err != nil {
				i.logger.Debugf("Timed flush batch error: %w", err)
				break
			}

			if err := i.flushBatch(ctx, i.cp, flushedBatch, i.currentLogPosition); err != nil {
				break
			}
		case me := <-i.rawMessageEvents:
			row, err := json.Marshal(me.Row)
			if err != nil {
				return
			}

			mb := service.NewMessage(row)
			mb.MetaSet("operation", string(me.Operation))
			mb.MetaSet("table", me.Table)
			if me.Position != nil {
				i.mutex.Lock()
				i.currentLogPosition = me.Position
				i.mutex.Unlock()
				// Lexicographically ordered
				mb.MetaSet("binlog_position", me.Position.String())
			}

			if i.batchPolicy.Add(mb) {
				nextTimedBatchChan = nil
				flushedBatch, err := i.batchPolicy.Flush(ctx)
				if err != nil {
					i.logger.Debugf("Flush batch error: %w", err)
					break
				}
				if err := i.flushBatch(ctx, i.cp, flushedBatch, i.currentLogPosition); err != nil {
					break
				}
			} else {
				d, ok := i.batchPolicy.UntilNext()
				if ok {
					nextTimedBatchChan = time.After(d)
				}
			}
		case err := <-i.errors:
			i.logger.Warnf("stream error: %s", err)
			i.shutSig.TriggerSoftStop()
			// If the stream has errored then we should stop and restart processing
			return
		}
	}
}

func (i *mysqlStreamInput) startMySQLSync() {
	ctx, _ := i.shutSig.SoftStopCtx(context.Background())

	i.canal.SetEventHandler(i)
	go i.readMessages(ctx)
	// If we require snapshot streaming && we don't have a binlog position cache
	// initiate default run for Canal to process snapshot and start incremental sync of binlog
	if i.streamSnapshot && i.startBinLogPosition == nil {
		startPos, err := i.snapshot.prepareSnapshot(ctx)
		if err != nil {
			i.errors <- err
			return
		}

		i.logger.Debugf("Starting snapshot while holding binglog pos on: %v", startPos)
		var wg errgroup.Group
		wg.SetLimit(i.snapshotMaxParallelTables)

		for _, table := range i.tables {
			wg.Go(func() (err error) {
				tablePks, err := i.snapshot.getTablePrimaryKeys(ctx, table)
				if err != nil {
					return err
				}

				i.logger.Debugf("Primary keys for table %s %v", table, tablePks)

				var numRowsProcessed int
				lastSeenPksValues := map[string]any{}
				for _, pk := range tablePks {
					lastSeenPksValues[pk] = nil
				}

				for {
					var batchRows *sql.Rows
					if numRowsProcessed == 0 {
						batchRows, err = i.snapshot.querySnapshotTable(ctx, table, tablePks, nil, i.fieldSnapshotMaxBatchSize)
					} else {
						batchRows, err = i.snapshot.querySnapshotTable(ctx, table, tablePks, &lastSeenPksValues, i.fieldSnapshotMaxBatchSize)
					}
					if err != nil {
						return err
					}

					var batchRowsCount int
					for batchRows.Next() {
						numRowsProcessed++
						batchRowsCount++

						columns, err := batchRows.Columns()
						if err != nil {
							batchRows.Close()
							return err
						}

						values := make([]interface{}, len(columns))
						valuePtrs := make([]interface{}, len(columns))
						for i := range values {
							valuePtrs[i] = &values[i]
						}

						if err := batchRows.Scan(valuePtrs...); err != nil {
							batchRows.Close()
							return err
						}

						row := map[string]any{}
						for idx, value := range values {
							row[columns[idx]] = value
							if _, ok := lastSeenPksValues[columns[idx]]; ok {
								lastSeenPksValues[columns[idx]] = value
							}
						}

						// build message
						i.rawMessageEvents <- MessageEvent{
							Row:       row,
							Operation: MessageOperationRead,
							Table:     table,
							Position:  nil,
						}
					}

					if batchRowsCount < i.fieldSnapshotMaxBatchSize {
						break
					}
				}

				return nil
			})
		}

		if err = wg.Wait(); err != nil {
			i.errors <- fmt.Errorf("snapshot processing failed: %w", err)
		}

		if err = i.snapshot.releaseSnapshot(ctx); err != nil {
			i.logger.Errorf("Failed to properly release snapshot %v", err)
		}

		i.mutex.Lock()
		i.currentLogPosition = startPos
		i.mutex.Unlock()

		i.logger.Infof("Snapshot is done...Running CDC from BingLog: %s  on pos: %d", startPos.Name, startPos.Pos)
		if err := i.canal.RunFrom(*startPos); err != nil {
			i.errors <- fmt.Errorf("failed to start streaming: %v", err)
		}
	} else {
		coords, _ := i.canal.GetMasterPos()
		i.mutex.Lock()
		// starting from the last stored binlog position
		if i.startBinLogPosition != nil {
			coords = *i.startBinLogPosition
		}

		i.currentLogPosition = &coords
		i.mutex.Lock()
		if err := i.canal.RunFrom(coords); err != nil {
			i.errors <- fmt.Errorf("failed to start streaming: %v", err)
		}
	}
}

func (i *mysqlStreamInput) flushBatch(ctx context.Context, checkpointer *checkpoint.Capped[*int64], batch service.MessageBatch, binLogPos *mysqlcdc.Position) error {
	if len(batch) == 0 {
		return nil
	}

	var intPos *int64
	if binLogPos != nil {
		posInInt := int64(binLogPos.Pos)
		intPos = &posInInt
	}

	resolveFn, err := checkpointer.Track(ctx, intPos, int64(len(batch)))
	if err != nil {
		return fmt.Errorf("failed to track checkpoint for batch: %w", err)
	}

	select {
	case i.msgChan <- asyncMessage{
		msg: batch,
		ackFn: func(ctx context.Context, res error) error {
			maxOffset := resolveFn()
			// Nothing to commit, this wasn't the latest message
			if maxOffset == nil {
				return nil
			}
			offset := *maxOffset
			// This has no offset - it's a snapshot message
			// TODO(rockwood): We should be storing the primary key for
			// each table in the snapshot so we can properly resume the
			// primary key scan.
			if offset == nil {
				return nil
			}
			return i.syncBinlogPosition(ctx, offset)
		},
	}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (i *mysqlStreamInput) onMessage(e *canal.RowsEvent, initValue, incrementValue int) error {
	i.mutex.Lock()
	i.currentLogPosition.Pos = e.Header.LogPos
	i.mutex.Unlock()

	for pi := initValue; pi < len(e.Rows); pi += incrementValue {
		message := map[string]any{}
		for i, v := range e.Rows[pi] {
			message[e.Table.Columns[i].Name] = v
		}

		i.rawMessageEvents <- MessageEvent{
			Row:       message,
			Operation: MessageOperation(e.Action),
			Table:     e.Table.Name,
			Position:  i.currentLogPosition,
		}
	}

	return nil
}

func (i *mysqlStreamInput) syncBinlogPosition(ctx context.Context, binLogPos *int64) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	if i.currentLogPosition == nil {
		i.logger.Warn("No current bingLog position")
		return errors.New("no current binlog position")
	}

	if binLogPos != nil {
		i.currentLogPosition.Pos = uint32(*binLogPos)
	}

	var (
		positionInByte []byte
		err            error
	)
	if positionInByte, err = json.Marshal(*i.currentLogPosition); err != nil {
		i.logger.Errorf("Failed to marshal binlog position: %v", err)
		return err
	}

	var cErr error
	if err := i.res.AccessCache(ctx, i.binLogCache, func(c service.Cache) {
		cErr = c.Set(ctx, i.binLogCacheKey, positionInByte, nil)
		if cErr != nil {
			i.logger.Errorf("Failed to store binlog position: %v", cErr)
		}
	}); err != nil {
		i.logger.Errorf("Access cache error %v", err)
		return err
	}

	return cErr
}

func (i *mysqlStreamInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	i.mutex.Lock()
	msgChan := i.msgChan
	i.mutex.Unlock()
	if msgChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case m, open := <-msgChan:
		if !open {
			return nil, nil, service.ErrNotConnected
		}
		return m.msg, m.ackFn, nil
	case <-i.shutSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case <-ctx.Done():
	}

	return nil, nil, ctx.Err()
}

func (i *mysqlStreamInput) Close(ctx context.Context) error {
	i.shutSig.TriggerSoftStop()
	select {
	case <-ctx.Done():
	case <-time.After(shutdownTimeout):
	case <-i.shutSig.HasStoppedChan():
	}
	i.shutSig.TriggerHardStop()
	if i.canal != nil {
		i.canal.SyncedPosition()
		i.canal.Close()
	}

	return i.snapshot.close()
}

// ---- Redpanda Connect specific methods end----

// --- MySQL Canal handler methods ----

func (i *mysqlStreamInput) OnRotate(eh *replication.EventHeader, re *replication.RotateEvent) error {
	i.mutex.Lock() // seems sketch
	flushedBatch, err := i.batchPolicy.Flush(i.streamCtx)
	if err != nil {
		i.logger.Debugf("Flush batch error: %w", err)
		return err
	}

	if err := i.flushBatch(i.streamCtx, i.cp, flushedBatch, i.currentLogPosition); err != nil {
		return fmt.Errorf("failed to flush batch: %w", err)
	}

	i.currentLogPosition.Pos = uint32(re.Position)
	i.currentLogPosition.Name = string(re.NextLogName)
	i.mutex.Unlock()

	return nil
}

func (i *mysqlStreamInput) OnRow(e *canal.RowsEvent) error {
	if _, ok := i.tablesFilterMap[e.Table.Name]; !ok {
		return nil
	}
	switch e.Action {
	case canal.InsertAction:
		return i.onMessage(e, 0, 1)
	case canal.DeleteAction:
		return i.onMessage(e, 0, 1)
	case canal.UpdateAction:
		return i.onMessage(e, 1, 2)
	default:
		return errors.New("invalid rows action")
	}
}

// --- MySQL Canal handler methods end ----
