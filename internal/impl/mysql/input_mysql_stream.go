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
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/go-sql-driver/mysql"
	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/sync/errgroup"

	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	fieldMySQLDSN             = "dsn"
	fieldMySQLTables          = "tables"
	fieldStreamSnapshot       = "stream_snapshot"
	fieldSnapshotMaxBatchSize = "snapshot_max_batch_size"
	fieldBatching             = "batching"
	fieldCheckpointKey        = "checkpoint_key"
	fieldCheckpointCache      = "checkpoint_cache"
	fieldCheckpointLimit      = "checkpoint_limit"

	shutdownTimeout = 5 * time.Second
)

var mysqlStreamConfigSpec = service.NewConfigSpec().
	Beta().
	Categories("Services").
	Version("4.45.0").
	Summary("Enables MySQL streaming for RedPanda Connect.").
	Description(`
== Metadata

This input adds the following metadata fields to each message:

- operation
- table
- binlog_position
`).
	Fields(
		service.NewStringField(fieldMySQLDSN).
			Description("The DSN of the MySQL database to connect to.").
			Example("user:password@tcp(localhost:3306)/database"),
		service.NewStringListField(fieldMySQLTables).
			Description("A list of tables to stream from the database.").
			Example([]string{"table1", "table2"}),
		service.NewStringField(fieldCheckpointCache).
			Description("A https://www.docs.redpanda.com/redpanda-connect/components/caches/about[cache resource^] to use for storing the current latest BinLog Position that has been successfully delivered, this allows Redpanda Connect to continue from that BinLog Position upon restart, rather than consume the entire state of the table."),
		service.NewStringField(fieldCheckpointKey).
			Description("The key to use to store the snapshot position in `"+fieldCheckpointCache+"`. An alternative key can be provided if multiple CDC inputs share the same cache.").
			Default("mysql_binlog_position"),
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
	canal             *canal.Canal
	mysqlConfig       *mysql.Config
	binLogCache       string
	binLogCacheKey    string
	currentBinlogName string

	dsn            string
	tables         []string
	streamSnapshot bool

	batching                  service.BatchPolicy
	batchPolicy               *service.Batcher
	tablesFilterMap           map[string]bool
	checkPointLimit           int
	fieldSnapshotMaxBatchSize int

	logger *service.Logger
	res    *service.Resources

	rawMessageEvents chan MessageEvent
	msgChan          chan asyncMessage
	cp               *checkpoint.Capped[*position]

	shutSig *shutdown.Signaller
}

func newMySQLStreamInput(conf *service.ParsedConfig, res *service.Resources) (s service.BatchInput, err error) {
	if err := license.CheckRunningEnterprise(res); err != nil {
		return nil, err
	}

	i := mysqlStreamInput{
		logger:           res.Logger(),
		rawMessageEvents: make(chan MessageEvent),
		msgChan:          make(chan asyncMessage),
		res:              res,
	}

	var batching service.BatchPolicy

	if i.dsn, err = conf.FieldString(fieldMySQLDSN); err != nil {
		return nil, err
	}

	i.mysqlConfig, err = mysql.ParseDSN(i.dsn)
	if err != nil {
		return nil, fmt.Errorf("error parsing mysql DSN: %v", err)
	}

	if i.tables, err = conf.FieldStringList(fieldMySQLTables); err != nil {
		return nil, err
	}

	if i.streamSnapshot, err = conf.FieldBool(fieldStreamSnapshot); err != nil {
		return nil, err
	}

	if i.fieldSnapshotMaxBatchSize, err = conf.FieldInt(fieldSnapshotMaxBatchSize); err != nil {
		return nil, err
	}

	if i.checkPointLimit, err = conf.FieldInt(fieldCheckpointLimit); err != nil {
		return nil, err
	}

	if i.binLogCache, err = conf.FieldString(fieldCheckpointCache); err != nil {
		return nil, err
	}
	if !conf.Resources().HasCache(i.binLogCache) {
		return nil, fmt.Errorf("unknown cache resource: %s", i.binLogCache)
	}
	if i.binLogCacheKey, err = conf.FieldString(fieldCheckpointKey); err != nil {
		return nil, err
	}

	i.cp = checkpoint.NewCapped[*position](int64(i.checkPointLimit))

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

	r, err := service.AutoRetryNacksBatchedToggled(conf, &i)
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
	// Parse time values as time.Time values not strings
	canalConfig.ParseTime = true
	// canalConfig.Logger

	for _, table := range i.tables {
		canalConfig.IncludeTableRegex = append(canalConfig.IncludeTableRegex, regexp.QuoteMeta(table))
	}

	c, err := canal.NewCanal(canalConfig)
	if err != nil {
		return err
	}
	c.AddDumpTables(i.mysqlConfig.DBName, i.tables...)

	i.canal = c

	pos, err := i.getCachedBinlogPosition(ctx)
	if err != nil {
		return fmt.Errorf("unable to get cached binlog position: %s", err)
	}
	// create snapshot instance if we were requested and haven't finished it before.
	var snapshot *Snapshot
	if i.streamSnapshot && pos == nil {
		db, err := sql.Open("mysql", i.dsn)
		if err != nil {
			return fmt.Errorf("failed to connect to MySQL server: %s", err)
		}
		snapshot = NewSnapshot(i.logger, db)
	}

	// Reset the shutSig
	sig := shutdown.NewSignaller()
	i.shutSig = sig
	go func() {
		ctx, _ := sig.SoftStopCtx(context.Background())
		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			<-ctx.Done()
			i.canal.Close()
			return nil
		})
		wg.Go(func() error { return i.readMessages(ctx) })
		wg.Go(func() error { return i.startMySQLSync(ctx, pos, snapshot) })
		if err := wg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			i.logger.Errorf("error during MySQL CDC: %s", err)
		} else {
			i.logger.Info("successfully shutdown MySQL CDC stream")
		}
		sig.TriggerHasStopped()
	}()

	return nil
}

func (i *mysqlStreamInput) startMySQLSync(ctx context.Context, pos *position, snapshot *Snapshot) error {
	// If we are given a snapshot, then we need to read it.
	if snapshot != nil {
		startPos, err := snapshot.prepareSnapshot(ctx)
		if err != nil {
			_ = snapshot.close()
			return fmt.Errorf("unable to prepare snapshot: %w", err)
		}
		if err = i.readSnapshot(ctx, snapshot); err != nil {
			_ = snapshot.close()
			return fmt.Errorf("failed reading snapshot: %w", err)
		}
		if err = snapshot.releaseSnapshot(ctx); err != nil {
			_ = snapshot.close()
			return fmt.Errorf("unable to release snapshot: %w", err)
		}
		if err = snapshot.close(); err != nil {
			return fmt.Errorf("unable to close snapshot: %w", err)
		}
		pos = startPos
	} else if pos == nil {
		coords, err := i.canal.GetMasterPos()
		if err != nil {
			return fmt.Errorf("unable to get start binlog position: %w", err)
		}
		pos = &coords
	}
	i.logger.Infof("starting MySQL CDC stream from binlog %s at offset %d", pos.Name, pos.Pos)
	i.currentBinlogName = pos.Name
	i.canal.SetEventHandler(i)
	if err := i.canal.RunFrom(*pos); err != nil {
		return fmt.Errorf("failed to start streaming: %w", err)
	}
	return nil
}

func (i *mysqlStreamInput) readSnapshot(ctx context.Context, snapshot *Snapshot) error {
	// TODO(cdc): Process tables in parallel
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
		for {
			var batchRows *sql.Rows
			if numRowsProcessed == 0 {
				batchRows, err = snapshot.querySnapshotTable(ctx, table, tablePks, nil, i.fieldSnapshotMaxBatchSize)
			} else {
				batchRows, err = snapshot.querySnapshotTable(ctx, table, tablePks, &lastSeenPksValues, i.fieldSnapshotMaxBatchSize)
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
					Row:       row,
					Operation: MessageOperationRead,
					Table:     table,
					Position:  nil,
				}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			if err := batchRows.Err(); err != nil {
				return fmt.Errorf("failed to iterate snapshot table: %s", err)
			}

			if batchRowsCount < i.fieldSnapshotMaxBatchSize {
				break
			}
		}
	}
	return nil
}

func snapshotValueMapper[T any](v any) (any, error) {
	s, ok := v.(*sql.Null[T])
	if !ok {
		var e T
		return nil, fmt.Errorf("expected %T got %T", e, v)
	}
	if !s.Valid {
		return nil, nil
	}
	return s.V, nil
}

func prepSnapshotScannerAndMappers(cols []*sql.ColumnType) (values []any, mappers []func(any) (any, error)) {
	stringMapping := func(mapper func(s string) (any, error)) func(any) (any, error) {
		return func(v any) (any, error) {
			s, ok := v.(*sql.NullString)
			if !ok {
				return nil, fmt.Errorf("expected %T got %T", "", v)
			}
			if !s.Valid {
				return nil, nil
			}
			return mapper(s.String)
		}
	}
	for _, col := range cols {
		var val any
		var mapper func(any) (any, error)
		switch col.DatabaseTypeName() {
		case "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
			val = new(sql.Null[[]byte])
			mapper = snapshotValueMapper[[]byte]
		case "DATETIME", "TIMESTAMP":
			val = new(sql.NullTime)
			mapper = func(v any) (any, error) {
				s, ok := v.(*sql.NullTime)
				if !ok {
					return nil, fmt.Errorf("expected %T got %T", time.Time{}, v)
				}
				if !s.Valid {
					return nil, nil
				}
				return s.Time, nil
			}
		case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT", "YEAR":
			val = new(sql.NullInt64)
			mapper = func(v any) (any, error) {
				s, ok := v.(*sql.NullInt64)
				if !ok {
					return nil, fmt.Errorf("expected %T got %T", int64(0), v)
				}
				if !s.Valid {
					return nil, nil
				}
				return int(s.Int64), nil
			}
		case "DECIMAL", "NUMERIC":
			val = new(sql.NullString)
			mapper = stringMapping(func(s string) (any, error) {
				return json.Number(s), nil
			})
		case "FLOAT", "DOUBLE":
			val = new(sql.Null[float64])
			mapper = snapshotValueMapper[float64]
		case "SET":
			val = new(sql.NullString)
			mapper = stringMapping(func(s string) (any, error) {
				// This might be a little simplistic, we may need to handle escaped values
				// here...
				out := []any{}
				for _, elem := range strings.Split(s, ",") {
					out = append(out, elem)
				}
				return out, nil
			})
		case "JSON":
			val = new(sql.NullString)
			mapper = stringMapping(func(s string) (v any, err error) {
				err = json.Unmarshal([]byte(s), &v)
				return
			})
		default:
			val = new(sql.Null[string])
			mapper = snapshotValueMapper[string]
		}
		values = append(values, val)
		mappers = append(mappers, mapper)
	}
	return
}
func (i *mysqlStreamInput) readMessages(ctx context.Context) error {
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

			if err := i.flushBatch(ctx, i.cp, flushedBatch); err != nil {
				return fmt.Errorf("failed to flush periodic batch: %w", err)
			}
		case me := <-i.rawMessageEvents:
			row, err := json.Marshal(me.Row)
			if err != nil {
				return fmt.Errorf("failed to serialize row: %w", err)
			}

			mb := service.NewMessage(row)
			mb.MetaSet("operation", string(me.Operation))
			mb.MetaSet("table", me.Table)
			if me.Position != nil {
				mb.MetaSet("binlog_position", binlogPositionToString(*me.Position))
			}

			if i.batchPolicy.Add(mb) {
				nextTimedBatchChan = nil
				flushedBatch, err := i.batchPolicy.Flush(ctx)
				if err != nil {
					return fmt.Errorf("flush batch error: %w", err)
				}
				if err := i.flushBatch(ctx, i.cp, flushedBatch); err != nil {
					return fmt.Errorf("failed to flush batch: %w", err)
				}
			} else {
				d, ok := i.batchPolicy.UntilNext()
				if ok {
					nextTimedBatchChan = time.After(d)
				}
			}
		}
	}
}

func (i *mysqlStreamInput) flushBatch(
	ctx context.Context,
	checkpointer *checkpoint.Capped[*position],
	batch service.MessageBatch,
) error {
	if len(batch) == 0 {
		return nil
	}

	lastMsg := batch[len(batch)-1]
	strPosition, ok := lastMsg.MetaGet("binlog_position")
	var binLogPos *position
	if ok {
		pos, err := parseBinlogPosition(strPosition)
		if err != nil {
			return err
		}
		binLogPos = &pos
	}

	resolveFn, err := checkpointer.Track(ctx, binLogPos, int64(len(batch)))
	if err != nil {
		return fmt.Errorf("failed to track checkpoint for batch: %w", err)
	}
	msg := asyncMessage{
		msg: batch,
		ackFn: func(ctx context.Context, res error) error {
			i.mutex.Lock()
			defer i.mutex.Unlock()
			maxOffset := resolveFn()
			// Nothing to commit, this wasn't the latest message
			if maxOffset == nil {
				return nil
			}
			offset := *maxOffset
			// This has no offset - it's a snapshot message
			if offset == nil {
				return nil
			}
			return i.setCachedBinlogPosition(ctx, *offset)
		},
	}
	select {
	case i.msgChan <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (i *mysqlStreamInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case m := <-i.msgChan:
		return m.msg, m.ackFn, nil
	case <-i.shutSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case <-ctx.Done():
	}
	return nil, nil, ctx.Err()
}

func (i *mysqlStreamInput) Close(ctx context.Context) error {
	if i.shutSig == nil {
		return nil // Never connected
	}
	i.shutSig.TriggerSoftStop()
	select {
	case <-ctx.Done():
	case <-time.After(shutdownTimeout):
	case <-i.shutSig.HasStoppedChan():
	}
	i.shutSig.TriggerHardStop()
	select {
	case <-ctx.Done():
	case <-time.After(shutdownTimeout):
		i.logger.Error("failed to shutdown mysql_cdc within the timeout")
	case <-i.shutSig.HasStoppedChan():
	}
	return nil
}

// ---- input methods end ----

// ---- cache methods start ----

func (i *mysqlStreamInput) getCachedBinlogPosition(ctx context.Context) (*position, error) {
	var (
		cacheVal []byte
		cErr     error
	)
	if err := i.res.AccessCache(ctx, i.binLogCache, func(c service.Cache) {
		cacheVal, cErr = c.Get(ctx, i.binLogCacheKey)
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
	pos, err := parseBinlogPosition(string(cacheVal))
	return &pos, err
}

func (i *mysqlStreamInput) setCachedBinlogPosition(ctx context.Context, binLogPos position) error {
	var cErr error
	if err := i.res.AccessCache(ctx, i.binLogCache, func(c service.Cache) {
		cErr = c.Set(
			ctx,
			i.binLogCacheKey,
			[]byte(binlogPositionToString(binLogPos)),
			nil,
		)
	}); err != nil {
		return fmt.Errorf("unable to access cache for writing: %w", err)
	}
	if cErr != nil {
		return fmt.Errorf("unable persist checkpoint to cache: %w", cErr)
	}
	return nil
}

// ---- cache methods end ----

// --- MySQL Canal handler methods ----

func (i *mysqlStreamInput) OnRotate(eh *replication.EventHeader, re *replication.RotateEvent) error {
	i.currentBinlogName = string(re.NextLogName)
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
		// Updates send both the new and old data - we only emit the new data.
		return i.onMessage(e, 1, 2)
	default:
		return errors.New("invalid rows action")
	}
}

func (i *mysqlStreamInput) onMessage(e *canal.RowsEvent, initValue, incrementValue int) error {
	for pi := initValue; pi < len(e.Rows); pi += incrementValue {
		message := map[string]any{}
		for i, v := range e.Rows[pi] {
			col := e.Table.Columns[i]
			v, err := mapMessageColumn(v, col)
			if err != nil {
				return err
			}
			message[col.Name] = v
		}
		i.rawMessageEvents <- MessageEvent{
			Row:       message,
			Operation: MessageOperation(e.Action),
			Table:     e.Table.Name,
			Position:  &position{Name: i.currentBinlogName, Pos: e.Header.LogPos},
		}
	}
	return nil
}

func mapMessageColumn(v any, col schema.TableColumn) (any, error) {
	if v == nil {
		return v, nil
	}
	switch col.Type {
	case schema.TYPE_DECIMAL:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected string value for decimal column got: %T", v)
		}
		return json.Number(s), nil
	case schema.TYPE_SET:
		bitset, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("expected int value for set column got: %T", v)
		}
		out := []any{}
		for i, element := range col.SetValues {
			if (bitset>>i)&1 == 1 {
				out = append(out, element)
			}
		}
		return out, nil
	case schema.TYPE_DATE:
		date, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected string value for date column got: %T", v)
		}
		return time.Parse("2006-01-02", date)
	case schema.TYPE_ENUM:
		ordinal, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("expected int value for enum column got: %T", v)
		}
		if ordinal < 1 || int(ordinal) > len(col.EnumValues) {
			return nil, fmt.Errorf("enum ordinal out of range: %d when there are %d variants", ordinal, len(col.EnumValues))
		}
		return col.EnumValues[ordinal-1], nil
	case schema.TYPE_JSON:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected string value for json column got: %T", v)
		}
		var decoded any
		if err := json.Unmarshal([]byte(s), &decoded); err != nil {
			return nil, err
		}
		return decoded, nil
	case schema.TYPE_STRING:
		// Blob types should come through as binary, but are marked type 5,
		// instead skip them here and have those fallthrough to the binary case.
		if !strings.Contains(col.RawType, "blob") {
			if s, ok := v.(string); ok {
				return s, nil
			}
			s, ok := v.([]byte)
			if !ok {
				return nil, fmt.Errorf("unexpected type for STRING column: %T", v)
			}
			return string(s), nil
		}
		fallthrough
	case schema.TYPE_BINARY:
		if s, ok := v.([]byte); ok {
			return s, nil
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected type for BINARY column: %T", v)
		}
		return []byte(s), nil
	default:
		return v, nil
	}
}

// --- MySQL Canal handler methods end ----
