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
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/go-mysql-org/go-mysql/canal"
	mysqlReplications "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-sql-driver/mysql"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	fieldMySQLDSN        = "dsn"
	fieldMySQLTables     = "tables"
	fieldStreamSnapshot  = "stream_snapshot"
	fieldBatching        = "batching"
	fieldCheckpointKey   = "checkpoint_key"
	fieldCheckpointLimit = "checkpoint_limit"
	fieldFlavor          = "flavor"
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
		service.NewStringField(fieldCheckpointKey).
			Description("The key to store the last processed binlog position."),
		service.NewStringField(fieldFlavor).
			Description("The flavor of MySQL to connect to.").
			Example("mysql"),
		service.NewBoolField(fieldStreamSnapshot).
			Description("If set to true, the connector will perform a backup to get a snapshot of the database. Otherwise, it will start from the current binlog position."),
		service.NewAutoRetryNacksToggleField(),
		service.NewIntField(fieldCheckpointLimit).
			Description("The maximum number of messages that can be processed at a given time. Increasing this limit enables parallel processing and batching at the output level. Any given LSN will not be acknowledged unless all messages under that offset are delivered in order to preserve at least once delivery guarantees.").
			Default(1024),
		service.NewBatchPolicyField(fieldBatching),
	)

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

type mysqlStreamInput struct {
	mutex sync.Mutex
	// canal stands for mysql binlog listener connection
	canal       *canal.Canal
	mysqlConfig *mysql.Config
	canal.DummyEventHandler
	startBinLogPosition *mysqlReplications.Position
	currentLogPosition  *mysqlReplications.Position
	binLogCache         string

	dsn            string
	tables         []string
	flavor         string
	streamSnapshot bool

	rawMessageEvents chan MessageEvent
	msgChan          chan asyncMessage
	batching         service.BatchPolicy
	batchPolicy      *service.Batcher
	checkPointLimit  int
	lastGtid         *string

	logger *service.Logger
	res    *service.Resources

	streaCtx context.Context
	cp       *checkpoint.Capped[*int64]
}

const binLogCacheKey = "mysql_binlog_position"

func newMySQLStreamInput(conf *service.ParsedConfig, res *service.Resources) (s service.BatchInput, err error) {
	streamInput := mysqlStreamInput{
		logger:           res.Logger(),
		rawMessageEvents: make(chan MessageEvent),
		msgChan:          make(chan asyncMessage),
		res:              res,
		streaCtx:         context.Background(),
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

	if streamInput.flavor, err = conf.FieldString(fieldFlavor); err != nil {
		return nil, err
	}

	if streamInput.streamSnapshot, err = conf.FieldBool(fieldStreamSnapshot); err != nil {
		return nil, err
	}

	if streamInput.checkPointLimit, err = conf.FieldInt(fieldCheckpointLimit); err != nil {
		return nil, err
	}

	if streamInput.binLogCache, err = conf.FieldString(fieldCheckpointKey); err != nil {
		return nil, err
	} else {
		if err := res.AccessCache(context.Background(), streamInput.binLogCache, func(c service.Cache) {
			binLogPositionBytes, cErr := c.Get(context.Background(), binLogCacheKey)
			if err != nil {
				if !errors.Is(cErr, service.ErrKeyNotFound) {
					res.Logger().Errorf("failed to obtain cursor cache item. %v", cErr)
				}
				return
			}

			var storedMySQLBinLogPosition mysqlReplications.Position
			if err = json.Unmarshal(binLogPositionBytes, &storedMySQLBinLogPosition); err != nil {
				res.Logger().With("error", err.Error()).Error("Failed to unmarshal stored binlog position.")
				return
			}

			streamInput.startBinLogPosition = &storedMySQLBinLogPosition
		}); err != nil {

			res.Logger().With("error", err.Error()).Error("Failed to access cursor cache.")
		}
	}

	i := &streamInput
	i.cp = checkpoint.NewCapped[*int64](int64(i.checkPointLimit))

	res.Logger().Info("Starting MySQL stream input")

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

	r, err := service.AutoRetryNacksBatchedToggled(conf, i)
	if err != nil {
		return nil, err
	}

	return conf.WrapBatchInputExtractTracingSpanMapping("mysql_stream", r)
}

func init() {
	err := service.RegisterBatchInput(
		"mysql_stream", mysqlStreamConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newMySQLStreamInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

// ---- Redpanda Connect specific methods----

func (i *mysqlStreamInput) Connect(ctx context.Context) error {
	canalConfig := canal.NewDefaultConfig()
	canalConfig.Flavor = i.flavor
	canalConfig.Addr = i.mysqlConfig.Addr
	canalConfig.User = i.mysqlConfig.User
	canalConfig.Password = i.mysqlConfig.Passwd
	canalConfig.Dump.TableDB = i.mysqlConfig.DBName

	// Parse and set additional parameters
	canalConfig.Charset = i.mysqlConfig.Collation
	if i.mysqlConfig.TLS != nil {
		canalConfig.TLSConfig = i.mysqlConfig.TLS
	}
	canalConfig.ParseTime = true
	canalConfig.IncludeTableRegex = i.tables

	c, err := canal.NewCanal(canalConfig)
	if err != nil {
		return err
	}
	c.AddDumpTables(i.mysqlConfig.DBName, i.tables...)

	i.canal = c
	go i.startMySQLSync(ctx)
	return nil
}

func (i *mysqlStreamInput) readMessages(ctx context.Context) error {
	var nextTimedBatchChan <-chan time.Time
	var latestPos *mysqlReplications.Position

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-nextTimedBatchChan:
			nextTimedBatchChan = nil
			flushedBatch, err := i.batchPolicy.Flush(ctx)
			if err != nil {
				i.logger.Debugf("Timed flush batch error: %w", err)
				break
			}

			if ok := i.flushBatch(ctx, i.cp, flushedBatch, latestPos); !ok {
				break
			}
		case me := <-i.rawMessageEvents:
			row, err := json.Marshal(me.Row)
			if err != nil {
				return err
			}

			mb := service.NewMessage(row)
			mb.MetaSet("operation", string(me.Operation))
			mb.MetaSet("table", me.Table)
			mb.MetaSet("type", string(me.Type))
			if me.Position != nil {
				latestPos = me.Position
			}

			if i.batchPolicy.Add(mb) {
				nextTimedBatchChan = nil
				flushedBatch, err := i.batchPolicy.Flush(ctx)
				if err != nil {
					i.logger.Debugf("Flush batch error: %w", err)
					break
				}
				if ok := i.flushBatch(ctx, i.cp, flushedBatch, latestPos); !ok {
					break
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

func (i *mysqlStreamInput) startMySQLSync(ctx context.Context) {
	i.canal.SetEventHandler(i)
	go i.readMessages(ctx)
	// If we require snapshot streaming && we don't have a binlog position cache
	// initiate default run for Canal to process snapshot and start incremental sync of binlog
	if i.streamSnapshot && i.startBinLogPosition == nil {
		// Doesn't work at the moment
		if err := i.canal.Run(); err != nil {
			panic(err)
		}
	} else {
		coords, _ := i.canal.GetMasterPos()
		// starting from the last stored binlog position
		if i.startBinLogPosition != nil {
			coords = *i.startBinLogPosition
		}

		i.currentLogPosition = &coords
		if err := i.canal.RunFrom(coords); err != nil {
			panic(err)
		}
	}
}

func (i *mysqlStreamInput) flushBatch(ctx context.Context, checkpointer *checkpoint.Capped[*int64], msg service.MessageBatch, binLogPos *mysqlReplications.Position) bool {
	if msg == nil {
		return true
	}

	posInInt := int64(binLogPos.Pos)
	resolveFn, err := checkpointer.Track(ctx, &posInInt, int64(len(msg)))
	if err != nil {
		if ctx.Err() == nil {
			i.logger.Errorf("Failed to checkpoint offset: %v\n", err)
		}
		return false
	}

	select {
	case i.msgChan <- asyncMessage{
		msg: msg,
		ackFn: func(ctx context.Context, res error) error {
			maxOffset := resolveFn()
			if maxOffset == nil {
				return nil
			}

			if err := i.syncBinlogPosition(context.Background()); err != nil {
				return err
			}

			return nil
		},
	}:
	case <-ctx.Done():
		return false
	}

	return true
}

func (i *mysqlStreamInput) onMessage(e *canal.RowsEvent, params ProcessEventParams) error {
	i.mutex.Lock()
	i.currentLogPosition.Pos = e.Header.LogPos
	i.mutex.Unlock()

	for pi := params.initValue; pi < len(e.Rows); pi += params.incrementValue {
		message := map[string]any{}
		for i, v := range e.Rows[pi] {
			message[e.Table.Columns[i].Name] = v
		}

		i.rawMessageEvents <- MessageEvent{
			Row:       message,
			Operation: MessageOperation(e.Action),
			Type:      MessageTypeStreaming,
			Table:     e.Table.Name,
			Position:  i.currentLogPosition,
		}
	}

	return nil
}

func (i *mysqlStreamInput) syncBinlogPosition(ctx context.Context) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	if i.currentLogPosition == nil {
		i.logger.Warn("No current bingLog position")
		return errors.New("no current binlog position")
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
		cErr = c.Set(ctx, binLogCacheKey, positionInByte, nil)
		if cErr != nil {
			i.logger.Errorf("Failed to store binlog position: %v", cErr)
		}
	}); err != nil {
		fmt.Println("Access cache error", err)
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
	case <-ctx.Done():
	}

	return nil, nil, ctx.Err()
}

func (i *mysqlStreamInput) Close(ctx context.Context) error {
	i.canal.SyncedPosition()
	i.canal.Close()
	return nil
}

// ---- Redpanda Connect specific methods end----

// --- MySQL Canal handler methods ----

func (i *mysqlStreamInput) OnRotate(eh *replication.EventHeader, re *replication.RotateEvent) error {
	i.mutex.Lock()
	flushedBatch, err := i.batchPolicy.Flush(i.streaCtx)
	if err != nil {
		i.logger.Debugf("Flush batch error: %w", err)
		return err
	}

	if ok := i.flushBatch(i.streaCtx, i.cp, flushedBatch, i.currentLogPosition); !ok {
		return errors.New("failed to flush batch")
	}

	i.currentLogPosition.Pos = uint32(re.Position)
	i.currentLogPosition.Name = string(re.NextLogName)
	i.mutex.Unlock()

	return nil
}

func (i *mysqlStreamInput) OnRow(e *canal.RowsEvent) error {
	switch e.Action {
	case canal.InsertAction:
		return i.onMessage(e, ProcessEventParams{initValue: 0, incrementValue: 1})
	case canal.DeleteAction:
		return i.onMessage(e, ProcessEventParams{initValue: 0, incrementValue: 1})
	case canal.UpdateAction:
		return i.onMessage(e, ProcessEventParams{initValue: 1, incrementValue: 2})
	default:
		return errors.New("invalid rows action")
	}
}

func (i *mysqlStreamInput) OnPosSynced(eh *replication.EventHeader, pos mysqlReplications.Position, gtid mysqlReplications.GTIDSet, synced bool) error {
	i.mutex.Lock()
	i.currentLogPosition = &pos
	i.mutex.Unlock()

	return i.syncBinlogPosition(context.Background())
}

// --- MySQL Canal handler methods end ----
