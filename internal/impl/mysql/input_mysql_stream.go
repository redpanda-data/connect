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
	"github.com/Jeffail/checkpoint"
	"github.com/go-mysql-org/go-mysql/canal"
	mysqlReplications "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-sql-driver/mysql"
	"github.com/redpanda-data/benthos/v4/public/service"
	"sync"
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
	// canal represents mysql binlog listener connection
	canal       *canal.Canal
	mysqlConfig *mysql.Config
	canal.DummyEventHandler
	startBinLogPosition *mysqlReplications.Position

	dsn            string
	tables         []string
	flavor         string
	streamSnapshot bool

	cMut     sync.Mutex
	msgChan  chan asyncMessage
	batching service.BatchPolicy

	logger *service.Logger
}

func newMySQLStreamInput(conf *service.ParsedConfig, mgr *service.Resources) (s service.BatchInput, err error) {
	streamInput := mysqlStreamInput{
		logger: mgr.Logger(),
	}

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

	if binLogCacheKey, err := conf.FieldString(fieldCheckpointKey); err != nil {
		return nil, err
	} else {
		if err := mgr.AccessCache(context.Background(), binLogCacheKey, func(c service.Cache) {
			binLogPositionBytes, cErr := c.Get(context.Background(), binLogCacheKey)
			if err != nil {
				if !errors.Is(cErr, service.ErrKeyNotFound) {
					mgr.Logger().With("error", cErr.Error()).Error("Failed to obtain cursor cache item.")
				}
				return
			}

			var storedMySQLBinLogPosition mysqlReplications.Position
			if err = json.Unmarshal(binLogPositionBytes, &storedMySQLBinLogPosition); err != nil {
				mgr.Logger().With("error", err.Error()).Error("Failed to unmarshal stored binlog position.")
				return
			}

			streamInput.startBinLogPosition = &storedMySQLBinLogPosition
			binLogCacheKey = string(binLogPositionBytes)
		}); err != nil {
			mgr.Logger().With("error", err.Error()).Error("Failed to access cursor cache.")
		}
	}

	i := &streamInput

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

	// register event handler
	c.SetEventHandler(i)

	i.canal = c
	go i.startMySQLSync()
	return nil
}

func (i *mysqlStreamInput) startMySQLSync() {
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

		if err := i.canal.RunFrom(coords); err != nil {
			panic(err)
		}
	}
}

func (i *mysqlStreamInput) flushBatch(ctx context.Context, checkpointer *checkpoint.Capped[*int64], msg service.MessageBatch, lsn *int64) bool {
	if msg == nil {
		return true
	}

	resolveFn, err := checkpointer.Track(ctx, lsn, int64(len(msg)))
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
			i.cMut.Lock()
			defer i.cMut.Unlock()

			// todo;; store offset

			return nil
		},
	}:
	case <-ctx.Done():
		return false
	}

	return true
}

func (i *mysqlStreamInput) onMessage(e *canal.RowsEvent, params ProcessEventParams) error {
	for i := params.initValue; i < len(e.Rows); i += params.incrementValue {
		message := map[string]any{}
		for i, v := range e.Rows[i] {
			message[e.Table.Columns[i].Name] = v
		}

		fmt.Println("mysql row", message)
	}
	return nil
}

func (i *mysqlStreamInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	i.cMut.Lock()
	msgChan := i.msgChan
	i.cMut.Unlock()
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
	i.canal.Close()
	return nil
}

// ---- Redpanda Connect specific methods end----

// --- MySQL Canal handler methods ----

func (i *mysqlStreamInput) OnRotate(*replication.EventHeader, *replication.RotateEvent) error {
	return nil
}
func (i *mysqlStreamInput) OnTableChanged(*replication.EventHeader, string, string) error {
	return nil
}
func (i *mysqlStreamInput) OnDDL(*replication.EventHeader, mysqlReplications.Position, *replication.QueryEvent) error {
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

func (i *mysqlStreamInput) OnXID(*replication.EventHeader, mysqlReplications.Position) error {
	return nil
}
func (i *mysqlStreamInput) OnGTID(*replication.EventHeader, mysqlReplications.BinlogGTIDEvent) error {
	return nil
}
func (i *mysqlStreamInput) OnPosSynced(*replication.EventHeader, mysqlReplications.Position, mysqlReplications.GTIDSet, bool) error {
	return nil
}

// --- MySQL Canal handler methods end ----
