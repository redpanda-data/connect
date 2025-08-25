// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mssqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	_ "github.com/microsoft/go-mssqldb"
	"golang.org/x/sync/errgroup"

	"github.com/Jeffail/shutdown"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	fieldMSSQLTables      = "tables"
	fieldConnectionString = "connection_string"
	fieldBatching         = "batching"
	fieldStreamSnapshot   = "stream_snapshot"
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
	Fields(
		service.NewAutoRetryNacksToggleField(),
		service.NewBatchPolicyField(fieldBatching),
		service.NewStringListField(fieldMSSQLTables).
			Description("A list of tables to stream from the database.").
			Example([]string{"table1", "table2"}).
			LintRule("root = if this.length() == 0 { [ \"field 'tables' must contain at least one table\" ] }"),

		service.NewStringField(fieldConnectionString).
			Description("The connection string of the Microsoft SQL Server database to connect to.").
			Example("sqlserver://username:password@host/instance?param1=value&param2=value"),
		service.NewBoolField(fieldStreamSnapshot).
			Description("If set to true, the connector will query all the existing data as a part of snapshot process. Otherwise, it will start from the current Log Sequence Number position."),
	)

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

type msSqlServerCDCReader struct {
	batching    service.BatchPolicy
	batchPolicy *service.Batcher

	dbMu sync.Mutex
	db   *sql.DB

	connectionString string
	streamSnapshot   bool
	tables           []string
	trackedTables    []changeTable

	logger *service.Logger
	res    *service.Resources

	resCh   chan asyncMessage
	stopSig *shutdown.Signaller
}

func newMssqlCDCReader(conf *service.ParsedConfig, res *service.Resources) (s service.BatchInput, err error) {
	// if err := license.CheckRunningEnterprise(res); err != nil {
	// 	return nil, err
	// }

	r := msSqlServerCDCReader{
		logger:  res.Logger(),
		res:     res,
		resCh:   make(chan asyncMessage),
		stopSig: shutdown.NewSignaller(),
	}

	//TODO: Can we validate the connection string?
	if r.connectionString, err = conf.FieldString(fieldConnectionString); err != nil {
		return nil, err
	}

	// TODO: support regular expression on tablenames
	if r.tables, err = conf.FieldStringList(fieldMSSQLTables); err != nil {
		return nil, err
	}

	if r.streamSnapshot, err = conf.FieldBool(fieldStreamSnapshot); err != nil {
		return nil, err
	}

	var batching service.BatchPolicy

	if batching, err = conf.FieldBatchPolicy(fieldBatching); err != nil {
		return nil, err
	} else if batching.IsNoop() {
		batching.Count = 1
	}

	r.batching = batching
	if r.batchPolicy, err = r.batching.NewBatcher(res); err != nil {
		return nil, err
	} else if batching.IsNoop() {
		batching.Count = 1
	}

	batchInput, err := service.AutoRetryNacksBatchedToggled(conf, &r)
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
	defer r.dbMu.Unlock()
	r.db = db

	// TODO: Get lsn from cache

	var snapshot *Snapshot
	if r.streamSnapshot {
		snapshot = NewSnapshot(r.logger, r.db)
	}

	sig := shutdown.NewSignaller()
	ctx, done := sig.SoftStopCtx(context.Background())
	defer done()

	// var startLSN LSN
	if snapshot != nil {
		if err := snapshot.prepare(ctx, r.tables); err != nil {
			return fmt.Errorf("failed to process snapshot: %w", err)
		}
	}

	if r.trackedTables, err = r.fetchChangeTables(ctx); err != nil {
		return fmt.Errorf("failed to connect to Microsoft SQL Server: %s", err)
	}

	r.stopSig = shutdown.NewSignaller()
	go func() {
		ctx, _ = r.stopSig.SoftStopCtx(context.Background())
		wg, ctx := errgroup.WithContext(ctx)

		wg.Go(func() error { return r.readMessages(ctx) })
		if err := wg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			r.logger.Errorf("error during Microsoft SQL Server CDC: %s", err)
		} else {
			r.logger.Info("successfully shutdown Microsoft SQL Server CDC stream")
		}

		sig.TriggerHasStopped()
	}()

	return nil
}

func (r *msSqlServerCDCReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-r.stopSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case am := <-r.resCh:
		return am.msg, am.ackFn, nil
	}
}

func (r *msSqlServerCDCReader) readMessages(ctx context.Context) error {
	return nil
}

func (r *msSqlServerCDCReader) Close(ctx context.Context) error {
	r.dbMu.Lock()
	defer r.dbMu.Unlock()
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}

type changeTable struct {
	captureInstance string
	startLSN        []byte
}

// fetchChangeTables returns a slice of all change tables matching those configured.
func (r *msSqlServerCDCReader) fetchChangeTables(ctx context.Context) ([]changeTable, error) {
	rows, err := r.db.QueryContext(ctx, "SELECT capture_instance, start_lsn FROM cdc.change_tables")
	if err != nil {
		return nil, fmt.Errorf("fetching change tables: %w", err)
	}

	var result []changeTable
	for rows.Next() {
		var t changeTable
		if err := rows.Scan(&t.captureInstance, &t.startLSN); err != nil {
			return nil, fmt.Errorf("loading table: %w", err)
		}
		result = append(result, t)
	}
	return result, nil
}
