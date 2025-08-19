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
	"fmt"

	"github.com/Jeffail/shutdown"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	fieldMSSQLTables      = "tables"
	fieldConnectionString = "connection_string"
	fieldBatching         = "batching"
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

		service.NewStringField(fieldConnectionString).
			Description("The connection string of the Microsoft SQL Server database to connect to.").
			Example("sqlserver://username:password@host/instance"),
	)

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

type msSqlServerCDCReader struct {
	connectionString string
	batching         service.BatchPolicy
	batchPolicy      *service.Batcher
	logger           *service.Logger
	res              *service.Resources
	db               *sql.DB

	resCh   chan asyncMessage
	stopSig *shutdown.Signaller
}

func newMssqlCDCReader(conf *service.ParsedConfig, res *service.Resources) (s service.BatchInput, err error) {
	if err := license.CheckRunningEnterprise(res); err != nil {
		return nil, err
	}

	r := msSqlServerCDCReader{
		logger: res.Logger(),
		resCh:  make(chan asyncMessage),
		res:    res,
	}

	if r.connectionString, err = conf.FieldString(fieldConnectionString); err != nil {
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

func (r *msSqlServerCDCReader) Connect(_ context.Context) error {
	db, err := sql.Open("sqlserver", r.connectionString)
	if err != nil {
		return fmt.Errorf("failed to connect to Microsoft SQL Server: %s", err)
	}
	r.db = db

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

func (r *msSqlServerCDCReader) Close(ctx context.Context) error {
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}
