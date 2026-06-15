// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package saphana

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	gohdb "github.com/SAP/go-hdb/driver"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	shoFieldDSN         = "dsn"
	shoFieldSchemaName  = "schema_name"
	shoFieldTable       = "table"
	shoFieldColumns     = "columns"
	shoFieldArgsMapping = "args_mapping"
	shoFieldMaxInFlight = "max_in_flight"
)

var sapHANAOutputConfigSpec = service.NewConfigSpec().
	Categories("Services").
	Version("4.92.0").
	Summary("Writes rows to a SAP HANA table using native bulk insert.").
	Description(`Inserts batches of rows into a SAP HANA table using a prepared statement. go-hdb batches all rows into a single ` + "`MtInsert`" + ` RPC when the argument count is a multiple of the column count, minimising round-trips.

Each message is mapped to an ordered list of column values via ` + "`args_mapping`" + `. Configure ` + "`batching`" + ` on this output to control how many rows are bundled per INSERT call.
`).
	Field(service.NewStringField(shoFieldDSN).
		Description("SAP HANA connection DSN in `hdb://user:password@host:port` form.").
		Example("hdb://user:password@host:39017").
		Secret(),
	).
	Field(service.NewStringField(shoFieldSchemaName).
		Description("Database schema for the table.").
		Optional(),
	).
	Field(service.NewStringField(shoFieldTable).
		Description("Table to insert into.").
		Example("MY_TABLE"),
	).
	Field(service.NewStringListField(shoFieldColumns).
		Description("Ordered list of column names to insert."),
	).
	Field(service.NewBloblangField(shoFieldArgsMapping).
		Description("Bloblang mapping that produces an array of values for each message, one element per column in the same order as `columns`.").
		Example("root = [this.id, this.name, this.value]"),
	).
	Field(service.NewIntField(shoFieldMaxInFlight).
		Description("Maximum number of concurrent batch INSERT calls.").
		Default(1),
	).
	Field(service.NewBatchPolicyField("batching"))

func init() {
	service.MustRegisterBatchOutput("sap_hana", sapHANAOutputConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			o, err := newSAPHANAOutput(conf, mgr)
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			batchPolicy, err := conf.FieldBatchPolicy("batching")
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			maxInFlight, err := conf.FieldInt(shoFieldMaxInFlight)
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			return o, batchPolicy, maxInFlight, nil
		})
}

type sapHANAOutput struct {
	dsn        string
	schemaName string
	tableName  string
	insertSQL  string
	numCols    int
	argsExec   *bloblang.Executor

	db    *sql.DB
	stmt  *sql.Stmt
	dbMut sync.Mutex
	log   *service.Logger
}

func (o *sapHANAOutput) tableRef() string {
	if o.schemaName != "" {
		return quoteIdentifier(o.schemaName) + "." + quoteIdentifier(o.tableName)
	}
	return quoteIdentifier(o.tableName)
}

func newSAPHANAOutput(conf *service.ParsedConfig, mgr *service.Resources) (*sapHANAOutput, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}
	o := &sapHANAOutput{log: mgr.Logger()}

	var err error
	if o.dsn, err = conf.FieldString(shoFieldDSN); err != nil {
		return nil, err
	}

	if conf.Contains(shoFieldSchemaName) {
		if o.schemaName, err = conf.FieldString(shoFieldSchemaName); err != nil {
			return nil, err
		}
	}
	if o.tableName, err = conf.FieldString(shoFieldTable); err != nil {
		return nil, err
	}

	columns, err := conf.FieldStringList(shoFieldColumns)
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("field %q must not be empty", shoFieldColumns)
	}
	o.numCols = len(columns)

	quotedCols := make([]string, len(columns))
	for i, c := range columns {
		quotedCols[i] = quoteIdentifier(c)
	}
	placeholders := strings.Repeat("?,", len(columns))
	placeholders = placeholders[:len(placeholders)-1]
	o.insertSQL = fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`,
		o.tableRef(), strings.Join(quotedCols, ", "), placeholders)

	if o.argsExec, err = conf.FieldBloblang(shoFieldArgsMapping); err != nil {
		return nil, err
	}

	return o, nil
}

func (o *sapHANAOutput) Connect(ctx context.Context) error {
	o.dbMut.Lock()
	defer o.dbMut.Unlock()

	if o.db != nil {
		return nil
	}

	connector, err := gohdb.NewDSNConnector(o.dsn)
	if err != nil {
		return fmt.Errorf("creating SAP HANA connector: %w", err)
	}
	db := sql.OpenDB(connector)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("pinging SAP HANA: %w", err)
	}
	stmt, err := db.PrepareContext(ctx, o.insertSQL)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("preparing insert statement: %w", err)
	}

	o.db = db
	o.stmt = stmt
	o.log.Debug("Connected to SAP HANA.")
	return nil
}

func (o *sapHANAOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	o.dbMut.Lock()
	db := o.db
	stmt := o.stmt
	o.dbMut.Unlock()

	if db == nil {
		return service.ErrNotConnected
	}

	exec := batch.BloblangExecutor(o.argsExec)
	args := make([]any, 0, len(batch)*o.numCols)

	for i := range batch {
		msg, err := exec.Query(i)
		if err != nil {
			return fmt.Errorf("message %d: args_mapping failed: %w", i, err)
		}
		raw, err := msg.AsStructured()
		if err != nil {
			return fmt.Errorf("message %d: args_mapping result not structured: %w", i, err)
		}
		vals, ok := raw.([]any)
		if !ok {
			return fmt.Errorf("message %d: args_mapping must return an array, got %T", i, raw)
		}
		if len(vals) != o.numCols {
			return fmt.Errorf("message %d: args_mapping returned %d values, expected %d", i, len(vals), o.numCols)
		}
		args = append(args, vals...)
	}

	if _, err := stmt.ExecContext(ctx, args...); err != nil {
		return fmt.Errorf("bulk insert: %w", err)
	}
	return nil
}

func (o *sapHANAOutput) Close(_ context.Context) error {
	o.dbMut.Lock()
	defer o.dbMut.Unlock()

	if o.stmt != nil {
		_ = o.stmt.Close()
		o.stmt = nil
	}
	if o.db != nil {
		err := o.db.Close()
		o.db = nil
		return err
	}
	return nil
}
