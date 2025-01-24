// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package snowflake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming"
)

type schemaMigrationNeededError struct {
	runMigration func(ctx context.Context, evolver *snowpipeSchemaEvolver) error
}

func (*schemaMigrationNeededError) Error() string {
	return "schema migration was required and the operation needs to be retried after the migration"
}

func asSchemaMigrationError(err error) (*schemaMigrationNeededError, bool) {
	var nullColumnErr *streaming.NonNullColumnError
	if errors.As(err, &nullColumnErr) {
		// Return an error so that we release our read lock and can take the write lock
		// to forcibly reopen all our channels to get a new schema.
		return &schemaMigrationNeededError{
			runMigration: func(ctx context.Context, evolver *snowpipeSchemaEvolver) error {
				return evolver.MigrateNotNullColumn(ctx, nullColumnErr)
			},
		}, true
	}
	var missingColumnErr *streaming.MissingColumnError
	if errors.As(err, &missingColumnErr) {
		return &schemaMigrationNeededError{
			runMigration: func(ctx context.Context, evolver *snowpipeSchemaEvolver) error {
				return evolver.MigrateMissingColumn(ctx, missingColumnErr)
			},
		}, true
	}
	var batchErr *streaming.BatchSchemaMismatchError[*streaming.MissingColumnError]
	if errors.As(err, &batchErr) {
		return &schemaMigrationNeededError{
			runMigration: func(ctx context.Context, evolver *snowpipeSchemaEvolver) error {
				for _, missingCol := range batchErr.Errors {
					// TODO(rockwood): Consider a batch SQL statement that adds N columns at a time
					if err := evolver.MigrateMissingColumn(ctx, missingCol); err != nil {
						return err
					}
				}
				return nil
			},
		}, true
	}
	return nil, false
}

type snowpipeSchemaEvolver struct {
	schemaEvolutionMapping *bloblang.Executor
	pipeline               []*service.OwnedProcessor
	logger                 *service.Logger
	// The evolver does not close nor own this rest client.
	restClient              *streaming.SnowflakeRestClient
	db, schema, table, role string
}

func (o *snowpipeSchemaEvolver) ComputeMissingColumnType(ctx context.Context, col *streaming.MissingColumnError) (string, error) {
	if len(o.pipeline) == 0 && o.schemaEvolutionMapping == nil {
		// The default mapping if not specified by a user
		switch col.Value().(type) {
		case []byte:
			return "BINARY", nil
		case string:
			return "STRING", nil
		case bool:
			return "BOOLEAN", nil
		case time.Time:
			return "TIMESTAMP", nil
		case json.Number, int, int64, int32, int16, int8, uint, uint64, uint32, uint16, uint8, float32, float64:
			return "DOUBLE", nil
		default:
			return "VARIANT", nil
		}
	}
	msg := col.Message().Copy()
	original, err := msg.AsStructuredMut()
	if err != nil {
		// This should never happen, we had to get the data as structured to be able to know it was a missing column type
		return "", fmt.Errorf("unable to extract JSON data from message that caused schema evolution: %w", err)
	}
	msg.SetError(nil) // Clear error
	msg.SetStructuredMut(map[string]any{
		"name":    col.RawName(),
		"value":   col.Value(),
		"message": original,
		"db":      o.db,
		"schema":  o.schema,
		"table":   o.table,
	})
	batches, err := service.ExecuteProcessors(ctx, o.pipeline, service.MessageBatch{msg})
	if err != nil {
		return "", fmt.Errorf("failure to execute %s.%s prior to schema evolution: %w", ssoFieldSchemaEvolution, ssoFieldSchemaEvolutionProcessors, err)
	}
	if len(batches) != 1 {
		return "", fmt.Errorf("expected a single batch output from %s.%s, got: %d", ssoFieldSchemaEvolution, ssoFieldSchemaEvolutionProcessors, len(batches))
	}
	batch := batches[0]
	if len(batch) != 1 {
		return "", fmt.Errorf("expected a single message output from %s.%s, got: %d", ssoFieldSchemaEvolution, ssoFieldSchemaEvolutionProcessors, len(batch))
	}
	msg = batch[0]
	if err := msg.GetError(); err != nil {
		return "", fmt.Errorf("message failure executing %s.%s prior to schema evolution: %w", ssoFieldSchemaEvolution, ssoFieldSchemaEvolutionProcessors, err)
	}
	if o.schemaEvolutionMapping != nil {
		msg, err = msg.BloblangQuery(o.schemaEvolutionMapping)
		if err != nil {
			return "", fmt.Errorf("unable to compute new column type for %s: %w", col.ColumnName(), err)
		}
	}
	v, err := msg.AsBytes()
	if err != nil {
		return "", fmt.Errorf("unable to extract result from new column type mapping for %s: %w", col.ColumnName(), err)
	}
	columnType := string(v)
	if err := validateColumnType(columnType); err != nil {
		return "", err
	}
	return columnType, nil
}

func (o *snowpipeSchemaEvolver) MigrateMissingColumn(ctx context.Context, col *streaming.MissingColumnError) error {
	columnType, err := o.ComputeMissingColumnType(ctx, col)
	if err != nil {
		return err
	}
	o.logger.Infof("identified new schema - attempting to alter table to add column: %s %s", col.ColumnName(), columnType)
	err = o.RunSQLMigration(
		ctx,
		// This looks very scary and it *should*. This is prone to SQL injection attacks. The column name is
		// quoted according to the rules in Snowflake's documentation. This is also why we need to
		// validate the data type, so that you can't sneak an injection attack in there.
		fmt.Sprintf(`ALTER TABLE IDENTIFIER(?)
    ADD COLUMN IF NOT EXISTS %s %s
      COMMENT 'column created by schema evolution from Redpanda Connect'`,
			col.ColumnName(),
			columnType,
		),
	)
	if err != nil {
		o.logger.Warnf("unable to add new column %s, this maybe due to a race with another request, error: %s", col.ColumnName(), err)
	}
	return nil
}

func (o *snowpipeSchemaEvolver) MigrateNotNullColumn(ctx context.Context, col *streaming.NonNullColumnError) error {
	o.logger.Infof("identified new schema - attempting to alter table to remove null constraint on column: %s", col.ColumnName())
	err := o.RunSQLMigration(
		ctx,
		// This looks very scary and it *should*. This is prone to SQL injection attacks. The column name here
		// comes directly from the Snowflake API so it better not have a SQL injection :)
		fmt.Sprintf(`ALTER TABLE IDENTIFIER(?) ALTER
      %s DROP NOT NULL,
      %s COMMENT 'column altered to be nullable by schema evolution from Redpanda Connect'`,
			col.ColumnName(),
			col.ColumnName(),
		),
	)
	if err != nil {
		o.logger.Warnf("unable to mark column %s as null, this maybe due to a race with another request, error: %s", col.ColumnName(), err)
	}
	return nil
}

func (o *snowpipeSchemaEvolver) CreateOutputTable(ctx context.Context, batch service.MessageBatch) error {
	if len(batch) == 0 {
		return errors.New("cannot create a table from an empty batch")
	}
	o.logger.Infof("identified write to non-existing table - attempting to create table: %s", o.table)
	msg := batch[0] // we assume messages are uniform - otherwise normal schema evolution will be able to evolve the table.
	v, err := msg.AsStructured()
	if err != nil {
		return err
	}
	row, ok := v.(map[string]any)
	if !ok {
		return fmt.Errorf("unable to extract row from column, expected object but got: %T", v)
	}
	columns := []string{}
	for k, v := range row {
		col := streaming.NewMissingColumnError(msg, k, v)
		colType, err := o.ComputeMissingColumnType(ctx, col)
		if err != nil {
			return err
		}
		columns = append(columns, fmt.Sprintf("%s %s", col.ColumnName(), colType))
	}
	return o.RunSQLMigration(
		ctx,
		// This looks very scary and it *should*. This is prone to SQL injection attacks. The column name is
		// quoted according to the rules in Snowflake's documentation (via col.ColumnName()). This is also why we need to
		// validate the data type, so that you can't sneak an injection attack in there.
		fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS IDENTIFIER(?) (%s) COMMENT = 'table created via schema evolution from Redpanda Connect'`,
			strings.Join(columns, ", "),
		),
	)
}

func (o *snowpipeSchemaEvolver) RunSQLMigration(ctx context.Context, statement string) error {
	_, err := o.restClient.RunSQL(ctx, streaming.RunSQLRequest{
		Statement: statement,
		// Currently we set a of timeout of 30 seconds so that we don't have to handle async operations
		// that need polling to wait until they finish (results are made async when execution is longer
		// than 45 seconds).
		Timeout:  30,
		Database: o.db,
		Schema:   o.schema,
		Role:     o.role,
		Bindings: map[string]streaming.BindingValue{
			"1": {Type: "TEXT", Value: o.table},
		},
	})
	return err
}

// This doesn't need to fully match, but be enough to prevent SQL injection as well as
// catch common errors.
var validColumnTypeRegex = regexp.MustCompile(`^\s*(?i:NUMBER|DECIMAL|NUMERIC|INT|INTEGER|BIGINT|SMALLINT|TINYINT|BYTEINT|FLOAT|FLOAT4|FLOAT8|DOUBLE|DOUBLE\s+PRECISION|REAL|VARCHAR|CHAR|CHARACTER|STRING|TEXT|BINARY|VARBINARY|BOOLEAN|DATE|DATETIME|TIME|TIMESTAMP|TIMESTAMP_LTZ|TIMESTAMP_NTZ|TIMESTAMP_TZ|VARIANT|OBJECT|ARRAY)\s*(?:\(\s*\d+\s*\)|\(\s*\d+\s*,\s*\d+\s*\))?\s*$`)

func validateColumnType(v string) error {
	if validColumnTypeRegex.MatchString(v) {
		return nil
	}
	return fmt.Errorf("invalid Snowflake column data type: %s", v)
}
