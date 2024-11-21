// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/Masterminds/squirrel"

	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// InsertProcessorConfig returns a config spec for an sql_insert processor.
func InsertProcessorConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Categories("Integration").
		Summary("Inserts rows into an SQL database for each message, and leaves the message unchanged.").
		Description(`
If the insert fails to execute then the message will still remain unchanged and the error can be caught using xref:configuration:error_handling.adoc[error handling methods].`).
		Field(driverField).
		Field(dsnField).
		Field(service.NewStringField("table").
			Description("The table to insert to.").
			Example("foo")).
		Field(service.NewStringListField("columns").
			Description("A list of columns to insert.").
			Example([]string{"foo", "bar", "baz"})).
		Field(service.NewBloblangField("args_mapping").
			Description("A xref:guides:bloblang/about.adoc[Bloblang mapping] which should evaluate to an array of values matching in size to the number of columns specified.").
			Example("root = [ this.cat.meow, this.doc.woofs[0] ]").
			Example(`root = [ meta("user.id") ]`)).
		Field(service.NewStringField("prefix").
			Description("An optional prefix to prepend to the insert query (before INSERT).").
			Optional().
			Advanced()).
		Field(service.NewStringField("suffix").
			Description("An optional suffix to append to the insert query.").
			Optional().
			Advanced().
			Example("ON CONFLICT (name) DO NOTHING")).
		Field(service.NewStringListField("options").
			Description("A list of keyword options to add before the INTO clause of the query.").
			Optional().
			Advanced().
			Example([]string{"DELAYED", "IGNORE"}))

	for _, f := range connFields() {
		spec = spec.Field(f)
	}

	spec = spec.Version("3.59.0").
		Example("Table Insert (MySQL)",
			`
Here we insert rows into a database by populating the columns id, name and topic with values extracted from messages and metadata:`,
			`
pipeline:
  processors:
    - sql_insert:
        driver: mysql
        dsn: foouser:foopassword@tcp(localhost:3306)/foodb
        table: footable
        columns: [ id, name, topic ]
        args_mapping: |
          root = [
            this.user.id,
            this.user.name,
            meta("kafka_topic"),
          ]
`,
		)
	return spec
}

func init() {
	err := service.RegisterBatchProcessor(
		"sql_insert", InsertProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return NewSQLInsertProcessorFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type sqlInsertProcessor struct {
	db      *sql.DB
	builder squirrel.InsertBuilder
	dbMut   sync.RWMutex

	useTxStmt     bool
	argsMapping   *bloblang.Executor
	argsConverter argsConverter

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

// NewSQLInsertProcessorFromConfig returns an internal sql_insert processor.
func NewSQLInsertProcessorFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*sqlInsertProcessor, error) {
	s := &sqlInsertProcessor{
		logger:  mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
	}

	driverStr, err := conf.FieldString("driver")
	if err != nil {
		return nil, err
	}
	if _, in := map[string]struct{}{
		"clickhouse": {},
		"oracle":     {},
	}[driverStr]; in {
		s.useTxStmt = true
	}

	dsnStr, err := conf.FieldString("dsn")
	if err != nil {
		return nil, err
	}

	tableStr, err := conf.FieldString("table")
	if err != nil {
		return nil, err
	}

	columns, err := conf.FieldStringList("columns")
	if err != nil {
		return nil, err
	}

	if conf.Contains("args_mapping") {
		if s.argsMapping, err = conf.FieldBloblang("args_mapping"); err != nil {
			return nil, err
		}
	}

	s.builder = squirrel.Insert(tableStr).Columns(columns...)
	if driverStr == "postgres" || driverStr == "clickhouse" {
		s.builder = s.builder.PlaceholderFormat(squirrel.Dollar)
	} else if driverStr == "oracle" || driverStr == "gocosmos" {
		s.builder = s.builder.PlaceholderFormat(squirrel.Colon)
	}

	if driverStr == "postgres" {
		s.argsConverter = bloblValuesToPgSQLValues
	} else {
		s.argsConverter = func(v []any) []any { return v }
	}

	if s.useTxStmt {
		values := make([]any, 0, len(columns))
		for _, c := range columns {
			values = append(values, c)
		}
		s.builder = s.builder.Values(values...)
	}

	if conf.Contains("prefix") {
		prefixStr, err := conf.FieldString("prefix")
		if err != nil {
			return nil, err
		}
		s.builder = s.builder.Prefix(prefixStr)
	}

	if conf.Contains("suffix") {
		suffixStr, err := conf.FieldString("suffix")
		if err != nil {
			return nil, err
		}
		s.builder = s.builder.Suffix(suffixStr)
	}

	if conf.Contains("options") {
		options, err := conf.FieldStringList("options")
		if err != nil {
			return nil, err
		}
		s.builder = s.builder.Options(options...)
	}

	connSettings, err := connSettingsFromParsed(conf, mgr)
	if err != nil {
		return nil, err
	}

	if s.db, err = sqlOpenWithReworks(mgr.Logger(), driverStr, dsnStr); err != nil {
		return nil, err
	}

	connSettings.apply(context.Background(), s.db, s.logger)

	go func() {
		<-s.shutSig.HardStopChan()

		s.dbMut.Lock()
		_ = s.db.Close()
		s.dbMut.Unlock()

		s.shutSig.TriggerHasStopped()
	}()
	return s, nil
}

func (s *sqlInsertProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	s.dbMut.RLock()
	defer s.dbMut.RUnlock()

	insertBuilder := s.builder

	var tx *sql.Tx
	var stmt *sql.Stmt
	if s.useTxStmt {
		var err error
		if tx, err = s.db.Begin(); err != nil {
			return nil, err
		}
		sqlStr, _, err := insertBuilder.ToSql()
		if err != nil {
			return nil, err
		}
		if stmt, err = tx.Prepare(sqlStr); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
	}

	var argsExec *service.MessageBatchBloblangExecutor
	if s.argsMapping != nil {
		argsExec = batch.BloblangExecutor(s.argsMapping)
	}

	for i, msg := range batch {
		var args []any
		if argsExec != nil {
			resMsg, err := argsExec.Query(i)
			if err != nil {
				s.logger.Debugf("Arguments mapping failed: %v", err)
				msg.SetError(err)
				continue
			}

			iargs, err := resMsg.AsStructured()
			if err != nil {
				s.logger.Debugf("Mapping returned non-structured result: %v", err)
				msg.SetError(fmt.Errorf("mapping returned non-structured result: %w", err))
				continue
			}

			var ok bool
			if args, ok = iargs.([]any); !ok {
				s.logger.Debugf("Mapping returned non-array result: %T", iargs)
				msg.SetError(fmt.Errorf("mapping returned non-array result: %T", iargs))
				continue
			}
			args = s.argsConverter(args)
		}

		if tx == nil {
			insertBuilder = insertBuilder.Values(args...)
		} else if _, err := stmt.Exec(args...); err != nil {
			return nil, err
		}
	}

	var err error
	if tx == nil {
		_, err = insertBuilder.RunWith(s.db).ExecContext(ctx)
	} else {
		err = tx.Commit()
	}
	if err != nil {
		s.logger.Debugf("Failed to run query: %v", err)
		return nil, err
	}
	return []service.MessageBatch{batch}, nil
}

func (s *sqlInsertProcessor) Close(ctx context.Context) error {
	s.shutSig.TriggerHardStop()
	select {
	case <-s.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
