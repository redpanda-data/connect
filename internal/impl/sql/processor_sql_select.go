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

// SelectProcessorConfig returns a config spec for an sql_select processor.
func SelectProcessorConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Categories("Integration").
		Summary("Runs an SQL select query against a database and returns the result as an array of objects, one for each row returned, containing a key for each column queried and its value.").
		Description(`
If the query fails to execute then the message will remain unchanged and the error can be caught using xref:configuration:error_handling.adoc[error handling methods].`).
		Field(driverField).
		Field(dsnField).
		Field(service.NewStringField("table").
			Description("The table to query.").
			Example("foo")).
		Field(service.NewStringListField("columns").
			Description("A list of columns to query.").
			Example([]string{"*"}).
			Example([]string{"foo", "bar", "baz"})).
		Field(service.NewStringField("where").
			Description("An optional where clause to add. Placeholder arguments are populated with the `args_mapping` field. Placeholders should always be question marks, and will automatically be converted to dollar syntax when the postgres or clickhouse drivers are used.").
			Example("meow = ? and woof = ?").
			Example("user_id = ?").
			Optional()).
		Field(service.NewBloblangField("args_mapping").
			Description("An optional xref:guides:bloblang/about.adoc[Bloblang mapping] which should evaluate to an array of values matching in size to the number of placeholder arguments in the field `where`.").
			Example("root = [ this.cat.meow, this.doc.woofs[0] ]").
			Example(`root = [ meta("user.id") ]`).
			Optional()).
		Field(service.NewStringField("prefix").
			Description("An optional prefix to prepend to the query (before SELECT).").
			Optional().
			Advanced()).
		Field(service.NewStringField("suffix").
			Description("An optional suffix to append to the select query.").
			Optional().
			Advanced())

	for _, f := range connFields() {
		spec = spec.Field(f)
	}

	spec = spec.Version("3.59.0").
		Example("Table Query (PostgreSQL)",
			`
Here we query a database for columns of footable that share a `+"`user_id`"+`
with the message `+"`user.id`"+`. A `+"xref:components:processors/branch.adoc[`branch` processor]"+`
is used in order to insert the resulting array into the original message at the
path `+"`foo_rows`"+`:`,
			`
pipeline:
  processors:
    - branch:
        processors:
          - sql_select:
              driver: postgres
              dsn: postgres://foouser:foopass@localhost:5432/testdb?sslmode=disable
              table: footable
              columns: [ '*' ]
              where: user_id = ?
              args_mapping: '[ this.user.id ]'
        result_map: 'root.foo_rows = this'
`,
		)
	return spec
}

func init() {
	service.MustRegisterBatchProcessor(
		"sql_select", SelectProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return NewSQLSelectProcessorFromConfig(conf, mgr)
		})
}

//------------------------------------------------------------------------------

type sqlSelectProcessor struct {
	db      *sql.DB
	builder squirrel.SelectBuilder
	dbMut   sync.RWMutex

	where       string
	argsMapping *bloblang.Executor

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

// NewSQLSelectProcessorFromConfig returns an internal sql_select processor.
func NewSQLSelectProcessorFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*sqlSelectProcessor, error) {
	s := &sqlSelectProcessor{
		logger:  mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
	}

	driverStr, err := conf.FieldString("driver")
	if err != nil {
		return nil, err
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

	if conf.Contains("where") {
		if s.where, err = conf.FieldString("where"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("args_mapping") {
		if s.argsMapping, err = conf.FieldBloblang("args_mapping"); err != nil {
			return nil, err
		}
	}

	s.builder = squirrel.Select(columns...).From(tableStr)
	switch driverStr {
	case "postgres", "clickhouse":
		s.builder = s.builder.PlaceholderFormat(squirrel.Dollar)
	case "oracle", "gocosmos":
		s.builder = s.builder.PlaceholderFormat(squirrel.Colon)
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

func (s *sqlSelectProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	s.dbMut.RLock()
	defer s.dbMut.RUnlock()

	var argsExec *service.MessageBatchBloblangExecutor
	if s.argsMapping != nil {
		argsExec = batch.BloblangExecutor(s.argsMapping)
	}

	batch = batch.Copy()
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
		}

		queryBuilder := s.builder
		if s.where != "" {
			queryBuilder = queryBuilder.Where(s.where, args...)
		}

		rows, err := queryBuilder.RunWith(s.db).QueryContext(ctx)
		if err != nil {
			s.logger.Debugf("Failed to run query: %v", err)
			msg.SetError(err)
			continue
		}

		if jArray, err := sqlRowsToArray(rows); err != nil {
			s.logger.Debugf("Failed to convert rows: %v", err)
			msg.SetError(err)
		} else {
			msg.SetStructuredMut(jArray)
		}
	}
	return []service.MessageBatch{batch}, nil
}

func (s *sqlSelectProcessor) Close(ctx context.Context) error {
	s.shutSig.TriggerHardStop()
	select {
	case <-s.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
