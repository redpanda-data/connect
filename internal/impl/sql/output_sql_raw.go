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
	"errors"
	"fmt"
	"sync"

	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func sqlRawOutputConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary("Executes an arbitrary SQL query for each message.").
		Description(``).
		Field(driverField).
		Field(dsnField).
		Field(rawQueryField().
			Example("INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);").Optional()).
		Field(service.NewBoolField("unsafe_dynamic_query").
			Description("Whether to enable xref:configuration:interpolation.adoc#bloblang-queries[interpolation functions] in the query. Great care should be made to ensure your queries are defended against injection attacks.").
			Advanced().
			Default(false)).
		Field(service.NewStringListField("columns").
			Description("A list of columns to insert.").
			Example([]string{"foo", "bar", "baz"}).
			Default([]string{})).
		Field(service.NewAnyListField("data_types").Description("The columns data types.").Optional().Example([]any{
			map[string]any{
				"name": "foo",
				"type": "VARCHAR",
			},
			map[string]any{
				"name": "bar",
				"type": "DATETIME",
				"datetime": map[string]any{
					"format": "2006-01-02 15:04:05.999",
				},
			},
			map[string]any{
				"name": "baz",
				"type": "DATE",
				"date": map[string]any{
					"format": "2006-01-02",
				},
			},
		}).
			Default([]any{})).
		Field(service.NewBloblangField("args_mapping").
			Description("An optional xref:guides:bloblang/about.adoc[Bloblang mapping] which should evaluate to an array of values matching in size to the number of placeholder arguments in the field `query`.").
			Example("root = [ this.cat.meow, this.doc.woofs[0] ]").
			Example(`root = [ meta("user.id") ]`).
			Optional()).
		Field(service.NewObjectListField(
			"queries",
			rawQueryField(),
			rawQueryArgsMappingField(),
		).
			Description("A list of statements to run in addition to `query`. When specifying multiple statements, they are all executed within a transaction.").
			Optional()).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of statements to execute in parallel.").
			Default(64))

	for _, f := range connFields() {
		spec = spec.Field(f)
	}

	spec = spec.Field(service.NewBatchPolicyField("batching")).
		Version("3.65.0").
		Example("Table Insert (MySQL)",
			`
Here we insert rows into a database by populating the columns id, name and topic with values extracted from messages and metadata:`,
			`
output:
  sql_raw:
    driver: mysql
    dsn: foouser:foopassword@tcp(localhost:3306)/foodb
    query: "INSERT INTO footable (id, name, topic) VALUES (?, ?, ?);"
    args_mapping: |
      root = [
        this.user.id,
        this.user.name,
        meta("kafka_topic"),
      ]
`,
		).
		Example(
			"Dynamically Creating Tables (PostgreSQL)",
			`Here we dynamically create output tables transactionally with inserting a record into the newly created table.`,
			`
output:
  processors:
    - mapping: |
        root = this
        # Prevent SQL injection when using unsafe_dynamic_query
        meta table_name = "\"" + metadata("table_name").replace_all("\"", "\"\"") + "\""
  sql_raw:
    driver: postgres
    dsn: postgres://localhost/postgres
    unsafe_dynamic_query: true
    queries:
      - query: |
          CREATE TABLE IF NOT EXISTS ${!metadata("table_name")} (id varchar primary key, document jsonb);
      - query: |
          INSERT INTO ${!metadata("table_name")} (id, document) VALUES ($1, $2)
          ON CONFLICT (id) DO UPDATE SET document = EXCLUDED.document;
        args_mapping: |
          root = [ this.id, this.document.string() ]

`,
		).
		LintRule(`root = match {
        !this.exists("queries") && !this.exists("query") => [ "either ` + "`query`" + ` or ` + "`queries`" + ` is required" ],
    }`)
	return spec
}

func init() {
	err := service.RegisterBatchOutput(
		"sql_raw", sqlRawOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			out, err = newSQLRawOutputFromConfig(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type sqlRawOutput struct {
	driver string
	dsn    string
	db     *sql.DB
	dbMut  sync.RWMutex

	queryStatic string
	queryDyn    *service.InterpolatedString

	argsMapping *bloblang.Executor

	connSettings *connSettings

	logger  *service.Logger
	shutSig *shutdown.Signaller

	columns   []string
	dataTypes map[string]any
}

func newSQLRawOutputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*sqlRawOutput, error) {
	driverStr, err := conf.FieldString("driver")
	if err != nil {
		return nil, err
	}

	dsnStr, err := conf.FieldString("dsn")
	if err != nil {
		return nil, err
	}

	queryStatic, err := conf.FieldString("query")
	if err != nil {
		return nil, err
	}

	var queryDyn *service.InterpolatedString
	if unsafeDyn, err := conf.FieldBool("unsafe_dynamic_query"); err != nil {
		return nil, err
	} else if unsafeDyn {
		if queryDyn, err = conf.FieldInterpolatedString("query"); err != nil {
			return nil, err
		}
	}

	columns, err := conf.FieldStringList("columns")
	if err != nil {
		return nil, err
	}

	dataTypesField, err := conf.FieldAnyList("data_types")
	if err != nil {
		return nil, err
	}
	dataTypes := map[string]any{}
	for _, dataTypeField := range dataTypesField {
		field, err := dataTypeField.FieldAny()
		if err != nil {
			return nil, err
		}
		dataTypes[field.(map[string]any)["name"].(string)] = field
	}

	var argsMapping *bloblang.Executor
	if conf.Contains("args_mapping") {
		if argsMapping, err = conf.FieldBloblang("args_mapping"); err != nil {
			return nil, err
		}
	}

	connSettings, err := connSettingsFromParsed(conf, mgr)
	if err != nil {
		return nil, err
	}
	return newSQLRawOutput(mgr.Logger(), driverStr, dsnStr, queryStatic, queryDyn, argsMapping, connSettings, columns, dataTypes), nil
}

func newSQLRawOutput(
	logger *service.Logger,
	driverStr, dsnStr string,
	queryStatic string,
	queryDyn *service.InterpolatedString,
	argsMapping *bloblang.Executor,
	connSettings *connSettings,
	columns []string,
	dataTypes map[string]any,
) *sqlRawOutput {
	return &sqlRawOutput{
		logger:       logger,
		shutSig:      shutdown.NewSignaller(),
		driver:       driverStr,
		dsn:          dsnStr,
		queryStatic:  queryStatic,
		queryDyn:     queryDyn,
		argsMapping:  argsMapping,
		connSettings: connSettings,
		columns:      columns,
		dataTypes:    dataTypes,
	}
}

func (s *sqlRawOutput) Connect(ctx context.Context) error {
	s.dbMut.Lock()
	defer s.dbMut.Unlock()

	if s.db != nil {
		return nil
	}

	var err error
	if s.db, err = sqlOpenWithReworks(s.logger, s.driver, s.dsn); err != nil {
		return err
	}

	s.connSettings.apply(ctx, s.db, s.logger)

	go func() {
		<-s.shutSig.HardStopChan()

		s.dbMut.Lock()
		_ = s.db.Close()
		s.dbMut.Unlock()

		s.shutSig.TriggerHasStopped()
	}()
	return nil
}

func (s *sqlRawOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	s.dbMut.RLock()
	defer s.dbMut.RUnlock()

	for i := range batch {
		var args []any
		if s.argsMapping != nil {
			resMsg, err := batch.BloblangQuery(i, s.argsMapping)
			if err != nil {
				return err
			}

			iargs, err := resMsg.AsStructured()
			if err != nil {
				return err
			}

			var ok bool
			if args, ok = iargs.([]any); !ok {
				return fmt.Errorf("mapping returned non-array result: %T", iargs)
			}

			if applyDataTypeFn, found := applyDataTypeMap[s.driver]; found && len(s.dataTypes) > 0 {
				if len(s.dataTypes) == len(args) {
					for i, arg := range args {
						newArg, err := applyDataTypeFn(arg, s.columns[i], s.dataTypes)
						if err != nil {
							return err
						}
						args[i] = newArg
					}
				} else {
					return errors.New("number of data types must match number of columns")
				}
			}
		}

		queryStr := s.queryStatic
		if s.queryDyn != nil {
			var err error
			if queryStr, err = batch.TryInterpolatedString(i, s.queryDyn); err != nil {
				return fmt.Errorf("query interpolation error: %w", err)
			}
		}

		if _, err := s.db.ExecContext(ctx, queryStr, args...); err != nil {
			return err
		}
	}
	return nil
}

func (s *sqlRawOutput) Close(ctx context.Context) error {
	s.shutSig.TriggerHardStop()
	s.dbMut.RLock()
	isNil := s.db == nil
	s.dbMut.RUnlock()
	if isNil {
		return nil
	}
	select {
	case <-s.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
