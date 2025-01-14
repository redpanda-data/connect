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

	"github.com/redpanda-data/benthos/v4/public/service"
)

// RawProcessorConfig returns a config spec for an sql_raw processor.
func RawProcessorConfig() *service.ConfigSpec {
	rawQueryExecOnly := func() *service.ConfigField {
		return service.NewBoolField("exec_only").
			Description("Whether the query result should be discarded. When set to `true` the message contents will remain unchanged, which is useful in cases where you are executing inserts, updates, etc. By default this is true for the last query, and previous queries don't change the results. If set to true for any query but the last one, the subsequent `args_mappings` input is overwritten.").
			Optional()
	}

	return service.NewConfigSpec().
		Stable().
		Version("3.65.0").
		Categories("Integration").
		Summary("Runs an arbitrary SQL query against a database and (optionally) returns the result as an array of objects, one for each row returned.").
		Description(`
If the query fails to execute then the message will remain unchanged and the error can be caught using xref:configuration:error_handling.adoc[error handling methods].`).
		Field(driverField).
		Field(dsnField).
		Field(rawQueryField().
			Example("INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);").
			Example("SELECT * FROM footable WHERE user_id = $1;").
			Optional()).
		Field(service.NewBoolField("unsafe_dynamic_query").
			Description("Whether to enable xref:configuration:interpolation.adoc#bloblang-queries[interpolation functions] in the query. Great care should be made to ensure your queries are defended against injection attacks.").
			Advanced().
			Default(false)).
		Field(rawQueryArgsMappingField()).
		Field(rawQueryExecOnly()).
		Field(service.NewObjectListField(
			"queries",
			rawQueryField(),
			rawQueryArgsMappingField(),
			rawQueryExecOnly(),
		).
			Description("A list of statements to run in addition to `query`. When specifying multiple statements, they are all executed within a transaction. The output of the processor is always the last query that runs, unless `exec_only` is used.").
			Optional()).
		Fields(connFields()...).
		Example(
			"Table Insert (MySQL)",
			"The following example inserts rows into the table footable with the columns foo, bar and baz populated with values extracted from messages.",
			`
pipeline:
  processors:
    - sql_raw:
        driver: mysql
        dsn: foouser:foopassword@tcp(localhost:3306)/foodb
        query: "INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);"
        args_mapping: '[ document.foo, document.bar, meta("kafka_topic") ]'
        exec_only: true
`,
		).
		Example(
			"Table Query (PostgreSQL)",
			`Here we query a database for columns of footable that share a `+"`user_id`"+` with the message field `+"`user.id`"+`. A `+"xref:components:processors/branch.adoc[`branch` processor]"+` is used in order to insert the resulting array into the original message at the path `+"`foo_rows`"+`.`,
			`
pipeline:
  processors:
    - branch:
        processors:
          - sql_raw:
              driver: postgres
              dsn: postgres://foouser:foopass@localhost:5432/testdb?sslmode=disable
              query: "SELECT * FROM footable WHERE user_id = $1;"
              args_mapping: '[ this.user.id ]'
        result_map: 'root.foo_rows = this'
`,
		).
		Example(
			"Dynamically Creating Tables (PostgreSQL)",
			`Here we query a database for columns of footable that share a `+"`user_id`"+` with the message field `+"`user.id`"+`. A `+"xref:components:processors/branch.adoc[`branch` processor]"+` is used in order to insert the resulting array into the original message at the path `+"`foo_rows`"+`.`,
			`
pipeline:
  processors:
    - mapping: |
        root = this
        # Prevent SQL injection when using unsafe_dynamic_query
        meta table_name = "\"" + metadata("table_name").replace_all("\"", "\"\"") + "\""
    - sql_raw:
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
}

func init() {
	err := service.RegisterBatchProcessor(
		"sql_raw", RawProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return NewSQLRawProcessorFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type sqlRawProcessor struct {
	db    *sql.DB
	dbMut sync.RWMutex

	queries []rawQueryStatement

	argsConverter argsConverter

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

// NewSQLRawProcessorFromConfig returns an internal sql_raw processor.
func NewSQLRawProcessorFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*sqlRawProcessor, error) {
	driverStr, err := conf.FieldString("driver")
	if err != nil {
		return nil, err
	}

	dsnStr, err := conf.FieldString("dsn")
	if err != nil {
		return nil, err
	}

	unsafeDyn, err := conf.FieldBool("unsafe_dynamic_query")
	if err != nil {
		return nil, err
	}

	queriesConf := []*service.ParsedConfig{}
	if conf.Contains("query") {
		queriesConf = append(queriesConf, conf)
	}
	if conf.Contains("queries") {
		qc, err := conf.FieldObjectList("queries")
		if err != nil {
			return nil, err
		}
		queriesConf = append(queriesConf, qc...)
	}

	if len(queriesConf) == 0 {
		return nil, errors.New("either field 'query' or field 'queries' is required")
	}

	var queries []rawQueryStatement
	for i, qc := range queriesConf {
		var statement rawQueryStatement
		if unsafeDyn {
			statement.dynamic, err = qc.FieldInterpolatedString("query")
			if err != nil {
				return nil, err
			}
		} else {
			statement.static, err = qc.FieldString("query")
			if err != nil {
				return nil, err
			}
		}

		if qc.Contains("args_mapping") {
			if statement.argsMapping, err = qc.FieldBloblang("args_mapping"); err != nil {
				return nil, err
			}
		}
		statement.execOnly = i < len(queriesConf)-1
		if qc.Contains("exec_only") {
			statement.execOnly, err = qc.FieldBool("exec_only")
			if err != nil {
				return nil, err
			}
		}
		queries = append(queries, statement)
	}

	connSettings, err := connSettingsFromParsed(conf, mgr)
	if err != nil {
		return nil, err
	}

	var argsConverter argsConverter
	if driverStr == "postgres" {
		argsConverter = bloblValuesToPgSQLValues
	} else {
		argsConverter = func(v []any) []any { return v }
	}

	return newSQLRawProcessor(mgr.Logger(), driverStr, dsnStr, queries, argsConverter, connSettings)
}

func newSQLRawProcessor(
	logger *service.Logger,
	driverStr, dsnStr string,
	queries []rawQueryStatement,
	argsConverter argsConverter,
	connSettings *connSettings,
) (*sqlRawProcessor, error) {
	s := &sqlRawProcessor{
		logger:        logger,
		shutSig:       shutdown.NewSignaller(),
		queries:       queries,
		argsConverter: argsConverter,
	}

	var err error
	if s.db, err = sqlOpenWithReworks(logger, driverStr, dsnStr); err != nil {
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

func (s *sqlRawProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	s.dbMut.RLock()
	defer s.dbMut.RUnlock()

	argsExec := make([]*service.MessageBatchBloblangExecutor, len(s.queries))
	for i, q := range s.queries {
		if q.argsMapping != nil {
			argsExec[i] = batch.BloblangExecutor(q.argsMapping)
		}
	}
	dynQueries := make([]*service.MessageBatchInterpolationExecutor, len(s.queries))
	for i, q := range s.queries {
		if q.dynamic != nil {
			dynQueries[i] = batch.InterpolationExecutor(q.dynamic)
		}
	}

	batch = batch.Copy()

	for i, msg := range batch {
		var tx *sql.Tx
		var err error
		if len(s.queries) > 1 {
			tx, err = s.db.BeginTx(ctx, nil)
			if err != nil {
				msg.SetError(err)
				continue
			}
		}
		argsUpdated := false
		for j, query := range s.queries {
			var args []any
			if argsExec[j] != nil {
				var resMsg *service.Message
				if argsUpdated {
					exec := batch.BloblangExecutor(query.argsMapping)
					resMsg, err = exec.Query(i)
				} else {
					resMsg, err = argsExec[j].Query(i)
				}
				if err != nil {
					err = fmt.Errorf("arguments mapping failed: %v", err)
					break
				}

				var iargs any
				iargs, err = resMsg.AsStructured()
				if err != nil {
					err = fmt.Errorf("mapping returned non-structured result: %w", err)
					break
				}

				var ok bool
				if args, ok = iargs.([]any); !ok {
					err = fmt.Errorf("mapping returned non-array result: %T", iargs)
					break
				}
				args = s.argsConverter(args)
			}

			queryStr := query.static
			if query.dynamic != nil {
				if queryStr, err = dynQueries[j].TryString(i); err != nil {
					err = fmt.Errorf("query interpolation error: %w", err)
					break
				}
			}

			if query.execOnly {
				if tx == nil {
					_, err = s.db.ExecContext(ctx, queryStr, args...)
				} else {
					_, err = tx.ExecContext(ctx, queryStr, args...)
				}
				if err != nil {
					err = fmt.Errorf("failed to run query: %w", err)
					break
				}
			} else {
				var rows *sql.Rows
				if tx == nil {
					rows, err = s.db.QueryContext(ctx, queryStr, args...)
				} else {
					rows, err = tx.QueryContext(ctx, queryStr, args...)
				}
				if err != nil {
					err = fmt.Errorf("failed to run query: %w", err)
					break
				}

				var jArray []any
				if jArray, err = sqlRowsToArray(rows); err != nil {
					err = fmt.Errorf("failed to convert rows: %w", err)
					break
				}

				msg.SetStructuredMut(jArray)
				argsUpdated = true
			}
		}
		if err != nil {
			s.logger.Debugf("%v", err)
			msg.SetError(err)
		}
		if tx != nil {
			if err != nil {
				if err = tx.Rollback(); err != nil {
					s.logger.Debugf("Failed to rollback transaction: %v", err)
				}
			} else {
				if err = tx.Commit(); err != nil {
					s.logger.Debugf("Failed to commit transaction: %v", err)
					msg.SetError(err)
				}
			}
		}
	}

	return []service.MessageBatch{batch}, nil
}

func (s *sqlRawProcessor) Close(ctx context.Context) error {
	s.shutSig.TriggerHardStop()
	select {
	case <-s.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
