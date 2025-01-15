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

func sqlRawOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
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
			Default(64)).
		Fields(connFields()...).
		Field(service.NewBatchPolicyField("batching")).
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

	queries []rawQueryStatement

	argsConverter argsConverter

	connSettings *connSettings

	logger  *service.Logger
	shutSig *shutdown.Signaller
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
	for _, qc := range queriesConf {
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

	return newSQLRawOutput(mgr.Logger(), driverStr, dsnStr, queries, argsConverter, connSettings), nil
}

func newSQLRawOutput(
	logger *service.Logger,
	driverStr, dsnStr string,
	queries []rawQueryStatement,
	argsConverter argsConverter,
	connSettings *connSettings,
) *sqlRawOutput {
	return &sqlRawOutput{
		logger:        logger,
		shutSig:       shutdown.NewSignaller(),
		driver:        driverStr,
		dsn:           dsnStr,
		queries:       queries,
		argsConverter: argsConverter,
		connSettings:  connSettings,
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
	return batch.WalkWithBatchedErrors(func(i int, msg *service.Message) (err error) {
		var tx *sql.Tx
		if len(s.queries) > 1 {
			tx, err = s.db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer func() {
				if err != nil {
					s.logger.Debugf("%v", err)
					if rerr := tx.Rollback(); rerr != nil {
						s.logger.Debugf("Failed to rollback transaction: %v", rerr)
					}
				} else {
					// NB: this sets the return value to the error
					if err = tx.Commit(); err != nil {
						s.logger.Debugf("Failed to commit transaction: %v", err)
					}
				}
			}()
		}
		for j, query := range s.queries {
			var args []any
			if argsExec[j] != nil {
				var resMsg *service.Message
				resMsg, err = argsExec[j].Query(i)
				if err != nil {
					return fmt.Errorf("arguments mapping failed: %w", err)
				}

				var iargs any
				iargs, err = resMsg.AsStructured()
				if err != nil {
					return fmt.Errorf("mapping returned non-structured result: %w", err)
				}

				var ok bool
				if args, ok = iargs.([]any); !ok {
					return fmt.Errorf("mapping returned non-array result: %T", iargs)
				}
				args = s.argsConverter(args)
			}

			queryStr := query.static
			if query.dynamic != nil {
				if queryStr, err = dynQueries[j].TryString(i); err != nil {
					return fmt.Errorf("query interpolation error: %w", err)
				}
			}

			if tx == nil {
				_, err = s.db.ExecContext(ctx, queryStr, args...)
			} else {
				_, err = tx.ExecContext(ctx, queryStr, args...)
			}
			if err != nil {
				return fmt.Errorf("failed to run query: %w", err)
			}
		}
		return nil
	})
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
