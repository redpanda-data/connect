package sql

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

// RawProcessorConfig returns a config spec for an sql_raw processor.
func RawProcessorConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Categories("Integration").
		Summary("Runs an arbitrary SQL query against a database and (optionally) returns the result as an array of objects, one for each row returned.").
		Description(`
If the query fails to execute then the message will remain unchanged and the error can be caught using error handling methods outlined [here](/docs/configuration/error_handling).`).
		Field(driverField).
		Field(dsnField).
		Field(rawQueryField().
			Example("INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);").
			Example("SELECT * FROM footable WHERE user_id = $1;")).
		Field(service.NewBoolField("unsafe_dynamic_query").
			Description("Whether to enable [interpolation functions](/docs/configuration/interpolation/#bloblang-queries) in the query. Great care should be made to ensure your queries are defended against injection attacks.").
			Advanced().
			Default(false)).
		Field(service.NewBloblangField("args_mapping").
			Description("An optional [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to an array of values matching in size to the number of placeholder arguments in the field `query`.").
			Example("root = [ this.cat.meow, this.doc.woofs[0] ]").
			Example(`root = [ meta("user.id") ]`).
			Optional()).
		Field(service.NewBoolField("exec_only").
			Description("Whether the query result should be discarded. When set to `true` the message contents will remain unchanged, which is useful in cases where you are executing inserts, updates, etc.").
			Default(false))

	for _, f := range connFields() {
		spec = spec.Field(f)
	}

	spec = spec.Version("3.65.0").
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
			`Here we query a database for columns of footable that share a `+"`user_id`"+` with the message field `+"`user.id`"+`. A `+"[`branch` processor](/docs/components/processors/branch)"+` is used in order to insert the resulting array into the original message at the path `+"`foo_rows`"+`.`,
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
		)
	return spec
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

	queryStatic string
	queryDyn    *service.InterpolatedString
	onlyExec    bool

	argsMapping *bloblang.Executor

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

// NewSQLRawProcessorFromConfig returns an internal sql_raw processor.
// nolint:revive // Not bothered as this is internal anyway
func NewSQLRawProcessorFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*sqlRawProcessor, error) {
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

	onlyExec, err := conf.FieldBool("exec_only")
	if err != nil {
		return nil, err
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
	return newSQLRawProcessor(mgr.Logger(), driverStr, dsnStr, queryStatic, queryDyn, onlyExec, argsMapping, connSettings)
}

func newSQLRawProcessor(
	logger *service.Logger,
	driverStr, dsnStr string,
	queryStatic string,
	queryDyn *service.InterpolatedString,
	onlyExec bool,
	argsMapping *bloblang.Executor,
	connSettings *connSettings,
) (*sqlRawProcessor, error) {
	s := &sqlRawProcessor{
		logger:      logger,
		shutSig:     shutdown.NewSignaller(),
		queryStatic: queryStatic,
		queryDyn:    queryDyn,
		onlyExec:    onlyExec,
		argsMapping: argsMapping,
	}

	var err error
	if s.db, err = sqlOpenWithReworks(logger, driverStr, dsnStr); err != nil {
		return nil, err
	}
	connSettings.apply(context.Background(), s.db, s.logger)

	go func() {
		<-s.shutSig.CloseNowChan()

		s.dbMut.Lock()
		_ = s.db.Close()
		s.dbMut.Unlock()

		s.shutSig.ShutdownComplete()
	}()
	return s, nil
}

func (s *sqlRawProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	s.dbMut.RLock()
	defer s.dbMut.RUnlock()

	batch = batch.Copy()
	for i, msg := range batch {
		var args []any
		if s.argsMapping != nil {
			resMsg, err := batch.BloblangQuery(i, s.argsMapping)
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

		queryStr := s.queryStatic
		if s.queryDyn != nil {
			var err error
			if queryStr, err = batch.TryInterpolatedString(i, s.queryDyn); err != nil {
				s.logger.Errorf("Query interoplation error: %v", err)
				msg.SetError(fmt.Errorf("query interpolation error: %w", err))
				continue
			}
		}

		if s.onlyExec {
			if _, err := s.db.ExecContext(ctx, queryStr, args...); err != nil {
				s.logger.Debugf("Failed to run query: %v", err)
				msg.SetError(err)
				continue
			}
		} else {
			rows, err := s.db.QueryContext(ctx, queryStr, args...)
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
	}
	return []service.MessageBatch{batch}, nil
}

func (s *sqlRawProcessor) Close(ctx context.Context) error {
	s.shutSig.CloseNow()
	select {
	case <-s.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
