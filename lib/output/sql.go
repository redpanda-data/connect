package output

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"

	// SQL Drivers
	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSQL] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			s, err := newSQLWriter(conf.SQL, log)
			if err != nil {
				return nil, err
			}
			w, err := NewAsyncWriter(TypeSQL, conf.SQL.MaxInFlight, s, log, stats)
			if err != nil {
				return nil, err
			}
			return NewBatcherFromConfig(conf.SQL.Batching, w, mgr, log, stats)
		}),
		Status:  docs.StatusBeta,
		Batches: true,
		Async:   true,
		Version: "3.33.0",
		Categories: []Category{
			CategoryServices,
		},
		Summary: `
Runs an SQL prepared query against a target database for each message.`,
		Description: `
Query arguments are set using [interpolation functions](/docs/configuration/interpolation#bloblang-queries) in the ` + "`args`" + ` field.

## Drivers

The following is a list of supported drivers and their respective DSN formats:

| Driver | Data Source Name Format |
|---|---|
` + "| `clickhouse` | [`tcp://[netloc][:port][?param1=value1&...&paramN=valueN]`](https://github.com/ClickHouse/clickhouse-go#dsn)" + `
` + "| `mysql` | `[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]` |" + `
` + "| `postgres` | `postgres://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]` |" + `
` + "| `mssql` | `sqlserver://[user[:password]@][netloc][:port][?database=dbname&param1=value1&...]` |" + `

Please note that the ` + "`postgres`" + ` driver enforces SSL by default, you can override this with the parameter ` + "`sslmode=disable`" + ` if required.`,
		Examples: []docs.AnnotatedExample{
			{
				Title: "Table Insert (MySQL)",
				Summary: `
The following example inserts rows into the table footable with the columns foo,
bar and baz populated with values extracted from messages:`,
				Config: `
output:
  sql:
    driver: mysql
    data_source_name: foouser:foopassword@tcp(localhost:3306)/foodb
    query: "INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);"
    args_mapping: '[ this.document.foo, this.document.bar, meta("kafka_topic") ]'
    batching:
      count: 500
`,
			},
			{
				Title: "Table Insert (PostgreSQL)",
				Summary: `
The following example inserts rows into the table footable with the columns foo,
bar and baz populated with values extracted from messages:`,
				Config: `
output:
  sql:
    driver: postgres
    data_source_name: postgres://foouser:foopassword@localhost:5432/foodb?sslmode=disable
    query: "INSERT INTO footable (foo, bar, baz) VALUES ($1, $2, $3);"
    args_mapping: '[ this.document.foo, this.document.bar, meta("kafka_topic") ]'
    batching:
      count: 500
`,
			},
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"driver",
				"A database [driver](#drivers) to use.",
			).HasOptions("mysql", "postgres", "clickhouse", "mssql"),
			docs.FieldCommon(
				"data_source_name", "A Data Source Name to identify the target database.",
				"tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000",
				"foouser:foopassword@tcp(localhost:3306)/foodb",
				"postgres://foouser:foopass@localhost:5432/foodb?sslmode=disable",
			),
			docs.FieldCommon(
				"query", "The query to run against the database.",
				"INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);",
			),
			docs.FieldDeprecated(
				"args",
				"A list of arguments for the query to be resolved for each message.",
			).IsInterpolated().Array(),
			docs.FieldString(
				"args_mapping",
				"A [Bloblang mapping](/docs/guides/bloblang/about) that produces the arguments for the query. The mapping must return an array containing the number of arguments in the query.",
				`[ this.foo, this.bar.not_empty().catch(null), meta("baz") ]`,
				`root = [ uuid_v4() ].merge(this.document.args)`,
			).Linter(docs.LintBloblangMapping).AtVersion("3.47.0"),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			batch.FieldSpec(),
		},
	}
}

//------------------------------------------------------------------------------

// SQLConfig contains configuration fields for the SQL processor.
type SQLConfig struct {
	Driver         string             `json:"driver" yaml:"driver"`
	DataSourceName string             `json:"data_source_name" yaml:"data_source_name"`
	Query          string             `json:"query" yaml:"query"`
	Args           []string           `json:"args" yaml:"args"`
	ArgsMapping    string             `json:"args_mapping" yaml:"args_mapping"`
	MaxInFlight    int                `json:"max_in_flight" yaml:"max_in_flight"`
	Batching       batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewSQLConfig returns a SQLConfig with default values.
func NewSQLConfig() SQLConfig {
	return SQLConfig{
		Driver:         "mysql",
		DataSourceName: "",
		Query:          "",
		Args:           []string{},
		ArgsMapping:    "",
		MaxInFlight:    1,
		Batching:       batch.NewPolicyConfig(),
	}
}

//------------------------------------------------------------------------------

func insertOnlyBatchDriver(driver string) bool {
	_, exists := map[string]struct{}{
		"clickhouse": {},
	}[driver]
	return exists
}

//------------------------------------------------------------------------------

type sqlWriter struct {
	log  log.Modular
	conf SQLConfig

	db          *sql.DB
	dbMut       sync.Mutex
	args        []*field.Expression
	argsMapping *mapping.Executor

	query *sql.Stmt
}

func newSQLWriter(conf SQLConfig, log log.Modular) (*sqlWriter, error) {
	if len(conf.Args) > 0 && conf.ArgsMapping != "" {
		return nil, errors.New("cannot specify both `args` and an `args_mapping` in the same output")
	}

	var args []*field.Expression
	for i, v := range conf.Args {
		expr, err := bloblang.NewField(v)
		if err != nil {
			return nil, fmt.Errorf("failed to parse arg %v expression: %v", i, err)
		}
		args = append(args, expr)
	}

	var argsMapping *mapping.Executor
	if conf.ArgsMapping != "" {
		var err error
		if argsMapping, err = bloblang.NewMapping("", conf.ArgsMapping); err != nil {
			return nil, fmt.Errorf("failed to parse `args_mapping`: %w", err)
		}
	}

	s := &sqlWriter{
		log:         log,
		conf:        conf,
		args:        args,
		argsMapping: argsMapping,
	}

	return s, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext attempts to establish a connection to the target database.
func (s *sqlWriter) ConnectWithContext(ctx context.Context) error {
	s.dbMut.Lock()
	defer s.dbMut.Unlock()

	if s.db != nil {
		return nil
	}

	var err error
	db, err := sql.Open(s.conf.Driver, s.conf.DataSourceName)
	if err != nil {
		return err
	}

	// Some drivers only support transactional prepared inserts.
	if !insertOnlyBatchDriver(s.conf.Driver) {
		if s.query, err = db.Prepare(s.conf.Query); err != nil {
			db.Close()
			return fmt.Errorf("failed to prepare query: %v", err)
		}
	}

	s.log.Infof("Writing messages to %v database.\n", s.conf.Driver)
	s.db = db
	return nil
}

func (s *sqlWriter) doExecute(argSets [][]interface{}) (errs []error) {
	s.dbMut.Lock()
	db := s.db
	stmt := s.query
	s.dbMut.Unlock()

	var err error
	defer func() {
		if err != nil {
			if len(errs) == 0 {
				errs = make([]error, len(argSets))
			}
			for i := range errs {
				if errs[i] == nil {
					errs[i] = err
				}
			}
		}
	}()

	if db == nil {
		err = types.ErrNotConnected
		return
	}

	var tx *sql.Tx
	if tx, err = db.Begin(); err != nil {
		return
	}

	if stmt == nil {
		if stmt, err = tx.Prepare(s.conf.Query); err != nil {
			return
		}
		defer stmt.Close()
	} else {
		stmt = tx.Stmt(stmt)
	}

	for i, args := range argSets {
		if _, serr := stmt.Exec(args...); serr != nil {
			if len(errs) == 0 {
				errs = make([]error, len(argSets))
			}
			errs[i] = serr
		}
	}

	err = tx.Commit()
	return
}

func (s *sqlWriter) getArgs(index int, msg types.Message) ([]interface{}, error) {
	if len(s.args) > 0 {
		args := make([]interface{}, len(s.args))
		for i, v := range s.args {
			args[i] = v.String(index, msg)
		}
		return args, nil
	}

	if s.argsMapping == nil {
		return nil, nil
	}

	pargs, err := s.argsMapping.MapPart(index, msg)
	if err != nil {
		return nil, err
	}

	iargs, err := pargs.JSON()
	if err != nil {
		return nil, fmt.Errorf("mapping returned non-structured result: %w", err)
	}

	args, ok := iargs.([]interface{})
	if !ok {
		return nil, fmt.Errorf("mapping returned non-array result: %T", iargs)
	}
	return args, nil
}

// WriteWithContext attempts to write a message to the database.
func (s *sqlWriter) WriteWithContext(ctx context.Context, msg types.Message) error {
	argSets := make([][]interface{}, msg.Len())
	if err := msg.Iter(func(index int, p types.Part) error {
		args, err := s.getArgs(index, msg)
		if err != nil {
			return err
		}
		argSets[index] = args
		return nil
	}); err != nil {
		return err
	}

	errs := s.doExecute(argSets)
	return writer.IterateBatchedSend(msg, func(i int, _ types.Part) error {
		if len(errs) > i {
			return errs[i]
		}
		return nil
	})
}

// CloseAsync shuts down the processor and stops processing requests.
func (s *sqlWriter) CloseAsync() {
	go func() {
		s.dbMut.Lock()
		if s.db != nil {
			s.db.Close()
		}
		if s.query != nil {
			s.query.Close()
		}
		s.db = nil
		s.query = nil
		s.dbMut.Unlock()
	}()
}

// WaitForClose blocks until the processor has closed down.
func (s *sqlWriter) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
