package processor

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"

	// SQL Drivers
	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSQL] = TypeSpec{
		constructor: NewSQL,
		Categories: []Category{
			CategoryIntegration,
		},
		Status: docs.StatusDeprecated,
		Summary: `
Runs an SQL prepared query against a target database for each message and, for
queries that return rows, replaces it with the result according to a
[codec](#result-codecs).`,
		Description: `
## Alternatives

Use either the ` + "[`sql_insert`](/docs/components/processors/sql_insert)" + ` or the ` + "[`sql_select`](/docs/components/processors/sql_select)" + ` processor instead.

If a query contains arguments they can be set as an array of strings supporting
[interpolation functions](/docs/configuration/interpolation#bloblang-queries) in
the ` + "`args`" + ` field.

## Drivers

The following is a list of supported drivers and their respective DSN formats:

| Driver | Data Source Name Format |
|---|---|
` + "| `clickhouse` | [`tcp://[netloc][:port][?param1=value1&...&paramN=valueN]`](https://github.com/ClickHouse/clickhouse-go#dsn)" + `
` + "| `mysql` | `[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]` |" + `
` + "| `postgres` | `postgres://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]` |" + `
` + "| `mssql` | `sqlserver://[user[:password]@][netloc][:port][?database=dbname&param1=value1&...]` |" + `

Please note that the ` + "`postgres`" + ` driver enforces SSL by default, you
can override this with the parameter ` + "`sslmode=disable`" + ` if required.`,
		Examples: []docs.AnnotatedExample{
			{
				Title: "Table Insert (MySQL)",
				Summary: `
The following example inserts rows into the table footable with the columns foo,
bar and baz populated with values extracted from messages:`,
				Config: `
pipeline:
  processors:
    - sql:
        driver: mysql
        data_source_name: foouser:foopassword@tcp(localhost:3306)/foodb
        query: "INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);"
        args_mapping: '[ document.foo, document.bar, meta("kafka_topic") ]'
`,
			},
			{
				Title: "Table Query (PostgreSQL)",
				Summary: `
Here we query a database for columns of footable that share a ` + "`user_id`" + `
with the message ` + "`user.id`" + `. The ` + "`result_codec`" + ` is set to
` + "`json_array`" + ` and a ` + "[`branch` processor](/docs/components/processors/branch)" + `
is used in order to insert the resulting array into the original message at the
path ` + "`foo_rows`" + `:`,
				Config: `
pipeline:
  processors:
    - branch:
        processors:
          - sql:
              driver: postgres
              result_codec: json_array
              data_source_name: postgres://foouser:foopass@localhost:5432/testdb?sslmode=disable
              query: "SELECT * FROM footable WHERE user_id = $1;"
              args_mapping: '[ this.user.id ]'
        result_map: 'root.foo_rows = this'
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
			docs.FieldBool(
				"unsafe_dynamic_query",
				"Whether to enable dynamic queries that support interpolation functions. WARNING: This feature opens up the possibility of SQL injection attacks and is considered unsafe.",
			).Advanced().HasDefault(false),
			docs.FieldBloblang(
				"args_mapping",
				"A [Bloblang mapping](/docs/guides/bloblang/about) that produces the arguments for the query. The mapping must return an array containing the number of arguments in the query.",
				`[ this.foo, this.bar.not_empty().catch(null), meta("baz") ]`,
				`root = [ uuid_v4() ].merge(this.document.args)`,
			).AtVersion("3.47.0"),
			docs.FieldCommon(
				"result_codec",
				"A [codec](#result-codecs) to determine how resulting rows are converted into messages.",
			).HasOptions("none", "json_array"),
		},
		Footnotes: `
## Result Codecs

When a query returns rows they are serialised according to a chosen codec, and
the message contents are replaced with the serialised result.

### ` + "`none`" + `

The result of the query is ignored and the message remains unchanged. If your
query does not return rows then this is the appropriate codec.

### ` + "`json_array`" + `

The resulting rows are serialised into an array of JSON objects, where each
object represents a row, where the key is the column name and the value is that
columns value in the row.`,
	}
}

//------------------------------------------------------------------------------

// SQLConfig contains configuration fields for the SQL processor.
type SQLConfig struct {
	Driver             string `json:"driver" yaml:"driver"`
	DataSourceName     string `json:"data_source_name" yaml:"data_source_name"`
	Query              string `json:"query" yaml:"query"`
	UnsafeDynamicQuery bool   `json:"unsafe_dynamic_query" yaml:"unsafe_dynamic_query"`
	ArgsMapping        string `json:"args_mapping" yaml:"args_mapping"`
	ResultCodec        string `json:"result_codec" yaml:"result_codec"`
}

// NewSQLConfig returns a SQLConfig with default values.
func NewSQLConfig() SQLConfig {
	return SQLConfig{
		Driver:             "mysql",
		DataSourceName:     "",
		Query:              "",
		UnsafeDynamicQuery: false,
		ArgsMapping:        "",
		ResultCodec:        "none",
	}
}

//------------------------------------------------------------------------------

// Some SQL drivers (such as clickhouse) require prepared inserts to be local to
// a transaction, rather than general.
func insertRequiresTransactionPrepare(driver string) bool {
	_, exists := map[string]struct{}{
		"clickhouse": {},
	}[driver]
	return exists
}

//------------------------------------------------------------------------------

// SQL is a processor that executes an SQL query for each message.
type SQL struct {
	log   log.Modular
	stats metrics.Type

	conf        SQLConfig
	db          *sql.DB
	dbMux       sync.RWMutex
	argsMapping *mapping.Executor
	resCodec    sqlResultCodec

	queryStr string
	dynQuery *field.Expression
	query    *sql.Stmt

	closeChan  chan struct{}
	closedChan chan struct{}
	closeOnce  sync.Once

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewSQL returns a SQL processor.
func NewSQL(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	dsn := conf.SQL.DataSourceName

	var argsMapping *mapping.Executor
	if conf.SQL.ArgsMapping != "" {
		log.Warnln("using unsafe_dynamic_query leaves you vulnerable to SQL injection attacks")
		var err error
		if argsMapping, err = interop.NewBloblangMapping(mgr, conf.SQL.ArgsMapping); err != nil {
			return nil, fmt.Errorf("failed to parse `args_mapping`: %w", err)
		}
	}

	if conf.SQL.Driver == "mssql" {
		// For MSSQL, if the user part of the connection string is in the
		// `DOMAIN\username` format, then the backslash character needs to be
		// URL-encoded.
		conf.SQL.DataSourceName = strings.ReplaceAll(conf.SQL.DataSourceName, `\`, "%5C")
	}

	s := &SQL{
		log:         log,
		stats:       stats,
		conf:        conf.SQL,
		argsMapping: argsMapping,

		queryStr: conf.SQL.Query,

		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}

	var err error
	if s.resCodec, err = strToSQLResultCodec(conf.SQL.ResultCodec); err != nil {
		return nil, err
	}

	if s.db, err = sql.Open(conf.SQL.Driver, dsn); err != nil {
		return nil, err
	}

	if conf.SQL.UnsafeDynamicQuery {
		if s.dynQuery, err = interop.NewBloblangField(mgr, s.queryStr); err != nil {
			return nil, fmt.Errorf("failed to parse dynamic query expression: %v", err)
		}
	}

	isSelectQuery := s.resCodec != nil

	// Some drivers only support transactional prepared inserts.
	if s.dynQuery == nil && (isSelectQuery || !insertRequiresTransactionPrepare(conf.SQL.Driver)) {
		if s.query, err = s.db.Prepare(s.queryStr); err != nil {
			s.db.Close()
			return nil, fmt.Errorf("failed to prepare query: %v", err)
		}
	}

	go func() {
		defer func() {
			s.dbMux.Lock()
			s.db.Close()
			if s.query != nil {
				s.query.Close()
			}
			s.dbMux.Unlock()
			close(s.closedChan)
		}()
		<-s.closeChan
	}()
	return s, nil
}

//------------------------------------------------------------------------------

type sqlResultCodec func(rows *sql.Rows, part types.Part) error

func sqlResultJSONArrayCodec(rows *sql.Rows, part types.Part) error {
	columnNames, err := rows.Columns()
	if err != nil {
		return err
	}
	jArray := []interface{}{}
	for rows.Next() {
		values := make([]interface{}, len(columnNames))
		valuesWrapped := make([]interface{}, len(columnNames))
		for i := range values {
			valuesWrapped[i] = &values[i]
		}
		if err := rows.Scan(valuesWrapped...); err != nil {
			return err
		}
		jObj := map[string]interface{}{}
		for i, v := range values {
			switch t := v.(type) {
			case string:
				jObj[columnNames[i]] = t
			case []byte:
				jObj[columnNames[i]] = string(t)
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				jObj[columnNames[i]] = t
			case float32, float64:
				jObj[columnNames[i]] = t
			case bool:
				jObj[columnNames[i]] = t
			default:
				jObj[columnNames[i]] = t
			}
		}
		jArray = append(jArray, jObj)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return part.SetJSON(jArray)
}

func strToSQLResultCodec(codec string) (sqlResultCodec, error) {
	switch codec {
	case "json_array":
		return sqlResultJSONArrayCodec, nil
	case "none":
		return nil, nil
	}
	return nil, fmt.Errorf("unrecognised result codec: %v", codec)
}

//------------------------------------------------------------------------------

func (s *SQL) doExecute(argSets [][]interface{}) (errs []error) {
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

	var tx *sql.Tx
	if tx, err = s.db.Begin(); err != nil {
		return
	}

	stmt := s.query
	if stmt == nil {
		if stmt, err = tx.Prepare(s.queryStr); err != nil {
			return
		}
		defer stmt.Close()
	} else {
		stmt = tx.Stmt(stmt)
	}

	for i, args := range argSets {
		if len(args) == 0 {
			continue
		}
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

func (s *SQL) getArgs(index int, msg types.Message) ([]interface{}, error) {
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

// ProcessMessage logs an event and returns the message unchanged.
func (s *SQL) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	s.dbMux.RLock()
	defer s.dbMux.RUnlock()

	s.mCount.Incr(1)
	newMsg := msg.Copy()

	if s.resCodec == nil && s.dynQuery == nil {
		argSets := make([][]interface{}, newMsg.Len())
		newMsg.Iter(func(index int, p types.Part) error {
			args, err := s.getArgs(index, msg)
			if err != nil {
				s.mErr.Incr(1)
				s.log.Errorf("Args mapping error: %v\n", err)
				FlagErr(newMsg.Get(index), err)
				return nil
			}
			argSets[index] = args
			return nil
		})

		for i, err := range s.doExecute(argSets) {
			if err != nil {
				s.mErr.Incr(1)
				s.log.Errorf("SQL error: %v\n", err)
				FlagErr(newMsg.Get(i), err)
			}
		}
	} else {
		IteratePartsWithSpanV2(TypeSQL, nil, newMsg, func(index int, span *tracing.Span, part types.Part) error {
			args, err := s.getArgs(index, msg)
			if err != nil {
				s.mErr.Incr(1)
				s.log.Errorf("Args mapping error: %v\n", err)
				return err
			}

			if s.resCodec == nil {
				if s.dynQuery != nil {
					queryStr := s.dynQuery.String(index, msg)
					_, err = s.db.Exec(queryStr, args...)
				} else {
					_, err = s.query.Exec(args...)
				}
				if err != nil {
					return fmt.Errorf("failed to execute query: %w", err)
				}
				return nil
			}

			var rows *sql.Rows
			if s.dynQuery != nil {
				queryStr := s.dynQuery.String(index, msg)
				rows, err = s.db.Query(queryStr, args...)
			} else {
				rows, err = s.query.Query(args...)
			}
			if err == nil {
				defer rows.Close()
				if err = s.resCodec(rows, part); err != nil {
					err = fmt.Errorf("failed to apply result codec: %v", err)
				}
			} else {
				err = fmt.Errorf("failed to execute query: %v", err)
			}
			if err != nil {
				s.mErr.Incr(1)
				s.log.Errorf("SQL error: %v\n", err)
				return err
			}
			return nil
		})
	}

	s.mBatchSent.Incr(1)
	s.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (s *SQL) CloseAsync() {
	s.closeOnce.Do(func() {
		close(s.closeChan)
	})
}

// WaitForClose blocks until the processor has closed down.
func (s *SQL) WaitForClose(timeout time.Duration) error {
	select {
	case <-time.After(timeout):
		return types.ErrTimeout
	case <-s.closedChan:
	}
	return nil
}

//------------------------------------------------------------------------------
