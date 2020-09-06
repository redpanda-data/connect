package processor

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/opentracing/opentracing-go"

	// SQL Drivers
	_ "github.com/go-sql-driver/mysql"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSQL] = TypeSpec{
		constructor: NewSQL,
		Categories: []Category{
			CategoryIntegration,
		},
		Summary: `
Runs an SQL prepared query against a target database for each message and, for
queries that return rows, replaces it with the result according to a
[codec](#result-codecs).`,
		Description: `
If a query contains arguments they can be set as an array of strings supporting
[interpolation functions](/docs/configuration/interpolation#bloblang-queries) in
the ` + "`args`" + ` field.

## Drivers

The following is a list of supported drivers and their respective DSN formats:

| Driver | Data Source Name Format |
|---|---|
` + "| `mysql` | `[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]` |" + `
` + "| `postgres` | `postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]` |" + `

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
        args:
          - ${! json("document.foo") }
          - ${! json("document.bar") }
          - ${! meta("kafka_topic") }
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
              driver: postgresql
              result_codec: json_array
              data_source_name: postgres://foouser:foopass@localhost:5432/testdb?sslmode=disable
              query: "SELECT * FROM footable WHERE user_id = $1;"
              args:
                - ${! json("user.id") }
        result_map: 'root.foo_rows = this'
`,
			},
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"driver",
				"A database [driver](#drivers) to use.",
			).HasOptions("mysql", "postgres"),
			docs.FieldCommon(
				"data_source_name", "A Data Source Name to identify the target database.",
				"foouser:foopassword@tcp(localhost:3306)/foodb",
				"postgres://foouser:foopass@localhost:5432/foodb?sslmode=disable",
			),
			docs.FieldDeprecated("dsn"),
			docs.FieldCommon(
				"query", "The query to run against the database.",
				"INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);",
			),
			docs.FieldCommon(
				"args",
				"A list of arguments for the query to be resolved for each message.",
			).SupportsInterpolation(true),
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
	Driver         string   `json:"driver" yaml:"driver"`
	DataSourceName string   `json:"data_source_name" yaml:"data_source_name"`
	DSN            string   `json:"dsn" yaml:"dsn"`
	Query          string   `json:"query" yaml:"query"`
	Args           []string `json:"args" yaml:"args"`
	ResultCodec    string   `json:"result_codec" yaml:"result_codec"`
}

// NewSQLConfig returns a SQLConfig with default values.
func NewSQLConfig() SQLConfig {
	return SQLConfig{
		Driver:         "mysql",
		DataSourceName: "",
		DSN:            "",
		Query:          "",
		Args:           []string{},
		ResultCodec:    "none",
	}
}

//------------------------------------------------------------------------------

// SQL is a processor that executes an SQL query for each message.
type SQL struct {
	log   log.Modular
	stats metrics.Type

	conf     SQLConfig
	db       *sql.DB
	dbMux    sync.Mutex
	args     []field.Expression
	resCodec sqlResultCodec

	// TODO: V4 Remove this
	deprecated         bool
	resCodecDeprecated sqlResultCodecDeprecated

	query *sql.Stmt

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
	deprecated := false
	dsn := conf.SQL.DataSourceName
	if len(conf.SQL.DSN) > 0 {
		if len(dsn) > 0 {
			return nil, errors.New("specified both a deprecated `dsn` as well as a `data_source_name`")
		}
		dsn = conf.SQL.DSN
		deprecated = true
	}

	var db *sql.DB
	var err error
	if db, err = sql.Open(conf.SQL.Driver, dsn); err != nil {
		return nil, err
	}

	var args []field.Expression
	for i, v := range conf.SQL.Args {
		expr, err := bloblang.NewField(v)
		if err != nil {
			return nil, fmt.Errorf("failed to parse arg %v expression: %v", i, err)
		}
		args = append(args, expr)
	}

	s := &SQL{
		log:        log,
		stats:      stats,
		conf:       conf.SQL,
		db:         db,
		args:       args,
		deprecated: deprecated,
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}

	if deprecated {
		if s.resCodecDeprecated, err = strToSQLResultCodecDeprecated(conf.SQL.ResultCodec); err != nil {
			return nil, err
		}
	} else {
		if s.resCodec, err = strToSQLResultCodec(conf.SQL.ResultCodec); err != nil {
			return nil, err
		}
	}

	if s.query, err = db.Prepare(conf.SQL.Query); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to prepare query: %v", err)
	}

	go func() {
		defer func() {
			s.db.Close()
			if s.query != nil {
				s.query.Close()
			}
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

func (s *SQL) doExecute(args ...interface{}) error {
	_, err := s.query.Exec(args...)
	return err
}

func (s *SQL) doQuery(args ...interface{}) (*sql.Rows, error) {
	return s.query.Query(args...)
}

// ProcessMessage logs an event and returns the message unchanged.
func (s *SQL) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	if s.deprecated {
		return s.processMessageDeprecated(msg)
	}

	s.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span opentracing.Span, part types.Part) error {
		args := make([]interface{}, len(s.args))
		for i, v := range s.args {
			args[i] = v.String(index, msg)
		}
		var err error
		if s.resCodec == nil {
			if err = s.doExecute(args...); err != nil {
				err = fmt.Errorf("failed to execute query: %v", err)
			}
		} else {
			var rows *sql.Rows
			if rows, err = s.doQuery(args...); err == nil {
				defer rows.Close()
				if err = s.resCodec(rows, part); err != nil {
					err = fmt.Errorf("failed to apply result codec: %v", err)
				}
			} else {
				err = fmt.Errorf("failed to execute query: %v", err)
			}
		}
		if err != nil {
			s.mErr.Incr(1)
			s.log.Debugf("SQL error: %v\n", err)
			return err
		}
		return nil
	}

	IteratePartsWithSpan(TypeSQL, nil, newMsg, proc)

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
