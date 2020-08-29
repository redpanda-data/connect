package processor

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	olog "github.com/opentracing/opentracing-go/log"

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
Runs an SQL prepared query against a target database for each message batch and,
for queries that return rows, replaces the batch with the result according to a
[codec](#result-codecs).`,
		Description: `
If a query contains arguments they can be set as an array of strings supporting
[interpolation functions](/docs/configuration/interpolation#bloblang-queries) in
the ` + "`args`" + ` field.

## Drivers

The following is a list of supported drivers and their respective DSN formats:

- ` + "`mysql`: `[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]`" + `
- ` + "`postgres`: `postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]`" + `

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
    - for_each:
      - sql:
          driver: mysql
          dsn: foouser:foopassword@tcp(localhost:3306)/foodb
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
              dsn: postgres://foouser:foopass@localhost:5432/testdb?sslmode=disable
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
				"dsn", "A Data Source Name to identify the target database.",
				"foouser:foopassword@tcp(localhost:3306)/foodb",
			),
			docs.FieldCommon(
				"query", "The query to run against the database.",
				"INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);",
			),
			docs.FieldCommon(
				"args",
				"A list of arguments for the query to be resolved for each message batch.",
			).SupportsInterpolation(true),
			docs.FieldCommon(
				"result_codec",
				"A [codec](#result-codecs) to determine how resulting rows are converted into messages.",
			).HasOptions("none", "json_array"),
		},
		Footnotes: `
## Result Codecs

When a query returns rows they are serialised according to a chosen codec, and
the batch contents are replaced with the serialised result.

### ` + "`none`" + `

The result of the query is ignored and the message batch remains unchanged. If
your query does not return rows then this is the appropriate codec.

### ` + "`json_array`" + `

The resulting rows are serialised into an array of JSON objects, where each
object represents a row, where the key is the column name and the value is that
columns value in the row.`,
	}
}

//------------------------------------------------------------------------------

// SQLConfig contains configuration fields for the SQL processor.
type SQLConfig struct {
	Driver      string   `json:"driver" yaml:"driver"`
	DSN         string   `json:"dsn" yaml:"dsn"`
	Query       string   `json:"query" yaml:"query"`
	Args        []string `json:"args" yaml:"args"`
	ResultCodec string   `json:"result_codec" yaml:"result_codec"`
}

// NewSQLConfig returns a SQLConfig with default values.
func NewSQLConfig() SQLConfig {
	return SQLConfig{
		Driver:      "mysql",
		DSN:         "",
		Query:       "",
		Args:        []string{},
		ResultCodec: "none",
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
	resCodec, err := strToSQLResultCodec(conf.SQL.ResultCodec)
	if err != nil {
		return nil, err
	}
	var db *sql.DB
	if db, err = sql.Open(conf.SQL.Driver, conf.SQL.DSN); err != nil {
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
		resCodec:   resCodec,
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
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

type sqlResultCodec func(rows *sql.Rows, msg types.Message) error

func sqlResultJSONArrayCodec(rows *sql.Rows, msg types.Message) error {
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
	if msg.Len() > 0 {
		p := msg.Get(0)
		msg.SetAll([]types.Part{p})
		return msg.Get(0).SetJSON(jArray)
	}
	msg.Append(message.NewPart(nil))
	return msg.Get(0).SetJSON(jArray)
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
	s.mCount.Incr(1)
	result := msg.Copy()

	spans := tracing.CreateChildSpans(TypeSQL, result)

	args := make([]interface{}, len(s.args))
	for i, v := range s.args {
		args[i] = v.String(0, result)
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
			if err = s.resCodec(rows, result); err != nil {
				err = fmt.Errorf("failed to apply result codec: %v", err)
			}
		} else {
			err = fmt.Errorf("failed to execute query: %v", err)
		}
	}
	if err != nil {
		result.Iter(func(i int, p types.Part) error {
			FlagErr(p, err)
			spans[i].LogFields(
				olog.String("event", "error"),
				olog.String("type", err.Error()),
			)
			return nil
		})
		s.log.Errorf("SQL error: %v\n", err)
		s.mErr.Incr(1)
	}
	for _, s := range spans {
		s.Finish()
	}

	s.mSent.Incr(int64(result.Len()))
	s.mBatchSent.Incr(1)

	return []types.Message{result}, nil
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
