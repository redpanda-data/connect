// Copyright (c) 2019 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package processor

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	olog "github.com/opentracing/opentracing-go/log"

	// SQL Drivers
	_ "github.com/go-sql-driver/mysql"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSQL] = TypeSpec{
		constructor: NewSQL,
		description: `
SQL is a processor that runs a query against a target database for each message
batch and, for queries that return rows, replaces the batch with the result.

If a query contains arguments they can be set as an array of strings supporting
[interpolation functions](../config_interpolation.md#functions) in the
` + "`args`" + ` field.

In order to execute an SQL query for each message of the batch use this
processor within a ` + "[`for_each`](#for_each)" + ` processor:

` + "``` yaml" + `
for_each:
- sql:
    driver: mysql
    dsn: foouser:foopassword@tcp(localhost:3306)/foodb
    query: "INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);"
    args:
    - ${!json_field:document.foo}
    - ${!json_field:document.bar}
    - ${!metadata:kafka_topic}
` + "```" + `

### Result Codecs

When a query returns rows they are serialised according to a chosen codec, and
the batch contents are replaced with the serialised result.

#### ` + "`none`" + `

The result of the query is ignored and the message batch remains unchanged. If
your query does not return rows then this is the appropriate codec.

#### ` + "`json_array`" + `

The resulting rows are serialised into an array of JSON objects, where each
object represents a row, where the key is the column name and the value is that
columns value in the row.

### Drivers

The following is a list of supported drivers and their respective DSN formats:

- ` + "`mysql`: `[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]`" + `
- ` + "`postgres`: `postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]`" + `

Please note that the ` + "`postgres`" + ` driver enforces SSL by default, you
can override this with the parameter ` + "`sslmode=disable`" + ` if required.`,
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
	args     []*text.InterpolatedString
	resCodec sqlResultCodec

	queryStr *text.InterpolatedString
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
	resCodec, err := strToSQLResultCodec(conf.SQL.ResultCodec)
	if err != nil {
		return nil, err
	}
	var db *sql.DB
	if db, err = sql.Open(conf.SQL.Driver, conf.SQL.DSN); err != nil {
		return nil, err
	}
	var args []*text.InterpolatedString
	for _, v := range conf.SQL.Args {
		args = append(args, text.NewInterpolatedString(v))
	}
	s := &SQL{
		log:        log,
		stats:      stats,
		conf:       conf.SQL,
		db:         db,
		args:       args,
		queryStr:   text.NewInterpolatedString(""),
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

func (s *SQL) doExecute(query string, args ...interface{}) error {
	if s.query != nil {
		_, err := s.query.Exec(args...)
		return err
	}
	if len(query) == 0 {
		return errors.New("query string is empty")
	}
	_, err := s.db.Exec(query, args...)
	return err
}

func (s *SQL) doQuery(query string, args ...interface{}) (*sql.Rows, error) {
	if s.query != nil {
		return s.query.Query(args...)
	}
	if len(query) == 0 {
		return nil, errors.New("query string is empty")
	}
	return s.db.Query(query, args...)
}

// ProcessMessage logs an event and returns the message unchanged.
func (s *SQL) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	s.mCount.Incr(1)
	result := msg.Copy()

	spans := tracing.CreateChildSpans(TypeSQL, result)

	args := make([]interface{}, len(s.args))
	for i, v := range s.args {
		args[i] = v.Get(result)
	}
	var err error
	if s.resCodec == nil {
		if err = s.doExecute(s.queryStr.Get(result), args...); err != nil {
			err = fmt.Errorf("failed to execute query: %v", err)
		}
	} else {
		var rows *sql.Rows
		if rows, err = s.doQuery(s.queryStr.Get(result), args...); err == nil {
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
