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
	"strings"
	"sync"
	"time"

	"github.com/Masterminds/squirrel"
	hdbdriver "github.com/SAP/go-hdb/driver"

	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func sqlInsertOutputConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary("Inserts a row into an SQL database for each message.").
		Description(``).
		Field(driverField).
		Field(dsnField).
		Field(service.NewStringField("table").
			Description("The table to insert to.").
			Example("foo")).
		Field(service.NewStringListField("columns").
			Description("A list of columns to insert.").
			Example([]string{"foo", "bar", "baz"})).
		Field(service.NewBloblangField("args_mapping").
			Description("A xref:guides:bloblang/about.adoc[Bloblang mapping] which should evaluate to an array of values matching in size to the number of columns specified.").
			Example("root = [ this.cat.meow, this.doc.woofs[0] ]").
			Example(`root = [ meta("user.id") ]`)).
		Field(service.NewStringField("prefix").
			Description("An optional prefix to prepend to the insert query (before INSERT).").
			Optional().
			Advanced()).
		Field(service.NewStringField("suffix").
			Description("An optional suffix to append to the insert query.").
			Optional().
			Advanced().
			Example("ON CONFLICT (name) DO NOTHING")).
		Field(service.NewStringListField("options").
			Description("A list of keyword options to add before the INTO clause of the query.").
			Optional().
			Advanced().
			Example([]string{"DELAYED", "IGNORE"})).
		Field(service.NewBoolField("upsert").
			Description("When true and driver is `hana`, emit `UPSERT … WITH PRIMARY KEY` instead of `INSERT INTO`. The table must have a primary key; matching rows are updated rather than inserted, preventing duplicates on retry.").
			Default(false).
			Advanced()).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of inserts to run in parallel.").
			Default(64))

	for _, f := range connFields() {
		spec = spec.Field(f)
	}

	spec = spec.Field(service.NewBatchPolicyField("batching")).
		Version("3.59.0").
		Example("Table Insert (MySQL)",
			`
Here we insert rows into a database by populating the columns id, name and topic with values extracted from messages and metadata:`,
			`
output:
  sql_insert:
    driver: mysql
    dsn: foouser:foopassword@tcp(localhost:3306)/foodb
    table: footable
    columns: [ id, name, topic ]
    args_mapping: |
      root = [
        this.user.id,
        this.user.name,
        meta("kafka_topic"),
      ]
`,
		)
	return spec
}

func init() {
	service.MustRegisterBatchOutput(
		"sql_insert", sqlInsertOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			out, err = newSQLInsertOutputFromConfig(conf, mgr)
			return
		})
}

//------------------------------------------------------------------------------

type sqlInsertOutput struct {
	driver  string
	dsn     string
	table   string
	columns []string
	db      *sql.DB
	builder squirrel.InsertBuilder
	dbMut   sync.RWMutex

	useTxStmt     bool
	hana          *hanaWriter // non-nil when driver == "hana"
	argsMapping   *bloblang.Executor
	argsConverter argsConverter

	connSettings *connSettings

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

func newSQLInsertOutputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*sqlInsertOutput, error) {
	s := &sqlInsertOutput{
		logger:  mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
	}

	var err error

	if s.driver, err = conf.FieldString("driver"); err != nil {
		return nil, err
	}
	if _, in := map[string]struct{}{
		"clickhouse": {},
		"oracle":     {},
	}[s.driver]; in {
		s.useTxStmt = true
	}

	if s.dsn, err = conf.FieldString("dsn"); err != nil {
		return nil, err
	}

	tableStr, err := conf.FieldString("table")
	if err != nil {
		return nil, err
	}
	s.table = tableStr

	columns, err := conf.FieldStringList("columns")
	if err != nil {
		return nil, err
	}
	s.columns = columns

	if conf.Contains("args_mapping") {
		if s.argsMapping, err = conf.FieldBloblang("args_mapping"); err != nil {
			return nil, err
		}
	}

	s.builder = squirrel.Insert(tableStr).Columns(columns...)
	switch s.driver {
	case "postgres", "pgx", "clickhouse":
		s.builder = s.builder.PlaceholderFormat(squirrel.Dollar)
	case "oracle", "gocosmos":
		s.builder = s.builder.PlaceholderFormat(squirrel.Colon)
	}

	if s.driver == "postgres" || s.driver == "pgx" {
		s.argsConverter = bloblValuesToPgSQLValues
	} else {
		s.argsConverter = func(v []any) []any { return v }
	}

	if s.useTxStmt {
		values := make([]any, 0, len(columns))
		for _, c := range columns {
			values = append(values, c)
		}
		s.builder = s.builder.Values(values...)
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

	if conf.Contains("options") {
		options, err := conf.FieldStringList("options")
		if err != nil {
			return nil, err
		}
		s.builder = s.builder.Options(options...)
	}

	if s.driver == "hana" {
		upsert, err := conf.FieldBool("upsert")
		if err != nil {
			return nil, err
		}
		s.hana = newHANAWriter(tableStr, columns, upsert)
	}

	if s.connSettings, err = connSettingsFromParsed(conf, mgr); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *sqlInsertOutput) Connect(ctx context.Context) error {
	s.dbMut.Lock()
	defer s.dbMut.Unlock()

	if s.db != nil {
		return nil
	}

	var err error
	if s.hana != nil {
		if s.db, err = openHANADB(s.dsn); err != nil {
			return err
		}
	} else {
		if s.db, err = sqlOpenWithReworks(s.logger, s.driver, s.dsn); err != nil {
			return err
		}
	}

	s.connSettings.apply(ctx, s.db, s.logger)
	// For ClickHouse, replace the default no-op argsConverter with one that
	// normalizes map/slice arguments to the concrete Go types the driver expects.
	// This must happen at connect time because we need a live DB connection to
	// introspect column types.
	if s.driver == "clickhouse" {
		if s.argsConverter, err = newClickhouseArgsConverter(ctx, s.db, s.table, s.columns, s.logger); err != nil {
			_ = s.db.Close()
			s.db = nil
			return err
		}
	}

	go func() {
		<-s.shutSig.HardStopChan()

		s.dbMut.Lock()
		_ = s.db.Close()
		s.dbMut.Unlock()

		s.shutSig.TriggerHasStopped()
	}()
	return nil
}

func (s *sqlInsertOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	s.dbMut.RLock()
	defer s.dbMut.RUnlock()

	if s.hana != nil {
		return s.hana.writeBatch(ctx, s.db, batch, s.argsMapping)
	}

	insertBuilder := s.builder

	var tx *sql.Tx
	var stmt *sql.Stmt
	if s.useTxStmt {
		var err error
		if tx, err = s.db.Begin(); err != nil {
			return err
		}
		sqlStr, _, err := insertBuilder.ToSql()
		if err != nil {
			return err
		}
		if stmt, err = tx.Prepare(sqlStr); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	var argsExec *service.MessageBatchBloblangExecutor
	if s.argsMapping != nil {
		argsExec = batch.BloblangExecutor(s.argsMapping)
	}
	for i := range batch {
		var args []any
		if argsExec != nil {
			resMsg, err := argsExec.Query(i)
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
			args = s.argsConverter(args)
		}

		if tx == nil {
			insertBuilder = insertBuilder.Values(args...)
		} else if _, err := stmt.Exec(args...); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	var err error
	if tx == nil {
		_, err = insertBuilder.RunWith(s.db).ExecContext(ctx)
	} else {
		err = tx.Commit()
	}
	return err
}

func (s *sqlInsertOutput) Close(ctx context.Context) error {
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

//------------------------------------------------------------------------------
// SAP HANA bulk insert/upsert support.
//
// go-hdb (github.com/SAP/go-hdb) is used directly instead of the generic
// sql.Open path because HANA's MT_EXECUTE bulk protocol needs driver-specific
// connector options (BulkSize) that database/sql's generic interface can't
// express. Only active when driver == "hana"; every other driver above is
// unaffected.
//------------------------------------------------------------------------------

// hanaWriter holds state for go-hdb bulk-insert/upsert operations.
// It is only created when driver == "hana".
type hanaWriter struct {
	execSQL string
	mu      sync.Mutex
}

func newHANAWriter(table string, columns []string, upsert bool) *hanaWriter {
	phs := make([]string, len(columns))
	for i := range phs {
		phs[i] = "?"
	}
	colList := strings.Join(columns, ", ")
	phList := strings.Join(phs, ", ")
	var execSQL string
	if upsert {
		execSQL = "UPSERT " + table + " (" + colList + ") VALUES (" + phList + ") WITH PRIMARY KEY"
	} else {
		execSQL = "INSERT INTO " + table + " (" + colList + ") VALUES (" + phList + ")"
	}
	return &hanaWriter{execSQL: execSQL}
}

// openHANADB opens a go-hdb connection tuned for bulk insert.
//
// We bypass sql.Open and use a DSN connector directly so we can set BulkSize
// (guarantees one MT_EXECUTE per WriteBatch call) and restore the TCP timeout
// that NewDSNConnector zeros when the DSN has no timeout= parameter.
// MaxIdleConns=0 closes the connection after each batch, avoiding HANA
// server-side post-commit state that can block the next MT_EXECUTE.
func openHANADB(dsn string) (*sql.DB, error) {
	ctr, err := hdbdriver.NewDSNConnector(dsn)
	if err != nil {
		return nil, err
	}
	ctr.SetTimeout(30 * time.Second)
	ctr.SetBulkSize(100_000)
	db := sql.OpenDB(ctr)
	db.SetMaxIdleConns(0)
	return db, nil
}

// writeBatch performs a go-hdb bulk insert/upsert for one benthos batch.
//
// mu serialises concurrent calls: concurrent MT_EXECUTE to the same table
// causes HANA row-level lock contention. (*sql.Conn).ExecContext is used
// instead of (*sql.DB).ExecContext to avoid the DB-level retry loop that
// re-invokes the callback with idx already exhausted, silently writing 0 rows.
func (h *hanaWriter) writeBatch(ctx context.Context, db *sql.DB, batch service.MessageBatch, argsMapping *bloblang.Executor) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	var argsExec *service.MessageBatchBloblangExecutor
	if argsMapping != nil {
		argsExec = batch.BloblangExecutor(argsMapping)
	}
	batchArgs := make([][]any, 0, len(batch))
	for i := range batch {
		if argsExec == nil {
			break
		}
		resMsg, err := argsExec.Query(i)
		if err != nil {
			return err
		}
		iargs, err := resMsg.AsStructured()
		if err != nil {
			return err
		}
		args, ok := iargs.([]any)
		if !ok {
			return fmt.Errorf("mapping returned non-array result: %T", iargs)
		}
		batchArgs = append(batchArgs, args)
	}

	execCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, err := db.Conn(execCtx)
	if err != nil {
		return err
	}
	defer conn.Close()

	idx := 0
	_, err = conn.ExecContext(execCtx, h.execSQL, func(args []any) error {
		if idx >= len(batchArgs) {
			return hdbdriver.ErrEndOfRows
		}
		copy(args, batchArgs[idx])
		idx++
		return nil
	})
	return err
}
