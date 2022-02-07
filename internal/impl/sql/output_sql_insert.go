package sql

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/public/bloblang"
	"github.com/Jeffail/benthos/v3/public/service"
	"github.com/Masterminds/squirrel"
)

func sqlInsertOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
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
			Description("A [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to an array of values matching in size to the number of columns specified.").
			Example("root = [ this.cat.meow, this.doc.woofs[0] ]").
			Example(`root = [ meta("user.id") ]`)).
		Field(service.NewStringField("prefix").
			Description("An optional prefix to prepend to the insert query (before INSERT).").
			Optional().
			Advanced()).
		Field(service.NewStringField("suffix").
			Description("An optional suffix to append to the insert query.").
			Optional().
			Advanced()).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of inserts to run in parallel.").
			Default(64)).
		Field(service.NewBatchPolicyField("batching")).
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
}

func init() {
	err := service.RegisterBatchOutput(
		"sql_insert", sqlInsertOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			out, err = newSQLInsertOutputFromConfig(conf, mgr.Logger())
			return
		})

	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type sqlInsertOutput struct {
	driver  string
	dsn     string
	db      *sql.DB
	builder squirrel.InsertBuilder
	dbMut   sync.RWMutex

	useTxStmt   bool
	argsMapping *bloblang.Executor

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

func newSQLInsertOutputFromConfig(conf *service.ParsedConfig, logger *service.Logger) (*sqlInsertOutput, error) {
	s := &sqlInsertOutput{
		logger:  logger,
		shutSig: shutdown.NewSignaller(),
	}

	var err error

	if s.driver, err = conf.FieldString("driver"); err != nil {
		return nil, err
	}
	if _, in := map[string]struct{}{
		"clickhouse": {},
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

	columns, err := conf.FieldStringList("columns")
	if err != nil {
		return nil, err
	}

	if conf.Contains("args_mapping") {
		if s.argsMapping, err = conf.FieldBloblang("args_mapping"); err != nil {
			return nil, err
		}
	}

	s.builder = squirrel.Insert(tableStr).Columns(columns...)
	if s.driver == "postgres" {
		s.builder = s.builder.PlaceholderFormat(squirrel.Dollar)
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

	return s, nil
}

func (s *sqlInsertOutput) Connect(ctx context.Context) error {
	s.dbMut.Lock()
	defer s.dbMut.Unlock()

	if s.db != nil {
		return nil
	}

	var err error
	if s.db, err = sql.Open(s.driver, s.dsn); err != nil {
		return err
	}

	go func() {
		<-s.shutSig.CloseNowChan()

		s.dbMut.Lock()
		_ = s.db.Close()
		s.dbMut.Unlock()

		s.shutSig.ShutdownComplete()
	}()
	return nil
}

func (s *sqlInsertOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	s.dbMut.RLock()
	defer s.dbMut.RUnlock()

	insertBuilder := s.builder

	var tx *sql.Tx
	var stmt *sql.Stmt
	if s.useTxStmt {
		var err error
		if tx, err = s.db.Begin(); err != nil {
			return err
		}
		sqlStr, _, err := insertBuilder.Values().ToSql()
		if err != nil {
			return err
		}
		if stmt, err = tx.Prepare(sqlStr); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	for i := range batch {
		var args []interface{}
		resMsg, err := batch.BloblangQuery(i, s.argsMapping)
		if err != nil {
			return err
		}

		iargs, err := resMsg.AsStructured()
		if err != nil {
			return err
		}

		var ok bool
		if args, ok = iargs.([]interface{}); !ok {
			return fmt.Errorf("mapping returned non-array result: %T", iargs)
		}

		if tx == nil {
			insertBuilder = insertBuilder.Values(args...)
		} else if _, err = stmt.Exec(args...); err != nil {
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
	s.shutSig.CloseNow()
	s.dbMut.RLock()
	isNil := s.db == nil
	s.dbMut.RUnlock()
	if isNil {
		return nil
	}
	select {
	case <-s.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
