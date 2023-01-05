package sql

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/Masterminds/squirrel"

	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
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
			Advanced().
			Example("ON CONFLICT (name) DO NOTHING")).
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
	err := service.RegisterBatchOutput(
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
	if s.driver == "postgres" || s.driver == "clickhouse" {
		s.builder = s.builder.PlaceholderFormat(squirrel.Dollar)
	} else if s.driver == "oracle" {
		s.builder = s.builder.PlaceholderFormat(squirrel.Colon)
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
	if s.db, err = sqlOpenWithReworks(s.logger, s.driver, s.dsn); err != nil {
		return err
	}

	s.connSettings.apply(ctx, s.db, s.logger)

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
		sqlStr, _, err := insertBuilder.ToSql()
		if err != nil {
			return err
		}
		if stmt, err = tx.Prepare(sqlStr); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	for i := range batch {
		var args []any
		if s.argsMapping != nil {
			resMsg, err := batch.BloblangQuery(i, s.argsMapping)
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
