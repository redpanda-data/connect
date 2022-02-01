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

func InsertProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Integration").
		Summary("Inserts rows into an SQL database for each message, and leaves the message unchanged.").
		Description(`
If the insert fails to execute then the message will still remain unchanged and the error can be caught using error handling methods outlined [here](/docs/configuration/error_handling).`).
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
		Version("3.59.0").
		Example("Table Insert (MySQL)",
			`
Here we insert rows into a database by populating the columns id, name and topic with values extracted from messages and metadata:`,
			`
pipeline:
  processors:
    - sql_insert:
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
	err := service.RegisterBatchProcessor(
		"sql_insert", InsertProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return NewSQLInsertProcessorFromConfig(conf, mgr.Logger())
		})

	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type sqlInsertProcessor struct {
	db      *sql.DB
	builder squirrel.InsertBuilder
	dbMut   sync.RWMutex

	useTxStmt   bool
	argsMapping *bloblang.Executor

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

func NewSQLInsertProcessorFromConfig(conf *service.ParsedConfig, logger *service.Logger) (*sqlInsertProcessor, error) {
	s := &sqlInsertProcessor{
		logger:  logger,
		shutSig: shutdown.NewSignaller(),
	}

	driverStr, err := conf.FieldString("driver")
	if err != nil {
		return nil, err
	}
	if _, in := map[string]struct{}{
		"clickhouse": {},
	}[driverStr]; in {
		s.useTxStmt = true
	}

	dsnStr, err := conf.FieldString("dsn")
	if err != nil {
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
	if driverStr == "postgres" {
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

	if s.db, err = sql.Open(driverStr, dsnStr); err != nil {
		return nil, err
	}

	go func() {
		<-s.shutSig.CloseNowChan()

		s.dbMut.Lock()
		_ = s.db.Close()
		s.dbMut.Unlock()

		s.shutSig.ShutdownComplete()
	}()
	return s, nil
}

func (s *sqlInsertProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	s.dbMut.RLock()
	defer s.dbMut.RUnlock()

	insertBuilder := s.builder

	var tx *sql.Tx
	var stmt *sql.Stmt
	if s.useTxStmt {
		var err error
		if tx, err = s.db.Begin(); err != nil {
			return nil, err
		}
		sqlStr, _, err := insertBuilder.Values().ToSql()
		if err != nil {
			return nil, err
		}
		if stmt, err = tx.Prepare(sqlStr); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
	}

	batch = batch.Copy()
	for i, msg := range batch {
		var args []interface{}
		resMsg, err := batch.BloblangQuery(i, s.argsMapping)
		if err != nil {
			msg.SetError(err)
			continue
		}

		iargs, err := resMsg.AsStructured()
		if err != nil {
			msg.SetError(fmt.Errorf("mapping returned non-structured result: %w", err))
			continue
		}

		var ok bool
		if args, ok = iargs.([]interface{}); !ok {
			msg.SetError(fmt.Errorf("mapping returned non-array result: %T", iargs))
			continue
		}

		if tx == nil {
			insertBuilder = insertBuilder.Values(args...)
		} else if _, err = stmt.Exec(args...); err != nil {
			return nil, err
		}
	}

	var err error
	if tx == nil {
		_, err = insertBuilder.RunWith(s.db).ExecContext(ctx)
	} else {
		err = tx.Commit()
	}
	if err != nil {
		return nil, err
	}
	return []service.MessageBatch{batch}, nil
}

func (s *sqlInsertProcessor) Close(ctx context.Context) error {
	s.shutSig.CloseNow()
	select {
	case <-s.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
