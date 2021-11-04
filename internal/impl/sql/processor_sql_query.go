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

func sqlQueryProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Integration").
		Summary("Runs an SQL query against a database and returns the result as a two-dimensional JSON array of rows of column values.").
		Description(`
If the query fails to execute then the message will remain unchanged and the error can be caught using error handling methods outlined [here](/docs/configuration/error_handling).`).
		Field(driverField).
		Field(dsnField).
		Field(service.NewStringField("table").
			Description("The table to query.").
			Example("foo")).
		Field(service.NewStringListField("columns").
			Description("A list of columns to query.").
			Example([]string{"*"}).
			Example([]string{"foo", "bar", "baz"})).
		Field(service.NewStringField("where").
			Description("An optional where clause to add. Placeholder arguments are populated with the `args_mapping` field. Placeholders should always be question marks, and will automatically be converted to dollar syntax when the postgres driver is used.").
			Example("meow = ? and woof = ?").
			Example("user_id = ?").
			Optional()).
		Field(service.NewBloblangField("args_mapping").
			Description("An optional [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to an array of values matching in size to the number of placeholder arguments in the field `where`.").
			Example("root = [ this.cat.meow, this.doc.woofs[0] ]").
			Example(`root = [ meta("user.id") ]`).
			Optional()).
		Version("3.59.0").
		Example("Table Query (PostgreSQL)",
			`
Here we query a database for columns of footable that share a `+"`user_id`"+`
with the message `+"`user.id`"+`. A `+"[`branch` processor](/docs/components/processors/branch)"+`
is used in order to insert the resulting array into the original message at the
path `+"`foo_rows`"+`:`,
			`
pipeline:
  processors:
    - branch:
        processors:
          - sql_query:
              driver: postgres
              dsn: postgres://foouser:foopass@localhost:5432/testdb?sslmode=disable
              table: footable
              columns: [ '*' ]
              where: user_id = ?
              args_mapping: '[ this.user.id ]'
        result_map: 'root.foo_rows = this'
`,
		)
}

func init() {
	err := service.RegisterBatchProcessor(
		"sql_query", sqlQueryProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newSQLQueryProcessorFromConfig(conf, mgr.Logger())
		})

	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type sqlQueryProcessor struct {
	db      *sql.DB
	builder squirrel.SelectBuilder
	dbMut   sync.Mutex

	table       string
	columns     []string
	where       string
	argsMapping *bloblang.Executor

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

func newSQLQueryProcessorFromConfig(conf *service.ParsedConfig, logger *service.Logger) (*sqlQueryProcessor, error) {
	s := &sqlQueryProcessor{
		logger:  logger,
		shutSig: shutdown.NewSignaller(),
	}

	driverStr, err := conf.FieldString("driver")
	if err != nil {
		return nil, err
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

	if conf.Contains("where") {
		if s.where, err = conf.FieldString("where"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("args_mapping") {
		if s.argsMapping, err = conf.FieldBloblang("args_mapping"); err != nil {
			return nil, err
		}
	}

	if s.db, err = sql.Open(driverStr, dsnStr); err != nil {
		return nil, err
	}

	s.builder = squirrel.Select(columns...).From(tableStr)
	if driverStr == "postgres" {
		s.builder = s.builder.PlaceholderFormat(squirrel.Dollar)
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

func (s *sqlQueryProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	s.dbMut.Lock()
	defer s.dbMut.Unlock()

	batch = batch.Copy()
	for i, msg := range batch {
		var args []interface{}
		if s.argsMapping != nil {
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
		}

		queryBuilder := s.builder
		if s.where != "" {
			queryBuilder = queryBuilder.Where(s.where, args...)
		}

		rows, err := queryBuilder.RunWith(s.db).QueryContext(ctx)
		if err != nil {
			msg.SetError(err)
			continue
		}

		if err = sqlResultJSONArrayCodec(rows, msg); err != nil {
			msg.SetError(err)
		}
	}
	return []service.MessageBatch{batch}, nil
}

func (s *sqlQueryProcessor) Close(ctx context.Context) error {
	s.shutSig.CloseNow()
	select {
	case <-s.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
