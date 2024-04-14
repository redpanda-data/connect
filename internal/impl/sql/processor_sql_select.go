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

// SelectProcessorConfig returns a config spec for an sql_select processor.
func SelectProcessorConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Categories("Integration").
		Summary("Runs an SQL select query against a database and returns the result as an array of objects, one for each row returned, containing a key for each column queried and its value.").
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
			Description("An optional where clause to add. Placeholder arguments are populated with the `args_mapping` field. Placeholders should always be question marks, and will automatically be converted to dollar syntax when the postgres or clickhouse drivers are used.").
			Example("meow = ? and woof = ?").
			Example("user_id = ?").
			Optional()).
		Field(service.NewBloblangField("args_mapping").
			Description("An optional [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to an array of values matching in size to the number of placeholder arguments in the field `where`.").
			Example("root = [ this.cat.meow, this.doc.woofs[0] ]").
			Example(`root = [ meta("user.id") ]`).
			Optional()).
		Field(service.NewStringField("prefix").
			Description("An optional prefix to prepend to the query (before SELECT).").
			Optional().
			Advanced()).
		Field(service.NewStringField("suffix").
			Description("An optional suffix to append to the select query.").
			Optional().
			Advanced())

	for _, f := range connFields() {
		spec = spec.Field(f)
	}

	spec = spec.Version("3.59.0").
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
          - sql_select:
              driver: postgres
              dsn: postgres://foouser:foopass@localhost:5432/testdb?sslmode=disable
              table: footable
              columns: [ '*' ]
              where: user_id = ?
              args_mapping: '[ this.user.id ]'
        result_map: 'root.foo_rows = this'
`,
		)
	return spec
}

func init() {
	err := service.RegisterBatchProcessor(
		"sql_select", SelectProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return NewSQLSelectProcessorFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type sqlSelectProcessor struct {
	db      *sql.DB
	builder squirrel.SelectBuilder
	dbMut   sync.RWMutex

	where       string
	argsMapping *bloblang.Executor

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

// NewSQLSelectProcessorFromConfig returns an internal sql_select processor.
func NewSQLSelectProcessorFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*sqlSelectProcessor, error) {
	s := &sqlSelectProcessor{
		logger:  mgr.Logger(),
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

	s.builder = squirrel.Select(columns...).From(tableStr)
	if driverStr == "postgres" || driverStr == "clickhouse" {
		s.builder = s.builder.PlaceholderFormat(squirrel.Dollar)
	} else if driverStr == "oracle" || driverStr == "gocosmos" {
		s.builder = s.builder.PlaceholderFormat(squirrel.Colon)
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

	connSettings, err := connSettingsFromParsed(conf, mgr)
	if err != nil {
		return nil, err
	}

	if s.db, err = sqlOpenWithReworks(mgr.Logger(), driverStr, dsnStr); err != nil {
		return nil, err
	}
	connSettings.apply(context.Background(), s.db, s.logger)

	go func() {
		<-s.shutSig.CloseNowChan()

		s.dbMut.Lock()
		_ = s.db.Close()
		s.dbMut.Unlock()

		s.shutSig.ShutdownComplete()
	}()
	return s, nil
}

func (s *sqlSelectProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	s.dbMut.RLock()
	defer s.dbMut.RUnlock()

	batch = batch.Copy()
	for i, msg := range batch {
		var args []any
		if s.argsMapping != nil {
			resMsg, err := batch.BloblangQuery(i, s.argsMapping)
			if err != nil {
				s.logger.Debugf("Arguments mapping failed: %v", err)
				msg.SetError(err)
				continue
			}

			iargs, err := resMsg.AsStructured()
			if err != nil {
				s.logger.Debugf("Mapping returned non-structured result: %v", err)
				msg.SetError(fmt.Errorf("mapping returned non-structured result: %w", err))
				continue
			}

			var ok bool
			if args, ok = iargs.([]any); !ok {
				s.logger.Debugf("Mapping returned non-array result: %T", iargs)
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
			s.logger.Debugf("Failed to run query: %v", err)
			msg.SetError(err)
			continue
		}

		if jArray, err := sqlRowsToArray(rows); err != nil {
			s.logger.Debugf("Failed to convert rows: %v", err)
			msg.SetError(err)
		} else {
			msg.SetStructuredMut(jArray)
		}
	}
	return []service.MessageBatch{batch}, nil
}

func (s *sqlSelectProcessor) Close(ctx context.Context) error {
	s.shutSig.CloseNow()
	select {
	case <-s.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
