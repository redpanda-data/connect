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

func sqlSelectInputConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Categories("Services").
		Summary("Executes a select query and creates a message for each row received.").
		Description(`Once the rows from the query are exhausted this input shuts down, allowing the pipeline to gracefully terminate (or the next input in a [sequence](/docs/components/inputs/sequence) to execute).`).
		Field(driverField).
		Field(dsnField).
		Field(service.NewStringField("table").
			Description("The table to select from.").
			Example("foo")).
		Field(service.NewStringListField("columns").
			Description("A list of columns to select.").
			Example([]string{"*"}).
			Example([]string{"foo", "bar", "baz"})).
		Field(service.NewStringField("where").
			Description("An optional where clause to add. Placeholder arguments are populated with the `args_mapping` field. Placeholders should always be question marks, and will automatically be converted to dollar syntax when the postgres or clickhouse drivers are used.").
			Example("type = ? and created_at > ?").
			Example("user_id = ?").
			Optional()).
		Field(service.NewBloblangField("args_mapping").
			Description("An optional [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to an array of values matching in size to the number of placeholder arguments in the field `where`.").
			Example(`root = [ "article", now().ts_format("2006-01-02") ]`).
			Optional()).
		Field(service.NewStringField("prefix").
			Description("An optional prefix to prepend to the select query (before SELECT).").
			Optional().
			Advanced()).
		Field(service.NewStringField("suffix").
			Description("An optional suffix to append to the select query.").
			Optional().
			Advanced())

	for _, f := range connFields() {
		spec = spec.Field(f)
	}

	spec = spec.
		Version("3.59.0").
		Example("Consume a Table (PostgreSQL)",
			`
Here we define a pipeline that will consume all rows from a table created within the last hour by comparing the unix timestamp stored in the row column "created_at":`,
			`
input:
  sql_select:
    driver: postgres
    dsn: postgres://foouser:foopass@localhost:5432/testdb?sslmode=disable
    table: footable
    columns: [ '*' ]
    where: created_at >= ?
    args_mapping: |
      root = [
        now().ts_unix() - 3600
      ]
`,
		)
	return spec
}

func init() {
	err := service.RegisterInput(
		"sql_select", sqlSelectInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := newSQLSelectInputFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacks(i), nil
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type sqlSelectInput struct {
	driver  string
	dsn     string
	db      *sql.DB
	rows    *sql.Rows
	builder squirrel.SelectBuilder
	dbMut   sync.Mutex

	where       string
	argsMapping *bloblang.Executor

	connSettings *connSettings

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

func newSQLSelectInputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*sqlSelectInput, error) {
	s := &sqlSelectInput{
		logger:  mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
	}

	var err error

	if s.driver, err = conf.FieldString("driver"); err != nil {
		return nil, err
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
	if s.driver == "postgres" || s.driver == "clickhouse" {
		s.builder = s.builder.PlaceholderFormat(squirrel.Dollar)
	} else if s.driver == "oracle" || s.driver == "gocosmos" {
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

	if s.connSettings, err = connSettingsFromParsed(conf, mgr); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *sqlSelectInput) Connect(ctx context.Context) (err error) {
	s.dbMut.Lock()
	defer s.dbMut.Unlock()

	if s.db != nil {
		return nil
	}

	var db *sql.DB
	if db, err = sqlOpenWithReworks(s.logger, s.driver, s.dsn); err != nil {
		return
	}
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()

	s.connSettings.apply(ctx, db, s.logger)

	var args []any
	if s.argsMapping != nil {
		var iargs any
		if iargs, err = s.argsMapping.Query(nil); err != nil {
			return
		}

		var ok bool
		if args, ok = iargs.([]any); !ok {
			err = fmt.Errorf("mapping returned non-array result: %T", iargs)
			return
		}
	}

	queryBuilder := s.builder
	if s.where != "" {
		queryBuilder = queryBuilder.Where(s.where, args...)
	}
	var rows *sql.Rows
	if rows, err = queryBuilder.RunWith(db).Query(); err != nil {
		return
	}

	s.db = db
	s.rows = rows

	go func() {
		<-s.shutSig.CloseNowChan()

		s.dbMut.Lock()
		if s.rows != nil {
			_ = s.rows.Close()
			s.rows = nil
		}
		if s.db != nil {
			_ = s.db.Close()
		}
		s.dbMut.Unlock()

		s.shutSig.ShutdownComplete()
	}()
	return nil
}

func (s *sqlSelectInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	s.dbMut.Lock()
	defer s.dbMut.Unlock()

	if s.db == nil && s.rows == nil {
		return nil, nil, service.ErrNotConnected
	}

	if s.rows == nil {
		return nil, nil, service.ErrEndOfInput
	}

	if !s.rows.Next() {
		err := s.rows.Err()
		if err == nil {
			err = service.ErrEndOfInput
		}
		_ = s.rows.Close()
		s.rows = nil
		return nil, nil, err
	}

	obj, err := sqlRowToMap(s.rows)
	if err != nil {
		_ = s.rows.Close()
		s.rows = nil
		return nil, nil, err
	}

	msg := service.NewMessage(nil)
	msg.SetStructuredMut(obj)
	return msg, func(ctx context.Context, err error) error {
		// Nacks are handled by AutoRetryNacks because we don't have an explicit
		// ack mechanism right now.
		return nil
	}, nil
}

func (s *sqlSelectInput) Close(ctx context.Context) error {
	s.shutSig.CloseNow()
	s.dbMut.Lock()
	isNil := s.db == nil
	s.dbMut.Unlock()
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
