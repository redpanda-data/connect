package sql

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/public/bloblang"
	"github.com/Jeffail/benthos/v3/public/service"
	"github.com/Masterminds/squirrel"
)

func sqlSelectInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
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
			Description("An optional where clause to add. Placeholder arguments are populated with the `args_mapping` field. Placeholders should always be question marks, and will automatically be converted to dollar syntax when the postgres driver is used.").
			Example("type = ? and created_at > ?").
			Example("user_id = ?").
			Optional()).
		Field(service.NewBloblangField("args_mapping").
			Description("An optional [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to an array of values matching in size to the number of placeholder arguments in the field `where`.").
			Example(`root = [ "article", now().format_timestamp("2006-01-02") ]`).
			Optional()).
		Field(service.NewStringField("prefix").
			Description("An optional prefix to prepend to the select query (before SELECT).").
			Optional().
			Advanced()).
		Field(service.NewStringField("suffix").
			Description("An optional suffix to append to the select query.").
			Optional().
			Advanced()).
		Field(service.NewDurationField("conn_max_idle_time").
			Description(`An optional maximum amount of time a connection may be idle.
Expired connections may be closed lazily before reuse.
If value <= 0, connections are not closed due to a connection's idle time.`).
			Optional().
			Advanced()).
		Field(service.NewDurationField("conn_max_lifetime").
			Description(`An optiona maximum amount of time a connection may be reused.
Expired connections may be closed lazily before reuse.
If value <= 0, connections are not closed due to a connection's age.`).
			Optional().
			Advanced()).
		Field(service.NewIntField("max_idle_cons").
			Description(`An optional maximum number of connections in the idle connection pool.
If MaxOpenConns is greater than 0 but less than the new MaxIdleConns, then the new MaxIdleConns will be reduced to match the MaxOpenConns limit.
If value <= 0, no idle connections are retained.
The default max idle connections is currently 2. This may change in a future release.`).
			Optional().
			Advanced()).
		Field(service.NewIntField("max_open_conns").
			Description(`An optional maximum number of open connections to the database.
If MaxIdleConns is greater than 0 and the new MaxOpenConns is less than MaxIdleConns, then MaxIdleConns will be reduced to match the new MaxOpenConns limit.
If value <= 0, then there is no limit on the number of open connections. The default is 0 (unlimited).`).
			Optional().
			Advanced()).
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
        now().format_timestamp_unix() - 3600
      ]
`,
		)
}

func init() {
	err := service.RegisterInput(
		"sql_select", sqlSelectInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := newSQLSelectInputFromConfig(conf, mgr.Logger())
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

	connMaxLifetime time.Duration
	connMaxIdleTime time.Duration
	maxIdleConns    int
	maxOpenConns    int

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

func newSQLSelectInputFromConfig(conf *service.ParsedConfig, logger *service.Logger) (*sqlSelectInput, error) {
	s := &sqlSelectInput{
		logger:  logger,
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

	if conf.Contains("conn_max_lifetime") {
		connMaxLifetime, err := conf.FieldDuration()
		if err != nil {
			return nil, err
		}
		s.connMaxLifetime = connMaxLifetime
	}

	if conf.Contains("conn_max_idle_time") {
		connMaxIdleTime, err := conf.FieldDuration("conn_max_idle_time")
		if err != nil {
			return nil, err
		}
		s.connMaxIdleTime = connMaxIdleTime
	}

	if conf.Contains("max_idle_conns") {
		maxIdleCons, err := conf.FieldInt("max_idle_conns")
		if err != nil {
			return nil, err
		}
		s.maxIdleConns = maxIdleCons
	} else {
		s.maxIdleConns = 2 // default value for the library
	}

	if conf.Contains("max_open_conns") {
		maxOpenConns, err := conf.FieldInt("max_open_conns")
		if err != nil {
			return nil, err
		}
		s.maxOpenConns = maxOpenConns
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
	if db, err = sql.Open(s.driver, s.dsn); err != nil {
		return
	}
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()

	db.SetConnMaxIdleTime(s.connMaxIdleTime)
	db.SetConnMaxLifetime(s.connMaxIdleTime)
	db.SetMaxIdleConns(s.maxIdleConns)
	db.SetMaxOpenConns(s.maxOpenConns)

	var args []interface{}
	if s.argsMapping != nil {
		var iargs interface{}
		if iargs, err = s.argsMapping.Query(nil); err != nil {
			return err
		}

		var ok bool
		if args, ok = iargs.([]interface{}); !ok {
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
	msg.SetStructured(obj)
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
