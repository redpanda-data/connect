package sql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"

	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	cacheKeyColumnField   = "key_column"
	cacheValueColumnField = "value_column"
	cacheSetSuffixField   = "set_suffix"
)

func sqlCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Categories("Services").
		Summary("Uses an SQL database table as a destination for storing cache key/value items.").
		Version("4.26.0").
		Description(`
Each cache key/value pair will exist as a row within the specified table. Currently only the key and value columns are set, and therefore any other columns present within the target table must allow NULL values if this cache is going to be used for set and add operations.

Cache operations are translated into SQL statements as follows:

### Get

All ` + "`get`" + ` operations are performed with a traditional ` + "`select`" + ` statement.

### Delete

All ` + "`delete`" + ` operations are performed with a traditional ` + "`delete`" + ` statement.

### Set

The ` + "`set`" + ` operation is performed with a traditional ` + "`insert`" + ` statement.

This will behave as an ` + "`add`" + ` operation by default, and so ideally needs to be adapted in order to provide updates instead of failing on collision	s. Since different SQL engines implement upserts differently it is necessary to specify a ` + "`set_suffix`" + ` that modifies an ` + "`insert`" + ` statement in order to perform updates on conflict.

### Add

The ` + "`add`" + ` operation is performed with a traditional ` + "`insert`" + ` statement.
`).
		Field(driverField).
		Field(dsnField).
		Field(service.NewStringField("table").
			Description("The table to insert/read/delete cache items.").
			Example("foo")).
		Field(service.NewStringField(cacheKeyColumnField).
			Description("The name of a column to be used for storing cache item keys. This column should support strings of arbitrary size.").
			Example("foo")).
		Field(service.NewStringField(cacheValueColumnField).
			Description("The name of a column to be used for storing cache item values. This column should support strings of arbitrary size.").
			Example("bar")).
		Field(service.NewStringField(cacheSetSuffixField).
			Description("An optional suffix to append to each insert query for a cache `set` operation. This should modify an insert statement into an upsert appropriate for the given SQL engine.").
			Optional().
			Examples(
				"ON DUPLICATE KEY UPDATE bar=VALUES(bar)",
				"ON CONFLICT (foo) DO UPDATE SET bar=excluded.bar",
				"ON CONFLICT (foo) DO NOTHING",
			))

	for _, f := range connFields() {
		spec = spec.Field(f)
	}
	return spec
}

func init() {
	err := service.RegisterCache("sql", sqlCacheConfig(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
		return newSQLCacheFromConfig(conf, mgr)
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type sqlCache struct {
	driver string
	dsn    string
	db     *sql.DB

	keyColumn string

	selectBuilder squirrel.SelectBuilder
	insertBuilder squirrel.InsertBuilder
	upsertBuilder squirrel.InsertBuilder
	deleteBuilder squirrel.DeleteBuilder

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

func newSQLCacheFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*sqlCache, error) {
	s := &sqlCache{
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

	if s.keyColumn, err = conf.FieldString(cacheKeyColumnField); err != nil {
		return nil, err
	}

	valueColumn, err := conf.FieldString(cacheValueColumnField)
	if err != nil {
		return nil, err
	}

	s.selectBuilder = squirrel.Select(valueColumn).From(tableStr)
	s.insertBuilder = squirrel.Insert(tableStr).Columns(s.keyColumn, valueColumn)
	s.upsertBuilder = squirrel.Insert(tableStr).Columns(s.keyColumn, valueColumn)
	s.deleteBuilder = squirrel.Delete(tableStr)

	if s.driver == "postgres" || s.driver == "clickhouse" {
		s.selectBuilder = s.selectBuilder.PlaceholderFormat(squirrel.Dollar)
		s.insertBuilder = s.insertBuilder.PlaceholderFormat(squirrel.Dollar)
		s.upsertBuilder = s.upsertBuilder.PlaceholderFormat(squirrel.Dollar)
		s.deleteBuilder = s.deleteBuilder.PlaceholderFormat(squirrel.Dollar)
	} else if s.driver == "oracle" || s.driver == "gocosmos" {
		s.selectBuilder = s.selectBuilder.PlaceholderFormat(squirrel.Colon)
		s.insertBuilder = s.insertBuilder.PlaceholderFormat(squirrel.Colon)
		s.upsertBuilder = s.upsertBuilder.PlaceholderFormat(squirrel.Colon)
		s.deleteBuilder = s.deleteBuilder.PlaceholderFormat(squirrel.Colon)
	}

	if conf.Contains(cacheSetSuffixField) {
		suffixStr, err := conf.FieldString(cacheSetSuffixField)
		if err != nil {
			return nil, err
		}
		s.upsertBuilder = s.upsertBuilder.Suffix(suffixStr)
	}

	connSettings, err := connSettingsFromParsed(conf, mgr)
	if err != nil {
		return nil, err
	}

	if s.db, err = sqlOpenWithReworks(s.logger, s.driver, s.dsn); err != nil {
		return nil, err
	}
	connSettings.apply(context.Background(), s.db, s.logger)

	go func() {
		<-s.shutSig.CloseNowChan()
		_ = s.db.Close()
		s.shutSig.ShutdownComplete()
	}()
	return s, nil
}

func (s *sqlCache) Get(ctx context.Context, key string) (value []byte, err error) {
	err = s.selectBuilder.
		Where(squirrel.Eq{s.keyColumn: key}).
		RunWith(s.db).QueryRowContext(ctx).
		Scan(&value)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		err = service.ErrKeyNotFound
	}
	return
}

func (s *sqlCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	_, err := s.upsertBuilder.Values(key, value).RunWith(s.db).ExecContext(ctx)
	return err
}

func (s *sqlCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	_, err := s.insertBuilder.Values(key, value).RunWith(s.db).ExecContext(ctx)
	if err != nil {
		// This is difficult, ideally we need to translate any error that
		// indicates a collision into service.ErrKeyAlreadyExists, but this is
		// exhaustive as each SQL engine could return something different.
		if strings.Contains(err.Error(), "duplicate key") {
			err = service.ErrKeyAlreadyExists
		}
	}
	return err
}

func (s *sqlCache) Delete(ctx context.Context, key string) error {
	_, err := s.deleteBuilder.Where(squirrel.Eq{s.keyColumn: key}).RunWith(s.db).ExecContext(ctx)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		err = service.ErrKeyNotFound
	}
	return err
}

func (s *sqlCache) Close(ctx context.Context) error {
	s.shutSig.CloseNow()
	select {
	case <-s.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
