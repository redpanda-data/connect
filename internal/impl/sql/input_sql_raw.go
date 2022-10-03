package sql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
	"sync"
)

func sqlRawInputConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		// Stable(). TODO
		Categories("Services").
		Summary("Executes a select query and creates a message for each row received.").
		Description(`Once the rows from the query are exhausted this input shuts down, allowing the pipeline to gracefully terminate (or the next input in a [sequence](/docs/components/inputs/sequence) to execute).`).
		Field(driverField).
		Field(dsnField).
		Field(service.NewStringField("query").
			Description("query").
			Example("query")).
		Field(service.NewBloblangField("args_mapping").
			Description("A [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to an array of values matching in size to the number of columns specified.").
			Example("root = [ this.cat.meow, this.doc.woofs[0] ]").
			Example(`root = [ meta("user.id") ]`).
			Optional())
	for _, f := range connFields() {
		spec = spec.Field(f)
	}

	spec = spec.
		Version("3.59.0").
		Example("Consume a Table (PostgreSQL)",
			`
todo`,
			`
todo
`,
		)
	return spec
}

func init() {
	err := service.RegisterInput(
		"sql_raw", sqlRawInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := newSqlRawInputFromConfig(conf, mgr.Logger())
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

type sqlRawInput struct {
	driver string
	dsn    string
	db     *sql.DB
	dbMut  sync.RWMutex

	rows *sql.Rows

	queryStatic string

	argsMapping *bloblang.Executor

	connSettings connSettings

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

func newSqlRawInputFromConfig(conf *service.ParsedConfig, logger *service.Logger) (*sqlRawInput, error) {
	s := &sqlRawInput{
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

	if s.queryStatic, err = conf.FieldString("query"); err != nil {
		return nil, err
	}

	if conf.Contains("args_mapping") {
		if s.argsMapping, err = conf.FieldBloblang("args_mapping"); err != nil {
			return nil, err
		}
	}

	if err != nil {
		return nil, err
	}

	if s.connSettings, err = connSettingsFromParsed(conf); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *sqlRawInput) Connect(ctx context.Context) (err error) {
	s.dbMut.Lock()
	defer s.dbMut.Unlock()

	if s.db, err = sqlOpenWithReworks(s.logger, s.driver, s.dsn); err != nil {
		return err
	}
	s.connSettings.apply(s.db)
	go func() {
		<-s.shutSig.CloseNowChan()

		s.dbMut.Lock()
		_ = s.db.Close()
		s.dbMut.Unlock()

		s.shutSig.ShutdownComplete()
	}()

	var args []any
	if s.argsMapping != nil {
		var iargs any
		if iargs, err = s.argsMapping.Query(nil); err != nil {
			return err
		}

		var ok bool
		if args, ok = iargs.([]any); !ok {
			err = fmt.Errorf("mapping returned non-array result: %T", iargs)
			return
		}
	}

	s.rows, err = s.db.QueryContext(ctx, s.queryStatic, args...)
	if err != nil {
		s.logger.Debugf("Failed to run query: %v", err)
	}

	return nil
}

func (s *sqlRawInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	s.dbMut.RLock()
	defer s.dbMut.RUnlock()

	msg := service.NewMessage(nil)

	if !s.rows.Next() {
		err := s.rows.Err()
		if err == nil {
			err = service.ErrEndOfInput
		}
		_ = s.rows.Close()
		s.rows = nil
		return nil, nil, err
	}

	array_rows, newerror := sqlRowToMap(s.rows)
	if newerror != nil {
		return nil, nil, newerror
	}

	msg.SetStructured(array_rows)

	return msg, func(ctx context.Context, err error) error {
		// Nacks are handled by AutoRetryNacks because we don't have an explicit
		// ack mechanism right now.
		return nil
	}, nil
}

func (s *sqlRawInput) Close(ctx context.Context) error {
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
