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
	"sync"

	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func sqlRawInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Summary("Executes a select query and creates a message for each row received.").
		Description(`Once the rows from the query are exhausted this input shuts down, allowing the pipeline to gracefully terminate (or the next input in a xref:components:inputs/sequence.adoc[sequence] to execute).`).
		Field(driverField).
		Field(dsnField).
		Field(rawQueryField().
			Example("SELECT * FROM footable WHERE user_id = $1;")).
		Field(rawQueryArgsMappingField()).
		Field(service.NewAutoRetryNacksToggleField()).
		Fields(connFields()...).
		Version("4.10.0").
		Example("Consumes an SQL table using a query as an input.",
			`
Here we preform an aggregate over a list of names in a table that are less than 3600 seconds old.`,
			`
input:
  sql_raw:
    driver: postgres
    dsn: postgres://foouser:foopass@localhost:5432/testdb?sslmode=disable
    query: "SELECT name, count(*) FROM person WHERE last_updated < $1 GROUP BY name;"
    args_mapping: |
      root = [
        now().ts_unix() - 3600
      ]
`,
		)
}

func init() {
	err := service.RegisterInput(
		"sql_raw", sqlRawInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := newSQLRawInputFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, i)
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
	dbMut  sync.Mutex

	rows *sql.Rows

	queryStatic string

	argsMapping *bloblang.Executor

	connSettings *connSettings

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

func newSQLRawInputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*sqlRawInput, error) {
	s := &sqlRawInput{
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

	if s.connSettings, err = connSettingsFromParsed(conf, mgr); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *sqlRawInput) Connect(ctx context.Context) (err error) {
	s.dbMut.Lock()
	defer s.dbMut.Unlock()

	if s.db != nil {
		return nil
	}

	var db *sql.DB
	if db, err = sqlOpenWithReworks(s.logger, s.driver, s.dsn); err != nil {
		return err
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

	var rows *sql.Rows
	if rows, err = db.Query(s.queryStatic, args...); err != nil {
		return
	} else if err = rows.Err(); err != nil {
		s.logger.With("err", err).Warnf("unexpected error while execute raw query %q", s.queryStatic)
	}

	s.db = db
	s.rows = rows

	go func() {
		<-s.shutSig.HardStopChan()

		s.dbMut.Lock()
		if s.rows != nil {
			_ = s.rows.Close()
			s.rows = nil
		}
		if s.db != nil {
			_ = s.db.Close()
			s.db = nil
		}
		s.dbMut.Unlock()

		s.shutSig.TriggerHasStopped()
	}()
	return nil
}

func (s *sqlRawInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
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

func (s *sqlRawInput) Close(ctx context.Context) error {
	s.shutSig.TriggerHardStop()
	s.dbMut.Lock()
	isNil := s.db == nil
	s.dbMut.Unlock()
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
