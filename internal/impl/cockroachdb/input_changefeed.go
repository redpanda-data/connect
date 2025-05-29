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

package crdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Jeffail/gabs/v2"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/Jeffail/checkpoint"

	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/lib/pq"
)

var sampleString = `{
	"primary_key": "[\"1a7ff641-3e3b-47ee-94fe-a0cadb56cd8f\", 2]", // stringifed JSON array
	"row": "{\"after\": {\"k\": \"1a7ff641-3e3b-47ee-94fe-a0cadb56cd8f\", \"v\": 2}, \"updated\": \"1637953249519902405.0000000000\"}", // stringified JSON object
	"table": "strm_2"
}`

func crdbChangefeedInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Summary(fmt.Sprintf("Listens to a https://www.cockroachlabs.com/docs/stable/changefeed-examples[CockroachDB Core Changefeed^] and creates a message for each row received. Each message is a json object looking like: \n```json\n%s\n```", sampleString)).
		Description("This input will continue to listen to the changefeed until shutdown. A backfill of the full current state of the table will be delivered upon each run unless a cache is configured for storing cursor timestamps, as this is how Redpanda Connect keeps track as to which changes have been successfully delivered.\n\nNote: You must have `SET CLUSTER SETTING kv.rangefeed.enabled = true;` on your CRDB cluster, for more information refer to https://www.cockroachlabs.com/docs/stable/changefeed-examples?filters=core[the official CockroachDB documentation^].").
		Fields(
			service.NewStringField("dsn").
				Description(`A Data Source Name to identify the target database.`).
				Example("postgres://user:password@example.com:26257/defaultdb?sslmode=require"),
			service.NewTLSField("tls"),
			service.NewStringListField("tables").
				Description("CSV of tables to be included in the changefeed").
				Example([]string{"table1", "table2"}),
			service.NewStringField("cursor_cache").
				Description("A https://docs.redpanda.com/redpanda-connect/components/caches/about[cache resource^] to use for storing the current latest cursor that has been successfully delivered, this allows Redpanda Connect to continue from that cursor upon restart, rather than consume the entire state of the table.").
				Optional(),
			service.NewStringListField("options").
				Description("A list of options to be included in the changefeed (WITH X, Y...).\n\nNOTE: Both the CURSOR option and UPDATED will be ignored from these options when a `cursor_cache` is specified, as they are set explicitly by Redpanda Connect in this case.").
				Example([]string{`virtual_columns="omitted"`}).
				Advanced().
				Optional(),
			service.NewAutoRetryNacksToggleField(),
		)
}

type crdbChangefeedInput struct {
	statement          string
	cursorCache        string
	cursorCheckpointer *checkpoint.Capped[string]

	pgConfig *pgxpool.Config
	pgPool   *pgxpool.Pool
	rows     pgx.Rows
	dbMut    sync.Mutex

	res     *service.Resources
	logger  *service.Logger
	shutSig *shutdown.Signaller
}

const cursorCacheKey = "crdb_changefeed_cursor"

func newCRDBChangefeedInputFromConfig(conf *service.ParsedConfig, res *service.Resources) (*crdbChangefeedInput, error) {
	c := &crdbChangefeedInput{
		cursorCheckpointer: checkpoint.NewCapped[string](1024), // TODO: Configure this?
		res:                res,
		logger:             res.Logger(),
		shutSig:            shutdown.NewSignaller(),
	}

	dsn, err := conf.FieldString("dsn")
	if err != nil {
		return nil, err
	}

	if c.pgConfig, err = pgxpool.ParseConfig(dsn); err != nil {
		return nil, err
	}

	if c.pgConfig.ConnConfig.TLSConfig, err = conf.FieldTLS("tls"); err != nil {
		return nil, err
	}

	c.cursorCache, _ = conf.FieldString("cursor_cache")

	// Setup the query
	tables, err := conf.FieldStringList("tables")
	if err != nil {
		return nil, err
	}

	tmpOptions, _ := conf.FieldStringList("options")

	var options []string
	if c.cursorCache == "" {
		options = tmpOptions
	} else {
		for _, o := range tmpOptions {
			if strings.HasPrefix(strings.ToLower(o), "updated") {
				continue
			}
			if strings.HasPrefix(strings.ToLower(o), "cursor") {
				continue
			}
			options = append(options, o)
		}
		options = append(options, "UPDATED")
		if err := res.AccessCache(context.Background(), c.cursorCache, func(c service.Cache) {
			cursorBytes, cErr := c.Get(context.Background(), cursorCacheKey)
			if cErr != nil {
				if !errors.Is(cErr, service.ErrKeyNotFound) {
					res.Logger().With("error", cErr.Error()).Error("Failed to obtain cursor cache item.")
				}
				return
			}
			options = append(options, `CURSOR="`+string(cursorBytes)+`"`)
		}); err != nil {
			res.Logger().With("error", err.Error()).Error("Failed to access cursor cache.")
		}
	}

	changeFeedOptions := ""
	if len(options) > 0 {
		changeFeedOptions = " WITH " + strings.Join(options, ", ")
	}

	c.statement = fmt.Sprintf("EXPERIMENTAL CHANGEFEED FOR %s%s", strings.Join(tables, ", "), changeFeedOptions)
	res.Logger().Debug("Creating changefeed: " + c.statement)

	go func() {
		<-c.shutSig.SoftStopChan()

		c.closeConnection()
		c.shutSig.TriggerHasStopped()
	}()
	return c, nil
}

func init() {
	service.MustRegisterInput(
		"cockroachdb_changefeed", crdbChangefeedInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := newCRDBChangefeedInputFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, i)
		})
}

func (c *crdbChangefeedInput) Connect(ctx context.Context) (err error) {
	c.dbMut.Lock()
	defer c.dbMut.Unlock()

	if c.rows != nil {
		return
	}

	if c.shutSig.IsSoftStopSignalled() {
		return service.ErrEndOfInput
	}

	if c.pgPool == nil {
		if c.pgPool, err = pgxpool.ConnectConfig(ctx, c.pgConfig); err != nil {
			return
		}
		defer func() {
			if err != nil {
				c.pgPool.Close()
				c.pgPool = nil
			}
		}()
	}

	c.logger.Debug(fmt.Sprintf("Running query '%s'", c.statement))
	c.rows, err = c.pgPool.Query(ctx, c.statement)
	return
}

func (c *crdbChangefeedInput) closeConnection() {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Errorf("Recovered connection close panic: %v", r)
		}
	}()

	c.dbMut.Lock()
	defer c.dbMut.Unlock()

	if c.rows != nil {
		err := c.rows.Err()
		if err != nil {
			c.logger.With("err", err).Warn("unexpected error from cockroachdb before closing")
		}

		c.rows.Close()
		c.rows = nil
	}
	if c.pgPool != nil {
		c.pgPool.Close()
		c.pgPool = nil
	}
}

func (c *crdbChangefeedInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	c.dbMut.Lock()
	rows := c.rows
	c.dbMut.Unlock()

	if rows == nil {
		return nil, nil, service.ErrNotConnected
	}

	if !rows.Next() {
		go c.closeConnection()
		if c.shutSig.IsSoftStopSignalled() {
			return nil, nil, service.ErrNotConnected
		}

		err := rows.Err()
		if err == nil {
			err = service.ErrNotConnected
		} else {
			err = fmt.Errorf("row read: %w", err)
		}
		return nil, nil, err
	}

	values, err := rows.Values()
	if err != nil {
		return nil, nil, fmt.Errorf("row values: %w", err)
	}

	var cursorReleaseFn func() *string

	rowBytes := values[2].([]byte)
	if gObj, err := gabs.ParseJSON(rowBytes); err == nil {
		if cursorTimestamp, _ := gObj.S("updated").Data().(string); cursorTimestamp != "" {
			cursorReleaseFn, _ = c.cursorCheckpointer.Track(ctx, cursorTimestamp, 1)
		}
	}

	// Construct the new JSON
	var jsonBytes []byte
	if jsonBytes, err = json.Marshal(map[string]string{
		"table":       values[0].(string),
		"primary_key": string(values[1].([]byte)), // Stringified JSON (Array)
		"row":         string(rowBytes),           // Stringified JSON (Object)
	}); err != nil {
		return nil, nil, err
	}

	msg := service.NewMessage(jsonBytes)
	return msg, func(ctx context.Context, _ error) (cErr error) {
		if cursorReleaseFn == nil {
			return nil
		}
		cursorTimestamp := cursorReleaseFn()
		if cursorTimestamp == nil {
			return nil
		}
		if err := c.res.AccessCache(ctx, c.cursorCache, func(c service.Cache) {
			cErr = c.Set(ctx, cursorCacheKey, []byte(*cursorTimestamp), nil)
		}); err != nil {
			return err
		}
		return
	}, nil
}

func (c *crdbChangefeedInput) Close(ctx context.Context) error {
	c.shutSig.TriggerHardStop()
	select {
	case <-c.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
