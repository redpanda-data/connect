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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Jeffail/checkpoint"

	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/lib/pq"
)

var sampleString = `{
	"primary_key": "[\"1a7ff641-3e3b-47ee-94fe-a0cadb56cd8f\", 2]", // stringified JSON array
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
				ShortDescription("Cache resource storing the last delivered cursor, so restarts resume instead of re-reading the table.").
				Optional(),
			service.NewStringListField("options").
				Description("A list of options to be included in the changefeed (WITH X, Y...).\n\nNOTE: Both the CURSOR option and UPDATED will be ignored from these options when a `cursor_cache` is specified, as they are set explicitly by Redpanda Connect in this case. A RESOLVED option is also added (unless one is supplied here): the stored cursor only ever advances to resolved timestamps whose rows have all been acknowledged downstream, so a restart redelivers at most the changes since the last resolved timestamp (bounded by the `changefeed.min_checkpoint_frequency` cluster setting) and never skips data.").
				ShortDescription("Options to include in the changefeed. CURSOR and UPDATED are ignored when cursor_cache is set, and RESOLVED is added.").
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

	// queryCancel cancels the active changefeed query context, unblocking a
	// blocking rows.Next() call in Read(). Protected by dbMut.
	queryCancel context.CancelFunc

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
		hasResolved := false
		for _, o := range tmpOptions {
			if strings.HasPrefix(strings.ToLower(o), "updated") {
				continue
			}
			if strings.HasPrefix(strings.ToLower(o), "cursor") {
				continue
			}
			if strings.HasPrefix(strings.ToLower(o), "resolved") {
				hasResolved = true
			}
			options = append(options, o)
		}
		options = append(options, "UPDATED")
		if !hasResolved {
			// Only RESOLVED timestamps are safe cursors: every row of a
			// transaction (and the entire initial backfill) shares one
			// `updated` timestamp, and CURSOR resume is exclusive, so a
			// row-level cursor would skip that timestamp's remaining rows on
			// restart. A user-supplied resolved='interval' option is kept.
			options = append(options, "RESOLVED")
		}
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
		if c.pgPool, err = pgxpool.NewWithConfig(ctx, c.pgConfig); err != nil {
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

	queryCtx, queryCancel := c.shutSig.SoftStopCtx(context.Background())
	c.queryCancel = queryCancel

	c.rows, err = c.pgPool.Query(queryCtx, c.statement)
	if err != nil {
		queryCancel()
		c.queryCancel = nil
	}
	return
}

// closeQueryLocked cancels the query context and closes the active rows.
// Must be called with dbMut held.
func (c *crdbChangefeedInput) closeQueryLocked() {
	if c.queryCancel != nil {
		c.queryCancel()
		c.queryCancel = nil
	}
	if c.rows != nil {
		c.rows.Close()
		c.rows = nil
	}
}

func (c *crdbChangefeedInput) closeConnection() {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Errorf("Recovered connection close panic: %v", r)
		}
	}()

	c.dbMut.Lock()
	defer c.dbMut.Unlock()

	c.closeQueryLocked()
	if c.pgPool != nil {
		c.pgPool.Close()
		c.pgPool = nil
	}
}

// persistCursor writes a resolved cursor timestamp to the cursor cache.
func (c *crdbChangefeedInput) persistCursor(ctx context.Context, cursorTimestamp string) (cErr error) {
	if err := c.res.AccessCache(ctx, c.cursorCache, func(cache service.Cache) {
		cErr = cache.Set(ctx, cursorCacheKey, []byte(cursorTimestamp), nil)
	}); err != nil {
		return err
	}
	return
}

func (c *crdbChangefeedInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	c.dbMut.Lock()
	defer c.dbMut.Unlock()

	if c.rows == nil {
		return nil, nil, service.ErrNotConnected
	}

	for {
		// rows.Next() blocks until the next changefeed event. The mutex is held to
		// prevent closeConnection() from calling rows.Close() concurrently. On
		// shutdown, SoftStopCtx cancels the query context which unblocks this call.
		if !c.rows.Next() {
			err := c.rows.Err()
			c.closeQueryLocked()

			if c.shutSig.IsSoftStopSignalled() {
				return nil, nil, service.ErrNotConnected
			}
			if err == nil {
				err = service.ErrNotConnected
			} else {
				err = fmt.Errorf("row read: %w", err)
			}
			return nil, nil, err
		}

		values, err := c.rows.Values()
		if err != nil {
			return nil, nil, fmt.Errorf("row values: %w", err)
		}

		rowBytes := values[2].([]byte)
		gObj, gErr := gabs.ParseJSON(rowBytes)

		// Resolved records carry NULL table/key columns and are bookkeeping,
		// never emitted downstream. A resolved timestamp is CockroachDB's
		// guarantee that nothing at or below it will be emitted again — the
		// only safe cursor (and CockroachDB does not emit one until the
		// initial scan completes, so a persisted cursor always covers the
		// backfill). Register it behind the in-flight rows (immediately
		// resolved marker): the timestamp persists once every row before it
		// is acked, either right here or inside the last outstanding ack.
		if gErr == nil {
			if resolvedTs, _ := gObj.S("resolved").Data().(string); resolvedTs != "" {
				if c.cursorCache == "" {
					continue
				}
				releaseFn, err := c.cursorCheckpointer.Track(ctx, resolvedTs, 1)
				if err != nil {
					return nil, nil, fmt.Errorf("tracking resolved cursor: %w", err)
				}
				if cursorTimestamp := releaseFn(); cursorTimestamp != nil && *cursorTimestamp != "" {
					if err := c.persistCursor(ctx, *cursorTimestamp); err != nil {
						c.logger.Errorf("Failed to persist resolved cursor: %v", err)
					}
				}
				continue
			}
		}

		var cursorReleaseFn func() *string
		if c.cursorCache != "" {
			// Data rows are tracked with an empty payload: they hold the
			// ordered tracker's frontier (so no resolved timestamp can persist
			// past an un-acked row) but never advance the cursor themselves.
			// Row-level `updated` timestamps are unsafe cursors: every row of
			// a transaction — and the entire initial backfill — shares one,
			// and CURSOR resume is exclusive.
			if cursorReleaseFn, err = c.cursorCheckpointer.Track(ctx, "", 1); err != nil {
				return nil, nil, fmt.Errorf("tracking row checkpoint: %w", err)
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
		return msg, func(ctx context.Context, err error) error {
			if cursorReleaseFn == nil {
				return nil
			}
			if err != nil {
				// auto_replay_nacks is user-toggleable, so a nack can be
				// terminal. Never resolve: the cursor stays pinned before this
				// row so no resolved timestamp can be persisted past its
				// undelivered data.
				c.logger.Errorf("Row rejected downstream: the cursor is now pinned before this row and the input will stall once the checkpoint limit is reached, unless the row is redelivered (auto_replay_nacks) or the pipeline restarts: %v", err)
				return err
			}
			cursorTimestamp := cursorReleaseFn()
			if cursorTimestamp == nil || *cursorTimestamp == "" {
				return nil
			}
			return c.persistCursor(ctx, *cursorTimestamp)
		}, nil
	}
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
