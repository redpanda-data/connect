package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
	"go.uber.org/multierr"
)

const (
	outboxIDSelector = "__benthos_id"
)

func sqlOutboxInputConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		// Stable(). TODO
		Categories("Services").
		Summary("Consume rows from an outbox table.").
		Description(`This enables the implementation of the [Transaction Outbox](https://microservices.io/patterns/data/transactional-outbox.html) pattern by using Benthos to process items from a service's outbox table. Successfully processed items will be deleted from the table while failed items are retried.`).
		Field(service.NewStringEnumField("driver", "sqlite", "mysql", "postgres").Description("A database [driver](#drivers) to use.")).
		Field(dsnField).
		Field(service.NewStringField("table").
			Description("The outbox table name to consume.").
			Example("outbox_items")).
		Field(service.NewStringListField("columns").
			Description("A list of columns to select.").
			Example([]string{"*"}).
			Example([]string{"foo", "bar", "baz"})).
		Field(service.NewStringField("where").
			Description("An optional where clause to add. Placeholder arguments are populated with the `args_mapping` field. Placeholders should always be question marks, and will automatically be converted to dollar syntax when the postgres or clickhouse drivers are used.").
			Example("type = ? and created_at > ?").
			Example("user_id = ?").
			Optional()).
		Field(service.NewIntField("item_count").
			Description("The number of items to consume with each request.").
			LintRule(`root = if this <= 0 { [ "field must be greater than 0" ] }`)).
		Field(service.NewBloblangField("args_mapping").
			Description("An optional [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to an array of values matching in size to the number of placeholder arguments in the field `where`.").
			Example(`root = [ "article", now().ts_format("2006-01-02") ]`).
			Optional()).
		Field(service.NewStringField("id_column").
			Description("The column containing the id of an outbox item.").
			Default("id")).
		Field(service.NewStringField("retry_count_column").
			Description("The column used to track the retry count for an outbox item.")).
		Field(service.NewIntField("max_retries").
			Description("The maximum number of attempts to process outbox items before skipping them.").
			Default(3).
			LintRule(`root = if this <= 0 { [ "field must be greater than 0" ] }`)).
		Field(
			service.NewStringField("rate_limit").
				Description("A [rate limit](/docs/components/rate_limits/about) to throttle transactions by."),
		)

	for _, f := range connFields() {
		spec = spec.Field(f)
	}

	spec = spec.
		Version("4.6.0").
		Example("Consume an Outbox table (PostgreSQL)",
			`
Here we define a pipeline that will consume all rows from an outbox table created that are ready to be delivered:`,
			`
rate_limit_resources:
  - label: outbox_polling_rate
    local:
      count: 1
      interval: 500ms

input:
  sql_outbox:
    driver: postgres
    dsn: postgres://foouser:foopass@localhost:5432/testdb?sslmode=disable
    table: outbox_items
    columns: [ '*' ]
    where: deliverable_at >= now() AND subject = ?
    item_count: 500
    retry_count_column: retry_count
    rate_limit: outbox_polling_rate
    args_mapping: |
      root = [ "registration" ]
`,
		)
	return spec
}

func init() {
	err := service.RegisterBatchInput(
		"sql_outbox", sqlOutboxInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			i, err := newSQLOutboxInputFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return i, nil
		})

	if err != nil {
		panic(err)
	}
}

type sqlOutboxConfig struct {
	driver string
	dsn    string

	table            string
	columns          []string
	where            string
	idColumn         string
	retryCountColumn string
	itemCount        int

	argsMapping *bloblang.Executor

	maxRetries int

	connSettings connSettings

	rateLimit string
}

type sqlOutboxInput struct {
	manager *service.Resources
	logger  *service.Logger

	config *sqlOutboxConfig

	dbMut sync.Mutex
	db    *sql.DB

	selectBuilder squirrel.SelectBuilder
	updateBuilder squirrel.UpdateBuilder
	deleteBuilder squirrel.DeleteBuilder
}

func newSQLOutboxInputFromConfig(
	conf *service.ParsedConfig,
	manager *service.Resources,
) (*sqlOutboxInput, error) {
	var config sqlOutboxConfig
	var err error

	if config.driver, err = conf.FieldString("driver"); err != nil {
		return nil, err
	}

	if config.dsn, err = conf.FieldString("dsn"); err != nil {
		return nil, err
	}

	config.table, err = conf.FieldString("table")
	if err != nil {
		return nil, err
	}

	config.columns, err = conf.FieldStringList("columns")
	if err != nil {
		return nil, err
	}

	if config.where, err = conf.FieldString("where"); err != nil {
		return nil, err
	}

	if config.itemCount, err = conf.FieldInt("item_count"); err != nil {
		return nil, err
	}

	if conf.Contains("args_mapping") {
		if config.argsMapping, err = conf.FieldBloblang("args_mapping"); err != nil {
			return nil, err
		}
	}

	config.idColumn, err = conf.FieldString("id_column")
	if err != nil {
		return nil, err
	}

	config.retryCountColumn, err = conf.FieldString("retry_count_column")
	if err != nil {
		return nil, err
	}

	config.maxRetries, err = conf.FieldInt("max_retries")
	if err != nil {
		return nil, err
	}

	config.rateLimit, err = conf.FieldString("rate_limit")
	if err != nil {
		return nil, err
	}

	if config.connSettings, err = connSettingsFromParsed(conf); err != nil {
		return nil, err
	}

	columns := append(
		[]string{fmt.Sprintf("%s as %s", config.idColumn, outboxIDSelector)},
		config.columns...,
	)
	input := &sqlOutboxInput{
		manager:       manager,
		logger:        manager.Logger(),
		config:        &config,
		selectBuilder: squirrel.Select(columns...).From(config.table).Limit(uint64(config.itemCount)),
		updateBuilder: squirrel.Update(config.table),
		deleteBuilder: squirrel.Delete(config.table),
	}

	if config.driver == "postgres" {
		input.selectBuilder = input.selectBuilder.PlaceholderFormat(squirrel.Dollar)
		input.updateBuilder = input.updateBuilder.PlaceholderFormat(squirrel.Dollar)
		input.deleteBuilder = input.deleteBuilder.PlaceholderFormat(squirrel.Dollar)
	}

	if config.driver == "postgres" || config.driver == "mysql" {
		input.selectBuilder = input.selectBuilder.Suffix("FOR UPDATE SKIP LOCKED")
	}

	input.selectBuilder = input.selectBuilder.Where(fmt.Sprintf("%s < ?", config.retryCountColumn), config.maxRetries)

	return input, nil
}

func (inp *sqlOutboxInput) Connect(ctx context.Context) error {
	inp.dbMut.Lock()
	defer inp.dbMut.Unlock()

	if inp.db != nil {
		return nil
	}

	db, err := sqlOpenWithReworks(inp.logger, inp.config.driver, inp.config.dsn)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	inp.config.connSettings.apply(db)
	inp.db = db

	return nil
}

func (inp *sqlOutboxInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if err := inp.waitForAccess(ctx); err != nil {
		return nil, nil, err
	}

	var db *sql.DB
	inp.dbMut.Lock()
	db = inp.db
	inp.dbMut.Unlock()

	if db == nil {
		return nil, nil, service.ErrNotConnected
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to start a db transaction: %w", err)
	}

	res, ack, err := inp.processOutboxItems(ctx, tx)
	if err != nil {
		return nil, nil, multierr.Combine(err, tx.Rollback())
	}

	return res, ack, nil
}

func (inp *sqlOutboxInput) processOutboxItems(ctx context.Context, tx *sql.Tx) (service.MessageBatch, service.AckFunc, error) {
	args, err := resolveArgs(inp.config.argsMapping)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to resolve args mapping: %w", err)
	}

	selector := inp.selectBuilder.Where(inp.config.where, args...)

	rows, err := selector.RunWith(tx).QueryContext(ctx)
	switch {
	case errors.Is(err, sql.ErrConnDone):
		return nil, nil, service.ErrNotConnected
	case errors.Is(err, sql.ErrNoRows):
		return service.MessageBatch{}, nil, nil
	case err != nil:
		return nil, nil, fmt.Errorf("failed to query for outbox items: %w", err)
	}
	defer rows.Close()

	var result service.MessageBatch
	var ids []any
	for rows.Next() {
		item, err := sqlRowToMap(rows)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to convert row to map: %w", err)
		}

		itemID := item[outboxIDSelector]

		// Remove the special field that we inject to make it easy to get item ids.
		delete(item, outboxIDSelector)

		msg := service.NewMessage(nil)
		msg.SetStructured(item)
		result = append(result, msg)

		ids = append(ids, itemID)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("error iterating over database result: %w", err)
	}

	retryCol := inp.config.retryCountColumn
	ackFunc := func(ctx context.Context, err error) error {
		defer func(logger *service.Logger) {
			if rerr := tx.Rollback(); rerr != nil && !errors.Is(rerr, sql.ErrTxDone) {
				logger.Errorf("failed to rollback transaction: %s", rerr)
			}
		}(inp.logger)

		// We are setting up a walker that goes through each message in the batch
		// and tells us if there was an error processing them.
		var walker func(fn func(messageIndex int, err error) bool)

		var werr *service.BatchError
		if err == nil {
			// If there were no errors then we'll create a walker that doesn't report
			// errors for any message in the batch.

			walker = func(fn func(int, error) bool) {
				for i := range ids {
					if cont := fn(i, nil); !cont {
						return
					}
				}
			}
		} else if ok := errors.As(err, &werr); ok {
			// If we did get a granular batch.WalkableError then lets proxy it.

			walker = func(fn func(int, error) bool) {
				werr.WalkMessages(func(i int, p *service.Message, err error) bool {
					return fn(i, err)
				})
			}
		} else {
			// If we got an opaque error then lets create a walker that assumes it
			// applies to every message in the batch

			walker = func(fn func(int, error) bool) {
				for i := range ids {
					if cont := fn(i, err); !cont {
						return
					}
				}
			}
		}

		// Consume the walker to generate a list of outbox items that failed
		// to be processed and another for ones that succeeded.
		var failedIds, passedIds []any
		walker(func(messageIndex int, err error) bool {
			if err == nil {
				passedIds = append(passedIds, ids[messageIndex])
			} else {
				failedIds = append(failedIds, ids[messageIndex])
			}

			return true
		})

		// For items that passed, we're going to delete them from the outbox table.
		var derr error
		if len(passedIds) > 0 {
			delPlaceholders := strings.TrimSuffix(strings.Repeat("?, ", len(passedIds)), ", ")
			deletes := inp.deleteBuilder.Where(fmt.Sprintf("id in (%s)", delPlaceholders), passedIds...)
			if _, err := deletes.RunWith(tx).ExecContext(ctx); err != nil {
				derr = fmt.Errorf("failed to delete processed outbox items: %w", err)
			}
		}

		// For failed items, we'll increment the column containing the retry
		// counter.
		var uerr error
		if len(failedIds) > 0 {
			updPlaceholders := strings.TrimSuffix(strings.Repeat("?, ", len(failedIds)), ", ")
			updates := inp.updateBuilder.
				Set(retryCol, squirrel.Expr(fmt.Sprintf("%s + 1", retryCol))).
				Where(fmt.Sprintf("id in (%s)", updPlaceholders), failedIds...)
			if _, err := updates.RunWith(tx).ExecContext(ctx); err != nil {
				uerr = fmt.Errorf("failed to update failed outbox items: %w", err)
			}
		}

		// Combine any errors that might have occurred while attempting to
		// delete/update outbox items.
		errs := multierr.Combine(derr, uerr)
		if errs != nil {
			return errs
		}

		return tx.Commit()
	}

	return result, ackFunc, nil
}

func (inp *sqlOutboxInput) Close(ctx context.Context) error {
	inp.dbMut.Lock()
	defer inp.dbMut.Unlock()

	db := inp.db
	inp.db = nil

	if db == nil {
		return nil
	}

	return db.Close()
}

func resolveArgs(bl *bloblang.Executor) ([]any, error) {
	var err error
	var raw any
	var args []any

	if bl == nil {
		return args, nil
	}

	raw, err = bl.Query(nil)
	if err != nil {
		return nil, err
	}

	var isArgsList bool
	args, isArgsList = raw.([]any)
	if !isArgsList {
		return nil, fmt.Errorf("mapping returned non-array result: %T", raw)
	}

	return args, nil
}

func (inp *sqlOutboxInput) waitForAccess(ctx context.Context) error {
	if inp.config.rateLimit == "" {
		return nil
	}
	for {
		var period time.Duration
		var err error
		if rerr := inp.manager.AccessRateLimit(ctx, inp.config.rateLimit, func(rl service.RateLimit) {
			period, err = rl.Access(ctx)
		}); rerr != nil {
			err = rerr
		}
		if err != nil {
			return fmt.Errorf("rate limit error: %w", err)
		}

		if period > 0 {
			select {
			case <-time.After(period):
			case <-ctx.Done():
				return ctx.Err()
			}
		} else {
			return nil
		}
	}
}
