// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package saphana

import (
	"errors"
	"fmt"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	shFieldDSN                    = "dsn"
	shFieldFetchSize              = "fetch_size"
	shFieldSchemaName             = "schema_name"
	shFieldTable                  = "table"
	shFieldMode                   = "mode"
	shFieldQuery                  = "query"
	shFieldIncrementingColumn     = "incrementing_column"
	shFieldIncrementingInitialVal = "incrementing_initial_value"
	shFieldPollInterval           = "poll_interval"

	shFieldTimestampColumn     = "timestamp_column"
	shFieldTimestampInitialVal = "timestamp_initial_value"
	shFieldTimestampDelay      = "timestamp_delay"
	shFieldTimestampClock      = "timestamp_clock"

	shTimestampClockDatabase    = "database"
	shTimestampClockDatabaseUTC = "database_utc"

	shFieldCheckpointCache    = "checkpoint_cache"
	shFieldCheckpointCacheKey = "checkpoint_cache_key"
	shFieldCheckpointLimit    = "checkpoint_limit"

	shFieldNumericMapping   = "numeric_mapping"
	shFieldMaxRetries       = "max_retries"
	shFieldRetryBackoff     = "retry_backoff"
	shNumericMappingNone    = "none"
	shNumericMappingBestFit = "best_fit"

	shModeBulk                  = "bulk"
	shModeIncrementing          = "incrementing"
	shModeQuery                 = "query"
	shModeTimestamp             = "timestamp"
	shModeTimestampIncrementing = "timestamp+incrementing"
)

// Defaults of the mode-specific fields that carry one. The spec's Default(...)
// calls use these same constants, and rejectInertFields compares against them
// to tell an explicit value from the default.
const (
	shDefaultPollInterval       = "60s"
	shDefaultTimestampDelay     = "5s"
	shDefaultCheckpointCacheKey = "sap_hana_hwm"
)

var (
	shDefaultPollIntervalDur   = mustDuration(shDefaultPollInterval)
	shDefaultTimestampDelayDur = mustDuration(shDefaultTimestampDelay)
)

func mustDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		panic(err)
	}
	return d
}

var sapHANAInputConfigSpec = service.NewConfigSpec().
	Categories("Services").
	Version("4.114.0").
	Summary("Reads rows from a SAP HANA table.").
	Description(`Reads rows from a SAP HANA table. Supports five modes:

- `+"`bulk`"+`: reads all rows once then the input terminates (use with xref:components:inputs/sequence.adoc[sequence] for periodic re-reads).
- `+"`incrementing`"+`: polls for rows where `+"`incrementing_column`"+` exceeds the last seen value, emitting only net-new rows.
- `+"`query`"+`: executes a user-supplied SQL statement and emits one message per result row.
- `+"`timestamp`"+`: polls for rows where `+"`timestamp_column`"+` falls within `+"`(last_hwm, database_now - timestamp_delay]`"+`. The bound is read from the database clock (see `+"`timestamp_clock`"+`) and the delay absorbs commit lag. The HWM advances to the window bound once the window is fully consumed, so a restart mid-window re-reads that window from its start (rows sharing a timestamp cannot be split, hence no finer checkpoint).
- `+"`timestamp+incrementing`"+`: like `+"`timestamp`"+` but orders by `+"`(timestamp_column, incrementing_column)`"+` and resumes after the exact `+"`(timestamp, incrementing)`"+` pair of the last delivered row, so rows sharing a timestamp are neither duplicated nor missed and a restart mid-window continues from the last acknowledged batch rather than the window start.

== Metadata

Messages produced in `+"`bulk`, `incrementing`, `timestamp`, and `timestamp+incrementing`"+` modes carry the following metadata fields (`+"`query`"+` mode attaches none):

- `+"`table_name`"+`: The HANA table name.
- `+"`database_schema`"+`: The configured `+"`schema_name`"+`. Only present when `+"`schema_name`"+` is set.
- `+"`schema`"+`: Avro-compatible schema derived from `+"`SYS.TABLE_COLUMNS`"+`, suitable for use with `+"`schema_registry_encode`"+`. Column additions are detected automatically without a pipeline restart. Only present when `+"`schema_name`"+` is configured.
- `+"`primary_key_columns`"+`: JSON array of the table's primary-key column names in key order. Only present when `+"`schema_name`"+` is configured and the table has a primary key.

== Troubleshooting

*`+"`schema`"+` and `+"`primary_key_columns`"+` metadata are missing.* The input reads them from `+"`SYS.TABLE_COLUMNS`"+` and `+"`SYS.INDEX_COLUMNS`"+`; when that read fails it logs a warning and carries on without them, and `+"`incrementing_initial_value`"+` falls back to a guessed bind type. Check the warning in the logs and grant the connecting user `+"`CATALOG READ`"+` (or `+"`SELECT`"+` on the table's schema), and make sure `+"`schema_name`"+` is set.

*"invalid table name" or "invalid column name" for a table that exists.* Identifiers in this config are quoted when sent to HANA, so they must match the catalog's case exactly. HANA upper-cases identifiers that were created without quotes: a table created as `+"`create table orders`"+` is `+"`ORDERS`"+` in the catalog and must be configured as such.

*Batches arrive one at a time.* `+"`fetch_size`"+` larger than `+"`checkpoint_limit`"+` means a batch cannot be handed on until the previous one is acknowledged (a warning is logged at startup). Raise `+"`checkpoint_limit`"+` or lower `+"`fetch_size`"+`.

*Rows written around the time of a poll are missing in timestamp modes.* The poll window ends at the database clock minus `+"`timestamp_delay`"+`; a row whose timestamp was assigned before that bound but committed after the poll is never seen. Set `+"`timestamp_delay`"+` longer than your longest write transaction, and pick the `+"`timestamp_clock`"+` that matches how the column is populated.
`).
	Example("Incremental reads with a durable checkpoint",
		"Polls an orders table for new rows by primary key every 30 seconds and persists the high-water mark in a file cache so a restart resumes where it left off.",
		`
input:
  sap_hana:
    dsn: hdb://user:password@hana-host:39017
    mode: incrementing
    schema_name: SALES
    table: ORDERS
    incrementing_column: ORDER_ID
    poll_interval: 30s
    checkpoint_cache: hana_checkpoints

cache_resources:
  - label: hana_checkpoints
    file:
      directory: /var/lib/redpanda-connect/sap_hana
`).
	Example("Change tracking by timestamp with automatic Avro schema registration",
		"Follows an updated-at column together with the primary key so rows sharing a timestamp are neither duplicated nor missed, bounds each poll by the database's UTC clock with a commit-lag buffer, and registers the table's schema from the emitted metadata before encoding.",
		`
input:
  sap_hana:
    dsn: hdb://user:password@hana-host:39017
    mode: timestamp+incrementing
    schema_name: SALES
    table: ORDERS
    timestamp_column: UPDATED_AT
    incrementing_column: ORDER_ID
    timestamp_clock: database_utc
    timestamp_delay: 30s
    poll_interval: 10s
    checkpoint_cache: hana_checkpoints

pipeline:
  processors:
    - schema_registry_encode:
        url: http://schema-registry:8081
        subject: sales.orders-value
        schema_metadata: schema
        format: avro
        avro:
          raw_json: true

output:
  redpanda:
    seed_brokers: [ "broker:9092" ]
    topic: sales.orders

cache_resources:
  - label: hana_checkpoints
    file:
      directory: /var/lib/redpanda-connect/sap_hana
`).
	Field(service.NewStringField(shFieldDSN).
		Description("SAP HANA connection DSN in `hdb://user:password@host:port` form.").
		Example("hdb://user:password@host:39017").
		Secret(),
	).
	Field(service.NewIntField(shFieldFetchSize).
		Description("Number of rows requested per FetchNext round-trip. Larger values reduce round-trips on high-latency connections.").
		Default(128).
		Advanced(),
	).
	Field(service.NewStringField(shFieldSchemaName).
		Description("Database schema for the table. When set, an Avro-compatible `schema` metadata field is attached to every message using data from `SYS.TABLE_COLUMNS`.").
		Optional(),
	).
	Field(service.NewStringField(shFieldTable).
		Description("Table to read from. Required when `mode` is `bulk` or `incrementing`.").
		Optional(),
	).
	Field(service.NewStringEnumField(shFieldMode, shModeBulk, shModeIncrementing, shModeQuery, shModeTimestamp, shModeTimestampIncrementing).
		Description("Operation mode.").
		Default(shModeBulk),
	).
	Field(service.NewStringField(shFieldQuery).
		Description("Custom SQL statement to execute. Only used when `mode` is `query`.").
		Optional(),
	).
	Field(service.NewStringField(shFieldIncrementingColumn).
		Description("Column to use as the high-water mark for `incrementing` mode. Must be strictly monotonically increasing with no duplicate values — a BIGINT auto-increment column is ideal. Columns that produce duplicate values (e.g. a plain TIMESTAMP) will cause rows whose value ties span a `fetch_size` boundary to be re-delivered. Use `timestamp+incrementing` mode to handle timestamp columns safely.").
		Optional(),
	).
	Field(service.NewStringField(shFieldIncrementingInitialVal).
		Description("Initial high-water mark value. When empty, all existing rows are emitted on the first run. The value is converted to the `incrementing_column`'s type from the catalog on connect: integers for integer columns, RFC3339 or `YYYY-MM-DD[ HH:MM:SS]` for DATE/TIMESTAMP columns, hexadecimal for BINARY/VARBINARY columns, and the literal string (leading zeros preserved) for character columns. A persisted checkpoint takes precedence over this value.").
		Default(""),
	).
	Field(service.NewDurationField(shFieldPollInterval).
		Description("How long to wait between polls in `incrementing`, `timestamp`, and `timestamp+incrementing` modes.").
		Default(shDefaultPollInterval).
		Example("10s").
		Example("5m"),
	).
	Field(service.NewStringField(shFieldTimestampColumn).
		Description("Column to use as the high-water mark for `timestamp` and `timestamp+incrementing` modes. Must be a TIMESTAMP or LONGDATE column.").
		Optional(),
	).
	Field(service.NewStringField(shFieldTimestampInitialVal).
		Description("Initial high-water mark in RFC3339 format (e.g. `2024-01-01T00:00:00Z`). When empty, all existing rows are emitted on the first run.").
		Default(""),
	).
	Field(service.NewDurationField(shFieldTimestampDelay).
		Description("Commit-lag buffer for timestamp modes. The upper bound for each poll is the database clock minus `timestamp_delay`, so rows whose timestamp was assigned slightly before a still-uncommitted transaction finished are not missed.").
		Default(shDefaultTimestampDelay).
		Example("0s").
		Example("30s"),
	).
	Field(service.NewStringEnumField(shFieldTimestampClock, shTimestampClockDatabase, shTimestampClockDatabaseUTC).
		Description("Which database clock bounds each timestamp-mode poll window. The bound is always read from HANA, never from the connector host, so it shares the clock and timezone convention of the column it is compared against:\n\n- `database`: `CURRENT_TIMESTAMP` (session timezone), matching columns populated by `DEFAULT CURRENT_TIMESTAMP` or `NOW()`.\n- `database_utc`: `CURRENT_UTCTIMESTAMP`, for columns populated with UTC values.").
		Default(shTimestampClockDatabase).
		Advanced(),
	).
	Field(service.NewStringEnumField(shFieldNumericMapping, shNumericMappingNone, shNumericMappingBestFit).
		Description("Controls how DECIMAL/NUMERIC columns are emitted:\n\n- `none`: emit as a canonical decimal string preserving full precision, except integer-typed columns (`DECIMAL(p,0)` with precision <= 18) which are emitted as integers.\n- `best_fit`: additionally emit columns whose precision fits a double (precision <= 15) as floating-point numbers; wider values fall back to canonical decimal strings.").
		Default(shNumericMappingNone),
	).
	Field(service.NewIntField(shFieldMaxRetries).
		Description("Maximum number of times to retry a failed query before returning an error. Set to `0` to disable retries.").
		Default(3).
		Advanced(),
	).
	Field(service.NewDurationField(shFieldRetryBackoff).
		Description("Base delay between query retries. The delay grows linearly with the attempt number (attempt N waits N times this value).").
		Default("1s").
		Advanced(),
	).
	Field(service.NewStringField(shFieldCheckpointCache).
		Description("Name of a cache resource to persist the high-water mark across restarts. When set, the connector resumes from where it left off rather than starting from scratch. Only effective in `incrementing`, `timestamp`, and `timestamp+incrementing` modes. The cache must be declared under `cache_resources`. Choose a durable backend (Redis, PostgreSQL) for production; in-memory caches lose state on restart.").
		Example("redis_cache").
		Optional(),
	).
	Field(service.NewStringField(shFieldCheckpointCacheKey).
		Description("Key used to store the checkpoint in `checkpoint_cache`. Change this when multiple `sap_hana` inputs share the same cache resource to avoid key collisions.").
		Default(shDefaultCheckpointCacheKey).
		Advanced(),
	).
	Field(service.NewIntField(shFieldCheckpointLimit).
		Description("The maximum number of messages that can be in flight (read but not yet acknowledged) at a given time. The high-water mark is only checkpointed once every message before it has been acknowledged, preserving at-least-once delivery even when batches are acknowledged out of order. When `fetch_size` exceeds this limit, batches are delivered one at a time (each waits for the previous batch's acknowledgement), so raise this alongside large `fetch_size` values to keep batches flowing concurrently.").
		Default(1024).
		Advanced(),
	).
	Field(service.NewAutoRetryNacksToggleField())

func init() {
	service.MustRegisterBatchInput("sap_hana", sapHANAInputConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			i, err := newSAPHANAInput(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(conf, i)
		})
}

// sapHANAInputConfig holds the parsed configuration of a sap_hana input. It is
// set once by the constructor and never changes afterwards; everything that
// moves while the input runs lives on sapHANAInput itself.
type sapHANAInputConfig struct {
	dsn             string
	fetchSize       int
	schemaName      string
	tableName       string
	mode            string
	customQuery     string
	incrementingCol string
	incrInitialRaw  string // incrementing_initial_value as configured, before type coercion
	pollInterval    time.Duration

	timestampCol     string
	timestampInitial time.Time // timestamp_initial_value as configured; zero when unset
	timestampDelay   time.Duration
	timestampClock   string

	numericMapping     string
	maxRetries         int
	retryBackoff       time.Duration
	checkpointCache    string
	checkpointCacheKey string
	checkpointLimit    int
}

// usesIncrementing reports whether the mode tracks an incrementing_column HWM.
func (c *sapHANAInputConfig) usesIncrementing() bool {
	return c.mode == shModeIncrementing || c.mode == shModeTimestampIncrementing
}

// usesTimestamp reports whether the mode tracks a timestamp_column HWM.
func (c *sapHANAInputConfig) usesTimestamp() bool {
	return c.mode == shModeTimestamp || c.mode == shModeTimestampIncrementing
}

// polls reports whether the mode polls for new rows, as opposed to reading a
// result set once (bulk and query).
func (c *sapHANAInputConfig) polls() bool {
	return c.usesIncrementing() || c.usesTimestamp()
}

// checkpointingEnabled reports whether HWM state is loaded from and persisted
// to the cache: only when a cache is configured and the mode has an HWM to
// track. bulk and query modes have none, and writing an empty state from them
// would clobber a polling input sharing the same cache key.
func (c *sapHANAInputConfig) checkpointingEnabled() bool {
	return c.checkpointCache != "" && c.polls()
}

// tableRef returns a properly quoted and escaped table reference.
func (c *sapHANAInputConfig) tableRef() string {
	if c.schemaName != "" {
		return quoteIdentifier(c.schemaName) + "." + quoteIdentifier(c.tableName)
	}
	return quoteIdentifier(c.tableName)
}

func newSAPHANAInput(conf *service.ParsedConfig, mgr *service.Resources) (*sapHANAInput, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	s := &sapHANAInput{
		log: mgr.Logger(),

		mRowsRead:      mgr.Metrics().NewCounter("sap_hana_rows_read_total"),
		mPolls:         mgr.Metrics().NewCounter("sap_hana_polls_total"),
		mQueryRetries:  mgr.Metrics().NewCounter("sap_hana_query_retries_total"),
		mQueryDuration: mgr.Metrics().NewTimer("sap_hana_query_duration"),
		stopChan:       make(chan struct{}),
	}
	c := &s.conf

	var err error
	if c.dsn, err = conf.FieldString(shFieldDSN); err != nil {
		return nil, err
	}
	if c.fetchSize, err = conf.FieldInt(shFieldFetchSize); err != nil {
		return nil, err
	}
	if c.fetchSize < 1 {
		return nil, fmt.Errorf("field %q must be at least 1", shFieldFetchSize)
	}
	if conf.Contains(shFieldSchemaName) {
		if c.schemaName, err = conf.FieldString(shFieldSchemaName); err != nil {
			return nil, err
		}
	}
	if conf.Contains(shFieldTable) {
		if c.tableName, err = conf.FieldString(shFieldTable); err != nil {
			return nil, err
		}
	}
	if c.mode, err = conf.FieldString(shFieldMode); err != nil {
		return nil, err
	}
	if conf.Contains(shFieldQuery) {
		if c.customQuery, err = conf.FieldString(shFieldQuery); err != nil {
			return nil, err
		}
	}
	if conf.Contains(shFieldIncrementingColumn) {
		if c.incrementingCol, err = conf.FieldString(shFieldIncrementingColumn); err != nil {
			return nil, err
		}
	}
	var hwmInit string
	if hwmInit, err = conf.FieldString(shFieldIncrementingInitialVal); err != nil {
		return nil, err
	}
	if hwmInit != "" {
		// Best-effort guess at the bind type; Connect replaces it with the
		// incrementing column's catalog type once a connection exists.
		c.incrInitialRaw = hwmInit
		s.hwm = parseIncrHWMString(hwmInit)
	}
	if c.pollInterval, err = conf.FieldDuration(shFieldPollInterval); err != nil {
		return nil, err
	}
	if conf.Contains(shFieldTimestampColumn) {
		if c.timestampCol, err = conf.FieldString(shFieldTimestampColumn); err != nil {
			return nil, err
		}
	}
	var tsInitStr string
	if tsInitStr, err = conf.FieldString(shFieldTimestampInitialVal); err != nil {
		return nil, err
	}
	if tsInitStr != "" {
		if c.timestampInitial, err = time.Parse(time.RFC3339, tsInitStr); err != nil {
			return nil, fmt.Errorf("parsing %s: %w", shFieldTimestampInitialVal, err)
		}
		s.timestampHWM = c.timestampInitial
	}
	if c.timestampDelay, err = conf.FieldDuration(shFieldTimestampDelay); err != nil {
		return nil, err
	}
	if c.timestampClock, err = conf.FieldString(shFieldTimestampClock); err != nil {
		return nil, err
	}
	if c.numericMapping, err = conf.FieldString(shFieldNumericMapping); err != nil {
		return nil, err
	}
	if c.maxRetries, err = conf.FieldInt(shFieldMaxRetries); err != nil {
		return nil, err
	}
	if c.retryBackoff, err = conf.FieldDuration(shFieldRetryBackoff); err != nil {
		return nil, err
	}
	if conf.Contains(shFieldCheckpointCache) {
		if c.checkpointCache, err = conf.FieldString(shFieldCheckpointCache); err != nil {
			return nil, err
		}
	}
	if c.checkpointCacheKey, err = conf.FieldString(shFieldCheckpointCacheKey); err != nil {
		return nil, err
	}
	if c.checkpointLimit, err = conf.FieldInt(shFieldCheckpointLimit); err != nil {
		return nil, err
	}
	if c.checkpointLimit < 1 {
		return nil, fmt.Errorf("field %q must be at least 1", shFieldCheckpointLimit)
	}
	if c.fetchSize > c.checkpointLimit {
		s.log.Warnf("%s (%d) exceeds %s (%d): batches will be delivered one at a time, each waiting for the previous batch's acknowledgement. Raise %s to allow concurrent batches.",
			shFieldFetchSize, c.fetchSize, shFieldCheckpointLimit, c.checkpointLimit, shFieldCheckpointLimit)
	}

	switch c.mode {
	case shModeBulk, shModeIncrementing, shModeTimestamp, shModeTimestampIncrementing:
		if c.tableName == "" {
			return nil, fmt.Errorf("field %q is required when mode is %q", shFieldTable, c.mode)
		}
	case shModeQuery:
		if c.customQuery == "" {
			return nil, fmt.Errorf("field %q is required when mode is %q", shFieldQuery, c.mode)
		}
	}
	if c.mode == shModeIncrementing && c.incrementingCol == "" {
		return nil, fmt.Errorf("field %q is required when mode is %q", shFieldIncrementingColumn, shModeIncrementing)
	}
	if c.usesTimestamp() && c.timestampCol == "" {
		return nil, fmt.Errorf("field %q is required when mode is %q", shFieldTimestampColumn, c.mode)
	}
	if c.mode == shModeTimestampIncrementing && c.incrementingCol == "" {
		return nil, fmt.Errorf("field %q is required when mode is %q", shFieldIncrementingColumn, shModeTimestampIncrementing)
	}
	if err := c.rejectInertFields(conf); err != nil {
		return nil, err
	}

	s.cp = newCheckpointer(mgr, c)
	return s, nil
}

// rejectInertFields fails configs that set fields the selected mode never
// reads. Silently ignoring them hides intent errors: with mode defaulting to
// bulk, a config carrying incrementing_column and checkpoint_cache would run
// a one-shot full scan and exit instead of the incremental capture the user
// meant. Fields without defaults are checked for presence. Defaulted fields
// are always present in the parsed config, so for those only an explicit
// non-default value can reveal intent, and that is what is rejected.
func (c *sapHANAInputConfig) rejectInertFields(conf *service.ParsedConfig) error {
	usesIncrementing := c.usesIncrementing()
	usesTimestamp := c.usesTimestamp()
	polls := c.polls()

	inert := func(field string, set bool, allowed string) error {
		if !set {
			return nil
		}
		return fmt.Errorf("field %q has no effect when mode is %q (it applies to %s)", field, c.mode, allowed)
	}
	checks := []error{
		inert(shFieldIncrementingColumn, !usesIncrementing && conf.Contains(shFieldIncrementingColumn),
			"incrementing and timestamp+incrementing modes"),
		inert(shFieldIncrementingInitialVal, !usesIncrementing && c.incrInitialRaw != "",
			"incrementing and timestamp+incrementing modes"),
		inert(shFieldTimestampColumn, !usesTimestamp && conf.Contains(shFieldTimestampColumn),
			"timestamp and timestamp+incrementing modes"),
		inert(shFieldTimestampInitialVal, !usesTimestamp && !c.timestampInitial.IsZero(),
			"timestamp and timestamp+incrementing modes"),
		inert(shFieldQuery, c.mode != shModeQuery && conf.Contains(shFieldQuery), "query mode"),
		inert(shFieldTable, c.mode == shModeQuery && conf.Contains(shFieldTable), "table-driven modes"),
		inert(shFieldSchemaName, c.mode == shModeQuery && conf.Contains(shFieldSchemaName), "table-driven modes"),
		inert(shFieldCheckpointCache, !polls && conf.Contains(shFieldCheckpointCache),
			"incrementing, timestamp, and timestamp+incrementing modes"),
		inert(shFieldPollInterval, !polls && c.pollInterval != shDefaultPollIntervalDur,
			"incrementing, timestamp, and timestamp+incrementing modes"),
		inert(shFieldTimestampDelay, !usesTimestamp && c.timestampDelay != shDefaultTimestampDelayDur,
			"timestamp and timestamp+incrementing modes"),
		inert(shFieldTimestampClock, !usesTimestamp && c.timestampClock != shTimestampClockDatabase,
			"timestamp and timestamp+incrementing modes"),
	}
	// checkpoint_cache_key is inert for a different reason than the mode: it
	// only names where the checkpoint is stored, so without checkpoint_cache
	// there is nothing to store.
	if c.checkpointCache == "" && c.checkpointCacheKey != shDefaultCheckpointCacheKey {
		checks = append(checks, fmt.Errorf("field %q is set but %q is not: the key only names where the checkpoint is stored, so set %q or remove it",
			shFieldCheckpointCacheKey, shFieldCheckpointCache, shFieldCheckpointCache))
	}
	return errors.Join(checks...)
}
