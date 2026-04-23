// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"
	"github.com/go-mysql-org/go-mysql/canal"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	fieldMySQLFlavor               = "flavor"
	fieldMySQLDSN                  = "dsn"
	fieldMySQLTables               = "tables"
	fieldStreamSnapshot            = "stream_snapshot"
	fieldSnapshotMaxBatchSize      = "snapshot_max_batch_size"
	fieldSnapshotMaxParallelTables = "snapshot_max_parallel_tables"
	fieldSnapshotChunksPerTable    = "snapshot_chunks_per_table"
	fieldMaxReconnectAttempts      = "max_reconnect_attempts"
	fieldBatching                  = "batching"
	fieldCheckpointKey             = "checkpoint_key"
	fieldCheckpointCache           = "checkpoint_cache"
	fieldCheckpointLimit           = "checkpoint_limit"
	fieldAWSIAMAuth                = "aws"
	// FieldAWSIAMAuthEnabled enabled field.
	FieldAWSIAMAuthEnabled = "enabled"

	shutdownTimeout = 5 * time.Second

	// maxSnapshotParallelTables is an upper bound on the snapshot worker pool.
	// It guards against accidental denial-of-service from a mis-typed config
	// value that would otherwise try to open thousands of MySQL connections
	// at once. Operators with a legitimate need for more parallelism can open
	// an issue — 256 is already well beyond the point at which the MySQL
	// server's own connection limits dominate.
	maxSnapshotParallelTables = 256

	// maxSnapshotChunksPerTable caps chunks_per_table for the same reason as
	// maxSnapshotParallelTables: a mis-typed value should fail fast at config
	// parse time rather than produce thousands of MIN/MAX planning queries
	// and slow down startup. The actual concurrency ceiling is still
	// snapshot_max_parallel_tables — chunks above that just rebalance work
	// across the fixed worker pool.
	maxSnapshotChunksPerTable = 256
)

func notImportedAWSOptFn(_ context.Context, awsConf *service.ParsedConfig, _ *mysql.Config, _ *service.Logger) (TokenBuilder, error) {
	if enabled, _ := awsConf.FieldBool(FieldAWSIAMAuthEnabled); !enabled {
		return nil, nil
	}
	return nil, errors.New("unable to configure AWS authentication as this binary does not import components/aws")
}

// AWSOptFn is populated with the child `aws` package when imported.
var AWSOptFn = notImportedAWSOptFn

// TokenBuilder can be used for fetching passwords at runtime during connection (ie. IAM auth tokens)
type TokenBuilder func(context.Context) error

var mysqlStreamConfigSpec = service.NewConfigSpec().
	Stable().
	Categories("Services").
	Version("4.45.0").
	Summary("Enables MySQL streaming for RedPanda Connect.").
	Description(`
== Metadata

This input adds the following metadata fields to each message:

- operation: The type of operation (insert, update, delete, or read for snapshot messages)
- table: The name of the table
- binlog_position: The binlog position (for CDC messages only, not set for snapshot messages)
- schema: The table schema in benthos common schema format, compatible with processors like parquet_encode
`).
	Fields(
		service.NewStringAnnotatedEnumField(fieldMySQLFlavor, map[string]string{
			gomysql.MySQLFlavor:   "MySQL flavored databases.",
			gomysql.MariaDBFlavor: "MariaDB flavored databases.",
		}).
			Description("The type of MySQL database to connect to.").
			Default(gomysql.MySQLFlavor),
		service.NewStringField(fieldMySQLDSN).
			Description("The DSN of the MySQL database to connect to.").
			Example("user:password@tcp(localhost:3306)/database"),
		service.NewStringListField(fieldMySQLTables).
			Description("A list of tables to stream from the database.").
			Example([]string{"table1", "table2"}).
			LintRule("root = if this.length() == 0 { [ \"field 'tables' must contain at least one table\" ] }"),
		service.NewStringField(fieldCheckpointCache).
			Description("A https://www.docs.redpanda.com/redpanda-connect/components/caches/about[cache resource^] to use for storing the current latest BinLog Position that has been successfully delivered, this allows Redpanda Connect to continue from that BinLog Position upon restart, rather than consume the entire state of the table."),
		service.NewStringField(fieldCheckpointKey).
			Description("The key to use to store the snapshot position in `"+fieldCheckpointCache+"`. An alternative key can be provided if multiple CDC inputs share the same cache.").
			Default("mysql_binlog_position"),
		service.NewIntField(fieldSnapshotMaxBatchSize).
			Description("The maximum number of rows to be streamed in a single batch when taking a snapshot.").
			Default(1000),
		service.NewIntField(fieldSnapshotMaxParallelTables).
			Description("The maximum number of tables that may be snapshotted in parallel. When set to `1` (the default) tables are read sequentially using a single transaction, preserving the previous behaviour. When set higher, multiple `REPEATABLE READ` transactions are opened on separate connections under a single brief `FLUSH TABLES ... WITH READ LOCK` window so every worker observes an identical, globally-consistent snapshot at the same binlog position. Must be between `1` and `256`.").
			Advanced().
			Default(1),
		service.NewIntField(fieldSnapshotChunksPerTable).
			Description("The number of primary-key chunks each table is split into during the snapshot. When set to `1` (the default) each table is read as a single unit. When set higher, each table's first primary-key column is probed for `MIN` and `MAX` and the resulting integer range is split into N equal half-open chunks that are dispatched across the `"+fieldSnapshotMaxParallelTables+"` worker pool. This is how a single very large table is parallelised. Only tables whose first primary-key column is an integer type (`tinyint`, `smallint`, `mediumint`, `int`, `integer`, or `bigint`, signed or unsigned) are chunked; tables with non-numeric first PK columns fall back to a single whole-table read and log the reason. Composite primary keys are supported — chunking uses the leading column only, and per-chunk keyset pagination continues to respect the full PK ordering. Must be between `1` and `256`.").
			Advanced().
			Default(1),
		service.NewIntField(fieldMaxReconnectAttempts).
			Description("The maximum number of attempts the MySQL driver will try to re-establish a broken connection before Connect attempts reconnection. A zero or negative number means infinite retry attempts.").
			Advanced().
			Default(10),
		service.NewBoolField(fieldStreamSnapshot).
			Description("If set to true, the connector will query all the existing data as a part of snapshot process. Otherwise, it will start from the current binlog position."),
		service.NewAutoRetryNacksToggleField(),
		service.NewIntField(fieldCheckpointLimit).
			Description("The maximum number of messages that can be processed at a given time. Increasing this limit enables parallel processing and batching at the output level. Any given BinLog Position will not be acknowledged unless all messages under that offset are delivered in order to preserve at least once delivery guarantees.").
			Default(1024),
		service.NewTLSField("tls").
			Description("Using this field overrides the SSL/TLS settings in the environment and DSN.").
			Optional(),
		service.NewObjectField(fieldAWSIAMAuth,
			service.NewBoolField(FieldAWSIAMAuthEnabled).
				Description("Enable AWS IAM authentication for MySQL. When enabled, an IAM authentication token is generated and used as the password. When using IAM authentication ensure `"+fieldMaxReconnectAttempts+"` is set to a low value to ensure it can refresh credentials.").
				Default(false),
			service.NewStringField("region").
				Description("The AWS region where the MySQL instance is located. If no region is specified then the environment default will be used.").
				Optional(),
			service.NewStringField("endpoint").
				Description("The MySQL endpoint hostname (e.g., mydb.abc123.us-east-1.rds.amazonaws.com)."),
			service.NewStringField("id").
				Description("The ID of credentials to use.").
				Optional().Advanced(),
			service.NewStringField("secret").
				Description("The secret for the credentials being used.").
				Optional().Advanced().Secret(),
			service.NewStringField("token").
				Description("The token for the credentials being used, required when using short term credentials.").
				Optional().Advanced(),
			service.NewStringField("role").
				Description("Optional AWS IAM role ARN to assume for authentication. Alternatively, use `roles` array for role chaining instead.").
				Optional(),
			service.NewStringField("role_external_id").
				Description("Optional external ID for the role assumption. Only used with the `role` field. Alternatively, use `roles` array for role chaining instead.").
				Optional(),
			service.NewObjectListField("roles",
				service.NewStringField("role").
					Default("").
					Description("AWS IAM role ARN to assume."),
				service.NewStringField("role_external_id").
					Description("Optional external ID for the role assumption.").
					Default("").
					Optional(),
			).
				Description("Optional array of AWS IAM roles to assume for authentication. Roles can be assumed in sequence, enabling chaining for purposes such as cross-account access. Each role can optionally specify an external ID.").
				Optional(),
		).
			Description("AWS IAM authentication configuration for MySQL instances. When enabled, IAM credentials are used to generate temporary authentication tokens instead of a static password.").
			Advanced().
			Optional(),
		service.NewBatchPolicyField(fieldBatching),
	)

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

type mysqlStreamInput struct {
	canal.DummyEventHandler

	mutex  sync.Mutex
	flavor string
	// canal stands for mysql binlog listener connection
	canal                *canal.Canal
	canalMaxConnAttempts int
	mysqlConfig          *mysql.Config
	binLogCache          string
	binLogCacheKey       string
	currentBinlogName    string

	dsn            string
	tables         []string
	streamSnapshot bool

	batching                       service.BatchPolicy
	batchPolicy                    *service.Batcher
	checkPointLimit                int
	fieldSnapshotMaxBatchSize      int
	fieldSnapshotMaxParallelTables int
	fieldSnapshotChunksPerTable    int

	logger *service.Logger
	res    *service.Resources

	rawMessageEvents chan MessageEvent
	msgChan          chan asyncMessage
	cp               *checkpoint.Capped[*position]

	shutSig *shutdown.Signaller

	// TLS configuration
	customTLSConfig *tls.Config

	// IAM authentication fields
	iamAuthEnabled      bool
	iamAuthTokenBuilder TokenBuilder

	// Table schemas - stored as serialized format (map[string]any) for metadata
	tableSchemas   map[string]any
	tableSchemasMu sync.RWMutex
}

func newMySQLStreamInput(conf *service.ParsedConfig, res *service.Resources) (s service.BatchInput, err error) {
	if err := license.CheckRunningEnterprise(res); err != nil {
		return nil, err
	}

	i := mysqlStreamInput{
		logger:           res.Logger(),
		rawMessageEvents: make(chan MessageEvent),
		msgChan:          make(chan asyncMessage),
		res:              res,
		tableSchemas:     make(map[string]any),
	}

	var batching service.BatchPolicy

	if i.dsn, err = conf.FieldString(fieldMySQLDSN); err != nil {
		return nil, err
	}

	if i.flavor, err = conf.FieldString(fieldMySQLFlavor); err != nil {
		return nil, err
	}
	if err := gomysql.ValidateFlavor(i.flavor); err != nil {
		return nil, err
	}
	i.mysqlConfig, err = mysql.ParseDSN(i.dsn)
	if err != nil {
		return nil, fmt.Errorf("error parsing mysql DSN: %v", err)
	}
	// We require this configuration option is enabled.
	i.mysqlConfig.ParseTime = true

	// Configure TLS if specified
	if i.customTLSConfig, err = conf.FieldTLS("tls"); err != nil {
		return nil, err
	}
	if i.customTLSConfig != nil {
		// Get ServerName from the address, stripping the port if present
		host := i.mysqlConfig.Addr
		if idx := strings.Index(host, ":"); idx != -1 {
			host = host[:idx]
		}
		i.customTLSConfig.ServerName = host

		tlsConfigKey := "custom-tls"
		if err := mysql.RegisterTLSConfig(tlsConfigKey, i.customTLSConfig); err != nil {
			return nil, fmt.Errorf("registering TLS config: %w", err)
		}
		i.mysqlConfig.TLSConfig = tlsConfigKey
	}

	// Configure AWS IAM authentication if enabled
	awsConf := conf.Namespace(fieldAWSIAMAuth)
	i.iamAuthEnabled, _ = awsConf.FieldBool(FieldAWSIAMAuthEnabled)

	if i.iamAuthTokenBuilder, err = AWSOptFn(context.Background(), awsConf, i.mysqlConfig, res.Logger()); err != nil {
		return nil, err
	}

	i.dsn = i.mysqlConfig.FormatDSN()

	if i.tables, err = conf.FieldStringList(fieldMySQLTables); err != nil {
		return nil, err
	}

	if i.streamSnapshot, err = conf.FieldBool(fieldStreamSnapshot); err != nil {
		return nil, err
	}

	if i.fieldSnapshotMaxBatchSize, err = conf.FieldInt(fieldSnapshotMaxBatchSize); err != nil {
		return nil, err
	}

	if i.fieldSnapshotMaxParallelTables, err = conf.FieldInt(fieldSnapshotMaxParallelTables); err != nil {
		return nil, err
	}
	if i.fieldSnapshotMaxParallelTables < 1 {
		return nil, fmt.Errorf("field '%s' must be at least 1, got %d", fieldSnapshotMaxParallelTables, i.fieldSnapshotMaxParallelTables)
	}
	if i.fieldSnapshotMaxParallelTables > maxSnapshotParallelTables {
		return nil, fmt.Errorf("field '%s' must be at most %d, got %d", fieldSnapshotMaxParallelTables, maxSnapshotParallelTables, i.fieldSnapshotMaxParallelTables)
	}

	if i.fieldSnapshotChunksPerTable, err = conf.FieldInt(fieldSnapshotChunksPerTable); err != nil {
		return nil, err
	}
	if i.fieldSnapshotChunksPerTable < 1 {
		return nil, fmt.Errorf("field '%s' must be at least 1, got %d", fieldSnapshotChunksPerTable, i.fieldSnapshotChunksPerTable)
	}
	if i.fieldSnapshotChunksPerTable > maxSnapshotChunksPerTable {
		return nil, fmt.Errorf("field '%s' must be at most %d, got %d", fieldSnapshotChunksPerTable, maxSnapshotChunksPerTable, i.fieldSnapshotChunksPerTable)
	}

	if i.canalMaxConnAttempts, err = conf.FieldInt(fieldMaxReconnectAttempts); err != nil {
		return nil, err
	}

	if i.checkPointLimit, err = conf.FieldInt(fieldCheckpointLimit); err != nil {
		return nil, err
	}

	if i.binLogCache, err = conf.FieldString(fieldCheckpointCache); err != nil {
		return nil, err
	}
	if !conf.Resources().HasCache(i.binLogCache) {
		return nil, fmt.Errorf("unknown cache resource: %s", i.binLogCache)
	}
	if i.binLogCacheKey, err = conf.FieldString(fieldCheckpointKey); err != nil {
		return nil, err
	}

	i.cp = checkpoint.NewCapped[*position](int64(i.checkPointLimit))

	for _, table := range i.tables {
		if err = validateTableName(table); err != nil {
			return nil, err
		}
	}

	if batching, err = conf.FieldBatchPolicy(fieldBatching); err != nil {
		return nil, err
	} else if batching.IsNoop() {
		batching.Count = 1
	}

	i.batching = batching
	if i.batchPolicy, err = i.batching.NewBatcher(res); err != nil {
		return nil, err
	} else if batching.IsNoop() {
		batching.Count = 1
	}

	r, err := service.AutoRetryNacksBatchedToggled(conf, &i)
	if err != nil {
		return nil, err
	}

	return conf.WrapBatchInputExtractTracingSpanMapping("mysql_cdc", r)
}

func init() {
	service.MustRegisterBatchInput("mysql_cdc", mysqlStreamConfigSpec, newMySQLStreamInput)
}

// ---- Redpanda Connect specific methods----

func (i *mysqlStreamInput) Connect(ctx context.Context) error {
	// If IAM authentication is enabled, generate a new token
	if i.iamAuthEnabled && i.iamAuthTokenBuilder != nil {
		if err := i.iamAuthTokenBuilder(ctx); err != nil {
			return fmt.Errorf("unable to generate IAM auth token: %w", err)
		}
	}

	canalConfig := canal.NewDefaultConfig()
	canalConfig.Flavor = i.flavor
	canalConfig.Addr = i.mysqlConfig.Addr
	canalConfig.User = i.mysqlConfig.User
	canalConfig.Password = i.mysqlConfig.Passwd
	canalConfig.MaxReconnectAttempts = i.canalMaxConnAttempts
	// resetting dump path since we are doing snapshot manually
	// this is required since canal will try to prepare dumper on init stage
	canalConfig.Dump.ExecutionPath = ""

	// Parse and set additional parameters
	canalConfig.Charset = i.mysqlConfig.Collation
	if i.customTLSConfig != nil {
		canalConfig.TLSConfig = i.customTLSConfig
		i.logger.Debugf("Using custom TLS config with ServerName: '%s'", i.customTLSConfig.ServerName)
	} else if i.mysqlConfig.TLS != nil {
		canalConfig.TLSConfig = i.mysqlConfig.TLS
		i.logger.Debugf("Using TLS config from DSN")
	}
	// Parse time values as time.Time values not strings
	canalConfig.ParseTime = true
	// canalConfig.Logger

	for _, table := range i.tables {
		canalConfig.IncludeTableRegex = append(
			canalConfig.IncludeTableRegex,
			"^"+regexp.QuoteMeta(i.mysqlConfig.DBName+"."+table)+"$",
		)
	}

	c, err := canal.NewCanal(canalConfig)
	if err != nil {
		return err
	}

	i.canal = c

	pos, err := i.getCachedBinlogPosition(ctx)
	if err != nil {
		return fmt.Errorf("unable to get cached binlog position: %s", err)
	}
	// create snapshot instance if we were requested and haven't finished it before.
	var snapshot *Snapshot
	if i.streamSnapshot && pos == nil {
		db, err := sql.Open("mysql", i.mysqlConfig.FormatDSN())
		if err != nil {
			return fmt.Errorf("connecting to MySQL server: %s", err)
		}
		snapshot = NewSnapshot(i.logger, db)
	}

	// Reset the shutSig
	sig := shutdown.NewSignaller()
	i.shutSig = sig
	go func() {
		ctx, _ := sig.SoftStopCtx(context.Background())
		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			<-ctx.Done()
			i.canal.Close()
			return nil
		})
		wg.Go(func() error { return i.readMessages(ctx) })
		wg.Go(func() error { return i.startMySQLSync(ctx, pos, snapshot) })
		if err := wg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			i.logger.Errorf("error during MySQL CDC: %s", err)
		} else {
			i.logger.Info("successfully shutdown MySQL CDC stream")
		}
		sig.TriggerHasStopped()
	}()

	return nil
}

func (i *mysqlStreamInput) startMySQLSync(ctx context.Context, pos *position, snapshot *Snapshot) error {
	// If we are given a snapshot, then we need to read it.
	if snapshot != nil {
		var startPos *position
		var err error
		if i.fieldSnapshotMaxParallelTables <= 1 && i.fieldSnapshotChunksPerTable <= 1 {
			startPos, err = i.runSequentialSnapshot(ctx, snapshot)
		} else {
			startPos, err = i.runParallelSnapshot(ctx, snapshot)
		}
		if err != nil {
			return err
		}
		// Signal snapshot completion. readMessages will flush any partial batch
		// and pre-resolve a checkpoint entry for startPos so the cache is
		// updated once the last snapshot batch is acknowledged.
		select {
		case i.rawMessageEvents <- MessageEvent{Operation: messageOperationSnapshotComplete, Position: startPos}:
		case <-ctx.Done():
			return ctx.Err()
		}
		pos = startPos
	} else if pos == nil {
		coords, err := i.canal.GetMasterPos()
		if err != nil {
			return fmt.Errorf("unable to get start binlog position: %w", err)
		}
		pos = &coords
	}
	i.logger.Infof("starting MySQL CDC stream from binlog %s at offset %d", pos.Name, pos.Pos)
	i.currentBinlogName = pos.Name
	i.canal.SetEventHandler(i)
	if err := i.canal.RunFrom(*pos); err != nil {
		return fmt.Errorf("starting streaming: %w", err)
	}
	return nil
}

// runSequentialSnapshot executes the original single-transaction snapshot flow:
// one FLUSH TABLES WITH READ LOCK window, one consistent-snapshot transaction,
// tables read serially by a single goroutine. Preserves byte-identical
// behaviour from before parallel-snapshot support was introduced.
func (i *mysqlStreamInput) runSequentialSnapshot(ctx context.Context, snapshot *Snapshot) (*position, error) {
	startPos, err := snapshot.prepareSnapshot(ctx, i.tables)
	if err != nil {
		_ = snapshot.close()
		return nil, fmt.Errorf("unable to prepare snapshot: %w", err)
	}
	if err = i.readSnapshot(ctx, snapshot); err != nil {
		_ = snapshot.close()
		return nil, fmt.Errorf("failed reading snapshot: %w", err)
	}
	if err = snapshot.releaseSnapshot(ctx); err != nil {
		_ = snapshot.close()
		return nil, fmt.Errorf("unable to release snapshot: %w", err)
	}
	if err = snapshot.close(); err != nil {
		return nil, fmt.Errorf("unable to close snapshot: %w", err)
	}
	return startPos, nil
}

// runParallelSnapshot opens fieldSnapshotMaxParallelTables consistent-snapshot
// transactions under a single FLUSH TABLES WITH READ LOCK window and reads the
// configured tables concurrently. All workers share one binlog position so the
// downstream handoff to the binlog stream is unchanged from the sequential
// path. The original snapshot argument is used only as a carrier for the
// already-open *sql.DB; ownership of that db is transferred to the parallel
// set (which closes it when done) so the caller must not reuse the original
// Snapshot afterwards.
func (i *mysqlStreamInput) runParallelSnapshot(ctx context.Context, snapshot *Snapshot) (*position, error) {
	// Transfer db ownership to the parallel set before doing anything that
	// might fail: if prepare fails, the set's close will release the db, and
	// we want snapshot.close() to be a safe no-op in that case.
	db := snapshot.db
	snapshot.db = nil

	// Workers are capped by the plausible number of work units: at most
	// chunks_per_table * len(tables), and never more than requested. Planning
	// may emit fewer units (e.g. some tables fall back to whole-table reads)
	// but the over-provisioning cost is bounded and connections held by idle
	// workers are released when the snapshot completes.
	workerCount := i.fieldSnapshotMaxParallelTables
	if maxUnits := len(i.tables) * i.fieldSnapshotChunksPerTable; workerCount > maxUnits {
		workerCount = maxUnits
	}

	set, startPos, err := prepareParallelSnapshotSet(ctx, i.logger, db, i.tables, workerCount)
	if err != nil {
		// prepareParallelSnapshotSet closed db on its own error paths.
		return nil, fmt.Errorf("unable to prepare parallel snapshot: %w", err)
	}

	// Plan work units using any worker's consistent-snapshot transaction.
	// All workers observe identical state so MIN/MAX computed here apply
	// uniformly to every worker's subsequent reads.
	units, err := planSnapshotWork(ctx, set.workers[0], i.tables, i.fieldSnapshotChunksPerTable)
	if err != nil {
		_ = set.close()
		return nil, fmt.Errorf("plan snapshot work: %w", err)
	}
	i.logger.Infof("Parallel snapshot planned: %d tables -> %d work units across %d workers", len(i.tables), len(units), len(set.workers))

	if err := i.readSnapshotParallel(ctx, set, units); err != nil {
		_ = set.close()
		return nil, fmt.Errorf("failed reading snapshot: %w", err)
	}
	if err := set.release(ctx); err != nil {
		_ = set.close()
		return nil, fmt.Errorf("unable to release parallel snapshot: %w", err)
	}
	if err := set.close(); err != nil {
		return nil, fmt.Errorf("unable to close parallel snapshot: %w", err)
	}
	return startPos, nil
}

func (i *mysqlStreamInput) readSnapshot(ctx context.Context, snapshot *Snapshot) error {
	for _, table := range i.tables {
		if err := i.readSnapshotWorkUnit(ctx, snapshot, snapshotWorkUnit{table: table}); err != nil {
			return err
		}
	}
	return nil
}

// readSnapshotWorkUnit snapshots one work unit — either a whole table or a
// primary-key chunk of a table — by paging through its rows in primary-key
// order using the REPEATABLE READ / CONSISTENT SNAPSHOT transaction held by
// snapshot. When unit.bounds is nil the whole table is read; otherwise rows
// are filtered by the chunk's [lowerIncl, upperExcl) range on the first PK
// column. Both the sequential and the parallel paths use this same body so
// per-table semantics are identical regardless of chunking configuration.
func (i *mysqlStreamInput) readSnapshotWorkUnit(ctx context.Context, snapshot *Snapshot, unit snapshotWorkUnit) error {
	table := unit.table
	// Pre-populate schema cache so snapshot messages carry schema metadata.
	if tbl, err := i.canal.GetTable(i.mysqlConfig.DBName, table); err == nil {
		if _, err := i.getTableSchema(tbl); err != nil {
			i.logger.Warnf("Failed to pre-populate schema for table %s during snapshot: %v", table, err)
		}
	} else {
		i.logger.Warnf("Failed to fetch schema for table %s during snapshot: %v", table, err)
	}
	tablePks, err := snapshot.getTablePrimaryKeys(ctx, table)
	if err != nil {
		return err
	}
	i.logger.Tracef("primary keys for table %s: %v", table, tablePks)
	lastSeenPksValues := map[string]any{}
	for _, pk := range tablePks {
		lastSeenPksValues[pk] = nil
	}

	var numRowsProcessed int
	for {
		var batchRows *sql.Rows
		if numRowsProcessed == 0 {
			batchRows, err = snapshot.querySnapshotTable(ctx, table, tablePks, unit.bounds, nil, i.fieldSnapshotMaxBatchSize)
		} else {
			batchRows, err = snapshot.querySnapshotTable(ctx, table, tablePks, unit.bounds, &lastSeenPksValues, i.fieldSnapshotMaxBatchSize)
		}
		if err != nil {
			return fmt.Errorf("executing snapshot table query: %s", err)
		}

		types, err := batchRows.ColumnTypes()
		if err != nil {
			return fmt.Errorf("fetching column types: %s", err)
		}

		values, mappers := prepSnapshotScannerAndMappers(types)

		columns, err := batchRows.Columns()
		if err != nil {
			return fmt.Errorf("fetching columns: %s", err)
		}

		var batchRowsCount int
		for batchRows.Next() {
			numRowsProcessed++
			batchRowsCount++

			if err := batchRows.Scan(values...); err != nil {
				return err
			}

			row := map[string]any{}
			for idx, value := range values {
				v, err := mappers[idx](value)
				if err != nil {
					return err
				}
				row[columns[idx]] = v
				if _, ok := lastSeenPksValues[columns[idx]]; ok {
					lastSeenPksValues[columns[idx]] = value
				}
			}

			select {
			case i.rawMessageEvents <- MessageEvent{
				Row:       row,
				Operation: MessageOperationRead,
				Table:     table,
				Position:  nil,
			}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if err := batchRows.Err(); err != nil {
			return fmt.Errorf("iterating snapshot table: %s", err)
		}

		if batchRowsCount < i.fieldSnapshotMaxBatchSize {
			break
		}
	}
	return nil
}

func snapshotValueMapper[T any](v any) (any, error) {
	s, ok := v.(*sql.Null[T])
	if !ok {
		var e T
		return nil, fmt.Errorf("expected %T got %T", e, v)
	}
	if !s.Valid {
		return nil, nil
	}
	return s.V, nil
}

func prepSnapshotScannerAndMappers(cols []*sql.ColumnType) (values []any, mappers []func(any) (any, error)) {
	stringMapping := func(mapper func(s string) (any, error)) func(any) (any, error) {
		return func(v any) (any, error) {
			s, ok := v.(*sql.NullString)
			if !ok {
				return nil, fmt.Errorf("expected %T got %T", "", v)
			}
			if !s.Valid {
				return nil, nil
			}
			return mapper(s.String)
		}
	}
	for _, col := range cols {
		var val any
		var mapper func(any) (any, error)
		switch col.DatabaseTypeName() {
		case "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
			val = new(sql.Null[[]byte])
			mapper = snapshotValueMapper[[]byte]
		case "DATETIME", "TIMESTAMP":
			val = new(sql.NullTime)
			mapper = func(v any) (any, error) {
				s, ok := v.(*sql.NullTime)
				if !ok {
					return nil, fmt.Errorf("expected %T got %T", time.Time{}, v)
				}
				if !s.Valid {
					return nil, nil
				}
				return s.Time, nil
			}
		case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "YEAR",
			"UNSIGNED TINYINT", "UNSIGNED SMALLINT", "UNSIGNED MEDIUMINT":
			val = new(sql.NullInt32)
			mapper = func(v any) (any, error) {
				s, ok := v.(*sql.NullInt32)
				if !ok {
					return nil, fmt.Errorf("expected %T got %T", int32(0), v)
				}
				if !s.Valid {
					return nil, nil
				}
				return s.Int32, nil
			}
		case "BIGINT", "UNSIGNED INT", "UNSIGNED BIGINT":
			val = new(sql.NullInt64)
			mapper = func(v any) (any, error) {
				s, ok := v.(*sql.NullInt64)
				if !ok {
					return nil, fmt.Errorf("expected %T got %T", int64(0), v)
				}
				if !s.Valid {
					return nil, nil
				}
				return s.Int64, nil
			}
		case "DECIMAL", "NUMERIC":
			val = new(sql.NullString)
			mapper = stringMapping(func(s string) (any, error) {
				return s, nil
			})
		case "FLOAT":
			val = new(sql.Null[float32])
			mapper = snapshotValueMapper[float32]
		case "DOUBLE":
			val = new(sql.Null[float64])
			mapper = snapshotValueMapper[float64]
		case "SET":
			val = new(sql.NullString)
			mapper = stringMapping(func(s string) (any, error) {
				// This might be a little simplistic, we may need to handle escaped values
				// here...
				out := []any{}
				for elem := range strings.SplitSeq(s, ",") {
					out = append(out, elem)
				}
				return out, nil
			})
		case "JSON":
			val = new(sql.NullString)
			mapper = stringMapping(func(s string) (v any, err error) {
				err = json.Unmarshal([]byte(s), &v)
				return
			})
		case "BIT":
			val = new(sql.Null[[]byte])
			mapper = func(v any) (any, error) {
				s, ok := v.(*sql.Null[[]byte])
				if !ok {
					return nil, fmt.Errorf("expected %T got %T", &sql.Null[[]byte]{}, v)
				}
				if !s.Valid {
					return nil, nil
				}
				var n int64
				for _, b := range s.V {
					n = (n << 8) | int64(b)
				}
				return n, nil
			}
		case "DATE":
			val = new(sql.NullTime)
			mapper = func(v any) (any, error) {
				s, ok := v.(*sql.NullTime)
				if !ok {
					return nil, fmt.Errorf("expected %T got %T", &sql.NullTime{}, v)
				}
				if !s.Valid {
					return nil, nil
				}
				return s.Time, nil
			}
		default:
			val = new(sql.Null[string])
			mapper = snapshotValueMapper[string]
		}
		values = append(values, val)
		mappers = append(mappers, mapper)
	}
	return
}

func (i *mysqlStreamInput) readMessages(ctx context.Context) error {
	var nextTimedBatchChan <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-nextTimedBatchChan:
			nextTimedBatchChan = nil
			flushedBatch, err := i.batchPolicy.Flush(ctx)
			if err != nil {
				return fmt.Errorf("timed flush batch error: %w", err)
			}

			if err := i.flushBatch(ctx, i.cp, flushedBatch); err != nil {
				return fmt.Errorf("flushing periodic batch: %w", err)
			}
		case me := <-i.rawMessageEvents:
			if me.Operation == messageOperationSnapshotComplete {
				// Flush any remaining messages before post snapshot checkpoint
				flushedBatch, err := i.batchPolicy.Flush(ctx)
				if err != nil {
					return fmt.Errorf("flushing snapshot completion batch: %w", err)
				}
				if err := i.flushBatch(ctx, i.cp, flushedBatch); err != nil {
					return fmt.Errorf("flushing snapshot completion batch: %w", err)
				}

				if me.Position != nil {
					resolveFn, err := i.cp.Track(ctx, me.Position, 1)
					if err != nil {
						return fmt.Errorf("tracking snapshot completion checkpoint: %w", err)
					}

					// No mutex needed: checkpoint.Capped is thread-safe and snapshot batches never write to the cache
					if maxOffset := resolveFn(); maxOffset != nil {
						if offset := *maxOffset; offset != nil {
							if err := i.setCachedBinlogPosition(ctx, *offset); err != nil {
								return fmt.Errorf("persisting snapshot checkpoint: %w", err)
							}
							i.logger.Infof("Checkpointed binlog position following snapshot")
						}
					}
				}
				nextTimedBatchChan = nil
				continue
			}

			mb := service.NewMessage(nil)
			mb.SetStructuredMut(me.Row)
			mb.MetaSet("operation", string(me.Operation))
			mb.MetaSet("table", me.Table)
			if me.Position != nil {
				mb.MetaSet("binlog_position", binlogPositionToString(*me.Position))
			}

			// Add table schema if available
			if tableSchema := i.getOrExtractTableSchemaByName(me.Table); tableSchema != nil {
				mb.MetaSetImmut("schema", service.ImmutableAny{V: tableSchema})
			}

			if i.batchPolicy.Add(mb) {
				nextTimedBatchChan = nil
				flushedBatch, err := i.batchPolicy.Flush(ctx)
				if err != nil {
					return fmt.Errorf("flush batch error: %w", err)
				}
				if err := i.flushBatch(ctx, i.cp, flushedBatch); err != nil {
					return fmt.Errorf("flushing batch: %w", err)
				}
			} else {
				d, ok := i.batchPolicy.UntilNext()
				if ok {
					nextTimedBatchChan = time.After(d)
				}
			}
		}
	}
}

func (i *mysqlStreamInput) flushBatch(
	ctx context.Context,
	checkpointer *checkpoint.Capped[*position],
	batch service.MessageBatch,
) error {
	if len(batch) == 0 {
		return nil
	}

	lastMsg := batch[len(batch)-1]
	strPosition, ok := lastMsg.MetaGet("binlog_position")
	var binLogPos *position
	if ok {
		pos, err := parseBinlogPosition(strPosition)
		if err != nil {
			return err
		}
		binLogPos = &pos
	}

	resolveFn, err := checkpointer.Track(ctx, binLogPos, int64(len(batch)))
	if err != nil {
		return fmt.Errorf("tracking checkpoint for batch: %w", err)
	}
	msg := asyncMessage{
		msg: batch,
		ackFn: func(ctx context.Context, _ error) error {
			i.mutex.Lock()
			defer i.mutex.Unlock()
			maxOffset := resolveFn()
			// Nothing to commit, this wasn't the latest message
			if maxOffset == nil {
				return nil
			}
			offset := *maxOffset
			// This has no offset - it's a snapshot message
			if offset == nil {
				return nil
			}
			return i.setCachedBinlogPosition(ctx, *offset)
		},
	}
	select {
	case i.msgChan <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (i *mysqlStreamInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case m := <-i.msgChan:
		return m.msg, m.ackFn, nil
	case <-i.shutSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case <-ctx.Done():
	}
	return nil, nil, ctx.Err()
}

func (i *mysqlStreamInput) Close(ctx context.Context) error {
	if i.shutSig == nil {
		return nil // Never connected
	}
	i.shutSig.TriggerSoftStop()
	select {
	case <-ctx.Done():
	case <-time.After(shutdownTimeout):
	case <-i.shutSig.HasStoppedChan():
	}
	i.shutSig.TriggerHardStop()
	select {
	case <-ctx.Done():
	case <-time.After(shutdownTimeout):
		i.logger.Error("failed to shutdown mysql_cdc within the timeout")
	case <-i.shutSig.HasStoppedChan():
	}
	return nil
}

// ---- input methods end ----

// ---- cache methods start ----

func (i *mysqlStreamInput) getCachedBinlogPosition(ctx context.Context) (*position, error) {
	var (
		cacheVal []byte
		cErr     error
	)
	if err := i.res.AccessCache(ctx, i.binLogCache, func(c service.Cache) {
		cacheVal, cErr = c.Get(ctx, i.binLogCacheKey)
	}); err != nil {
		return nil, fmt.Errorf("unable to access cache for reading: %w", err)
	}
	if errors.Is(cErr, service.ErrKeyNotFound) {
		return nil, nil
	} else if cErr != nil {
		return nil, fmt.Errorf("unable read checkpoint from cache: %w", cErr)
	} else if cacheVal == nil {
		return nil, nil
	}
	pos, err := parseBinlogPosition(string(cacheVal))
	return &pos, err
}

func (i *mysqlStreamInput) setCachedBinlogPosition(ctx context.Context, binLogPos position) error {
	var cErr error
	if err := i.res.AccessCache(ctx, i.binLogCache, func(c service.Cache) {
		cErr = c.Set(
			ctx,
			i.binLogCacheKey,
			[]byte(binlogPositionToString(binLogPos)),
			nil,
		)
	}); err != nil {
		return fmt.Errorf("unable to access cache for writing: %w", err)
	}
	if cErr != nil {
		return fmt.Errorf("unable persist checkpoint to cache: %w", cErr)
	}
	return nil
}

// ---- cache methods end ----

// --- MySQL Canal handler methods ----

func (i *mysqlStreamInput) OnRotate(_ *replication.EventHeader, re *replication.RotateEvent) error {
	i.currentBinlogName = string(re.NextLogName)
	return nil
}

// OnTableChanged is called when a table is created, altered, renamed, or dropped.
// We invalidate the cached schema so it will be re-extracted on the next row event.
func (i *mysqlStreamInput) OnTableChanged(_ *replication.EventHeader, schema, table string) error {
	// Only invalidate cache for tables we're tracking
	fullTableName := table
	if schema != "" {
		fullTableName = schema + "." + table
	}

	// Check if this is one of our tracked tables
	isTracked := false
	for _, t := range i.tables {
		if t == table || t == fullTableName {
			isTracked = true
			break
		}
	}

	if isTracked {
		i.invalidateTableSchema(table)
		i.logger.Infof("Schema cache invalidated for table %s.%s due to DDL change", schema, table)
	}

	return nil
}

func (i *mysqlStreamInput) OnRow(e *canal.RowsEvent) error {
	// Extract and cache the table schema if we haven't seen this table yet
	if _, err := i.getTableSchema(e.Table); err != nil {
		return fmt.Errorf("extracting schema for table %s: %w", e.Table.Name, err)
	}

	switch e.Action {
	case canal.InsertAction:
		return i.onMessage(e, 0, 1)
	case canal.DeleteAction:
		return i.onMessage(e, 0, 1)
	case canal.UpdateAction:
		// Updates send both the new and old data - we only emit the new data.
		return i.onMessage(e, 1, 2)
	default:
		return errors.New("invalid rows action")
	}
}

func (i *mysqlStreamInput) onMessage(e *canal.RowsEvent, initValue, incrementValue int) error {
	for pi := initValue; pi < len(e.Rows); pi += incrementValue {
		message := map[string]any{}
		for i, v := range e.Rows[pi] {
			col := e.Table.Columns[i]
			v, err := mapMessageColumn(v, col)
			if err != nil {
				return err
			}
			message[col.Name] = v
		}
		i.rawMessageEvents <- MessageEvent{
			Row:       message,
			Operation: MessageOperation(e.Action),
			Table:     e.Table.Name,
			Position:  &position{Name: i.currentBinlogName, Pos: e.Header.LogPos},
		}
	}
	return nil
}

func mapMessageColumn(v any, col schema.TableColumn) (any, error) {
	if v == nil {
		return v, nil
	}
	switch col.Type {
	case schema.TYPE_NUMBER:
		switch n := v.(type) {
		case int:
			return int64(n), nil
		case int8:
			return int32(n), nil
		case int16:
			return int32(n), nil
		case int32:
			return n, nil
		case int64:
			return n, nil
		case uint:
			return int64(n), nil
		case uint8:
			return int32(n), nil
		case uint16:
			return int32(n), nil
		case uint32:
			return int64(n), nil
		case uint64:
			if n > math.MaxInt64 {
				return n, nil
			}
			return int64(n), nil
		default:
			return nil, fmt.Errorf("expected integer value for number column got: %T", v)
		}
	case schema.TYPE_MEDIUM_INT:
		switch n := v.(type) {
		case int32:
			return n, nil
		case uint32:
			return int32(n), nil
		default:
			return nil, fmt.Errorf("expected int32 or uint32 value for mediumint column got: %T", v)
		}
	case schema.TYPE_FLOAT:
		return v, nil
	case schema.TYPE_DECIMAL:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected string value for decimal column got: %T", v)
		}
		return s, nil
	case schema.TYPE_SET:
		bitset, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("expected int value for set column got: %T", v)
		}
		out := []any{}
		for i, element := range col.SetValues {
			if (bitset>>i)&1 == 1 {
				out = append(out, element)
			}
		}
		return out, nil
	case schema.TYPE_DATE:
		switch d := v.(type) {
		case string:
			return time.Parse("2006-01-02", d)
		case time.Time:
			return d, nil
		default:
			return nil, fmt.Errorf("expected string or time.Time for date column got: %T", v)
		}
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		if _, ok := v.(string); ok {
			return nil, nil
		}
		return v, nil
	case schema.TYPE_ENUM:
		ordinal, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("expected int value for enum column got: %T", v)
		}
		if ordinal < 1 || int(ordinal) > len(col.EnumValues) {
			return nil, fmt.Errorf("enum ordinal out of range: %d when there are %d variants", ordinal, len(col.EnumValues))
		}
		return col.EnumValues[ordinal-1], nil
	case schema.TYPE_JSON:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected string value for json column got: %T", v)
		}
		var decoded any
		if err := json.Unmarshal([]byte(s), &decoded); err != nil {
			return nil, err
		}
		return decoded, nil
	case schema.TYPE_STRING:
		// Blob types should come through as binary, but are marked type 5,
		// instead skip them here and have those fallthrough to the binary case.
		if !strings.Contains(col.RawType, "blob") {
			if s, ok := v.(string); ok {
				return s, nil
			}
			s, ok := v.([]byte)
			if !ok {
				return nil, fmt.Errorf("unexpected type for STRING column: %T", v)
			}
			return string(s), nil
		}
		fallthrough
	case schema.TYPE_BINARY:
		if s, ok := v.([]byte); ok {
			return s, nil
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected type for BINARY column: %T", v)
		}
		return []byte(s), nil
	default:
		return v, nil
	}
}

// --- MySQL Canal handler methods end ----

// ---- Schema extraction methods ----

// getTableSchema retrieves the cached schema for a table, or extracts it if not yet cached.
func (i *mysqlStreamInput) getTableSchema(table *schema.Table) (any, error) {
	i.tableSchemasMu.RLock()
	if cached, exists := i.tableSchemas[table.Name]; exists {
		i.tableSchemasMu.RUnlock()
		return cached, nil
	}
	i.tableSchemasMu.RUnlock()

	// Extract schema from MySQL table
	commonSchema, err := mysqlTableToCommonSchema(table)
	if err != nil {
		return nil, fmt.Errorf("converting table schema for %s: %w", table.Name, err)
	}

	// Serialize to generic format for metadata
	serialized := commonSchema.ToAny()

	// Cache it
	i.tableSchemasMu.Lock()
	i.tableSchemas[table.Name] = serialized
	i.tableSchemasMu.Unlock()

	return serialized, nil
}

// getOrExtractTableSchemaByName attempts to retrieve a cached schema by table name.
// For snapshot messages, we may not have the canal Table object, so we return nil
// and let the schema be extracted later when we see CDC events for this table.
func (i *mysqlStreamInput) getOrExtractTableSchemaByName(tableName string) any {
	i.tableSchemasMu.RLock()
	defer i.tableSchemasMu.RUnlock()
	return i.tableSchemas[tableName]
}

// invalidateTableSchema removes a table's schema from the cache.
// This is called when a DDL change is detected via OnTableChanged.
func (i *mysqlStreamInput) invalidateTableSchema(tableName string) {
	i.tableSchemasMu.Lock()
	defer i.tableSchemasMu.Unlock()
	delete(i.tableSchemas, tableName)
}

// ---- Schema extraction methods end ----
