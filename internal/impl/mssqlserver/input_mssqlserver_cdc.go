// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"
	"golang.org/x/sync/errgroup"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/confx"
	"github.com/redpanda-data/connect/v4/internal/impl/mssqlserver/replication"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	fieldConnectionString                = "connection_string"
	fieldStreamSnapshot                  = "stream_snapshot"
	fieldMaxParallelSnapshotTables       = "max_parallel_snapshot_tables"
	fieldSnapshotMaxBatchSize            = "snapshot_max_batch_size"
	fieldStreamBackoffInterval           = "stream_backoff_interval"
	fieldTablesExclude                   = "exclude"
	fieldTablesInclude                   = "include"
	fieldCheckpointLimit                 = "checkpoint_limit"
	fieldCheckpointCache                 = "checkpoint_cache"
	fieldCheckpointCacheKey              = "checkpoint_cache_key"
	fieldCheckpointCacheTableName        = "checkpoint_cache_table_name"
	fieldCheckpointCacheConnectionString = "checkpoint_cache_connection_string"
	fieldBatching                        = "batching"

	shutdownTimeout = 5 * time.Second
)

func init() {
	service.MustRegisterBatchInput("microsoft_sql_server_cdc", msSQLServerStreamConfigSpec, newMSSQLServerCDCInput)
}

var msSQLServerStreamConfigSpec = service.NewConfigSpec().
	Stable().
	Categories("Services").
	Version("0.0.1").
	Summary("Enables Change Data Capture by consuming from Microsoft SQL Server's change tables.").
	Description(`Streams changes from a Microsoft SQL Server database for Change Data Capture (CDC).
Additionally, if ` + "`" + fieldStreamSnapshot + "`" + ` is set to true, then the existing data in the database is also streamed too.

== Metadata

This input adds the following metadata fields to each message:
- database_schema (The database schema for the table where the message originates from)
- schema (The table schema in benthos common schema format, compatible with processors like parquet_encode)
- table (Name of the table that the message originated from)
- operation (Type of operation that generated the message: "read", "delete", "insert", or "update_before" and "update_after". "read" is from messages that are read in the initial snapshot phase.)
- lsn (the Log Sequence Number in Microsoft SQL Server)

== Permissions

When using the default Microsoft SQL Server based cache, the Connect user requires permission to create tables and stored procedures, and the ` + "rpcn" + `  schema must already exist. Refer to ` + "`" + fieldCheckpointCacheTableName + "`" + ` for more information.
		`).
	Field(service.NewStringField(fieldConnectionString).
		Description("The connection string of the Microsoft SQL Server database to connect to.").
		Example("sqlserver://username:password@host/instance?param1=value&param2=value"),
	).
	Field(service.NewBoolField(fieldStreamSnapshot).
		Description("If set to true, the connector will query all the existing data as a part of snapshot process. Otherwise, it will start from the current Log Sequence Number position.").
		ShortDescription("Query all existing data as a snapshot first. Otherwise streaming starts from the current LSN.").
		Example(true).
		Default(false),
	).
	Field(service.NewIntField(fieldMaxParallelSnapshotTables).
		Description("Specifies a number of tables that will be processed in parallel during the snapshot processing stage.").
		Default(1)).
	Field(service.NewIntField(fieldSnapshotMaxBatchSize).
		Description("The maximum number of rows to be streamed in a single batch when taking a snapshot.").
		Default(1000),
	).
	Field(service.NewStringListField(fieldTablesInclude).
		Description("Regular expressions for tables to include.").
		Example("dbo.products"),
	).
	Field(service.NewStringListField(fieldTablesExclude).
		Description("Regular expressions for tables to exclude.").
		Example("dbo.privatetable").
		Optional(),
	).
	Field(service.NewStringField(fieldCheckpointCache).
		Description("A https://www.docs.redpanda.com/redpanda-connect/components/caches/about[cache resource^] to use for storing the current Log Sequence Number (LSN) that has been successfully delivered, this allows Redpanda Connect to continue from that Log Sequence Number (LSN) upon restart, rather than consume the entire state of the change table. If not set the default Microsoft SQL Server based cache will be used, see `" + fieldCheckpointCacheTableName + "` for more information.").
		Optional(),
	).
	Field(service.NewStringField(fieldCheckpointCacheTableName).
		Description("The multipart identifier for the checkpoint cache table name. If no `" + fieldCheckpointCache + "` field is specified, this input will automatically create a table and stored procedure under the `rpcn` schema to act as a checkpoint cache. This table stores the latest processed Log Sequence Number (LSN) that has been successfully delivered, allowing Redpanda Connect to resume from that point upon restart rather than reconsume the entire change table.").
		Default(defaultCheckpointCache).
		Example("dbo.checkpoint_cache").
		Optional(),
	).
	Field(service.NewStringField(fieldCheckpointCacheConnectionString).
		Description("An optional connection string for a remote Microsoft SQL Server to use for the checkpoint cache. When set, the checkpoint cache table is created on this remote server instead of the source database. If `" + fieldCheckpointCache + "` is also set, that takes precedence.").
		Example("sqlserver://username:password@remotehost/instance?param1=value&param2=value").
		Optional(),
	).
	Field(service.NewStringField(fieldCheckpointCacheKey).
		Description("The key to use to store the snapshot position in `" + fieldCheckpointCache + "`. An alternative key can be provided if multiple CDC inputs share the same cache.").
		Default("microsoft_sql_server_cdc").
		Optional(),
	).
	Field(service.NewIntField(fieldCheckpointLimit).
		Description("The maximum number of messages that can be processed at a given time. Increasing this limit enables parallel processing and batching at the output level. Any given Log Sequence Number (LSN) will not be acknowledged unless all messages under that offset are delivered in order to preserve at least once delivery guarantees.").
		ShortDescription("The maximum number of messages that can be processed at a given time.").
		Default(1024),
	).
	Field(service.NewDurationField(fieldStreamBackoffInterval).
		Description("The interval between attempts to check for new changes once all data is processed. For low traffic tables increasing this value can reduce network traffic to the server.").
		ShortDescription("Interval between checks for new changes once all data is processed.").
		Default("5s").
		Example("5s").Example("1m"),
	).
	Field(service.NewAutoRetryNacksToggleField()).
	Field(service.NewBatchPolicyField(fieldBatching))

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

type config struct {
	connectionString        string
	streamSnapshot          bool
	streamBackoffInterval   time.Duration
	snapshotMaxBatchSize    int
	snapshotMaxWorkers      int
	tablesFilter            *confx.RegexpFilter
	lsnCache                string
	lsnCacheKey             string
	cpCacheTableName        string
	cpCacheConnectionString string
}

type sqlServerCDCInput struct {
	cfg *config
	db  *sql.DB

	res       *service.Resources
	publisher *batchPublisher
	metrics   *service.Metrics

	// batching and checkpointLimit rebuild the publisher (batcher + ordered
	// checkpoint tracker) on every Connect: a terminal nack pins a tracker
	// slot by design, and only a fresh tracker lets the restart resume from
	// the last durable LSN instead of staying wedged behind the stale slot.
	batching        service.BatchPolicy
	checkpointLimit int

	connMu  sync.Mutex
	stopSig *shutdown.Signaller
	log     *service.Logger
	cpCache service.Cache
}

func newMSSQLServerCDCInput(conf *service.ParsedConfig, resources *service.Resources) (s service.BatchInput, err error) {
	var (
		connectionString             string
		streamSnapshot               bool
		snapshotMaxWorkers           int
		streamBackoffInterval        time.Duration
		snapshotMaxBatchSize         int
		lsnCache, lsnCacheKey        string
		tableIncludes, tableExcludes []*regexp.Regexp
		batcher                      *service.Batcher
		cp                           *checkpoint.Capped[replication.LSN]
		cpCache                      service.Cache
		cpCacheTableName             string
	)

	if err := license.CheckRunningEnterprise(resources); err != nil {
		return nil, err
	}
	if connectionString, err = conf.FieldString(fieldConnectionString); err != nil {
		return nil, err
	}
	if streamSnapshot, err = conf.FieldBool(fieldStreamSnapshot); err != nil {
		return nil, err
	}
	if snapshotMaxWorkers, err = conf.FieldInt(fieldMaxParallelSnapshotTables); err != nil {
		return nil, err
	}
	if snapshotMaxBatchSize, err = conf.FieldInt(fieldSnapshotMaxBatchSize); err != nil {
		return nil, err
	}
	if streamBackoffInterval, err = conf.FieldDuration(fieldStreamBackoffInterval); err != nil {
		return nil, err
	}
	// tables
	if includes, err := conf.FieldStringList(fieldTablesInclude); err != nil {
		return nil, err
	} else if tableIncludes, err = confx.ParseRegexpPatterns(includes); err != nil {
		return nil, err
	}
	if excludes, err := conf.FieldStringList(fieldTablesExclude); err != nil {
		return nil, err
	} else if tableExcludes, err = confx.ParseRegexpPatterns(excludes); err != nil {
		return nil, err
	}
	// cache
	// if no cache component is specified then we fallback to default sql based version
	if conf.Contains(fieldCheckpointCache) {
		if lsnCache, err = conf.FieldString(fieldCheckpointCache); err != nil {
			return nil, err
		}
		if conf.Resources().HasCache(lsnCache) {
			if lsnCacheKey, err = conf.FieldString(fieldCheckpointCacheKey); err != nil {
				return nil, err
			}
		}
	}

	if cpCacheTableName, err = conf.FieldString(fieldCheckpointCacheTableName); err != nil {
		return nil, err
	}

	var cpCacheConnectionString string
	if conf.Contains(fieldCheckpointCacheConnectionString) {
		if cpCacheConnectionString, err = conf.FieldString(fieldCheckpointCacheConnectionString); err != nil {
			return nil, err
		}
	}

	// checkpointing
	var checkpointLimit int
	if checkpointLimit, err = conf.FieldInt(fieldCheckpointLimit); err != nil {
		return nil, err
	}
	cp = checkpoint.NewCapped[replication.LSN](int64(checkpointLimit))

	// batching
	var policy service.BatchPolicy
	if policy, err = conf.FieldBatchPolicy(fieldBatching); err != nil {
		return nil, err
	} else if policy.IsNoop() {
		policy.Count = 1
	}
	if batcher, err = policy.NewBatcher(resources); err != nil {
		return nil, err
	}

	logger := resources.Logger()

	i := sqlServerCDCInput{
		cfg: &config{
			connectionString:        connectionString,
			streamSnapshot:          streamSnapshot,
			streamBackoffInterval:   streamBackoffInterval,
			snapshotMaxWorkers:      snapshotMaxWorkers,
			snapshotMaxBatchSize:    snapshotMaxBatchSize,
			lsnCache:                lsnCache,
			lsnCacheKey:             lsnCacheKey,
			cpCacheTableName:        cpCacheTableName,
			cpCacheConnectionString: cpCacheConnectionString,
			tablesFilter: &confx.RegexpFilter{
				Include: tableIncludes,
				Exclude: tableExcludes,
			},
		},
		res:             resources,
		log:             logger,
		metrics:         resources.Metrics(),
		stopSig:         shutdown.NewSignaller(),
		publisher:       newBatchPublisher(batcher, cp, logger),
		batching:        policy,
		checkpointLimit: checkpointLimit,
		cpCache:         cpCache,
	}

	i.publisher.cacheLSN = i.cacheLSN

	// Has stopped is how we notify that we're not connected. This will get reset at connection time.
	i.stopSig.TriggerHasStopped()

	batchInput, err := service.AutoRetryNacksBatchedToggled(conf, &i)
	if err != nil {
		return nil, err
	}

	return conf.WrapBatchInputExtractTracingSpanMapping("microsoft_sql_server_cdc", batchInput)
}

func (i *sqlServerCDCInput) Connect(ctx context.Context) error {
	i.connMu.Lock()
	defer i.connMu.Unlock()

	// If the background goroutine from a previous Connect is still running,
	// skip reconnection. HasStoppedChan is closed initially (constructor) and
	// when the goroutine exits, so a blocking default means "still active".
	select {
	case <-i.stopSig.HasStoppedChan():
	default:
		return nil
	}

	var (
		err        error
		userTables []replication.UserDefinedTable
		cachedLSN  replication.LSN
	)
	if i.db, err = sql.Open("mssql", i.cfg.connectionString); err != nil {
		return fmt.Errorf("connecting to microsoft sql server: %s", err)
	}

	// no cache specified so use default, custom sql cache
	if i.cfg.lsnCache == "" {
		cacheConnStr := i.cfg.connectionString
		if i.cfg.cpCacheConnectionString != "" {
			cacheConnStr = i.cfg.cpCacheConnectionString
		}
		cache, err := newCheckpointCache(ctx, cacheConnStr, i.cfg.cpCacheTableName, i.log)
		if err != nil {
			return fmt.Errorf("initialising sql server based checkpoint cache: %s", err)
		}
		i.cpCache = cache
	}

	if userTables, err = replication.VerifyUserDefinedTables(ctx, i.db, i.cfg.tablesFilter, i.log); err != nil {
		return fmt.Errorf("verifying user defined tables: %w", err)
	}
	if cachedLSN, err = i.getCachedLSN(ctx); err != nil {
		return fmt.Errorf("unable to get cached LSN: %s", err)
	}

	// Rebuild the publisher (batcher + ordered checkpoint tracker) for this
	// connection attempt. A terminal nack pins a tracker slot by design;
	// reusing the old tracker would leave every future checkpoint stuck
	// behind the stale slot, wedging the input for the process lifetime
	// instead of letting this restart resume from the last durable LSN. The
	// old publisher is sealed so late acks from the previous session cannot
	// persist stale positions.
	i.publisher.seal()
	i.publisher.Close()
	newBatcher, err := i.batching.NewBatcher(i.res)
	if err != nil {
		return fmt.Errorf("creating batcher: %w", err)
	}
	i.publisher = newBatchPublisher(newBatcher, checkpoint.NewCapped[replication.LSN](int64(i.checkpointLimit)), i.log)
	i.publisher.cacheLSN = i.cacheLSN
	i.publisher.onTerminalNack = func(error) {
		// i.stopSig is only replaced while the input is stopped, and sealed
		// publishers never invoke this, so the signaller here is current.
		i.stopSig.TriggerSoftStop()
	}

	// setup snapshotting and streaming
	var (
		snapshotter *replication.Snapshot
		streaming   *replication.ChangeTableStream
	)
	// no cached LSN means we're not recovering from a restart
	if i.cfg.streamSnapshot && len(cachedLSN) == 0 {
		if snapshotter, err = replication.NewSnapshot(i.cfg.connectionString, userTables, i.publisher, i.log, i.metrics); err != nil {
			return fmt.Errorf("creating database snapshotter: %w", err)
		}
	} else {
		i.log.Infof("Snapshotting disabled, skipping...")
	}

	streaming = replication.NewChangeTableStream(userTables, i.publisher, i.cfg.streamBackoffInterval, i.log)

	// Reset our stop signal
	i.stopSig = shutdown.NewSignaller()

	go func() {
		var (
			err    error
			maxLSN = cachedLSN
		)
		softCtx, _ := i.stopSig.SoftStopCtx(context.Background())

		// snapshot if no LSN exists then store checkpoint once complete
		if snapshotter != nil {
			// The publisher outlives reconnects: clear any nack recorded by a
			// previous snapshot attempt so the gate judges only this run.
			i.publisher.resetSnapshotGate()
			if maxLSN, err = i.processSnapshot(softCtx, snapshotter); err != nil {
				if i.stopSig.IsHardStopSignalled() {
					i.log.Errorf("Shutting down snapshotting process: %s", err)
				} else {
					i.log.Infof("Gracefully shutting down snapshotting process: %s", err)
				}
				i.stopSig.TriggerHasStopped()
				return
			}

			// Flush the partial snapshot batch still held by the batcher, then
			// block until every snapshot batch is acknowledged downstream.
			// Persisting the LSN any earlier would let a crash in this window
			// skip un-acked snapshot rows on restart. Blocks until acks drain
			// or soft-stop (no timeout, by design; see postgres_cdc's
			// equivalent barrier).
			if err = i.publisher.flushCurrent(softCtx); err != nil {
				i.log.Errorf("Failed to flush remaining snapshot batches. Snapshot will re-run on restart (may cause duplicate data): %s", err)
				i.stopSig.TriggerHasStopped()
				return
			}
			if err = i.publisher.waitSnapshotAcks(softCtx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					i.log.Infof("Interrupted while waiting for snapshot acknowledgements. Snapshot will re-run on restart (may cause duplicate data): %s", err)
				} else {
					i.log.Errorf("Snapshot batch was rejected downstream. Snapshot will re-run on restart (may cause duplicate data): %s", err)
				}
				i.stopSig.TriggerHasStopped()
				return
			}

			if err = i.cacheLSN(softCtx, maxLSN); err != nil {
				if i.stopSig.IsHardStopSignalled() {
					i.log.Errorf("Shutting down snapshotting process: %s", err)
				} else {
					i.log.Infof("Gracefully shutting down snapshotting process: %s", err)
				}
				i.stopSig.TriggerHasStopped()
				return
			}
			i.log.Debugf("Cached LSN following snapshot: '%s'", maxLSN)
		}

		// streaming
		wg, ctx := errgroup.WithContext(softCtx)
		wg.Go(func() error {
			if err := streaming.ReadChangeTables(ctx, i.db, maxLSN); err != nil {
				return fmt.Errorf("streaming from change tables: %w", err)
			}
			return nil
		})
		if err := wg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			i.log.Errorf("Error during Microsoft SQL Server CDC Component: %s", err)
		} else {
			i.log.Info("Successfully shutdown Microsoft SQL Server CDC Component")
		}
		i.stopSig.TriggerHasStopped()
	}()

	return nil
}

func (i *sqlServerCDCInput) getCachedLSN(ctx context.Context) (replication.LSN, error) {
	var (
		cacheVal []byte
		cErr     error
	)

	if i.cpCache != nil {
		// use default custom sql server based cache
		cacheVal, cErr = i.cpCache.Get(ctx, i.cfg.lsnCacheKey)
	} else {
		if err := i.res.AccessCache(ctx, i.cfg.lsnCache, func(c service.Cache) {
			cacheVal, cErr = c.Get(ctx, i.cfg.lsnCacheKey)
		}); err != nil {
			return nil, fmt.Errorf("unable to access cache for reading: %w", err)
		}
	}

	if errors.Is(cErr, service.ErrKeyNotFound) {
		return nil, nil
	} else if cErr != nil {
		return nil, fmt.Errorf("unable read checkpoint from cache: %w", cErr)
	} else if len(cacheVal) == 0 {
		return nil, nil
	}
	return replication.LSN(cacheVal), nil
}

func (i *sqlServerCDCInput) cacheLSN(ctx context.Context, lsn replication.LSN) error {
	if len(lsn) == 0 {
		return errors.New("LSN for caching is empty")
	}

	var cErr error
	if i.cpCache != nil {
		cErr = i.cpCache.Set(ctx, i.cfg.lsnCacheKey, lsn, nil)
	} else {
		if err := i.res.AccessCache(ctx, i.cfg.lsnCache, func(c service.Cache) {
			cErr = c.Set(ctx, i.cfg.lsnCacheKey, lsn, nil)
		}); err != nil {
			return fmt.Errorf("unable to access cache for writing: %w", err)
		}
	}

	if cErr != nil {
		return fmt.Errorf("unable persist checkpoint to cache: %w", cErr)
	}
	return nil
}

func (i *sqlServerCDCInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case m := <-i.publisher.msgs():
		return m.msg, m.ackFn, nil
	case <-i.stopSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (i *sqlServerCDCInput) processSnapshot(ctx context.Context, snapshot *replication.Snapshot) (replication.LSN, error) {
	var (
		lsn replication.LSN
		err error
	)
	if lsn, err = snapshot.Prepare(ctx); err != nil {
		_ = snapshot.Close()
		return nil, fmt.Errorf("preparing snapshot: %w", err)
	}
	if err = snapshot.Read(ctx, i.cfg.snapshotMaxWorkers, i.cfg.snapshotMaxBatchSize); err != nil {
		_ = snapshot.Close()
		return nil, fmt.Errorf("reading snapshot: %w", err)
	}
	if err = snapshot.Close(); err != nil {
		return nil, fmt.Errorf("closing snapshot connections: %w", err)
	}
	i.log.Infof("Completed running snapshot process")

	return lsn, nil
}

func (i *sqlServerCDCInput) Close(ctx context.Context) error {
	if i.stopSig == nil {
		return nil // Never connected
	}
	i.stopSig.TriggerSoftStop()
	select {
	case <-ctx.Done():
	case <-time.After(shutdownTimeout):
	case <-i.stopSig.HasStoppedChan():
	}

	i.stopSig.TriggerHardStop()
	select {
	case <-ctx.Done():
	case <-time.After(shutdownTimeout):
		i.log.Error("failed to shutdown 'microsoft_sql_server_cdc' component within the timeout")
	case <-i.stopSig.HasStoppedChan():
	}
	if i.cpCache != nil {
		return i.cpCache.Close(ctx)
	}
	if i.db != nil {
		return i.db.Close()
	}
	return nil
}
