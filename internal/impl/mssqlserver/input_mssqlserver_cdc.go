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
	fieldConnectionString     = "connection_string"
	fieldStreamSnapshot       = "stream_snapshot"
	fieldSnapshotMaxBatchSize = "snapshot_max_batch_size"
	fieldCheckpointLimit      = "checkpoint_limit"
	fieldTablesExclude        = "exclude"
	fieldTablesInclude        = "include"
	fieldCheckpointCache      = "checkpoint_cache"
	fieldCheckpointKey        = "checkpoint_key"
	fieldBatching             = "batching"

	shutdownTimeout = 5 * time.Second
)

func init() {
	service.MustRegisterBatchInput("microsoft_sql_server_cdc", msSQLServerStreamConfigSpec, newMSSQLServerCDCInput)
}

var msSQLServerStreamConfigSpec = service.NewConfigSpec().
	Beta().
	Categories("Services").
	Version("0.0.1").
	Summary("Enables Change Data Capture by consuming from Microsoft SQL Server's change tables.").
	Description(`Streams changes from a Microsoft SQL Server database for Change Data Capture (CDC).
Additionally, if ` + "`" + fieldStreamSnapshot + "`" + ` is set to true, then the existing data in the database is also streamed too.

== Metadata

This input adds the following metadata fields to each message:
- schema (Schema of the table that the message originated from)
- table (Name of the table that the message originated from)
- operation (Type of operation that generated the message: "read", "delete", "insert", or "update_before" and "update_after". "read" is from messages that are read in the initial snapshot phase.)
- lsn (the Log Sequence Number in Microsoft SQL Server)
		`).
	Field(service.NewStringField(fieldConnectionString).
		Description("The connection string of the Microsoft SQL Server database to connect to.").
		Example("sqlserver://username:password@host/instance?param1=value&param2=value"),
	).
	Field(service.NewBoolField(fieldStreamSnapshot).
		Description("If set to true, the connector will query all the existing data as a part of snapshot process. Otherwise, it will start from the current Log Sequence Number position."),
	).
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
		Description("A https://www.docs.redpanda.com/redpanda-connect/components/caches/about[cache resource^] to use for storing the current latest Log Sequence Number (LSN) that has been successfully delivered, this allows Redpanda Connect to continue from that Log Sequence Number (LSN) upon restart, rather than consume the entire state of the change table."),
	).
	Field(service.NewStringField(fieldCheckpointKey).
		Description("The key to use to store the snapshot position in `" + fieldCheckpointCache + "`. An alternative key can be provided if multiple CDC inputs share the same cache.").
		Default("microsoft_sql_server_cdc"),
	).
	Field(service.NewIntField(fieldCheckpointLimit).
		Description("The maximum number of messages that can be processed at a given time. Increasing this limit enables parallel processing and batching at the output level. Any given Log Sequence Number (LSN) will not be acknowledged unless all messages under that offset are delivered in order to preserve at least once delivery guarantees.").
		Default(1024),
	).
	Field(service.NewAutoRetryNacksToggleField()).
	Field(service.NewBatchPolicyField(fieldBatching))

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

type config struct {
	connectionString     string
	streamSnapshot       bool
	snapshotMaxBatchSize int
	tablesFilter         *confx.RegexpFilter
	lsnCache             string
	lsnCacheKey          string
}

type sqlServerCDCInput struct {
	cfg *config
	db  *sql.DB

	res       *service.Resources
	publisher *batchPublisher
	metrics   *service.Metrics

	stopSig *shutdown.Signaller
	log     *service.Logger
}

func newMSSQLServerCDCInput(conf *service.ParsedConfig, resources *service.Resources) (s service.BatchInput, err error) {
	var (
		connectionString             string
		streamSnapshot               bool
		snapshotMaxBatchSize         int
		lsnCache, lsnCacheKey        string
		tableIncludes, tableExcludes []*regexp.Regexp
		batcher                      *service.Batcher
		cp                           *checkpoint.Capped[replication.LSN]
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
	if snapshotMaxBatchSize, err = conf.FieldInt(fieldSnapshotMaxBatchSize); err != nil {
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
	if lsnCache, err = conf.FieldString(fieldCheckpointCache); err != nil {
		return nil, err
	}
	if !conf.Resources().HasCache(lsnCache) {
		return nil, fmt.Errorf("unknown cache resource: %s", lsnCache)
	}
	if lsnCacheKey, err = conf.FieldString(fieldCheckpointKey); err != nil {
		return nil, err
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
			connectionString:     connectionString,
			streamSnapshot:       streamSnapshot,
			snapshotMaxBatchSize: snapshotMaxBatchSize,
			lsnCache:             lsnCache,
			lsnCacheKey:          lsnCacheKey,
			tablesFilter: &confx.RegexpFilter{
				Include: tableIncludes,
				Exclude: tableExcludes,
			},
		},
		res:       resources,
		log:       logger,
		metrics:   resources.Metrics(),
		stopSig:   shutdown.NewSignaller(),
		publisher: newBatchPublisher(batcher, cp, logger),
	}

	i.publisher.cacheLSN = func(ctx context.Context, lsn replication.LSN) error {
		return i.cacheLSN(ctx, lsn)
	}

	batchInput, err := service.AutoRetryNacksBatchedToggled(conf, &i)
	if err != nil {
		return nil, err
	}

	return conf.WrapBatchInputExtractTracingSpanMapping("microsoft_sql_server_cdc", batchInput)
}

func (i *sqlServerCDCInput) Connect(ctx context.Context) error {
	var (
		err        error
		userTables []replication.UserDefinedTable
		cachedLSN  replication.LSN
	)
	if i.db, err = sql.Open("mssql", i.cfg.connectionString); err != nil {
		return fmt.Errorf("failed to connect to microsoft sql server: %s", err)
	}
	if userTables, err = replication.VerifyUserDefinedTables(ctx, i.db, i.cfg.tablesFilter, i.log); err != nil {
		return fmt.Errorf("verifying user defined tables: %w", err)
	}
	if cachedLSN, err = i.getCachedLSN(ctx); err != nil {
		return fmt.Errorf("unable to get cached LSN: %s", err)
	}

	// setup snapshotting and streaming
	var (
		snapshotter *replication.Snapshot
		streaming   *replication.ChangeTableStream
	)
	// no cached LSN means we're not recovering from a restart
	if i.cfg.streamSnapshot && len(cachedLSN) == 0 {
		db, err := sql.Open("mssql", i.cfg.connectionString)
		if err != nil {
			return fmt.Errorf("connecting to microsoft sql server for snapshotting: %s", err)
		}
		snapshotter = replication.NewSnapshot(db, userTables, i.publisher, i.log, i.metrics)
	} else {
		i.log.Infof("Snapshotting disabled, skipping...")
	}

	streaming = replication.NewChangeTableStream(userTables, i.publisher, i.log)

	if i.stopSig == nil {
		i.stopSig = shutdown.NewSignaller()
	}

	go func() {
		softCtx, _ := i.stopSig.SoftStopCtx(context.Background())
		wg, _ := errgroup.WithContext(softCtx)
		wg.Go(func() error {
			var (
				err    error
				maxLSN = cachedLSN
			)

			// snapshot if no LSN exists then store checkpoint once complete
			if snapshotter != nil {
				if maxLSN, err = i.processSnapshot(softCtx, snapshotter); err != nil {
					return fmt.Errorf("processing snapshotting: %w", err)
				}
				if err := i.cacheLSN(softCtx, maxLSN); err != nil {
					return fmt.Errorf("caching LSN after snapshotting: %w", err)
				}
				i.log.Debugf("Cached LSN following snapshot: '%s'", maxLSN)
			}

			// start streaming changes
			if err := streaming.ReadChangeTables(softCtx, i.db, maxLSN); err != nil {
				return fmt.Errorf("streaming from change tables: %w", err)
			}

			return nil
		})

		if err := wg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			i.log.Errorf("Error during Microsoft SQL Server CDC: %s", err)
		} else {
			i.log.Info("Successfully shutdown Microsoft SQL Server CDC stream")
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
	if err := i.res.AccessCache(ctx, i.cfg.lsnCache, func(c service.Cache) {
		cacheVal, cErr = c.Get(ctx, i.cfg.lsnCacheKey)
	}); err != nil {
		return nil, fmt.Errorf("unable to access cache for reading: %w", err)
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
	if err := i.res.AccessCache(ctx, i.cfg.lsnCache, func(c service.Cache) {
		cErr = c.Set(ctx, i.cfg.lsnCacheKey, lsn, nil)
	}); err != nil {
		return fmt.Errorf("unable to access cache for writing: %w", err)
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
	if err = snapshot.Read(ctx, i.cfg.snapshotMaxBatchSize); err != nil {
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
	if i.db != nil {
		return i.db.Close()
	}
	return nil
}
