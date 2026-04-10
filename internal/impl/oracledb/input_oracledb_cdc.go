// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"
	_ "github.com/sijms/go-ora/v2"
	"golang.org/x/sync/errgroup"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/confx"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	ociFieldConnectionString          = "connection_string"
	ociFieldWalletPath                = "wallet_path"
	ociFieldWalletPassword            = "wallet_password"
	ociFieldStreamSnapshot            = "stream_snapshot"
	ociFieldMaxParallelSnapshotTables = "max_parallel_snapshot_tables"
	ociFieldSnapshotMaxBatchSize      = "snapshot_max_batch_size"
	ociFieldTablesExclude             = "exclude"
	ociFieldTablesInclude             = "include"
	ociFieldCheckpointLimit           = "checkpoint_limit"
	ociFieldCheckpointCache           = "checkpoint_cache"
	ociFieldCheckpointCacheKey        = "checkpoint_cache_key"
	ociFieldCheckpointCacheTableName  = "checkpoint_cache_table_name"
	ociFieldBatching                  = "batching"
	ociFieldPDBName                   = "pdb_name"

	shutdownTimeout = 5 * time.Second

	//-- logminer specific
	ociFieldLogMiner             = "logminer"
	ociFieldSCNWindowSize        = "scn_window_size"
	ociFieldBackoffInterval      = "backoff_interval"
	ociFieldMiningInterval       = "mining_interval"
	ociFieldMiningStrategy       = "strategy"
	ociFieldMaxTransactionEvents = "max_transaction_events"
	ociFieldLOBEnabled           = "lob_enabled"
)

func init() {
	service.MustRegisterBatchInput("oracledb_cdc", oracleDBStreamConfigSpec, newOracleDBCDCInput)
}

var oracleDBStreamConfigSpec = service.NewConfigSpec().
	Categories("Services").
	Version("4.83.0").
	Summary("Enables Change Data Capture by consuming from OracleDB.").
	Description(`Streams changes from an Oracle database for Change Data Capture (CDC).
Additionally, if ` + "`" + ociFieldStreamSnapshot + "`" + ` is set to true, then the existing data in the database is also streamed too.

== Metadata

This input adds the following metadata fields to each message:

- database_schema: The database schema for the table where the message originates from.
- table_name: Name of the table that the message originated from.
- operation: Type of operation that generated the message: "read", "delete", "insert", or "update". "read" is from messages that are read in the initial snapshot phase.
- scn: the System Change Number in Oracle.
- schema: The table schema, for use with schema-aware downstream processors such as ` + "`schema_registry_encode`" + `. When new columns are detected in CDC events, the schema is automatically refreshed from the Oracle catalog. Dropped columns are reflected after a connector restart.

== Permissions

When using the default Oracle based cache, the Connect user requires permission to create tables and stored procedures, and the ` + "rpcn" + `  schema must already exist. Refer to ` + "`" + ociFieldCheckpointCacheTableName + "`" + ` for more information.
		`).
	Field(service.NewStringField(ociFieldConnectionString).
		Description("The connection string of the Oracle database to connect to. Additional connection options can be supplied as URL query parameters, for example: `oracle://user:password@host:1522/service?WALLET=/opt/oracle/wallet&SSL=true`.").
		Example("oracle://username:password@host:port/service_name").
		Example("oracle://user:password@host:1522/service?WALLET=/opt/oracle/wallet&SSL=true"),
	).
	Field(service.NewStringField(ociFieldWalletPath).
		Description("Path to the Oracle Wallet directory. When set, SSL is enabled automatically. The directory must contain either `cwallet.sso` (auto-login, no password required) or `ewallet.p12` (requires `wallet_password`).").
		Example("/opt/oracle/wallet").
		Optional(),
	).
	Field(service.NewStringField(ociFieldWalletPassword).
		Secret().
		Description("Password for the `ewallet.p12` PKCS#12 wallet file. Only required when the wallet directory contains `ewallet.p12` rather than `cwallet.sso`.").
		Optional(),
	).
	Field(service.NewBoolField(ociFieldStreamSnapshot).
		Description("If set to true, the connector will query all the existing data as a part of snapshot process. Otherwise, it will start from the current System Change Number position.").
		Example(true).
		Default(false),
	).
	Field(service.NewIntField(ociFieldMaxParallelSnapshotTables).
		Description("Specifies a number of tables that will be processed in parallel during the snapshot processing stage.").
		Default(1)).
	Field(service.NewIntField(ociFieldSnapshotMaxBatchSize).
		Description("The maximum number of rows to be streamed in a single batch when taking a snapshot.").
		Default(1000),
	).
	// logminer config
	Field(service.NewObjectField(ociFieldLogMiner,
		service.NewIntField(ociFieldSCNWindowSize).
			Description("The SCN range to mine per cycle. Each cycle reads changes between the current SCN and current SCN + scn_window_size. Smaller values mean more frequent queries with lower memory usage but higher overhead; larger values reduce query frequency and improve throughput at the cost of higher memory usage per cycle.").
			Default(logminer.DefaultSCNWindowSize),
		service.NewDurationField(ociFieldBackoffInterval).
			Description("The interval between attempts to check for new changes once all data is processed. For low traffic tables increasing this value can reduce network traffic to the server.").
			Default(logminer.DefaultMiningBackoffInterval.String()).
			Example("5s").Example("1m"),
		service.NewDurationField(ociFieldMiningInterval).
			Description("The interval between mining cycles during normal operation. Controls how frequently LogMiner polls for new changes when not caught up.").
			Default(logminer.DefaultMiningInterval.String()).
			Example("100ms").Example("1s"),
		service.NewStringField(ociFieldMiningStrategy).
			Description("Controls how LogMiner retrieves data dictionary information. `online_catalog` (default) uses the current data dictionary for best performance but cannot capture DDL changes. `online_catalog` currently only supported.").
			Default(logminer.DefaultMiningStrategy),
		service.NewIntField(ociFieldMaxTransactionEvents).
			Description("The maximum number of events that can be buffered for a single transaction. If a transaction exceeds this limit it is discarded and its events will not be emitted. Set to 0 to disable the limit.").
			Default(logminer.DefaultMaxTransactionEvents),
		service.NewBoolField(ociFieldLOBEnabled).
			Description("When enabled, large object (CLOB, BLOB) columns are included in both snapshot and streaming change events. When disabled, these columns are still present but contain no values. Enabling this option introduces additional performance overhead and increases memory requirements.").
			Default(logminer.DefaultLOBEnabled),
	).Description("LogMiner configuration settings."),
	).
	Field(service.NewStringListField(ociFieldTablesInclude).
		Description("Regular expressions for tables to include.").
		Example("SCHEMA.PRODUCTS"),
	).
	Field(service.NewStringListField(ociFieldTablesExclude).
		Description("Regular expressions for tables to exclude.").
		Example("SCHEMA.PRIVATETABLE").
		Optional(),
	).
	Field(service.NewStringField(ociFieldCheckpointCache).
		Description("A https://www.docs.redpanda.com/redpanda-connect/components/caches/about[cache resource^] to use for storing the current System Change Number (SCN) that has been successfully delivered, this allows Redpanda Connect to continue from that System Change Number (SCN) upon restart, rather than consume the entire state of OracleDB's redo logs. If not set the default Oracle based cache will be used, see `" + ociFieldCheckpointCacheTableName + "` for more information.").
		Optional(),
	).
	Field(service.NewStringField(ociFieldCheckpointCacheTableName).
		Description("The identifier for the checkpoint cache table name. If no `" + ociFieldCheckpointCache + "` field is specified, this input will automatically create a table and stored procedure under the `rpcn` schema to act as a checkpoint cache. This table stores the latest processed System Change Number (SCN) that has been successfully delivered, allowing Redpanda Connect to resume from that point upon restart rather than reconsume the entire redo log.").
		Default(defaultCheckpointCache).
		Example("RPCN.CHECKPOINT_CACHE").
		Optional(),
	).
	Field(service.NewStringField(ociFieldCheckpointCacheKey).
		Description("The key to use to store the snapshot position in `" + ociFieldCheckpointCache + "`. An alternative key can be provided if multiple CDC inputs share the same cache.").
		Default("oracledb_cdc").
		Optional(),
	).
	Field(service.NewIntField(ociFieldCheckpointLimit).
		Description("The maximum number of messages that can be processed at a given time. Increasing this limit enables parallel processing and batching at the output level. Any given System Change Number (SCN) will not be acknowledged unless all messages under that offset are delivered in order to preserve at least once delivery guarantees.").
		Default(1024),
	).
	Field(service.NewStringField(ociFieldPDBName).
		Description("The name of the pluggable database (PDB) to monitor. When connecting to a CDB root, LogMiner output is scoped to this PDB via SRC_CON_NAME filtering and catalog queries use ALTER SESSION SET CONTAINER to switch context. Requires GRANT SET CONTAINER TO <user> CONTAINER=ALL.").
		Optional(),
	).
	Field(service.NewAutoRetryNacksToggleField()).
	Field(service.NewBatchPolicyField(ociFieldBatching))

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

// Config is the configuration for a Oracle connector.
type Config struct {
	ConnectionString     string
	StreamSnapshot       bool
	SnapshotMaxBatchSize int
	SnapshotMaxWorkers   int
	TablesFilter         *confx.RegexpFilter
	SCNCache             string
	SCNCacheKey          string
	CpCacheTableName     string
	PDBName              string
}

type oracleDBCDCInput struct {
	cfg   Config
	lmCfg *logminer.Config
	db    *sql.DB

	res       *service.Resources
	publisher *batchPublisher
	metrics   *service.Metrics

	stopSig *shutdown.Signaller
	log     *service.Logger
	cpCache service.Cache
}

func newOracleDBCDCInput(conf *service.ParsedConfig, resources *service.Resources) (s service.BatchInput, err error) {
	var (
		connectionString     string
		streamSnapshot       bool
		snapshotMaxWorkers   int
		snapshotMaxBatchSize int

		scnCache, scnCacheKey        string
		tableIncludes, tableExcludes []*regexp.Regexp
		batcher                      *service.Batcher
		cp                           *checkpoint.Capped[replication.SCN]
		cpCache                      service.Cache
		cpCacheTableName             string
		lmCfg                        *logminer.Config

		logger = resources.Logger()
	)

	if err := license.CheckRunningEnterprise(resources); err != nil {
		return nil, err
	}
	if connectionString, err = conf.FieldString(ociFieldConnectionString); err != nil {
		return nil, err
	}
	if streamSnapshot, err = conf.FieldBool(ociFieldStreamSnapshot); err != nil {
		return nil, err
	}
	if snapshotMaxWorkers, err = conf.FieldInt(ociFieldMaxParallelSnapshotTables); err != nil {
		return nil, err
	}
	if snapshotMaxBatchSize, err = conf.FieldInt(ociFieldSnapshotMaxBatchSize); err != nil {
		return nil, err
	}
	if lmCfg, err = parseLogMinerConfig(conf); err != nil {
		return nil, err
	}

	// tables
	if includes, err := conf.FieldStringList(ociFieldTablesInclude); err != nil {
		return nil, err
	} else if tableIncludes, err = confx.ParseRegexpPatterns(includes); err != nil {
		return nil, err
	}
	if excludes, err := conf.FieldStringList(ociFieldTablesExclude); err != nil {
		return nil, err
	} else if tableExcludes, err = confx.ParseRegexpPatterns(excludes); err != nil {
		return nil, err
	}

	// cache
	// if no cache component is specified then we fall back to default SQL based version
	if conf.Contains(ociFieldCheckpointCache) {
		if scnCache, err = conf.FieldString(ociFieldCheckpointCache); err != nil {
			return nil, err
		}
		if conf.Resources().HasCache(scnCache) {
			if scnCacheKey, err = conf.FieldString(ociFieldCheckpointCacheKey); err != nil {
				return nil, err
			}
		}
	}

	if cpCacheTableName, err = conf.FieldString(ociFieldCheckpointCacheTableName); err != nil {
		return nil, err
	}

	var pdbName string
	if conf.Contains(ociFieldPDBName) {
		if pdbName, err = conf.FieldString(ociFieldPDBName); err != nil {
			return nil, err
		}
		if pdbName != "" && !validOracleIdentifier.MatchString(pdbName) {
			return nil, fmt.Errorf("invalid pdb_name %q: must be a valid Oracle identifier (letters, digits, _ $ # — starting with a letter)", pdbName)
		}
		if pdbName != "" && cpCacheTableName == defaultCheckpointCache {
			cpCacheTableName = "RPCN.CDC_CHECKPOINT_" + strings.ToUpper(pdbName)
		}
		lmCfg.PDBName = pdbName
	}

	// checkpointing
	var checkpointLimit int
	if checkpointLimit, err = conf.FieldInt(ociFieldCheckpointLimit); err != nil {
		return nil, err
	}
	cp = checkpoint.NewCapped[replication.SCN](int64(checkpointLimit))

	// batching
	var policy service.BatchPolicy
	if policy, err = conf.FieldBatchPolicy(ociFieldBatching); err != nil {
		return nil, err
	} else if policy.IsNoop() {
		policy.Count = 1
	}
	if batcher, err = policy.NewBatcher(resources); err != nil {
		return nil, err
	}

	// connecting string flags
	overrides := make(map[string]string)
	if err := parseWalletConfig(conf, overrides); err != nil {
		return nil, fmt.Errorf("parsing oracle wallet config: %w", err)
	}

	if connectionString, err = buildConnectionString(connectionString, overrides, logger); err != nil {
		return nil, fmt.Errorf("building connection string: %w", err)
	}

	o := oracleDBCDCInput{
		cfg: Config{
			ConnectionString:     connectionString,
			StreamSnapshot:       streamSnapshot,
			SnapshotMaxWorkers:   snapshotMaxWorkers,
			SnapshotMaxBatchSize: snapshotMaxBatchSize,
			SCNCache:             scnCache,
			SCNCacheKey:          scnCacheKey,
			CpCacheTableName:     cpCacheTableName,
			PDBName:              pdbName,
			TablesFilter: &confx.RegexpFilter{
				Include: tableIncludes,
				Exclude: tableExcludes,
			},
		},
		lmCfg:     lmCfg,
		res:       resources,
		log:       logger,
		metrics:   resources.Metrics(),
		stopSig:   shutdown.NewSignaller(),
		publisher: newBatchPublisher(batcher, cp, logger),
		cpCache:   cpCache,
	}

	defer func() {
		if err != nil {
			o.publisher.Close()
		}
	}()

	o.publisher.cacheSCN = o.cacheSCN

	// Has stopped is how we notify that we're not connected. This will get reset at connection time.
	o.stopSig.TriggerHasStopped()

	batchInput, err := service.AutoRetryNacksBatchedToggled(conf, &o)
	if err != nil {
		return nil, err
	}

	return conf.WrapBatchInputExtractTracingSpanMapping("oracledb_cdc", batchInput)
}

func (o *oracleDBCDCInput) Connect(ctx context.Context) (resErr error) {
	var (
		userTables []replication.UserTable
		cachedSCN  replication.SCN
		err        error
		isCDB      bool
	)
	if o.db != nil {
		_ = o.db.Close()
		o.db = nil
	}

	if o.db, err = sql.Open("oracle", o.cfg.ConnectionString); err != nil {
		return fmt.Errorf("connecting to oracle database: %w", err)
	}
	defer func() {
		if resErr != nil {
			_ = o.db.Close()
		}
	}()

	if err = o.db.PingContext(ctx); err != nil {
		return fmt.Errorf("validating connection to oracle database: %w", err)
	}

	if isCDB, err = o.detectContainerContext(ctx); err != nil {
		return fmt.Errorf("detecting current container context: %w", err)
	}

	// In CDB mode the auto-derived checkpoint table uses the common-user prefix C##RPCN.
	// At parse time we don't yet know if we're in CDB mode, so fix up the name here.
	cpCacheTable := o.cfg.CpCacheTableName
	if isCDB {
		cpCacheTable = cdbCheckpointTable(cpCacheTable)
	}

	// no cache specified so use default, internal oracle based cache
	if o.cfg.SCNCache == "" && o.cpCache == nil {
		c, err := newCheckpointCache(ctx, o.cfg.ConnectionString, cpCacheTable, o.log)
		if err != nil {
			return fmt.Errorf("initialising oracle based checkpoint cache: %w", err)
		}
		o.cpCache = c
	}

	// For CDB mode, run VerifyUserTables on a dedicated connection switched to the PDB
	// (ALTER SESSION SET CONTAINER + ALL_* views, no CDB_* view privileges needed).
	if isCDB {
		if err = func() error {
			conn, err := o.db.Conn(ctx)
			if err != nil {
				return fmt.Errorf("getting connection for PDB '%s': %w", o.cfg.PDBName, err)
			}
			defer func() {
				if err := conn.Close(); err != nil {
					o.log.Errorf("Closing connection following table verification: %v", err)
				}
			}()

			if _, err = conn.ExecContext(ctx, "ALTER SESSION SET CONTAINER = "+o.cfg.PDBName); err != nil {
				return fmt.Errorf("switching session to PDB '%s' for user table verification: %w", o.cfg.PDBName, err)
			}
			defer func() {
				if _, resetErr := conn.ExecContext(context.Background(), "ALTER SESSION SET CONTAINER = CDB$ROOT"); resetErr != nil {
					o.log.Errorf("Failed to reset session to root container after user table verification: %v", resetErr)
				}
			}()

			if userTables, err = replication.VerifyUserTables(ctx, conn, o.cfg.TablesFilter, o.log); err != nil {
				return fmt.Errorf("verifying user defined tables: %w", err)
			}
			return nil
		}(); err != nil {
			return err
		}
	} else {
		if userTables, err = replication.VerifyUserTables(ctx, o.db, o.cfg.TablesFilter, o.log); err != nil {
			return fmt.Errorf("verifying user defined tables: %w", err)
		}
	}

	// Pre-fetch schemas for all monitored tables. A fresh cache is created on every Connect()
	// so reconnections always reflect the current catalog state. The schemaCache handles its
	// own container switching internally for CDB mode cache misses.
	var pdbNameForCache string
	if isCDB {
		pdbNameForCache = o.cfg.PDBName
	}
	schemas := newSchemaCache(o.db, pdbNameForCache, o.log)
	for _, t := range userTables {
		if _, _, err := schemas.schemaForEvent(ctx, t, nil); err != nil {
			o.log.Warnf("Failed to pre-fetch schema for %s.%s: %v", t.Schema, t.Name, err)
		}
	}
	o.publisher.schemas = schemas

	if cachedSCN, err = o.getCachedSCN(ctx); err != nil {
		if errors.Is(err, service.ErrKeyNotFound) {
			o.log.Infof("No SCN found in checkpoint cache")
			cachedSCN = replication.InvalidSCN
		} else {
			return fmt.Errorf("getting cached SCN: %w", err)
		}
	} else {
		switch {
		case cachedSCN != replication.InvalidSCN:
			o.log.Infof("Resuming from cached SCN value: %d", cachedSCN)
		default:
			// this is an edgecase, but re-snapshotting is the best solution here if/should this state be possible.
			return errors.New("unable to restore SCN from cache, consider clearing checkpoint cache and running snapshot to avoid missing data")
		}
	}

	// setup snapshotting and streaming

	type streamProcessor interface {
		FindStartPos(ctx context.Context) (replication.SCN, error)
		ReadChanges(ctx context.Context, startPos replication.SCN) error
	}
	var (
		snapshotter *replication.Snapshot
		// logminer processor
		streaming streamProcessor
	)

	// no cached SCN means we're not recovering from a restart
	if o.cfg.StreamSnapshot && cachedSCN == replication.InvalidSCN {
		if snapshotter, err = replication.NewSnapshot(ctx, o.cfg.ConnectionString, userTables, o.publisher, o.lmCfg.LOBEnabled, pdbNameForCache, o.log, o.metrics); err != nil {
			return fmt.Errorf("creating database snapshotter: %w", err)
		}
		defer func() {
			if err != nil {
				_ = snapshotter.Close()
			}
		}()
	} else {
		o.log.Infof("Snapshotting disabled, skipping...")
	}

	if o.lmCfg != nil {
		streaming = logminer.NewMiner(o.db, userTables, o.publisher, o.lmCfg, o.metrics, o.log)
	} else {
		return errors.New("logminer configuration required for streaming")
	}

	// Reset our stop signal
	o.stopSig = shutdown.NewSignaller()

	go func() {
		var (
			err    error
			maxSCN = cachedSCN
		)
		softCtx, _ := o.stopSig.SoftStopCtx(context.Background())

		// snapshot if no SCN exists then store checkpoint once complete
		if snapshotter != nil {
			if maxSCN, err = o.processSnapshot(softCtx, snapshotter); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					o.log.Infof("Snapshotting stopped: %s", err)
				} else {
					o.log.Errorf("Snapshotting failed: %s", err)
				}
				o.stopSig.TriggerHasStopped()
				return
			}

			if err = o.cacheSCN(softCtx, maxSCN); err != nil {
				o.log.Errorf("Failed to capture SCN after snapshot completion. Snapshot will re-run on restart (may cause duplicate data): %s", err)
				o.stopSig.TriggerHasStopped()
				return
			}

			o.log.Infof("Successfully captured SCN following snapshot: %d", maxSCN)
		}

		// If no SCN is available (no snapshot and no cached position), so get the start position from the DB
		if maxSCN == replication.InvalidSCN {
			if maxSCN, err = streaming.FindStartPos(softCtx); err != nil {
				o.log.Errorf("Failed to get start SCN from database: %s", err)
				o.stopSig.TriggerHasStopped()
				return
			}
			o.log.Infof("No cached SCN found, fetched starting position from database: %d", maxSCN)
			if err = o.cacheSCN(softCtx, maxSCN); err != nil {
				o.log.Warnf("Failed to cache initial SCN (non-critical): %s", err)
			}
		}

		// streaming
		wg, _ := errgroup.WithContext(softCtx)
		wg.Go(func() error {
			if err := streaming.ReadChanges(softCtx, maxSCN); err != nil {
				return fmt.Errorf("streaming from logminer: %w", err)
			}
			return nil
		})
		if err := wg.Wait(); err != nil && softCtx.Err() == nil && !errors.Is(err, context.Canceled) {
			o.log.Errorf("Error during Oracle CDC Component: %s", err)
		} else {
			o.log.Info("Successfully shutdown Oracle CDC Component")
		}
		o.stopSig.TriggerHasStopped()
	}()

	return nil
}

func (o *oracleDBCDCInput) getCachedSCN(ctx context.Context) (replication.SCN, error) {
	var (
		cacheVal []byte
		cErr     error
	)

	// Use internal Oracle-based cache if set (when no external cache configured),
	// otherwise use external cache resource
	if o.cpCache != nil {
		cacheVal, cErr = o.cpCache.Get(ctx, o.cfg.SCNCacheKey)
	} else {
		if err := o.res.AccessCache(ctx, o.cfg.SCNCache, func(c service.Cache) {
			cacheVal, cErr = c.Get(ctx, o.cfg.SCNCacheKey)
		}); err != nil {
			return replication.InvalidSCN, fmt.Errorf("accessing cache for reading: %w", err)
		}
	}

	if errors.Is(cErr, service.ErrKeyNotFound) {
		return replication.InvalidSCN, service.ErrKeyNotFound
	} else if cErr != nil {
		return replication.InvalidSCN, fmt.Errorf("reading checkpoint from cache: %w", cErr)
	} else if len(cacheVal) == 0 {
		return replication.InvalidSCN, errors.New("empty SCN cache value")
	}

	scn, err := replication.SCNFromBytes(cacheVal)
	if err != nil {
		return replication.InvalidSCN, fmt.Errorf("parsing SCN from cache: %w", err)
	}
	return scn, nil
}

func (o *oracleDBCDCInput) cacheSCN(ctx context.Context, scn replication.SCN) error {
	if scn == replication.InvalidSCN {
		return errors.New("SCN for caching is empty")
	}

	// Use internal Oracle-based cache if set (when no external cache configured),
	// otherwise use external cache resource
	var cErr error
	if o.cpCache != nil {
		cErr = o.cpCache.Set(ctx, o.cfg.SCNCacheKey, scn.Bytes(), nil)
	} else {
		if err := o.res.AccessCache(ctx, o.cfg.SCNCache, func(c service.Cache) {
			cErr = c.Set(ctx, o.cfg.SCNCacheKey, scn.Bytes(), nil)
		}); err != nil {
			return fmt.Errorf("accessing cache for writing: %w", err)
		}
	}

	if cErr != nil {
		return fmt.Errorf("persisting checkpoint to cache: %w", cErr)
	}
	return nil
}

func (o *oracleDBCDCInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case m := <-o.publisher.msgs():
		return m.msg, m.ackFn, nil
	case <-o.stopSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (o *oracleDBCDCInput) processSnapshot(ctx context.Context, snapshot *replication.Snapshot) (replication.SCN, error) {
	var (
		scn replication.SCN
		err error
	)
	if scn, err = snapshot.Prepare(ctx); err != nil {
		_ = snapshot.Close()
		return replication.InvalidSCN, fmt.Errorf("preparing snapshot: %w", err)
	}
	if err = snapshot.Read(ctx, o.cfg.SnapshotMaxWorkers, o.cfg.SnapshotMaxBatchSize); err != nil {
		_ = snapshot.Close()
		return replication.InvalidSCN, fmt.Errorf("reading snapshot: %w", err)
	}
	if err = snapshot.Close(); err != nil {
		return replication.InvalidSCN, fmt.Errorf("closing snapshot connections: %w", err)
	}
	o.log.Infof("Completed running snapshot process")

	return scn, nil
}

func (o *oracleDBCDCInput) Close(ctx context.Context) error {
	if o.stopSig == nil {
		return nil // Never connected
	}
	o.stopSig.TriggerSoftStop()
	select {
	case <-ctx.Done():
	case <-time.After(shutdownTimeout):
	case <-o.stopSig.HasStoppedChan():
	}

	o.stopSig.TriggerHardStop()
	select {
	case <-ctx.Done():
	case <-time.After(shutdownTimeout):
		o.log.Error("failed to shutdown 'oracledb_cdc' component within the timeout")
	case <-o.stopSig.HasStoppedChan():
	}

	if o.publisher != nil {
		o.publisher.Close()
	}

	// Close both resources and combine errors to avoid resource leaks
	var closeErr error
	if o.cpCache != nil {
		if err := o.cpCache.Close(ctx); err != nil {
			closeErr = fmt.Errorf("closing checkpoint cache: %w", err)
		}
	}
	if o.db != nil {
		if err := o.db.Close(); err != nil {
			if closeErr != nil {
				closeErr = fmt.Errorf("%w; closing database: %w", closeErr, err)
			} else {
				closeErr = fmt.Errorf("closing database: %w", err)
			}
		}
	}
	return closeErr
}

func parseLogMinerConfig(conf *service.ParsedConfig) (*logminer.Config, error) {
	var (
		err error
		cfg *logminer.Config
	)
	if conf.Contains(ociFieldLogMiner) {
		lmConf := conf.Namespace(ociFieldLogMiner)
		cfg = logminer.NewDefaultConfig()
		if cfg.SCNWindowSize, err = lmConf.FieldInt(ociFieldSCNWindowSize); err != nil {
			return nil, err
		}
		if cfg.SCNWindowSize <= 0 {
			return nil, fmt.Errorf("logminer.%s must be greater than 0, got %d", ociFieldSCNWindowSize, cfg.SCNWindowSize)
		}
		if cfg.MiningBackoffInterval, err = lmConf.FieldDuration(ociFieldBackoffInterval); err != nil {
			return nil, err
		}
		if cfg.MiningInterval, err = lmConf.FieldDuration(ociFieldMiningInterval); err != nil {
			return nil, err
		}
		if strategy, err := lmConf.FieldString(ociFieldMiningStrategy); err != nil {
			return nil, err
		} else {
			cfg.MiningStrategy = logminer.MiningStrategy(strategy)
		}
		if cfg.MaxTransactionEvents, err = lmConf.FieldInt(ociFieldMaxTransactionEvents); err != nil {
			return nil, err
		}
		if cfg.MaxTransactionEvents < 0 {
			return nil, fmt.Errorf("logminer.%s must be greater than or equal to 0, got %d", ociFieldMaxTransactionEvents, cfg.MaxTransactionEvents)
		}
		if cfg.LOBEnabled, err = lmConf.FieldBool(ociFieldLOBEnabled); err != nil {
			return nil, err
		}
	}

	return cfg, nil
}
