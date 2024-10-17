// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pgstream

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lucasepe/codename"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream"
)

var randomSlotName string

var pgStreamConfigSpec = service.NewConfigSpec().
	Beta().
	Categories("Services").
	Version("0.0.1").
	Summary(`Creates a PostgreSQL replication slot for Change Data Capture (CDC)
		== Metadata

This input adds the following metadata fields to each message:
- streaming (Indicates whether the message is part of a streaming operation or snapshot processing)
- table (Name of the table that the message originated from)
- operation (Type of operation that generated the message, such as INSERT, UPDATE, or DELETE)
		`).
	Field(service.NewStringField("host").
		Description("The hostname or IP address of the PostgreSQL instance.").
		Example("123.0.0.1")).
	Field(service.NewIntField("port").
		Description("The port number on which the PostgreSQL instance is listening.").
		Example(5432).
		Default(5432)).
	Field(service.NewStringField("user").
		Description("Username of a user with replication permissions. For AWS RDS, this typically requires superuser privileges.").
		Example("postgres"),
	).
	Field(service.NewStringField("password").
		Description("Password for the specified PostgreSQL user.")).
	Field(service.NewStringField("schema").
		Description("The PostgreSQL schema from which to replicate data.")).
	Field(service.NewStringField("database").
		Description("The name of the PostgreSQL database to connect to.")).
	Field(service.NewStringEnumField("tls", "require", "none").
		Description("Specifies whether to use TLS for the database connection. Set to 'require' to enforce TLS, or 'none' to disable it.").
		Example("none").
		Default("none")).
	Field(service.NewBoolField("stream_uncomited").
		Description("If set to true, the plugin will stream uncommitted transactions before receiving a commit message from PostgreSQL. This may result in duplicate records if the connector is restarted.").
		Default(false)).
	Field(service.NewStringField("pg_conn_options").
		Description("Additional PostgreSQL connection options as a string. Refer to PostgreSQL documentation for available options.").
		Default(""),
	).
	Field(service.NewBoolField("stream_snapshot").
		Description("When set to true, the plugin will first stream a snapshot of all existing data in the database before streaming changes.").
		Example(true).
		Default(false)).
	Field(service.NewFloatField("snapshot_memory_safety_factor").
		Description("Determines the fraction of available memory that can be used for streaming the snapshot. Values between 0 and 1 represent the percentage of memory to use. Lower values make initial streaming slower but help prevent out-of-memory errors.").
		Example(0.2).
		Default(1)).
	Field(service.NewIntField("snapshot_batch_size").
		Description("The number of rows to fetch in each batch when querying the snapshot. A value of 0 lets the plugin determine the batch size based on `snapshot_memory_safety_factor` property.").
		Example(10000).
		Default(0)).
	Field(service.NewStringEnumField("decoding_plugin", "pgoutput", "wal2json").
		Description("Specifies the logical decoding plugin to use for streaming changes from PostgreSQL. 'pgoutput' is the native logical replication protocol, while 'wal2json' provides change data as JSON.").
		Example("pgoutput").
		Default("pgoutput")).
	Field(service.NewStringListField("tables").
		Description("A list of table names to include in the logical replication. Each table should be specified as a separate item.").
		Example(`
			- my_table
			- my_table_2
		`)).
	Field(service.NewBoolField("temporary_slot").
		Description("If set to true, creates a temporary replication slot that is automatically dropped when the connection is closed.").
		Default(false)).
	Field(service.NewStringField("slot_name").
		Description("The name of the PostgreSQL logical replication slot to use. If not provided, a random name will be generated. You can create this slot manually before starting replication if desired.").
		Example("my_test_slot").
		Default(randomSlotName))

func newPgStreamInput(conf *service.ParsedConfig, logger *service.Logger, metrics *service.Metrics) (s service.Input, err error) {
	var (
		dbName                  string
		dbPort                  int
		dbHost                  string
		dbSchema                string
		dbUser                  string
		dbPassword              string
		dbSlotName              string
		temporarySlot           bool
		tlsSetting              string
		tables                  []string
		streamSnapshot          bool
		snapshotMemSafetyFactor float64
		decodingPlugin          string
		pgConnOptions           string
		streamUncomited         bool
		snapshotBatchSize       int
	)

	dbSchema, err = conf.FieldString("schema")
	if err != nil {
		return nil, err
	}

	dbSlotName, err = conf.FieldString("slot_name")
	if err != nil {
		return nil, err
	}

	temporarySlot, err = conf.FieldBool("temporary_slot")
	if err != nil {
		return nil, err
	}

	if dbSlotName == "" {
		dbSlotName = randomSlotName
	}

	dbPassword, err = conf.FieldString("password")
	if err != nil {
		return nil, err
	}

	dbUser, err = conf.FieldString("user")
	if err != nil {
		return nil, err
	}

	tlsSetting, err = conf.FieldString("tls")
	if err != nil {
		return nil, err
	}

	dbName, err = conf.FieldString("database")
	if err != nil {
		return nil, err
	}

	dbHost, err = conf.FieldString("host")
	if err != nil {
		return nil, err
	}

	dbPort, err = conf.FieldInt("port")
	if err != nil {
		return nil, err
	}

	tables, err = conf.FieldStringList("tables")
	if err != nil {
		return nil, err
	}

	streamSnapshot, err = conf.FieldBool("stream_snapshot")
	if err != nil {
		return nil, err
	}

	streamUncomited, err = conf.FieldBool("stream_uncomited")
	if err != nil {
		return nil, err
	}

	decodingPlugin, err = conf.FieldString("decoding_plugin")
	if err != nil {
		return nil, err
	}

	snapshotMemSafetyFactor, err = conf.FieldFloat("snapshot_memory_safety_factor")
	if err != nil {
		return nil, err
	}

	snapshotBatchSize, err = conf.FieldInt("snapshot_batch_size")
	if err != nil {
		return nil, err
	}

	if pgConnOptions, err = conf.FieldString("pg_conn_options"); err != nil {
		return nil, err
	}

	if pgConnOptions != "" {
		pgConnOptions = "options=" + pgConnOptions
	}

	pgconnConfig := pgconn.Config{
		Host:     dbHost,
		Port:     uint16(dbPort),
		Database: dbName,
		User:     dbUser,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		Password: dbPassword,
	}

	if tlsSetting == "none" {
		pgconnConfig.TLSConfig = nil
	}

	snapsotMetrics := metrics.NewGauge("snapshot_progress")
	replicationLag := metrics.NewGauge("replication_lag")

	return service.AutoRetryNacks(&pgStreamInput{
		dbConfig:                pgconnConfig,
		streamSnapshot:          streamSnapshot,
		snapshotMemSafetyFactor: snapshotMemSafetyFactor,
		slotName:                dbSlotName,
		schema:                  dbSchema,
		pgConnRuntimeParam:      pgConnOptions,
		tls:                     pglogicalstream.TLSVerify(tlsSetting),
		tables:                  tables,
		decodingPlugin:          decodingPlugin,
		streamUncomited:         streamUncomited,
		temporarySlot:           temporarySlot,
		snapshotBatchSize:       snapshotBatchSize,

		logger:          logger,
		metrics:         metrics,
		snapshotMetrics: snapsotMetrics,
		replicationLag:  replicationLag,
	}), err
}

func init() {
	rng, _ := codename.DefaultRNG()
	randomSlotName = strings.ReplaceAll(codename.Generate(rng, 5), "-", "_")

	err := service.RegisterInput(
		"pg_stream", pgStreamConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newPgStreamInput(conf, mgr.Logger(), mgr.Metrics())
		})
	if err != nil {
		panic(err)
	}
}

type pgStreamInput struct {
	dbConfig                pgconn.Config
	tls                     pglogicalstream.TLSVerify
	pglogicalStream         *pglogicalstream.Stream
	pgConnRuntimeParam      string
	slotName                string
	temporarySlot           bool
	schema                  string
	tables                  []string
	decodingPlugin          string
	streamSnapshot          bool
	snapshotMemSafetyFactor float64
	snapshotBatchSize       int
	streamUncomited         bool
	logger                  *service.Logger
	metrics                 *service.Metrics
	metricsTicker           *time.Ticker

	snapshotMetrics *service.MetricGauge
	replicationLag  *service.MetricGauge
}

func (p *pgStreamInput) Connect(ctx context.Context) error {
	pgStream, err := pglogicalstream.NewPgStream(pglogicalstream.Config{
		PgConnRuntimeParam:         p.pgConnRuntimeParam,
		DBHost:                     p.dbConfig.Host,
		DBPassword:                 p.dbConfig.Password,
		DBUser:                     p.dbConfig.User,
		DBPort:                     int(p.dbConfig.Port),
		DBTables:                   p.tables,
		DBName:                     p.dbConfig.Database,
		DBSchema:                   p.schema,
		ReplicationSlotName:        "rs_" + p.slotName,
		TLSVerify:                  p.tls,
		BatchSize:                  p.snapshotBatchSize,
		StreamOldData:              p.streamSnapshot,
		TemporaryReplicationSlot:   p.temporarySlot,
		StreamUncomited:            p.streamUncomited,
		DecodingPlugin:             p.decodingPlugin,
		SnapshotMemorySafetyFactor: p.snapshotMemSafetyFactor,
	})
	if err != nil {
		return err
	}

	p.metricsTicker = time.NewTicker(5 * time.Second)
	p.pglogicalStream = pgStream

	return err
}

func (p *pgStreamInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {

	select {
	case snapshotMessage := <-p.pglogicalStream.SnapshotMessageC():
		var (
			mb  []byte
			err error
		)
		if mb, err = json.Marshal(snapshotMessage); err != nil {
			return nil, nil, err
		}

		connectMessage := service.NewMessage(mb)
		connectMessage.MetaSet("streaming", "false")
		connectMessage.MetaSet("table", snapshotMessage.Changes[0].Table)
		connectMessage.MetaSet("operation", snapshotMessage.Changes[0].Operation)
		if snapshotMessage.Changes[0].TableSnapshotProgress != nil {
			p.snapshotMetrics.SetFloat64(*snapshotMessage.Changes[0].TableSnapshotProgress, snapshotMessage.Changes[0].Table)
		}

		return connectMessage, func(ctx context.Context, err error) error {
			// Nacks are retried automatically when we use service.AutoRetryNacks
			return nil
		}, nil
	case message := <-p.pglogicalStream.LrMessageC():
		var (
			mb  []byte
			err error
		)
		if mb, err = json.Marshal(message); err != nil {
			return nil, nil, err
		}
		connectMessage := service.NewMessage(mb)
		connectMessage.MetaSet("streaming", "true")
		connectMessage.MetaSet("table", message.Changes[0].Table)
		connectMessage.MetaSet("operation", message.Changes[0].Operation)
		if message.WALLagBytes != nil {
			p.replicationLag.Set(*message.WALLagBytes)
		}

		return connectMessage, func(ctx context.Context, err error) error {
			if message.Lsn != nil {
				if err := p.pglogicalStream.AckLSN(*message.Lsn); err != nil {
					return err
				}
				if p.streamUncomited {
					p.pglogicalStream.ConsumedCallback() <- true
				}
			}
			return nil
		}, nil
	case <-ctx.Done():
		return nil, nil, p.pglogicalStream.Stop()
	}
}

func (p *pgStreamInput) Close(ctx context.Context) error {
	if p.pglogicalStream != nil {
		return p.pglogicalStream.Stop()
	}
	return nil
}
