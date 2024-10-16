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
	Summary("Creates Postgres replication slot for CDC").
	Field(service.NewStringField("host").
		Description("PostgreSQL instance host").
		Example("123.0.0.1")).
	Field(service.NewIntField("port").
		Description("PostgreSQL instance port").
		Example(5432).
		Default(5432)).
	Field(service.NewStringField("user").
		Description("Username with permissions to start replication (RDS superuser)").
		Example("postgres"),
	).
	Field(service.NewStringField("password").
		Description("PostgreSQL database password")).
	Field(service.NewStringField("schema").
		Description("Schema that will be used to create replication")).
	Field(service.NewStringField("database").
		Description("PostgreSQL database name")).
	Field(service.NewStringEnumField("tls", "require", "none").
		Description("Defines whether benthos need to verify (skipinsecure) TLS configuration").
		Example("none").
		Default("none")).
	Field(service.NewBoolField("stream_uncomited").Default(false).Description("Defines whether you want to stream uncomitted messages before receiving commit message from postgres. This may lead to duplicated records after the the connector has been restarted")).
	Field(service.NewStringField("pg_conn_options").Default("")).
	Field(service.NewBoolField("stream_snapshot").
		Description("Set `true` if you want to receive all the data that currently exist in database").
		Example(true).
		Default(false)).
	Field(service.NewFloatField("snapshot_memory_safety_factor").
		Description("Sets amout of memory that can be used to stream snapshot. If affects batch sizes. If we want to use only 25% of the memory available - put 0.25 factor. It will make initial streaming slower, but it will prevent your worker from OOM Kill").
		Example(0.2).
		Default(0.5)).
	Field(service.NewStringEnumField("decoding_plugin", "pgoutput", "wal2json").Description("Specifies which decoding plugin to use when streaming data from PostgreSQL").
		Example("pgoutput").
		Default("pgoutput")).
	Field(service.NewStringListField("tables").
		Example(`
			- my_table
			- my_table_2
		`).
		Description("List of tables we have to create logical replication for")).
	Field(service.NewBoolField("temporary_slot").Default(false)).
	Field(service.NewStringField("slot_name").
		Description("PostgeSQL logical replication slot name. You can create it manually before starting the sync. If not provided will be replaced with a random one").
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
