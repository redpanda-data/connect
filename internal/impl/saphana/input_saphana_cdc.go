// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package saphana

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SAP/go-hdb/driver"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/saphana/replication"
)

func init() {
	service.MustRegisterBatchInput("saphana_cdc", hanaCDCConfigSpec(), newHanaCDCInput)
}

func hanaCDCConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("4.96.0").
		Categories("Services").
		Summary("Consumes change data capture (CDC) events from SAP HANA using trigger-based change tables.").
		Description(`
Monitors tables by installing AFTER INSERT/UPDATE/DELETE triggers that write to a
shared _RPCN_CDC.CHANGES table. Emits Debezium-format events with these metadata fields:

- `+"`saphana_schema`"+`: source schema name
- `+"`saphana_table`"+`: source table name
- `+"`saphana_op`"+`: Debezium op code (c/u/d/r/hb)
- `+"`saphana_lsn`"+`: change sequence number (empty for snapshot rows)
- `+"`saphana_ts_ms`"+`: source timestamp in milliseconds
`).
		Fields(
			service.NewStringField(fieldDSN).
				Description("go-hdb DSN, e.g. hdb://USER:PASS@host:39015?databaseName=HXE").
				Secret().
				Example("hdb://SYSTEM:HXEHana1@localhost:39015?databaseName=HXE"),
			service.NewStringListField(fieldTables).
				Description("Tables to monitor in SCHEMA.TABLE format.").
				Example([]string{"HR.EMPLOYEES", "SALES.ORDERS"}),
			service.NewStringEnumField(fieldSnapshotMode, "initial", "never", "always").
				Description("initial: snapshot once on first run; never: streaming only; always: re-snapshot every start.").
				Default("initial"),
			service.NewStringField(fieldCheckpointTable).
				Description("SCHEMA.TABLE for the durable LSN checkpoint.").
				Default("_RPCN_CDC.CHECKPOINT").
				Advanced(),
			service.NewStringField(fieldCheckpointKey).
				Description("Unique key within the checkpoint table for this connector instance.").
				Default("saphana_cdc").
				Advanced(),
			service.NewIntField(fieldPollBatchSize).
				Description("Rows fetched per poll cycle.").
				Default(2048).
				Advanced(),
			service.NewDurationField(fieldHeartbeatInterval).
				Description("Emit a heartbeat after this idle duration.").
				Default("30s").
				Advanced(),
			service.NewDurationField(fieldMinBackoff).
				Description("Initial poll backoff when no changes are found.").
				Default("100ms").
				Advanced(),
			service.NewDurationField(fieldMaxBackoff).
				Description("Maximum poll backoff.").
				Default("5s").
				Advanced(),
			service.NewAutoRetryNacksToggleField(),
			service.NewBatchPolicyField("batching"),
		)
}

const (
	fieldDSN               = "dsn"
	fieldTables            = "tables"
	fieldSnapshotMode      = "snapshot_mode"
	fieldCheckpointTable   = "checkpoint_table"
	fieldCheckpointKey     = "checkpoint_key"
	fieldPollBatchSize     = "poll_batch_size"
	fieldHeartbeatInterval = "heartbeat_interval"
	fieldMinBackoff        = "min_backoff"
	fieldMaxBackoff        = "max_backoff"
)

type asyncMsg struct {
	batch service.MessageBatch
	ackFn service.AckFunc
}

type hanaCDCInput struct {
	dsn          string
	tables       []string
	snapshotMode string
	cpCfg        CheckpointCacheConfig
	cpKey        string
	streamCfg    replication.StreamConfig

	db      *sql.DB
	cp      *CheckpointCache
	schemas *SchemaCache
	stream  *replication.Stream

	msgCh  chan asyncMsg
	stopCh chan struct{}
	doneCh chan struct{}
	once   sync.Once
	mu     sync.Mutex

	log *service.Logger
}

func newHanaCDCInput(conf *service.ParsedConfig, res *service.Resources) (service.BatchInput, error) {
	dsn, err := conf.FieldString(fieldDSN)
	if err != nil {
		return nil, err
	}
	tables, err := conf.FieldStringList(fieldTables)
	if err != nil {
		return nil, err
	}
	snapshotMode, err := conf.FieldString(fieldSnapshotMode)
	if err != nil {
		return nil, err
	}
	cpTable, err := conf.FieldString(fieldCheckpointTable)
	if err != nil {
		return nil, err
	}
	cpKey, err := conf.FieldString(fieldCheckpointKey)
	if err != nil {
		return nil, err
	}
	pollBatch, err := conf.FieldInt(fieldPollBatchSize)
	if err != nil {
		return nil, err
	}
	hbInterval, err := conf.FieldDuration(fieldHeartbeatInterval)
	if err != nil {
		return nil, err
	}
	minBackoff, err := conf.FieldDuration(fieldMinBackoff)
	if err != nil {
		return nil, err
	}
	maxBackoff, err := conf.FieldDuration(fieldMaxBackoff)
	if err != nil {
		return nil, err
	}
	cpCfg, err := NewCheckpointCacheConfig(cpTable)
	if err != nil {
		return nil, fmt.Errorf("invalid checkpoint_table: %w", err)
	}
	return &hanaCDCInput{
		dsn:          dsn,
		tables:       tables,
		snapshotMode: snapshotMode,
		cpCfg:        cpCfg,
		cpKey:        cpKey,
		streamCfg: replication.StreamConfig{
			PollBatchSize:     pollBatch,
			HeartbeatInterval: hbInterval,
			MinBackoff:        minBackoff,
			MaxBackoff:        maxBackoff,
		},
		msgCh:  make(chan asyncMsg, 4096),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
		log:    res.Logger(),
	}, nil
}

func (h *hanaCDCInput) Connect(ctx context.Context) error {
	// Hold mu for the entire setup so that every field write is visible to
	// concurrent callers (ReadBatch, Close) before the goroutine is started.
	h.mu.Lock()
	defer h.mu.Unlock()

	// Reset channels for reconnect safety (guard against double-start).
	h.stopCh = make(chan struct{})
	h.doneCh = make(chan struct{})
	h.once = sync.Once{} // reset close guard

	conn, err := driver.NewDSNConnector(h.dsn)
	if err != nil {
		return fmt.Errorf("building HANA connector: %w", err)
	}
	conn.SetTimeout(60 * time.Second)
	h.db = sql.OpenDB(conn)
	if err := h.db.PingContext(ctx); err != nil {
		return fmt.Errorf("pinging HANA: %w", err)
	}
	h.schemas = NewSchemaCache(h.db)
	h.cp, err = NewCheckpointCache(ctx, h.cpCfg, h.db, h.cpKey)
	if err != nil {
		return fmt.Errorf("initialising checkpoint cache: %w", err)
	}
	lastPos, err := h.cp.Load(ctx)
	if err != nil {
		return fmt.Errorf("loading checkpoint: %w", err)
	}

	// Install CDC infrastructure once, then install triggers per table.
	if err := replication.SetupCDCInfrastructure(ctx, h.db); err != nil {
		return fmt.Errorf("setting up CDC infrastructure: %w", err)
	}
	for _, qt := range h.tables {
		schema, table, err := SplitQualTable(qt)
		if err != nil {
			return err
		}
		cols, pks, err := h.schemas.ForTable(ctx, schema, table)
		if err != nil {
			return fmt.Errorf("fetching schema for %s: %w", qt, err)
		}
		if err := replication.SetupCDC(ctx, h.db, schema, table, ColumnNames(cols), pks); err != nil {
			return fmt.Errorf("setting up CDC for %s: %w", qt, err)
		}
	}

	h.stream = replication.NewStream(h.db, h.streamCfg)
	h.stream.StartFrom(lastPos)
	// Snapshot the channel references under the lock so the goroutine holds
	// stable references that cannot be replaced by a concurrent reconnect.
	stopCh := h.stopCh
	go h.run(lastPos, stopCh)
	return nil
}

func (h *hanaCDCInput) run(startPos replication.LogPos, stopCh <-chan struct{}) {
	defer close(h.doneCh)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-stopCh; cancel() }()

	// Capture the fields we need from h before using them without a lock.
	// These are safe to read here because Connect() set them under mu before
	// starting this goroutine, and they are never overwritten while run() is alive.
	schemas := h.schemas
	stream := h.stream
	cp := h.cp

	// Snapshot phase.
	if h.snapshotMode == "initial" && startPos.IsNull() || h.snapshotMode == "always" {
		if err := h.runSnapshot(ctx, schemas, stream, cp); err != nil && ctx.Err() == nil {
			h.log.Errorf("Snapshot failed: %v", err)
		}
	}

	// sleep waits for d, doubles it (up to MaxBackoff), and returns false if ctx is done.
	sleep := func(d *time.Duration) bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(*d):
			if *d < h.streamCfg.MaxBackoff {
				*d *= 2
			}
			return true
		}
	}

	// Streaming phase.
	backoff := h.streamCfg.MinBackoff
	idleTimer := time.NewTimer(h.streamCfg.HeartbeatInterval)
	defer idleTimer.Stop()

	for {
		if ctx.Err() != nil {
			return
		}
		events, err := stream.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			h.log.Errorf("Poll error (retrying in %s): %v", backoff, err)
			if !sleep(&backoff) {
				return
			}
			continue
		}

		if len(events) == 0 {
			select {
			case <-idleTimer.C:
				h.publish(ctx, replication.ChangeEvent{Operation: replication.OpTypeHeartbeat, Timestamp: time.Now()}, stream, cp)
				idleTimer.Reset(h.streamCfg.HeartbeatInterval)
			default:
			}
			if !sleep(&backoff) {
				return
			}
			continue
		}

		backoff = h.streamCfg.MinBackoff
		idleTimer.Reset(h.streamCfg.HeartbeatInterval)
		for _, ev := range events {
			h.publish(ctx, ev, stream, cp)
		}
		if err := cp.Save(ctx, stream.LastPos()); err != nil {
			h.log.Errorf("Saving checkpoint: %v", err)
		}
	}
}

func (h *hanaCDCInput) runSnapshot(ctx context.Context, schemas *SchemaCache, stream *replication.Stream, cp *CheckpointCache) error {
	if schemas == nil {
		return errors.New("schemas not initialized")
	}
	if stream == nil {
		return errors.New("stream not initialized")
	}
	if cp == nil {
		return errors.New("checkpoint cache not initialized")
	}
	var minWatermark replication.LogPos
	for _, qt := range h.tables {
		schema, table, err := SplitQualTable(qt)
		if err != nil {
			return err
		}
		cols, pks, err := schemas.ForTable(ctx, schema, table)
		if err != nil {
			return err
		}
		snap := replication.NewSnapshot(h.db, replication.SnapshotConfig{
			MaxBatchSize: h.streamCfg.PollBatchSize,
			PKColumns:    pks,
		})
		events, watermark, err := snap.Read(ctx, schema, table, ColumnNames(cols))
		if err != nil {
			return fmt.Errorf("snapshot of %s: %w", qt, err)
		}
		// Track the minimum watermark (earliest snapshot start) to ensure
		// the stream resumes from before any changes made during the snapshot.
		if minWatermark.IsNull() || (!watermark.IsNull() && watermark.Compare(minWatermark) < 0) {
			minWatermark = watermark
		}
		for _, ev := range events {
			h.publish(ctx, ev, stream, cp)
		}
	}
	// Start the stream from the snapshot watermark so no CDC events are skipped
	// or duplicated between snapshot end and stream start.
	if !minWatermark.IsNull() {
		stream.StartFrom(minWatermark)
	}
	return nil
}

func (h *hanaCDCInput) publish(ctx context.Context, ev replication.ChangeEvent, stream *replication.Stream, cp *CheckpointCache) {
	msg, err := EventToMessage(ev, "redpanda.saphana_cdc")
	if err != nil {
		h.log.Errorf("Building message for %s.%s op=%s: %v", ev.Schema, ev.Table, ev.Operation, err)
		return
	}
	pos := stream.LastPos()
	select {
	case h.msgCh <- asyncMsg{
		batch: service.MessageBatch{msg},
		ackFn: func(ctx context.Context, _ error) error {
			return cp.SaveIfHigher(ctx, pos)
		},
	}:
	case <-ctx.Done():
	}
}

func (h *hanaCDCInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case m := <-h.msgCh:
		return m.batch, m.ackFn, nil
	case <-h.doneCh:
		return nil, nil, service.ErrNotConnected
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (h *hanaCDCInput) Close(ctx context.Context) error {
	h.once.Do(func() { close(h.stopCh) })
	select {
	case <-h.doneCh:
	case <-ctx.Done():
	}
	if h.db != nil {
		return h.db.Close()
	}
	return nil
}

// EventToMessage converts a ChangeEvent to a Debezium-format service.Message.
// Exported for unit testing.
func EventToMessage(ev replication.ChangeEvent, connectorName string) (*service.Message, error) {
	tsMs := ev.Timestamp.UnixMilli()

	if ev.Operation == replication.OpTypeHeartbeat {
		msg := service.NewMessage(nil)
		msg.SetStructuredMut(map[string]any{"op": "hb", "ts_ms": tsMs})
		msg.MetaSetMut("saphana_op", "hb")
		msg.MetaSetMut("saphana_operation", "heartbeat")
		return msg, nil
	}

	var before, after any
	switch ev.Operation {
	case replication.OpTypeDelete:
		before = ev.Data
		after = nil
	case replication.OpTypeUpdate:
		before = ev.BeforeData
		after = ev.Data
	default: // insert, read
		before = nil
		after = ev.Data
	}

	lsn := ev.LogPos.String()
	envelope := map[string]any{
		"before": before,
		"after":  after,
		"source": map[string]any{
			"connector": "redpanda.saphana",
			"name":      connectorName,
			"schema":    ev.Schema,
			"table":     ev.Table,
			"lsn":       lsn,
			"snapshot":  snapshotFlag(ev.Operation),
			"ts_ms":     tsMs,
		},
		"op":    ev.Operation.DebeziumCode(),
		"ts_ms": tsMs,
	}

	msg := service.NewMessage(nil)
	msg.SetStructuredMut(envelope)
	msg.MetaSetMut("saphana_schema", ev.Schema)
	msg.MetaSetMut("saphana_table", ev.Table)
	msg.MetaSetMut("saphana_op", ev.Operation.DebeziumCode())
	msg.MetaSetMut("saphana_operation", string(ev.Operation))
	msg.MetaSetMut("saphana_lsn", lsn)
	msg.MetaSetMut("saphana_ts_ms", strconv.FormatInt(tsMs, 10))
	return msg, nil
}

func snapshotFlag(op replication.OpType) string {
	if op == replication.OpTypeRead {
		return "true"
	}
	return "false"
}

// SplitQualTable parses a "SCHEMA.TABLE" qualified name into its components.
// Exported for unit testing.
func SplitQualTable(qt string) (schema, table string, err error) {
	idx := strings.IndexByte(qt, '.')
	if idx <= 0 || idx == len(qt)-1 {
		return "", "", fmt.Errorf("table %q must be in SCHEMA.TABLE format", qt)
	}
	return qt[:idx], qt[idx+1:], nil
}

// ColumnNames extracts the Name field from a slice of ColumnInfo.
// Exported for unit testing.
func ColumnNames(cols []ColumnInfo) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}
