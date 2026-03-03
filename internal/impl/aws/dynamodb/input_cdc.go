// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/cenkalti/backoff/v4"

	"github.com/redpanda-data/benthos/v4/public/service"
	baws "github.com/redpanda-data/connect/v4/internal/impl/aws"
	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
)

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

const (
	defaultDynamoDBBatchSize       = 1000 // AWS max limit
	defaultDynamoDBPollInterval    = "1s"
	defaultDynamoDBThrottleBackoff = "100ms"
	defaultShutdownTimeout         = 10 * time.Second

	// Snapshot modes.
	snapshotModeNone   = "none"
	snapshotModeOnly   = "snapshot_only"
	snapshotModeAndCDC = "snapshot_and_cdc"

	// Snapshot state values (stored in snapshotState.state).
	snapshotStateNotStarted int32 = 0
	snapshotStateInProgress int32 = 1
	snapshotStateComplete   int32 = 2
	snapshotStateFailed     int32 = 3

	// Metrics.
	metricShardsTracked           = "dynamodb_cdc_shards_tracked"
	metricShardsActive            = "dynamodb_cdc_shards_active"
	metricSnapshotState           = "dynamodb_cdc_snapshot_state"
	metricSnapshotRecordsRead     = "dynamodb_cdc_snapshot_records_read"
	metricSnapshotSegmentsActive  = "dynamodb_cdc_snapshot_segments_active"
	metricSnapshotBufferOverflow  = "dynamodb_cdc_snapshot_buffer_overflow"
	metricCheckpointFailures      = "dynamodb_cdc_checkpoint_failures"
	metricSnapshotSegmentDuration = "dynamodb_cdc_snapshot_segment_duration"

	// Config field names.
	fieldTable              = "table"
	fieldCheckpointTable    = "checkpoint_table"
	fieldBatchSize          = "batch_size"
	fieldPollInterval       = "poll_interval"
	fieldStartFrom          = "start_from"
	fieldCheckpointLimit    = "checkpoint_limit"
	fieldMaxTrackedShards   = "max_tracked_shards"
	fieldThrottleBackoff    = "throttle_backoff"
	fieldSnapshotMode       = "snapshot_mode"
	fieldSnapshotSegments   = "snapshot_segments"
	fieldSnapshotBatchSize  = "snapshot_batch_size"
	fieldSnapshotThrottle   = "snapshot_throttle"
	fieldSnapshotMaxBackoff = "snapshot_max_backoff"
	fieldSnapshotDedupe     = "snapshot_deduplicate"
	fieldSnapshotBufferSize = "snapshot_buffer_size"
)

func dynamoDBCDCInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("4.79.0").
		Categories("Services", "AWS").
		Summary("Reads change data capture (CDC) events from DynamoDB Streams.").
		Description(`
Consumes records from DynamoDB Streams with automatic checkpointing and shard management.

DynamoDB Streams capture item-level changes in DynamoDB tables. This input supports:

- Automatic shard discovery and management
- Checkpoint-based resumption after restarts
- Concurrent processing of multiple shards
- Optional initial snapshot of existing table data

### Prerequisites

The source DynamoDB table must have streams enabled. You can enable streams with one of these view types:

- `+"`KEYS_ONLY`"+` - Only the key attributes of the modified item
- `+"`NEW_IMAGE`"+` - The entire item as it appears after the modification
- `+"`OLD_IMAGE`"+` - The entire item as it appeared before the modification
- `+"`NEW_AND_OLD_IMAGES`"+` - Both the new and old item images

### Snapshots

When `+"`snapshot_mode`"+` is set to `+"`snapshot_only`"+` or `+"`snapshot_and_cdc`"+`, the input will first scan the entire table before (or instead of) streaming changes. This is useful for:

- Building a replica or cache with all existing data
- Syncing historical data to a data warehouse
- Populating a search index with existing records

WARNING: Snapshots use the DynamoDB Scan API which consumes read capacity units (RCUs). For large tables, this can be expensive and take considerable time. Use `+"`snapshot_segments`"+` and `+"`snapshot_throttle`"+` to control RCU consumption.

NOTE: Snapshots use eventually consistent reads and do not provide point-in-time consistency. Records modified during the snapshot may appear in both the snapshot and CDC stream (with different values). Use `+"`snapshot_deduplicate`"+` to minimize duplicates.

### Checkpointing

Checkpoints are stored in a separate DynamoDB table (configured via `+"`checkpoint_table`"+`). This table is created automatically if it does not exist. On restart, the input resumes from the last checkpointed position for each shard. Snapshot progress is also checkpointed, allowing resumption mid-snapshot after failures.

### Alternative

For better performance and longer retention (up to 1 year vs 24 hours), consider using Kinesis Data Streams for DynamoDB with the `+"`aws_kinesis`"+` input instead.

### Metadata

This input adds the following metadata fields to each message:

- `+"`dynamodb_shard_id`"+` - The shard ID from which the record was read (empty for snapshot records)
- `+"`dynamodb_sequence_number`"+` - The sequence number of the record in the stream (empty for snapshot records)
- `+"`dynamodb_event_name`"+` - The type of change: INSERT, MODIFY, REMOVE, or READ (for snapshot records)
- `+"`dynamodb_table`"+` - The name of the DynamoDB table

### Metrics

This input emits the following metrics:

- `+"`dynamodb_cdc_shards_tracked`"+` - Total number of shards being tracked (gauge)
- `+"`dynamodb_cdc_shards_active`"+` - Number of shards currently being read from (gauge)
- `+"`dynamodb_cdc_snapshot_state`"+` - Snapshot state: 0=not_started, 1=in_progress, 2=complete (gauge)
- `+"`dynamodb_cdc_snapshot_records_read`"+` - Total records read during snapshot (counter)
- `+"`dynamodb_cdc_snapshot_segments_active`"+` - Number of active snapshot scan segments (gauge)
- `+"`dynamodb_cdc_snapshot_buffer_overflow`"+` - Incremented when the deduplication buffer exceeds its size limit, disabling dedup (counter)
- `+"`dynamodb_cdc_snapshot_segment_duration`"+` - Time taken by each snapshot scan segment to complete (timer)
- `+"`dynamodb_cdc_checkpoint_failures`"+` - Number of failed checkpoint writes to the checkpoint table (counter)
`).
		Fields(
			service.NewStringField(fieldTable).
				Description("The name of the DynamoDB table to read streams from.").
				LintRule(`root = if this == "" { ["table name cannot be empty"] }`),
			service.NewStringField(fieldCheckpointTable).
				Description("DynamoDB table name for storing checkpoints. Will be created if it doesn't exist.").
				Default("redpanda_dynamodb_checkpoints"),
			service.NewIntField(fieldBatchSize).
				Description("Maximum number of records to read per shard in a single request. Valid range: 1-1000.").
				Default(defaultDynamoDBBatchSize).
				Advanced(),
			service.NewDurationField(fieldPollInterval).
				Description("Time to wait between polling attempts when no records are available.").
				Default(defaultDynamoDBPollInterval).
				Advanced(),
			service.NewStringEnumField(fieldStartFrom, "trim_horizon", "latest").
				Description("Where to start reading when no checkpoint exists. `trim_horizon` starts from the oldest available record, `latest` starts from new records.").
				Default("trim_horizon"),
			service.NewIntField(fieldCheckpointLimit).
				Description("Maximum number of unacknowledged messages before forcing a checkpoint update. Lower values provide better recovery guarantees but increase write overhead.").
				Default(1000).
				Advanced(),
			service.NewIntField(fieldMaxTrackedShards).
				Description("Maximum number of shards to track simultaneously. Prevents memory issues with extremely large tables.").
				Default(10000).
				Advanced(),
			service.NewDurationField(fieldThrottleBackoff).
				Description("Time to wait when applying backpressure due to too many in-flight messages.").
				Default(defaultDynamoDBThrottleBackoff).
				Advanced(),
			service.NewStringEnumField(fieldSnapshotMode, "none", "snapshot_only", "snapshot_and_cdc").
				Description("Snapshot behavior. `none`: CDC only (default). `snapshot_only`: one-time table scan, no streaming. `snapshot_and_cdc`: scan entire table then stream changes.").
				Default("none"),
			service.NewIntField(fieldSnapshotSegments).
				Description("Number of parallel DynamoDB Scan segments. Each segment scans a portion of the table concurrently, increasing throughput at the cost of more provisioned read capacity. Higher values consume more RCUs. Experiment to find the optimal value for your table.").
				Default(1).
				LintRule(`root = if this < 1 || this > 1000 { ["snapshot_segments must be between 1 and 1000"] }`).
				Advanced(),
			service.NewIntField(fieldSnapshotBatchSize).
				Description("Records per scan request during snapshot. Maximum 1000. Lower values provide better backpressure control but require more API calls.").
				Default(100).
				LintRule(`root = if this < 1 || this > 1000 { ["snapshot_batch_size must be between 1 and 1000"] }`).
				Advanced(),
			service.NewDurationField(fieldSnapshotThrottle).
				Description("Minimum time between scan requests per segment. Use this to limit RCU consumption during snapshot.").
				Default("100ms").
				LintRule(`root = if this <= 0 { ["snapshot_throttle must be greater than 0"] }`).
				Advanced(),
			service.NewDurationField(fieldSnapshotMaxBackoff).
				Description("Maximum total time to retry throttled snapshot scan requests before giving up. Set to 0 for unlimited retries.").
				Default("0s").
				Advanced(),
			service.NewBoolField(fieldSnapshotDedupe).
				Description("Deduplicate records that appear in both snapshot and CDC stream. Requires buffering CDC events during snapshot. If buffer is exceeded, deduplication is disabled to prevent data loss.").
				Default(true).
				Advanced(),
			service.NewIntField(fieldSnapshotBufferSize).
				Description("Maximum CDC events to buffer for deduplication (approximately 100 bytes per entry). If exceeded, deduplication is disabled and duplicates may be emitted.").
				Default(100000).
				Advanced(),
		).
		Fields(config.SessionFields()...).
		Example(
			"Consume CDC events",
			"Read change events from a DynamoDB table with streams enabled.",
			`
input:
  aws_dynamodb_cdc:
    table: my-table
    region: us-east-1
`,
		).
		Example(
			"Start from latest",
			"Only process new changes, ignoring existing stream data.",
			`
input:
  aws_dynamodb_cdc:
    table: orders
    start_from: latest
    region: us-west-2
`,
		).
		Example(
			"Snapshot and CDC",
			"Scan all existing records, then stream ongoing changes.",
			`
input:
  aws_dynamodb_cdc:
    table: products
    snapshot_mode: snapshot_and_cdc
    snapshot_segments: 5
    region: us-east-1
`,
		)
}

func init() {
	err := service.RegisterBatchInput(
		"aws_dynamodb_cdc", dynamoDBCDCInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newDynamoDBCDCInputFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type snapshotConfig struct {
	mode       string
	segments   int
	batchSize  int
	throttle   time.Duration
	maxBackoff time.Duration
	dedupe     bool
	bufferSize int
}

type dynamoDBCDCConfig struct {
	table            string
	checkpointTable  string
	batchSize        int
	pollInterval     time.Duration
	startFrom        string
	checkpointLimit  int
	maxTrackedShards int
	throttleBackoff  time.Duration
	snapshot         snapshotConfig
}

type dynamoDBCDCInput struct {
	conf           dynamoDBCDCConfig
	awsConf        aws.Config
	dynamoClient   *dynamodb.Client
	streamsClient  *dynamodbstreams.Client
	streamArn      *string
	tableKeySchema []string // sorted key attribute names from DescribeTable
	log            *service.Logger
	metrics        dynamoDBCDCMetrics

	mu            sync.RWMutex
	msgChan       chan asyncMessage
	shutSig       *shutdown.Signaller
	checkpointer  *Checkpointer
	recordBatcher *RecordBatcher
	shardReaders  map[string]*dynamoDBShardReader
	snapshot      *snapshotState // nil if snapshot mode is snapshotModeNone

	pendingAcks sync.WaitGroup
	closed      atomic.Bool
}

type dynamoDBCDCMetrics struct {
	shardsTracked           *service.MetricGauge
	shardsActive            *service.MetricGauge
	snapshotState           *service.MetricGauge
	snapshotRecordsRead     *service.MetricCounter
	snapshotSegmentsActive  *service.MetricGauge
	snapshotBufferOverflow  *service.MetricCounter // Counts buffer overflow events
	snapshotSegmentDuration *service.MetricTimer   // Tracks segment scan duration
	checkpointFailures      *service.MetricCounter // Counts checkpoint write failures
}

type dynamoDBShardReader struct {
	shardID   string
	iterator  *string
	exhausted bool
}

// snapshotState encapsulates all state related to snapshot scanning.
// This is only allocated when snapshot mode is enabled (not snapshotModeNone).
type snapshotState struct {
	state         atomic.Int32 // see snapshotState* constants
	errOnce       sync.Once    // ensures error is set exactly once
	err           error        // error if snapshot fails (write-once, read-many)
	startTime     time.Time
	endTime       time.Time
	seqBuffer     *snapshotSequenceBuffer
	scanner       *SnapshotScanner
	recordsRead   atomic.Int64
	segmentsTotal int
}

// snapshotSequenceBuffer tracks sequence numbers seen during snapshot for deduplication.
// It uses sharded locks to reduce contention under concurrent access.
type snapshotSequenceBuffer struct {
	shards     [numBufferShards]bufferShard
	hashSeed   maphash.Seed
	maxSize    int
	totalCount atomic.Int64
	overflow   atomic.Bool
}

// numBufferShards is the number of lock shards in the deduplication buffer.
// 32 is a power of two (enabling bitmask instead of modulo) and provides
// enough shards to keep lock contention low on machines with up to 32+ cores,
// while keeping per-shard memory overhead negligible (~300 bytes each).
const numBufferShards = 32

// itemKey is a deterministic string representation of a DynamoDB item's primary key,
// used as a map key for deduplication between snapshot and CDC records.
// Format: "attr1=val1;attr2=val2" with attributes sorted lexicographically.
type itemKey string

// bufferShard is a single shard of the buffer with its own lock.
type bufferShard struct {
	mu        sync.RWMutex
	sequences map[itemKey]string // item key -> sequence number seen in snapshot
}

func newSnapshotSequenceBuffer(maxSize int) *snapshotSequenceBuffer {
	buf := &snapshotSequenceBuffer{
		hashSeed: maphash.MakeSeed(),
		maxSize:  maxSize,
	}
	for i := range buf.shards {
		buf.shards[i].sequences = make(map[itemKey]string, maxSize/numBufferShards)
	}
	return buf
}

// getShard returns the shard for a given key using maphash.
func (s *snapshotSequenceBuffer) getShard(key itemKey) *bufferShard {
	h := maphash.String(s.hashSeed, string(key))
	return &s.shards[h&(numBufferShards-1)]
}

// tryClaimSlot atomically reserves a slot in the buffer using a CAS loop.
// Returns true if a slot was claimed, false if the buffer is full (overflow).
// This is lock-free on the hot path: only the shared atomic counter is contested,
// while the per-shard map is protected by the caller's shard lock.
func (s *snapshotSequenceBuffer) tryClaimSlot() bool {
	for {
		current := s.totalCount.Load()
		if current >= int64(s.maxSize) {
			s.overflow.Store(true)
			return false
		}
		if s.totalCount.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

// RecordSnapshotItem records a snapshot item's sequence number for deduplication.
func (s *snapshotSequenceBuffer) RecordSnapshotItem(key itemKey, sequenceNum string) {
	if s.overflow.Load() {
		return
	}

	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, ok := shard.sequences[key]; ok {
		shard.sequences[key] = sequenceNum
		return
	}

	if !s.tryClaimSlot() {
		return
	}

	shard.sequences[key] = sequenceNum
}

// ShouldSkipCDCEvent returns true if the CDC event is a duplicate of a snapshot item.
func (s *snapshotSequenceBuffer) ShouldSkipCDCEvent(key itemKey, cdcTimestamp string) bool {
	if s.overflow.Load() {
		return false
	}

	shard := s.getShard(key)
	shard.mu.RLock()
	snapshotTimestamp, ok := shard.sequences[key]
	shard.mu.RUnlock()

	if !ok {
		return false
	}

	// Skip if CDC event timestamp <= snapshot timestamp
	// This means the CDC event represents a change that occurred before/during
	// the snapshot and is likely already captured in the snapshot data
	return cdcTimestamp <= snapshotTimestamp
}

func (s *snapshotSequenceBuffer) IsOverflow() bool {
	return s.overflow.Load()
}

func (s *snapshotSequenceBuffer) Size() int {
	return int(s.totalCount.Load())
}

func dynamoCDCInputConfigFromParsed(pConf *service.ParsedConfig) (conf dynamoDBCDCConfig, err error) {
	if conf.table, err = pConf.FieldString(fieldTable); err != nil {
		return
	}
	if conf.checkpointTable, err = pConf.FieldString(fieldCheckpointTable); err != nil {
		return
	}
	if conf.batchSize, err = pConf.FieldInt(fieldBatchSize); err != nil {
		return
	}
	if conf.pollInterval, err = pConf.FieldDuration(fieldPollInterval); err != nil {
		return
	}
	if conf.startFrom, err = pConf.FieldString(fieldStartFrom); err != nil {
		return
	}
	if conf.checkpointLimit, err = pConf.FieldInt(fieldCheckpointLimit); err != nil {
		return
	}
	if conf.maxTrackedShards, err = pConf.FieldInt(fieldMaxTrackedShards); err != nil {
		return
	}
	if conf.throttleBackoff, err = pConf.FieldDuration(fieldThrottleBackoff); err != nil {
		return
	}
	if conf.snapshot.mode, err = pConf.FieldString(fieldSnapshotMode); err != nil {
		return
	}
	if conf.snapshot.segments, err = pConf.FieldInt(fieldSnapshotSegments); err != nil {
		return
	}
	if conf.snapshot.batchSize, err = pConf.FieldInt(fieldSnapshotBatchSize); err != nil {
		return
	}
	if conf.snapshot.throttle, err = pConf.FieldDuration(fieldSnapshotThrottle); err != nil {
		return
	}
	if conf.snapshot.maxBackoff, err = pConf.FieldDuration(fieldSnapshotMaxBackoff); err != nil {
		return
	}
	if conf.snapshot.dedupe, err = pConf.FieldBool(fieldSnapshotDedupe); err != nil {
		return
	}
	if conf.snapshot.bufferSize, err = pConf.FieldInt(fieldSnapshotBufferSize); err != nil {
		return
	}
	// Validate snapshot_throttle is positive (required for time.NewTicker)
	if conf.snapshot.throttle <= 0 {
		err = fmt.Errorf("snapshot_throttle must be greater than 0, got %v", conf.snapshot.throttle)
		return
	}
	return
}

func newDynamoDBCDCInputFromConfig(pConf *service.ParsedConfig, mgr *service.Resources) (*dynamoDBCDCInput, error) {
	conf, err := dynamoCDCInputConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}

	awsConf, err := baws.GetSession(context.Background(), pConf)
	if err != nil {
		return nil, err
	}

	input := &dynamoDBCDCInput{
		conf:         conf,
		awsConf:      awsConf,
		shardReaders: make(map[string]*dynamoDBShardReader),
		shutSig:      shutdown.NewSignaller(),
		log:          mgr.Logger(),
		metrics: dynamoDBCDCMetrics{
			shardsTracked:           mgr.Metrics().NewGauge(metricShardsTracked),
			shardsActive:            mgr.Metrics().NewGauge(metricShardsActive),
			snapshotState:           mgr.Metrics().NewGauge(metricSnapshotState),
			snapshotRecordsRead:     mgr.Metrics().NewCounter(metricSnapshotRecordsRead),
			snapshotSegmentsActive:  mgr.Metrics().NewGauge(metricSnapshotSegmentsActive),
			snapshotBufferOverflow:  mgr.Metrics().NewCounter(metricSnapshotBufferOverflow),
			snapshotSegmentDuration: mgr.Metrics().NewTimer(metricSnapshotSegmentDuration),
			checkpointFailures:      mgr.Metrics().NewCounter(metricCheckpointFailures),
		},
	}

	// Always initialize snapshot state (needed for state tracking and metrics)
	input.snapshot = &snapshotState{
		segmentsTotal: conf.snapshot.segments,
	}
	// Initialize scanner and buffer only if snapshot mode is enabled
	if conf.snapshot.mode != snapshotModeNone && conf.snapshot.dedupe {
		input.snapshot.seqBuffer = newSnapshotSequenceBuffer(conf.snapshot.bufferSize)
	}

	return input, nil
}

func (d *dynamoDBCDCInput) Connect(ctx context.Context) error {
	d.dynamoClient = dynamodb.NewFromConfig(d.awsConf)
	d.streamsClient = dynamodbstreams.NewFromConfig(d.awsConf)

	// Get stream ARN
	descTable, err := d.dynamoClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &d.conf.table,
	})
	if err != nil {
		if _, ok := errors.AsType[*types.ResourceNotFoundException](err); ok {
			return fmt.Errorf("table %s does not exist", d.conf.table)
		}
		return fmt.Errorf("describing table %s: %w", d.conf.table, err)
	}

	d.streamArn = descTable.Table.LatestStreamArn
	if d.streamArn == nil {
		return fmt.Errorf("no stream enabled on table %s", d.conf.table)
	}

	// Extract key schema attribute names for snapshot deduplication
	d.tableKeySchema = make([]string, 0, len(descTable.Table.KeySchema))
	for _, ks := range descTable.Table.KeySchema {
		d.tableKeySchema = append(d.tableKeySchema, *ks.AttributeName)
	}
	slices.Sort(d.tableKeySchema)

	// Initialize checkpointer
	d.checkpointer, err = NewCheckpointer(ctx, d.dynamoClient, d.conf.checkpointTable, *d.streamArn, d.conf.checkpointLimit, d.log)
	if err != nil {
		return fmt.Errorf("creating checkpointer: %w", err)
	}

	// Initialize record batcher
	d.recordBatcher = NewRecordBatcher(d.conf.maxTrackedShards, d.conf.checkpointLimit, d.log)

	// Initialize message channel with buffer to reduce blocking between scanner and processor
	// Buffer size of 1000 allows scanner to work ahead without blocking
	d.msgChan = make(chan asyncMessage, 1000)

	d.log.Infof("Connected to DynamoDB stream: %s", *d.streamArn)

	// Handle snapshot mode
	if d.conf.snapshot.mode != snapshotModeNone {
		return d.connectWithSnapshot(ctx)
	}

	// CDC-only mode (existing behavior)
	return d.connectCDCOnly(ctx)
}

// connectCDCOnly starts CDC streaming without snapshot (original behavior).
func (d *dynamoDBCDCInput) connectCDCOnly(ctx context.Context) error {
	// Mark snapshot as complete (never started)
	d.snapshot.state.Store(snapshotStateComplete)
	d.metrics.snapshotState.Set(int64(snapshotStateComplete))

	// Initialize shards
	if err := d.refreshShards(ctx); err != nil {
		return fmt.Errorf("initializing shards: %w", err)
	}

	// Verify at least one shard reader started successfully
	d.mu.Lock()
	activeCount := len(d.shardReaders)
	d.mu.Unlock()

	if activeCount == 0 {
		return errors.New("no active shard readers available - stream may have no shards or all initializing")
	}

	// Start background goroutine to coordinate shard readers
	coordinatorCtx, coordinatorCancel := d.shutSig.SoftStopCtx(context.Background())
	go func() {
		defer coordinatorCancel()
		d.startShardCoordinator(coordinatorCtx)
	}()

	return nil
}

// connectWithSnapshot handles snapshot + CDC coordination.
func (d *dynamoDBCDCInput) connectWithSnapshot(ctx context.Context) error {
	// Record snapshot start time BEFORE doing anything else
	d.snapshot.startTime = time.Now()

	// Check if we have a partial snapshot checkpoint
	snapshotCheckpoint, err := d.checkpointer.SnapshotProgress(ctx)
	if err != nil {
		return fmt.Errorf("getting snapshot progress: %w", err)
	}

	if snapshotCheckpoint.IsComplete() {
		d.log.Info("Snapshot was completed in previous run")

		// CRITICAL SAFETY CHECK: Verify CDC checkpoints are still valid
		// If connector was down >24h, DynamoDB Streams data is gone!
		switch d.conf.snapshot.mode {
		case snapshotModeAndCDC:
			isCDCStale, err := d.isCDCCheckpointStale(ctx)
			if err != nil {
				d.log.Warnf("Failed to check CDC checkpoint staleness: %v, proceeding with caution", err)
			} else if isCDCStale {
				d.log.Warn("CDC checkpoint is stale (stream data no longer available), re-running snapshot to prevent data loss")
				d.log.Info("This happens when the connector was down >24 hours (DynamoDB Streams retention limit)")

				// Clear the snapshot completion marker to force re-snapshot
				// Don't return here - fall through to run snapshot again
				snapshotCheckpoint = NewSnapshotCheckpoint() // Reset to empty
			} else {
				// CDC checkpoint is valid, safe to skip snapshot
				d.snapshot.state.Store(snapshotStateComplete)
				d.metrics.snapshotState.Set(int64(snapshotStateComplete))
				return d.connectCDCOnly(ctx)
			}
		case snapshotModeOnly:
			// Snapshot already done, nothing more to do
			// Mark as complete and let ReadBatch return ErrEndOfInput
			d.log.Info("Snapshot-only mode: snapshot already complete")
			d.snapshot.state.Store(snapshotStateComplete)
			d.metrics.snapshotState.Set(int64(snapshotStateComplete))
			// Close msgChan immediately so ReadBatch can return ErrEndOfInput
			close(d.msgChan)
			// Signal that we've stopped so Close() doesn't wait for the shutdown timeout
			d.shutSig.TriggerHasStopped()
			return nil
		}
	}

	// CRITICAL ORDERING FOR DATA LOSS PREVENTION:
	// 1. Start CDC readers FIRST (if snapshot_and_cdc mode)
	//    This ensures we capture ALL changes that happen during snapshot
	if d.conf.snapshot.mode == snapshotModeAndCDC {
		d.log.Info("Starting CDC readers before snapshot to prevent data loss")

		// Initialize shards
		if err := d.refreshShards(ctx); err != nil {
			return fmt.Errorf("initializing shards: %w", err)
		}

		// Start shard coordinator in background
		coordinatorCtx, coordinatorCancel := d.shutSig.SoftStopCtx(context.Background())
		go func() {
			defer coordinatorCancel()
			d.startShardCoordinator(coordinatorCtx)
		}()

		d.log.Info("CDC readers started, will capture changes during snapshot")
	}

	// 2. NOW start snapshot (while CDC is capturing changes in parallel)
	d.snapshot.state.Store(snapshotStateInProgress)
	d.metrics.snapshotState.Set(int64(snapshotStateInProgress))

	// Initialize snapshot scanner
	d.snapshot.scanner = NewSnapshotScanner(SnapshotScannerConfig{
		Client:             d.dynamoClient,
		Table:              d.conf.table,
		Segments:           d.conf.snapshot.segments,
		BatchSize:          d.conf.snapshot.batchSize,
		Throttle:           d.conf.snapshot.throttle,
		MaxBackoff:         d.conf.snapshot.maxBackoff,
		Checkpointer:       d.checkpointer,
		CheckpointInterval: 10, // Checkpoint every 10 batches (10x cost reduction).
		Logger:             d.log,
	})

	// Set batch callback to send snapshot records to msgChan
	d.snapshot.scanner.SetBatchCallback(d.handleSnapshotBatch)

	// Set progress callback to update metrics
	d.snapshot.scanner.SetProgressCallback(func(_, _ int, _ int64) {
		d.metrics.snapshotSegmentsActive.Set(int64(d.snapshot.scanner.ActiveSegments()))
	})

	// Set checkpoint failure callback to track failures
	d.snapshot.scanner.SetCheckpointFailedCallback(func(_ int, _ error) {
		d.metrics.checkpointFailures.Incr(1)
	})

	// Set segment completion callback to track scan duration
	d.snapshot.scanner.SetSegmentCompleteCallback(func(_ int, duration time.Duration, _ int64) {
		d.metrics.snapshotSegmentDuration.Timing(duration.Nanoseconds())
	})

	// Start snapshot in background
	scanCtx, scanCancel := d.shutSig.SoftStopCtx(context.Background())
	go func() {
		defer scanCancel()
		d.log.Info("Starting snapshot scan")
		if err := d.snapshot.scanner.Scan(scanCtx, snapshotCheckpoint); err != nil {
			if !errors.Is(err, context.Canceled) {
				wrappedErr := fmt.Errorf("snapshot scan failed for table %s: %w", d.conf.table, err)
				d.log.Errorf("%v", wrappedErr)
				d.snapshot.errOnce.Do(func() {
					d.snapshot.err = wrappedErr
				})
				d.snapshot.state.Store(snapshotStateFailed)
				d.metrics.snapshotState.Set(int64(snapshotStateFailed))
				if d.conf.snapshot.mode == snapshotModeOnly {
					close(d.msgChan)
					d.shutSig.TriggerHasStopped()
				}
				return
			}
		}

		// Snapshot complete
		d.snapshot.endTime = time.Now()
		d.snapshot.state.Store(snapshotStateComplete)
		d.metrics.snapshotState.Set(int64(snapshotStateComplete))

		// Mark as complete in checkpoint
		if err := d.checkpointer.MarkSnapshotComplete(scanCtx); err != nil {
			d.log.Errorf("Failed to mark snapshot complete: %v", err)
		}

		d.log.Infof("Snapshot scan completed: %d records in %v",
			d.snapshot.recordsRead.Load(), d.snapshot.endTime.Sub(d.snapshot.startTime))

		// If snapshot_only mode, close the input
		if d.conf.snapshot.mode == snapshotModeOnly {
			d.log.Info("Snapshot-only mode complete, triggering shutdown")
			d.shutSig.TriggerSoftStop()
			// Close msgChan to unblock ReadBatch and signal completion
			close(d.msgChan)
			// Signal that we've stopped so Close() doesn't wait for the shutdown timeout
			d.shutSig.TriggerHasStopped()
		}
	}()

	return nil
}

// isCDCCheckpointStale checks if any CDC checkpoint points to expired stream data.
// Returns true if any checkpoint is stale (stream data no longer available).
// This happens when the connector was down >24 hours (DynamoDB Streams retention limit).
func (d *dynamoDBCDCInput) isCDCCheckpointStale(ctx context.Context) (bool, error) {
	// Get current shards from the stream
	streamDesc, err := d.streamsClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: d.streamArn,
	})
	if err != nil {
		return false, fmt.Errorf("describing stream: %w", err)
	}

	if len(streamDesc.StreamDescription.Shards) == 0 {
		// No shards = no data = checkpoint doesn't matter
		return false, nil
	}

	for _, shard := range streamDesc.StreamDescription.Shards {
		shardID := *shard.ShardId

		// Check if we have a checkpoint for this shard
		checkpoint, err := d.checkpointer.Get(ctx, shardID)
		if err != nil || checkpoint == "" {
			if err != nil {
				d.log.Warnf("Failed to get checkpoint for shard %s: %v", shardID, err)
			}
			continue
		}

		// Try to get a shard iterator using the checkpointed sequence number
		// If this fails, the sequence is too old and data has expired
		_, err = d.streamsClient.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
			StreamArn:         d.streamArn,
			ShardId:           shard.ShardId,
			ShardIteratorType: types.ShardIteratorTypeAfterSequenceNumber,
			SequenceNumber:    &checkpoint,
		})
		if err != nil {
			d.log.Warnf("Shard %s checkpoint is stale: %v", shardID, err)
			d.log.Warn("CDC checkpoint is stale - data may have been lost during downtime")
			return true, nil
		}
	}

	return false, nil
}

func (d *dynamoDBCDCInput) refreshShards(ctx context.Context) error {
	streamDesc, err := d.streamsClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: d.streamArn,
	})
	if err != nil {
		return err
	}

	// Collect new shards to add without holding locks during I/O operations
	type shardToAdd struct {
		shardID  string
		iterator *string
	}
	var newShards []shardToAdd

	for _, shard := range streamDesc.StreamDescription.Shards {
		shardID := *shard.ShardId

		// Check if shard already exists (minimize lock hold time)
		d.mu.RLock()
		_, exists := d.shardReaders[shardID]
		d.mu.RUnlock()

		if exists {
			continue
		}

		// Check checkpoint (I/O operation - do not hold lock)
		checkpoint, err := d.checkpointer.Get(ctx, shardID)
		if err != nil {
			return fmt.Errorf("getting checkpoint for shard %s: %w", shardID, err)
		}

		var (
			iteratorType   types.ShardIteratorType
			sequenceNumber *string
		)

		if checkpoint != "" {
			iteratorType = types.ShardIteratorTypeAfterSequenceNumber
			sequenceNumber = &checkpoint
			d.log.Infof("Resuming shard %s from checkpoint: %s", shardID, checkpoint)
		} else {
			if d.conf.startFrom == "latest" {
				iteratorType = types.ShardIteratorTypeLatest
			} else {
				iteratorType = types.ShardIteratorTypeTrimHorizon
			}
			d.log.Infof("Starting shard %s from %s", shardID, d.conf.startFrom)
		}

		// Get shard iterator (I/O operation - do not hold lock)
		iter, err := d.streamsClient.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
			StreamArn:         d.streamArn,
			ShardId:           shard.ShardId,
			ShardIteratorType: iteratorType,
			SequenceNumber:    sequenceNumber,
		})
		if err != nil {
			return fmt.Errorf("getting iterator for shard %s: %w", shardID, err)
		}

		newShards = append(newShards, shardToAdd{
			shardID:  shardID,
			iterator: iter.ShardIterator,
		})
	}

	// Add all new shard readers in a single critical section
	if len(newShards) > 0 {
		d.mu.Lock()
		for _, s := range newShards {
			// Double-check shard wasn't added by another goroutine
			if _, exists := d.shardReaders[s.shardID]; !exists {
				d.shardReaders[s.shardID] = &dynamoDBShardReader{
					shardID:   s.shardID,
					iterator:  s.iterator,
					exhausted: false,
				}
			}
		}
		totalShards := len(d.shardReaders)
		d.mu.Unlock()

		d.log.Infof("Tracking %d shards", totalShards)
		d.metrics.shardsTracked.Set(int64(totalShards))
	}

	return nil
}

// startShardCoordinator spawns goroutines for each shard and manages shard refresh.
func (d *dynamoDBCDCInput) startShardCoordinator(ctx context.Context) {
	defer func() {
		close(d.msgChan)
		d.shutSig.TriggerHasStopped()
	}()

	// Track running shard readers
	activeShards := make(map[string]context.CancelFunc)
	defer func() {
		// Cancel all active shard readers on shutdown
		for _, cancelFn := range activeShards {
			cancelFn()
		}
	}()

	refreshTicker := time.NewTicker(30 * time.Second)
	defer refreshTicker.Stop()

	cleanupTicker := time.NewTicker(5 * time.Minute)
	defer cleanupTicker.Stop()

	for {
		// Get current shard readers
		d.mu.RLock()
		currentReaders := make(map[string]*dynamoDBShardReader)
		maps.Copy(currentReaders, d.shardReaders)
		d.mu.RUnlock()

		// Start new shard readers for any new shards
		for shardID, reader := range currentReaders {
			if _, exists := activeShards[shardID]; !exists && !reader.exhausted {
				shardCtx, shardCancel := context.WithCancel(ctx)
				activeShards[shardID] = shardCancel
				go d.startShardReader(shardCtx, shardID)
			}
		}

		// Update active shards metric
		activeCount := 0
		for shardID := range activeShards {
			d.mu.RLock()
			reader, exists := d.shardReaders[shardID]
			d.mu.RUnlock()
			if exists && !reader.exhausted {
				activeCount++
			}
		}
		d.metrics.shardsActive.Set(int64(activeCount))

		select {
		case <-ctx.Done():
			return
		case <-refreshTicker.C:
			// Refresh shards periodically to discover new shards
			// Use a timeout context to prevent blocking on shutdown
			refreshCtx, refreshCancel := context.WithTimeout(ctx, 30*time.Second)
			if err := d.refreshShards(refreshCtx); err != nil && !errors.Is(err, context.Canceled) {
				d.log.Warnf("Failed to refresh shards: %v", err)
			}
			refreshCancel()
		case <-cleanupTicker.C:
			// Clean up exhausted shards to prevent unbounded map growth
			d.cleanupExhaustedShards(activeShards)
		}
	}
}

// cleanupExhaustedShards removes exhausted shards from tracking to prevent unbounded map growth.
// This is called periodically by the shard coordinator.
func (d *dynamoDBCDCInput) cleanupExhaustedShards(activeShards map[string]context.CancelFunc) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var cleaned []string
	for shardID, reader := range d.shardReaders {
		// Only remove shards that are both exhausted and no longer active
		if reader.exhausted {
			if cancelFn, isActive := activeShards[shardID]; isActive {
				// Cancel the goroutine for this shard
				cancelFn()
				delete(activeShards, shardID)
			}
			delete(d.shardReaders, shardID)
			cleaned = append(cleaned, shardID)
		}
	}

	if len(cleaned) > 0 {
		d.log.Infof("Cleaned up %d exhausted shards: %v", len(cleaned), cleaned)
		d.metrics.shardsTracked.Set(int64(len(d.shardReaders)))
	}
}

// startShardReader continuously reads from a single shard and sends batches to the channel.
func (d *dynamoDBCDCInput) startShardReader(ctx context.Context, shardID string) {
	d.log.Debugf("Starting reader for shard %s", shardID)
	defer d.log.Debugf("Stopped reader for shard %s", shardID)

	pollTicker := time.NewTicker(d.conf.pollInterval)
	defer pollTicker.Stop()

	// Initialize backoff for throttling errors
	boff := backoff.NewExponentialBackOff()
	boff.InitialInterval = 200 * time.Millisecond
	boff.MaxInterval = 2 * time.Second
	boff.MaxElapsedTime = 0 // Never give up

	for {
		select {
		case <-ctx.Done():
			return
		case <-pollTicker.C:
			// Check for cancellation before expensive operations
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Apply backpressure if too many messages are in flight
			for d.recordBatcher != nil && d.recordBatcher.ShouldThrottle() {
				d.log.Debugf("Throttling shard %s due to too many in-flight messages", shardID)
				select {
				case <-ctx.Done():
					return
				case <-time.After(d.conf.throttleBackoff):
				}
			}

			// Get current reader state
			d.mu.RLock()
			reader, exists := d.shardReaders[shardID]
			if !exists {
				d.mu.RUnlock()
				d.log.Errorf("BUG: shard reader for %s not found in map", shardID)
				return
			}
			if reader.exhausted || reader.iterator == nil {
				d.mu.RUnlock()
				return
			}
			iterator := reader.iterator
			d.mu.RUnlock()

			// Read records from the shard (I/O operation - no lock held)
			getRecords, err := d.streamsClient.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
				ShardIterator: iterator,
				Limit:         aws.Int32(int32(d.conf.batchSize)),
			})
			if err != nil {
				if isThrottlingError(err) {
					wait := boff.NextBackOff()
					d.log.Debugf("Throttled on shard %s, backing off for %v", shardID, wait)
					time.Sleep(wait)
					continue
				}
				d.log.Errorf("Failed to get records from shard %s: %v", shardID, err)
				// On error, wait and retry (don't mark as exhausted)
				continue
			}

			// Success - reset backoff
			boff.Reset()

			// Update iterator
			d.mu.Lock()
			reader.iterator = getRecords.NextShardIterator
			if reader.iterator == nil {
				reader.exhausted = true
				d.log.Infof("Shard %s exhausted", shardID)
				d.mu.Unlock()
				return
			}
			d.mu.Unlock()

			if len(getRecords.Records) == 0 {
				continue
			}

			// Convert records to messages
			batch := d.convertRecordsToBatch(getRecords.Records, shardID)
			if len(batch) == 0 {
				continue
			}

			// Track messages in batcher
			batch = d.recordBatcher.AddMessages(batch, shardID)

			// Track pending ack
			d.pendingAcks.Add(1)

			// Create ack function
			checkpointer := d.checkpointer
			recordBatcher := d.recordBatcher
			ackFunc := func(ackCtx context.Context, err error) error {
				defer d.pendingAcks.Done()

				// Check if already closed
				if d.closed.Load() {
					d.log.Warn("Received ack after close, dropping")
					if err == nil && recordBatcher != nil {
						recordBatcher.RemoveMessages(batch)
					}
					return nil
				}

				if err != nil {
					d.log.Warnf("Batch nacked from shard %s: %v", shardID, err)
					if recordBatcher != nil {
						recordBatcher.RemoveMessages(batch)
					}
					return err // Propagate nack error
				}

				// Mark messages as acked and checkpoint if needed
				if recordBatcher != nil && checkpointer != nil {
					if ackErr := recordBatcher.AckMessages(ackCtx, checkpointer, batch); ackErr != nil {
						d.log.Errorf("Failed to checkpoint shard %s after ack: %v", shardID, ackErr)
						return ackErr // Propagate checkpoint failure
					}
					d.log.Debugf("Successfully checkpointed %d messages from shard %s", len(batch), shardID)
				}
				return nil
			}

			// Send to channel
			select {
			case <-ctx.Done():
				return
			case d.msgChan <- asyncMessage{msg: batch, ackFn: ackFunc}:
				d.log.Debugf("Sent batch of %d records from shard %s", len(batch), shardID)
			}
		}
	}
}

// handleSnapshotBatch processes a batch of items from the snapshot scan.
func (d *dynamoDBCDCInput) handleSnapshotBatch(ctx context.Context, items []map[string]dynamodbtypes.AttributeValue, segment int) error {
	if len(items) == 0 {
		return nil
	}

	batch := make(service.MessageBatch, 0, len(items))

	for _, item := range items {
		msg := service.NewMessage(nil)

		// Structure the snapshot record similar to CDC events
		recordData := map[string]any{
			"tableName": d.conf.table,
			"eventName": "READ", // Distinguish snapshot reads from CDC events
		}

		// Add the full item as newImage (similar to CDC INSERT events)
		dynamoData := map[string]any{
			"newImage": unmarshalDynamoDBItem(item),
		}

		// Extract keys for deduplication if enabled
		d.mu.RLock()
		buffer := d.snapshot.seqBuffer
		startTime := d.snapshot.startTime
		d.mu.RUnlock()
		if buffer != nil {
			keyStr := d.buildSnapshotItemKey(item)
			if keyStr != "" {
				// Record this item in the snapshot buffer (with timestamp as sequence for deduplication)
				buffer.RecordSnapshotItem(keyStr, startTime.Format(time.RFC3339Nano))
			}
		}

		recordData["dynamodb"] = dynamoData
		msg.SetStructured(recordData)

		// Set metadata - note these are different from CDC events
		msg.MetaSetMut("dynamodb_event_name", "READ")
		msg.MetaSetMut("dynamodb_table", d.conf.table)
		msg.MetaSetMut("dynamodb_snapshot_segment", strconv.Itoa(segment))

		batch = append(batch, msg)
	}

	// Update metrics
	d.snapshot.recordsRead.Add(int64(len(batch)))
	d.metrics.snapshotRecordsRead.Incr(int64(len(batch)))

	// Check and report buffer overflow
	d.mu.RLock()
	buffer := d.snapshot.seqBuffer
	d.mu.RUnlock()
	if buffer != nil && buffer.IsOverflow() {
		// Increment metric (idempotent - only increments once per overflow event)
		d.metrics.snapshotBufferOverflow.Incr(1)
		d.log.Warn("Snapshot deduplication buffer overflowed - duplicates may occur during CDC overlap")
	}

	// Track pending ack
	d.pendingAcks.Add(1)

	// Create simple ack function for snapshot records
	ackFunc := func(_ context.Context, err error) error {
		defer d.pendingAcks.Done()

		if d.closed.Load() {
			d.log.Debug("Received snapshot ack after close, dropping")
			return nil
		}

		if err != nil {
			d.log.Warnf("Snapshot batch nacked from segment %d: %v", segment, err)
			return err
		}

		return nil
	}

	// Send to channel (with backpressure handling)
	select {
	case <-ctx.Done():
		d.pendingAcks.Done() // Undo the Add(1) above
		return ctx.Err()
	case d.msgChan <- asyncMessage{msg: batch, ackFn: ackFunc}:
		d.log.Debugf("Sent snapshot batch of %d records from segment %d", len(batch), segment)
		return nil
	}
}

// itemKeyBuilder constructs deterministic itemKey values from DynamoDB attribute maps.
// Key format: "attr1=val1;attr2=val2" with attributes sorted lexicographically.
type itemKeyBuilder struct {
	sb    strings.Builder
	count int
}

// addStreamAttr appends a key=value pair from a DynamoDB Streams attribute value.
func (b *itemKeyBuilder) addStreamAttr(name string, attr types.AttributeValue) {
	if b.count > 0 {
		b.sb.WriteByte(';')
	}
	b.sb.WriteString(name)
	b.sb.WriteByte('=')
	switch v := attr.(type) {
	case *types.AttributeValueMemberS:
		b.sb.WriteString(v.Value)
	case *types.AttributeValueMemberN:
		b.sb.WriteString(v.Value)
	case *types.AttributeValueMemberBOOL:
		if v.Value {
			b.sb.WriteString("true")
		} else {
			b.sb.WriteString("false")
		}
	case *types.AttributeValueMemberB:
		b.sb.WriteString("<binary>")
	default:
		fmt.Fprintf(&b.sb, "%v", convertAttributeValue(attr))
	}
	b.count++
}

// addTableAttr appends a key=value pair from a DynamoDB table attribute value.
func (b *itemKeyBuilder) addTableAttr(name string, attr dynamodbtypes.AttributeValue) {
	if b.count > 0 {
		b.sb.WriteByte(';')
	}
	b.sb.WriteString(name)
	b.sb.WriteByte('=')
	switch v := attr.(type) {
	case *dynamodbtypes.AttributeValueMemberS:
		b.sb.WriteString(v.Value)
	case *dynamodbtypes.AttributeValueMemberN:
		b.sb.WriteString(v.Value)
	case *dynamodbtypes.AttributeValueMemberBOOL:
		if v.Value {
			b.sb.WriteString("true")
		} else {
			b.sb.WriteString("false")
		}
	case *dynamodbtypes.AttributeValueMemberB:
		b.sb.WriteString("<binary>")
	default:
		fmt.Fprintf(&b.sb, "%v", unmarshalDynamoDBAttributeValue(attr))
	}
	b.count++
}

// build returns the constructed itemKey.
func (b *itemKeyBuilder) build() itemKey {
	return itemKey(b.sb.String())
}

// buildSnapshotItemKey creates an itemKey from a snapshot scan item using
// the actual table key schema (from DescribeTable). This produces the same key format
// as buildItemKeyFromStream, which uses record.Dynamodb.Keys from CDC events.
func (d *dynamoDBCDCInput) buildSnapshotItemKey(item map[string]dynamodbtypes.AttributeValue) itemKey {
	if len(d.tableKeySchema) == 0 {
		return ""
	}

	var kb itemKeyBuilder
	kb.sb.Grow(64)

	for _, k := range d.tableKeySchema {
		attr, ok := item[k]
		if !ok {
			return "" // Key attribute missing from item, can't build key
		}
		kb.addTableAttr(k, attr)
	}

	return kb.build()
}

// buildItemKeyFromStream creates an itemKey from DynamoDB Stream keys (for CDC deduplication).
// Uses types.AttributeValue (from streams) instead of dynamodbtypes.AttributeValue (from table).
func buildItemKeyFromStream(keys map[string]types.AttributeValue) itemKey {
	// Sort keys for deterministic ordering
	keyNames := make([]string, 0, len(keys))
	for k := range keys {
		keyNames = append(keyNames, k)
	}
	slices.Sort(keyNames)

	var kb itemKeyBuilder
	kb.sb.Grow(64)

	for _, k := range keyNames {
		kb.addStreamAttr(k, keys[k])
	}

	return kb.build()
}

// convertRecordsToBatch converts DynamoDB Stream records to Benthos messages.
func (d *dynamoDBCDCInput) convertRecordsToBatch(records []types.Record, shardID string) service.MessageBatch {
	batch := make(service.MessageBatch, 0, len(records))

	// Check if deduplication is enabled
	d.mu.RLock()
	dedupeBuffer := d.snapshot.seqBuffer
	d.mu.RUnlock()

	for _, record := range records {
		var sequenceNumber string
		var keyStr itemKey
		var cdcTimestamp string

		// Extract sequence number, timestamp, and build key string for deduplication
		if record.Dynamodb != nil {
			sequenceNumber = aws.ToString(record.Dynamodb.SequenceNumber)

			// Extract approximate creation timestamp for deduplication
			if record.Dynamodb.ApproximateCreationDateTime != nil {
				cdcTimestamp = record.Dynamodb.ApproximateCreationDateTime.Format(time.RFC3339Nano)
			}

			// Build key string from the record's keys for deduplication check
			if dedupeBuffer != nil && record.Dynamodb.Keys != nil {
				keyStr = buildItemKeyFromStream(record.Dynamodb.Keys)
			}
		}

		// Check if this CDC event should be skipped (already seen in snapshot)
		if dedupeBuffer != nil && keyStr != "" && cdcTimestamp != "" {
			if dedupeBuffer.ShouldSkipCDCEvent(keyStr, cdcTimestamp) {
				d.log.Debugf("Skipping duplicate CDC event for key %s (timestamp %s)", keyStr, cdcTimestamp)
				continue
			}
		}

		msg := service.NewMessage(nil)

		// Structure similar to Kinesis format for consistency
		recordData := map[string]any{
			"tableName":    d.conf.table,
			"eventID":      aws.ToString(record.EventID),
			"eventName":    string(record.EventName),
			"eventVersion": aws.ToString(record.EventVersion),
			"eventSource":  aws.ToString(record.EventSource),
			"awsRegion":    aws.ToString(record.AwsRegion),
		}

		if record.Dynamodb != nil {
			dynamoData := map[string]any{
				"sequenceNumber": sequenceNumber,
				"streamViewType": string(record.Dynamodb.StreamViewType),
			}

			if record.Dynamodb.Keys != nil {
				dynamoData["keys"] = convertAttributeMap(record.Dynamodb.Keys)
			}
			if record.Dynamodb.NewImage != nil {
				dynamoData["newImage"] = convertAttributeMap(record.Dynamodb.NewImage)
			}
			if record.Dynamodb.OldImage != nil {
				dynamoData["oldImage"] = convertAttributeMap(record.Dynamodb.OldImage)
			}
			if record.Dynamodb.SizeBytes != nil {
				dynamoData["sizeBytes"] = *record.Dynamodb.SizeBytes
			}

			recordData["dynamodb"] = dynamoData
		}

		msg.SetStructured(recordData)

		// Set metadata
		msg.MetaSetMut("dynamodb_shard_id", shardID)
		msg.MetaSetMut("dynamodb_sequence_number", sequenceNumber)
		msg.MetaSetMut("dynamodb_event_name", string(record.EventName))
		msg.MetaSetMut("dynamodb_table", d.conf.table)

		batch = append(batch, msg)
	}

	return batch
}

func (d *dynamoDBCDCInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	d.mu.RLock()
	msgChan := d.msgChan
	shutSig := d.shutSig
	d.mu.RUnlock()

	if msgChan == nil || shutSig == nil {
		return nil, nil, service.ErrNotConnected
	}

	// Check if snapshot failed and propagate the error
	if d.snapshot.state.Load() == snapshotStateFailed { // failed
		if d.snapshot.err != nil {
			return nil, nil, d.snapshot.err
		}
		return nil, nil, fmt.Errorf("snapshot scan failed for table %s", d.conf.table)
	}

	// Create a context that cancels on soft stop for snapshot-only mode
	softStopCtx, softStopCancel := shutSig.SoftStopCtx(ctx)
	defer softStopCancel()

	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-softStopCtx.Done():
		// Soft stop triggered - check if this is snapshot-only mode completion
		if d.conf.snapshot.mode == snapshotModeOnly && d.snapshot.state.Load() == snapshotStateComplete {
			// Drain any remaining messages in the channel before returning ErrEndOfInput
			select {
			case am, open := <-msgChan:
				if open {
					return am.msg, am.ackFn, nil
				}
			default:
			}
			return nil, nil, service.ErrEndOfInput
		}
		return nil, nil, service.ErrNotConnected
	case <-shutSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case am, open := <-msgChan:
		if !open {
			// Channel closed - check if this is clean snapshot-only completion
			if d.conf.snapshot.mode == snapshotModeOnly && d.snapshot.state.Load() == snapshotStateComplete {
				return nil, nil, service.ErrEndOfInput
			}
			return nil, nil, service.ErrNotConnected
		}
		return am.msg, am.ackFn, nil
	}
}

func (d *dynamoDBCDCInput) Close(ctx context.Context) error {
	// Mark as closed to reject new acks
	d.closed.Store(true)

	d.mu.RLock()
	shutSig := d.shutSig
	checkpointer := d.checkpointer
	batcher := d.recordBatcher
	d.mu.RUnlock()

	// Trigger graceful shutdown
	d.log.Debug("Initiating graceful shutdown")
	shutSig.TriggerSoftStop()

	// Wait for background goroutines to stop
	select {
	case <-shutSig.HasStoppedChan():
		d.log.Debug("Background goroutines stopped")
	case <-time.After(defaultShutdownTimeout):
		d.log.Warn("Timeout waiting for background goroutines to stop")
		// Trigger hard stop if graceful shutdown times out
		shutSig.TriggerHardStop()
	}

	// Wait for pending acknowledgments with timeout
	d.log.Debug("Waiting for pending acknowledgments")
	acksDone := make(chan struct{})
	go func() {
		d.pendingAcks.Wait()
		close(acksDone)
	}()

	select {
	case <-acksDone:
		d.log.Debug("All pending acks completed")
	case <-time.After(defaultShutdownTimeout):
		d.log.Warn("Timeout waiting for pending acks, proceeding with shutdown")
	}

	// Flush any pending checkpoints
	if checkpointer != nil && batcher != nil {
		pendingCheckpoints := batcher.PendingCheckpoints()
		if len(pendingCheckpoints) > 0 {
			d.log.Infof("Flushing %d pending checkpoints on close", len(pendingCheckpoints))
			if err := checkpointer.FlushCheckpoints(ctx, pendingCheckpoints); err != nil {
				d.log.Errorf("Failed to flush checkpoints: %v", err)
				d.metrics.checkpointFailures.Incr(1)
				// Don't return error - continue cleanup to avoid resource leaks
			}
		}
	} else {
		d.log.Debug("Skipping checkpoint flush - components not initialized")
	}

	// Clear references to help GC
	d.mu.Lock()
	d.dynamoClient = nil
	d.streamsClient = nil
	d.shardReaders = nil
	d.checkpointer = nil
	d.recordBatcher = nil
	d.msgChan = nil
	d.shutSig = nil
	if d.snapshot != nil {
		d.snapshot.seqBuffer = nil
		d.snapshot.scanner = nil
	}
	d.mu.Unlock()

	return nil
}

// convertAttributeMap converts DynamoDB stream attribute values to Go types.
// It pre-sizes the result map to reduce rehashing during growth.
func convertAttributeMap(attrs map[string]types.AttributeValue) map[string]any {
	// Pre-allocate with exact capacity to avoid rehashing
	result := make(map[string]any, len(attrs))
	for k, v := range attrs {
		result[k] = convertAttributeValue(v)
	}
	return result
}

// isThrottlingError is defined in snapshot.go and checks for both
// LimitExceededException and ProvisionedThroughputExceededException.
// Note: TrimmedDataAccessException means stream data expired, not throttling.

func convertAttributeValue(attr types.AttributeValue) any {
	switch v := attr.(type) {
	case *types.AttributeValueMemberS:
		return v.Value
	case *types.AttributeValueMemberN:
		return v.Value
	case *types.AttributeValueMemberB:
		return v.Value
	case *types.AttributeValueMemberSS:
		return v.Value
	case *types.AttributeValueMemberNS:
		return v.Value
	case *types.AttributeValueMemberBS:
		return v.Value
	case *types.AttributeValueMemberM:
		return convertAttributeMap(v.Value)
	case *types.AttributeValueMemberL:
		list := make([]any, len(v.Value))
		for i, item := range v.Value {
			list[i] = convertAttributeValue(item)
		}
		return list
	case *types.AttributeValueMemberNULL:
		return nil
	case *types.AttributeValueMemberBOOL:
		return v.Value
	default:
		return nil
	}
}

// unmarshalDynamoDBItem unmarshals a DynamoDB table item into a map of Go types (for snapshot).
func unmarshalDynamoDBItem(attrs map[string]dynamodbtypes.AttributeValue) map[string]any {
	// Pre-allocate with exact capacity to avoid rehashing
	result := make(map[string]any, len(attrs))
	for k, v := range attrs {
		result[k] = unmarshalDynamoDBAttributeValue(v)
	}
	return result
}

// unmarshalDynamoDBAttributeValue unmarshals a single DynamoDB table attribute value into a Go type.
func unmarshalDynamoDBAttributeValue(attr dynamodbtypes.AttributeValue) any {
	switch v := attr.(type) {
	case *dynamodbtypes.AttributeValueMemberS:
		return v.Value
	case *dynamodbtypes.AttributeValueMemberN:
		return v.Value
	case *dynamodbtypes.AttributeValueMemberB:
		return v.Value
	case *dynamodbtypes.AttributeValueMemberSS:
		return v.Value
	case *dynamodbtypes.AttributeValueMemberNS:
		return v.Value
	case *dynamodbtypes.AttributeValueMemberBS:
		return v.Value
	case *dynamodbtypes.AttributeValueMemberM:
		return unmarshalDynamoDBItem(v.Value)
	case *dynamodbtypes.AttributeValueMemberL:
		list := make([]any, len(v.Value))
		for i, item := range v.Value {
			list[i] = unmarshalDynamoDBAttributeValue(item)
		}
		return list
	case *dynamodbtypes.AttributeValueMemberNULL:
		return nil
	case *dynamodbtypes.AttributeValueMemberBOOL:
		return v.Value
	default:
		return nil
	}
}
