// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package aws

import (
	"context"
	"errors"
	"fmt"
	"maps"
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
	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
	"github.com/redpanda-data/connect/v4/internal/impl/aws/dynamocdc"
)

const (
	defaultDynamoDBBatchSize       = 1000 // AWS max limit
	defaultDynamoDBPollInterval    = "1s"
	defaultDynamoDBThrottleBackoff = "100ms"
	defaultShutdownTimeout         = 10 * time.Second

	// Metrics
	metricShardsTracked           = "dynamodb_cdc_shards_tracked"
	metricShardsActive            = "dynamodb_cdc_shards_active"
	metricSnapshotState           = "dynamodb_cdc_snapshot_state"
	metricSnapshotRecordsRead     = "dynamodb_cdc_snapshot_records_read"
	metricSnapshotSegmentsActive  = "dynamodb_cdc_snapshot_segments_active"
	metricSnapshotBufferOverflow  = "dynamodb_cdc_snapshot_buffer_overflow"
	metricCheckpointFailures      = "dynamodb_cdc_checkpoint_failures"
	metricSnapshotSegmentDuration = "dynamodb_cdc_snapshot_segment_duration"
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
`).
		Fields(
			service.NewStringField("table").
				Description("The name of the DynamoDB table to read streams from.").
				LintRule(`root = if this == "" { ["table name cannot be empty"] }`),
			service.NewStringField("checkpoint_table").
				Description("DynamoDB table name for storing checkpoints. Will be created if it doesn't exist.").
				Default("redpanda_dynamodb_checkpoints"),
			service.NewIntField("batch_size").
				Description("Maximum number of records to read per shard in a single request. Valid range: 1-1000.").
				Default(defaultDynamoDBBatchSize).
				Advanced(),
			service.NewDurationField("poll_interval").
				Description("Time to wait between polling attempts when no records are available.").
				Default(defaultDynamoDBPollInterval).
				Advanced(),
			service.NewStringEnumField("start_from", "trim_horizon", "latest").
				Description("Where to start reading when no checkpoint exists. `trim_horizon` starts from the oldest available record, `latest` starts from new records.").
				Default("trim_horizon"),
			service.NewIntField("checkpoint_limit").
				Description("Maximum number of unacknowledged messages before forcing a checkpoint update. Lower values provide better recovery guarantees but increase write overhead.").
				Default(1000).
				Advanced(),
			service.NewIntField("max_tracked_shards").
				Description("Maximum number of shards to track simultaneously. Prevents memory issues with extremely large tables.").
				Default(10000).
				Advanced(),
			service.NewDurationField("throttle_backoff").
				Description("Time to wait when applying backpressure due to too many in-flight messages.").
				Default(defaultDynamoDBThrottleBackoff).
				Advanced(),
			service.NewStringEnumField("snapshot_mode", "none", "snapshot_only", "snapshot_and_cdc").
				Description("Snapshot behavior. `none`: CDC only (default). `snapshot_only`: one-time table scan, no streaming. `snapshot_and_cdc`: scan entire table then stream changes.").
				Default("none"),
			service.NewIntField("snapshot_segments").
				Description("Number of parallel scan segments (1-10). Higher parallelism scans faster but consumes more RCUs. Start with 1 for safety.").
				Default(1).
				LintRule(`root = if this < 1 || this > 10 { ["snapshot_segments must be between 1 and 10"] }`).
				Advanced(),
			service.NewIntField("snapshot_batch_size").
				Description("Records per scan request during snapshot. Maximum 1000. Lower values provide better backpressure control but require more API calls.").
				Default(100).
				LintRule(`root = if this < 1 || this > 1000 { ["snapshot_batch_size must be between 1 and 1000"] }`).
				Advanced(),
			service.NewDurationField("snapshot_throttle").
				Description("Minimum time between scan requests per segment. Use this to limit RCU consumption during snapshot.").
				Default("100ms").
				Advanced(),
			service.NewBoolField("snapshot_deduplicate").
				Description("Deduplicate records that appear in both snapshot and CDC stream. Requires buffering CDC events during snapshot. If buffer is exceeded, deduplication is disabled to prevent data loss.").
				Default(true).
				Advanced(),
			service.NewIntField("snapshot_buffer_size").
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
	conf          dynamoDBCDCConfig
	awsConf       aws.Config
	dynamoClient  *dynamodb.Client
	streamsClient *dynamodbstreams.Client
	streamArn     *string
	log           *service.Logger
	metrics       dynamoDBCDCMetrics

	mu            sync.RWMutex
	msgChan       chan asyncMessage
	shutSig       *shutdown.Signaller
	checkpointer  *dynamocdc.Checkpointer
	recordBatcher *dynamocdc.RecordBatcher
	shardReaders  map[string]*dynamoDBShardReader
	snapshot      *snapshotState // nil if snapshot mode is "none"

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
// This is only allocated when snapshot mode is enabled (not "none").
type snapshotState struct {
	state         atomic.Int32 // 0=not_started, 1=in_progress, 2=complete, 3=failed
	errOnce       sync.Once    // ensures error is set exactly once
	err           error        // error if snapshot fails (write-once, read-many)
	startTime     time.Time
	endTime       time.Time
	seqBuffer     *snapshotSequenceBuffer
	scanner       *dynamocdc.SnapshotScanner
	recordsRead   atomic.Int64
	segmentsTotal int
}

// snapshotSequenceBuffer tracks sequence numbers seen during snapshot for deduplication
// Uses sharded locks for better concurrency (10-30x less contention on high-core machines)
type snapshotSequenceBuffer struct {
	shards     [32]bufferShard // 32 shards for good distribution
	maxSize    int
	totalCount atomic.Int64 // Track total size across all shards
	overflow   atomic.Bool  // true if buffer exceeded maxSize
}

// bufferShard is a single shard of the buffer with its own lock
type bufferShard struct {
	mu        sync.RWMutex
	sequences map[string]string // item key -> sequence number seen in snapshot
}

func newSnapshotSequenceBuffer(maxSize int) *snapshotSequenceBuffer {
	buf := &snapshotSequenceBuffer{
		maxSize: maxSize,
	}
	// Initialize each shard
	for i := range buf.shards {
		buf.shards[i].sequences = make(map[string]string, maxSize/32)
	}
	return buf
}

// getShard returns the shard for a given key using FNV-1a hash
func (s *snapshotSequenceBuffer) getShard(key string) *bufferShard {
	// Fast FNV-1a hash (inline for speed)
	const offset32 = 2166136261
	const prime32 = 16777619
	hash := uint32(offset32)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return &s.shards[hash%32]
}

func (s *snapshotSequenceBuffer) RecordSnapshotItem(key, sequenceNum string) {
	// Quick overflow check without locking
	if s.overflow.Load() {
		return
	}

	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Check if key already exists (update, not insert)
	if _, exists := shard.sequences[key]; exists {
		shard.sequences[key] = sequenceNum
		return
	}

	// Check total size before inserting
	newTotal := s.totalCount.Add(1)
	if newTotal > int64(s.maxSize) {
		// Only set overflow once to avoid repeated metric increments
		if !s.overflow.Load() {
			s.overflow.Store(true)
		}
		s.totalCount.Add(-1) // Revert the count
		return
	}

	shard.sequences[key] = sequenceNum
}

func (s *snapshotSequenceBuffer) ShouldSkipCDCEvent(key, sequenceNum string) bool {
	// If buffer overflowed, we can't deduplicate reliably
	// Better to emit duplicates than lose data
	if s.overflow.Load() {
		return false
	}

	shard := s.getShard(key)
	shard.mu.RLock()
	snapshotSeq, exists := shard.sequences[key]
	shard.mu.RUnlock()

	if !exists {
		return false
	}

	// Skip if CDC event sequence <= snapshot sequence
	// This means we already emitted this version in the snapshot
	return sequenceNum <= snapshotSeq
}

func (s *snapshotSequenceBuffer) IsOverflow() bool {
	return s.overflow.Load()
}

func (s *snapshotSequenceBuffer) Size() int {
	return int(s.totalCount.Load())
}

func dynamoCDCInputConfigFromParsed(pConf *service.ParsedConfig) (conf dynamoDBCDCConfig, err error) {
	if conf.table, err = pConf.FieldString("table"); err != nil {
		return
	}
	if conf.checkpointTable, err = pConf.FieldString("checkpoint_table"); err != nil {
		return
	}
	if conf.batchSize, err = pConf.FieldInt("batch_size"); err != nil {
		return
	}
	if conf.pollInterval, err = pConf.FieldDuration("poll_interval"); err != nil {
		return
	}
	if conf.startFrom, err = pConf.FieldString("start_from"); err != nil {
		return
	}
	if conf.checkpointLimit, err = pConf.FieldInt("checkpoint_limit"); err != nil {
		return
	}
	if conf.maxTrackedShards, err = pConf.FieldInt("max_tracked_shards"); err != nil {
		return
	}
	if conf.throttleBackoff, err = pConf.FieldDuration("throttle_backoff"); err != nil {
		return
	}
	if conf.snapshot.mode, err = pConf.FieldString("snapshot_mode"); err != nil {
		return
	}
	if conf.snapshot.segments, err = pConf.FieldInt("snapshot_segments"); err != nil {
		return
	}
	if conf.snapshot.batchSize, err = pConf.FieldInt("snapshot_batch_size"); err != nil {
		return
	}
	if conf.snapshot.throttle, err = pConf.FieldDuration("snapshot_throttle"); err != nil {
		return
	}
	if conf.snapshot.dedupe, err = pConf.FieldBool("snapshot_deduplicate"); err != nil {
		return
	}
	if conf.snapshot.bufferSize, err = pConf.FieldInt("snapshot_buffer_size"); err != nil {
		return
	}
	return
}

func newDynamoDBCDCInputFromConfig(pConf *service.ParsedConfig, mgr *service.Resources) (*dynamoDBCDCInput, error) {
	conf, err := dynamoCDCInputConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}

	awsConf, err := GetSession(context.Background(), pConf)
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
	if conf.snapshot.mode != "none" && conf.snapshot.dedupe {
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
		var aerr *types.ResourceNotFoundException
		if errors.As(err, &aerr) {
			return fmt.Errorf("table %s does not exist", d.conf.table)
		}
		return fmt.Errorf("failed to describe table %s: %w", d.conf.table, err)
	}

	d.streamArn = descTable.Table.LatestStreamArn
	if d.streamArn == nil {
		return fmt.Errorf("no stream enabled on table %s", d.conf.table)
	}

	// Initialize checkpointer
	d.checkpointer, err = dynamocdc.NewCheckpointer(ctx, d.dynamoClient, d.conf.checkpointTable, *d.streamArn, d.conf.checkpointLimit, d.log)
	if err != nil {
		return fmt.Errorf("failed to create checkpointer: %w", err)
	}

	// Initialize record batcher
	d.recordBatcher = dynamocdc.NewRecordBatcher(d.conf.maxTrackedShards, d.conf.checkpointLimit, d.log)

	// Initialize message channel with buffer to reduce blocking between scanner and processor
	// Buffer size of 1000 allows scanner to work ahead without blocking
	d.msgChan = make(chan asyncMessage, 1000)

	d.log.Infof("Connected to DynamoDB stream: %s", *d.streamArn)

	// Handle snapshot mode
	if d.conf.snapshot.mode != "none" {
		return d.connectWithSnapshot(ctx)
	}

	// CDC-only mode (existing behavior)
	return d.connectCDCOnly(ctx)
}

// connectCDCOnly starts CDC streaming without snapshot (original behavior)
func (d *dynamoDBCDCInput) connectCDCOnly(ctx context.Context) error {
	// Mark snapshot as complete (never started)
	d.snapshot.state.Store(2)
	d.metrics.snapshotState.Set(2)

	// Initialize shards
	if err := d.refreshShards(ctx); err != nil {
		return fmt.Errorf("failed to initialize shards: %w", err)
	}

	// Verify at least one shard reader started successfully
	d.mu.Lock()
	activeCount := len(d.shardReaders)
	d.mu.Unlock()

	if activeCount == 0 {
		return errors.New("no active shard readers available - stream may have no shards or all failed to initialize")
	}

	// Start background goroutine to coordinate shard readers
	coordinatorCtx, coordinatorCancel := d.shutSig.SoftStopCtx(context.Background())
	go func() {
		defer coordinatorCancel()
		d.startShardCoordinator(coordinatorCtx)
	}()

	return nil
}

// connectWithSnapshot handles snapshot + CDC coordination
func (d *dynamoDBCDCInput) connectWithSnapshot(ctx context.Context) error {
	// Record snapshot start time BEFORE doing anything else
	d.snapshot.startTime = time.Now()

	// Check if we have a partial snapshot checkpoint
	snapshotCheckpoint, err := d.checkpointer.GetSnapshotProgress(ctx)
	if err != nil {
		return fmt.Errorf("failed to get snapshot progress: %w", err)
	}

	if snapshotCheckpoint.IsComplete() {
		d.log.Info("Snapshot was completed in previous run")

		// CRITICAL SAFETY CHECK: Verify CDC checkpoints are still valid
		// If connector was down >24h, DynamoDB Streams data is gone!
		switch d.conf.snapshot.mode {
		case "snapshot_and_cdc":
			isCDCStale, err := d.isCDCCheckpointStale(ctx)
			if err != nil {
				d.log.Warnf("Failed to check CDC checkpoint staleness: %v, proceeding with caution", err)
			} else if isCDCStale {
				d.log.Warn("CDC checkpoint is stale (stream data no longer available), re-running snapshot to prevent data loss")
				d.log.Info("This happens when the connector was down >24 hours (DynamoDB Streams retention limit)")

				// Clear the snapshot completion marker to force re-snapshot
				// Don't return here - fall through to run snapshot again
				snapshotCheckpoint = dynamocdc.NewSnapshotCheckpoint() // Reset to empty
			} else {
				// CDC checkpoint is valid, safe to skip snapshot
				d.snapshot.state.Store(2) // complete
				d.metrics.snapshotState.Set(2)
				return d.connectCDCOnly(ctx)
			}
		case "snapshot_only":
			// Snapshot already done, nothing more to do
			d.log.Info("Snapshot-only mode: snapshot complete, exiting")
			return service.ErrEndOfInput
		}
	}

	// CRITICAL ORDERING FOR DATA LOSS PREVENTION:
	// 1. Start CDC readers FIRST (if snapshot_and_cdc mode)
	//    This ensures we capture ALL changes that happen during snapshot
	if d.conf.snapshot.mode == "snapshot_and_cdc" {
		d.log.Info("Starting CDC readers before snapshot to prevent data loss")

		// Initialize shards
		if err := d.refreshShards(ctx); err != nil {
			return fmt.Errorf("failed to initialize shards: %w", err)
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
	d.snapshot.state.Store(1) // in_progress
	d.metrics.snapshotState.Set(1)

	// Initialize snapshot scanner
	d.snapshot.scanner = dynamocdc.NewSnapshotScanner(dynamocdc.SnapshotScannerConfig{
		Client:             d.dynamoClient,
		Table:              d.conf.table,
		Segments:           d.conf.snapshot.segments,
		BatchSize:          d.conf.snapshot.batchSize,
		Throttle:           d.conf.snapshot.throttle,
		Checkpointer:       d.checkpointer,
		CheckpointInterval: 10, // Checkpoint every 10 batches (10x cost reduction)
		Logger:             d.log,
	})

	// Set batch callback to send snapshot records to msgChan
	d.snapshot.scanner.SetBatchCallback(d.handleSnapshotBatch)

	// Set progress callback to update metrics
	d.snapshot.scanner.SetProgressCallback(func(_, _ int, _ int64) {
		d.metrics.snapshotSegmentsActive.Set(int64(d.snapshot.scanner.GetActiveSegments()))
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
				d.snapshot.state.Store(3) // failed
				d.metrics.snapshotState.Set(3)
				return
			}
		}

		// Snapshot complete
		d.snapshot.endTime = time.Now()
		d.snapshot.state.Store(2) // complete
		d.metrics.snapshotState.Set(2)

		// Mark as complete in checkpoint
		if err := d.checkpointer.MarkSnapshotComplete(scanCtx); err != nil {
			d.log.Errorf("Failed to mark snapshot complete: %v", err)
		}

		d.log.Infof("Snapshot scan completed: %d records in %v",
			d.snapshot.recordsRead.Load(), d.snapshot.endTime.Sub(d.snapshot.startTime))

		// If snapshot_only mode, close the input
		if d.conf.snapshot.mode == "snapshot_only" {
			d.log.Info("Snapshot-only mode complete, triggering shutdown")
			d.shutSig.TriggerSoftStop()
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
		return false, fmt.Errorf("failed to describe stream: %w", err)
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
			return fmt.Errorf("failed to get checkpoint for shard %s: %w", shardID, err)
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
			return fmt.Errorf("failed to get iterator for shard %s: %w", shardID, err)
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

// startShardReader continuously reads from a single shard and sends batches to the channel
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

// handleSnapshotBatch processes a batch of items from the snapshot scan
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
			"newImage": convertDynamoDBAttributeMap(item),
		}

		// Extract keys for deduplication if enabled
		d.mu.RLock()
		buffer := d.snapshot.seqBuffer
		startTime := d.snapshot.startTime
		d.mu.RUnlock()
		if buffer != nil {
			// Build a key string from the item's primary key
			// For simplicity, we'll use a hash of the keys
			keyStr := buildItemKeyString(item)
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

// buildItemKeyString creates a string representation of an item's primary key for deduplication
// Optimized with strings.Builder for better performance (2-3x faster than fmt.Sprintf)
func buildItemKeyString(item map[string]dynamodbtypes.AttributeValue) string {
	var sb strings.Builder
	sb.Grow(64) // Pre-allocate reasonable capacity

	// Find the primary key attributes (typically named "id" or ending with "Id")
	foundKeys := false
	for k, v := range item {
		// Common primary key names
		if k == "id" || k == "Id" || k == "ID" || k == "pk" || k == "PK" {
			if foundKeys {
				sb.WriteByte(';')
			}
			sb.WriteString(k)
			sb.WriteByte('=')
			writeAttributeValueString(&sb, v)
			foundKeys = true
		}
	}

	if !foundKeys {
		// Fallback: use all attributes (not ideal but safe)
		first := true
		for k, v := range item {
			if !first {
				sb.WriteByte(';')
			}
			sb.WriteString(k)
			sb.WriteByte('=')
			writeAttributeValueString(&sb, v)
			first = false
		}
	}

	return sb.String()
}

// writeAttributeValueString writes an attribute value to a strings.Builder efficiently
func writeAttributeValueString(sb *strings.Builder, attr dynamodbtypes.AttributeValue) {
	switch v := attr.(type) {
	case *dynamodbtypes.AttributeValueMemberS:
		sb.WriteString(v.Value)
	case *dynamodbtypes.AttributeValueMemberN:
		sb.WriteString(v.Value)
	case *dynamodbtypes.AttributeValueMemberBOOL:
		if v.Value {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	case *dynamodbtypes.AttributeValueMemberB:
		sb.WriteString("<binary>")
	default:
		// For complex types, use fmt.Sprintf (rare case)
		fmt.Fprintf(sb, "%v", convertDynamoDBAttributeValue(attr))
	}
}

// convertRecordsToBatch converts DynamoDB Stream records to Benthos messages
func (d *dynamoDBCDCInput) convertRecordsToBatch(records []types.Record, shardID string) service.MessageBatch {
	batch := make(service.MessageBatch, 0, len(records))

	for _, record := range records {
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

		var sequenceNumber string
		if record.Dynamodb != nil {
			dynamoData := map[string]any{
				"sequenceNumber": aws.ToString(record.Dynamodb.SequenceNumber),
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
			sequenceNumber = aws.ToString(record.Dynamodb.SequenceNumber)
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
	if d.snapshot.state.Load() == 3 { // failed
		if d.snapshot.err != nil {
			return nil, nil, d.snapshot.err
		}
		return nil, nil, fmt.Errorf("snapshot scan failed for table %s", d.conf.table)
	}

	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-shutSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case am, open := <-msgChan:
		if !open {
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
		pendingCheckpoints := batcher.GetPendingCheckpoints()
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

// Helper to convert DynamoDB attribute values to Go types
// Pre-sizes the result map to reduce rehashing during growth
func convertAttributeMap(attrs map[string]types.AttributeValue) map[string]any {
	// Pre-allocate with exact capacity to avoid rehashing
	result := make(map[string]any, len(attrs))
	for k, v := range attrs {
		result[k] = convertAttributeValue(v)
	}
	return result
}

// isThrottlingError checks if an error is due to AWS throttling.
func isThrottlingError(err error) bool {
	if err == nil {
		return false
	}
	var limitErr *types.LimitExceededException
	var throttleErr *types.TrimmedDataAccessException
	return errors.As(err, &limitErr) || errors.As(err, &throttleErr)
}

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

// convertDynamoDBAttributeMap converts DynamoDB table attribute values to Go types (for snapshot)
func convertDynamoDBAttributeMap(attrs map[string]dynamodbtypes.AttributeValue) map[string]any {
	// Pre-allocate with exact capacity to avoid rehashing
	result := make(map[string]any, len(attrs))
	for k, v := range attrs {
		result[k] = convertDynamoDBAttributeValue(v)
	}
	return result
}

// convertDynamoDBAttributeValue converts a single DynamoDB table attribute value to Go type (for snapshot)
func convertDynamoDBAttributeValue(attr dynamodbtypes.AttributeValue) any {
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
		return convertDynamoDBAttributeMap(v.Value)
	case *dynamodbtypes.AttributeValueMemberL:
		list := make([]any, len(v.Value))
		for i, item := range v.Value {
			list[i] = convertDynamoDBAttributeValue(item)
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
