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
	defaultAPICallTimeout          = 30 * time.Second // Timeout for AWS API calls
	shardRefreshInterval           = 30 * time.Second // Interval for refreshing shard list
	shardCleanupInterval           = 5 * time.Minute  // Interval for cleaning up exhausted shards

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
- Multi-table streaming with auto-discovery by tags or explicit table lists

### Table Discovery Modes

This input supports three table discovery modes:

- `+"`single`"+` (default) - Stream from a single table specified by the `+"`table`"+` field
- `+"`tag`"+` - Auto-discover and stream from multiple tables based on DynamoDB table tags. Use `+"`table_tag_key`"+` and optionally `+"`table_tag_value`"+` to filter tables
- `+"`includelist`"+` - Stream from an explicit list of tables specified in the `+"`tables`"+` field

When using `+"`tag`"+` or `+"`includelist`"+` mode, the connector will stream from all matching tables simultaneously. Each table maintains its own checkpoint state. Use `+"`table_discovery_interval`"+` to periodically rescan for new tables (useful for dynamically tagged tables).

### Prerequisites

The source DynamoDB table(s) must have streams enabled. You can enable streams with one of these view types:

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
			service.NewStringListField("tables").
				Description("List of table names to stream from. For single table mode, provide one table. For multi-table mode, provide multiple tables.").
				Default([]any{}),
			service.NewStringEnumField("table_discovery_mode", "single", "tag", "includelist").
				Description("Table discovery mode. `single`: stream from tables specified in `tables` list. `tag`: auto-discover tables by tags (ignores `tables` field). `includelist`: stream from tables in `tables` list (alias for `single`, kept for compatibility).").
				Default("single").
				Advanced(),
			service.NewStringField("table_tag_key").
				Description("Tag key to filter tables by when `table_discovery_mode` is `tag`. Only tables with this tag will be discovered.").
				Default("").
				Advanced(),
			service.NewStringField("table_tag_value").
				Description("Optional tag value to match when `table_discovery_mode` is `tag`. If empty, any table with the tag key will match.").
				Default("").
				Advanced(),
			service.NewDurationField("table_discovery_interval").
				Description("Interval for rescanning and discovering new tables when using `tag` or `includelist` mode. Set to 0 to disable periodic rescanning.").
				Default("5m").
				Advanced(),
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
    tables: [my-table]
    region: us-east-1
`,
		).
		Example(
			"Start from latest",
			"Only process new changes, ignoring existing stream data.",
			`
input:
  aws_dynamodb_cdc:
    tables: [orders]
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
    tables: [products]
    snapshot_mode: snapshot_and_cdc
    snapshot_segments: 5
    region: us-east-1
`,
		).
		Example(
			"Auto-discover tables by tag",
			"Automatically discover and stream from all tables with a specific tag.",
			`
input:
  aws_dynamodb_cdc:
    table_discovery_mode: tag
    table_tag_key: "stream-enabled"
    table_tag_value: "true"
    table_discovery_interval: 5m
    region: us-east-1
`,
		).
		Example(
			"Stream from multiple specific tables",
			"Stream from an explicit list of tables simultaneously.",
			`
input:
  aws_dynamodb_cdc:
    table_discovery_mode: includelist
    tables:
      - orders
      - customers
      - products
    region: us-west-2
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
	tables                 []string
	tableDiscoveryMode     string
	tableTagKey            string
	tableTagValue          string
	tableDiscoveryInterval time.Duration
	checkpointTable        string
	batchSize              int
	pollInterval           time.Duration
	startFrom              string
	checkpointLimit        int
	maxTrackedShards       int
	throttleBackoff        time.Duration
	snapshot               snapshotConfig
}

type tableStream struct {
	tableName     string
	streamArn     string
	keySchema     []dynamodbtypes.KeySchemaElement // Table's primary key schema for deduplication
	checkpointer  *dynamocdc.Checkpointer
	recordBatcher *dynamocdc.RecordBatcher

	mu           sync.RWMutex // Level 2 lock - never hold when acquiring dynamoDBCDCInput.mu
	shardReaders map[string]*dynamoDBShardReader
	snapshot     *snapshotState
}

// dynamoDBCDCInput is the main input struct for DynamoDB CDC.
//
// Lock hierarchy: always acquire d.mu before ts.mu to prevent deadlocks.
// Never hold ts.mu when acquiring d.mu.
type dynamoDBCDCInput struct {
	conf          dynamoDBCDCConfig
	awsConf       aws.Config
	dynamoClient  *dynamodb.Client
	streamsClient *dynamodbstreams.Client
	log           *service.Logger
	metrics       dynamoDBCDCMetrics

	mu           sync.RWMutex            // Level 1 lock - acquire before tableStream.mu (protects tableStreams map only)
	msgChan      chan asyncMessage       // immutable after Connect()
	shutSig      *shutdown.Signaller     // immutable after Connect()
	tableStreams map[string]*tableStream // keyed by table name

	// Legacy fields for backward compatibility with single table mode
	streamArn     *string
	keySchema     []dynamodbtypes.KeySchemaElement // Table's primary key schema for deduplication
	checkpointer  *dynamocdc.Checkpointer
	recordBatcher *dynamocdc.RecordBatcher
	shardReaders  map[string]*dynamoDBShardReader
	snapshot      *snapshotState // nil if snapshot mode is "none"

	pendingAcks       sync.WaitGroup
	backgroundWorkers sync.WaitGroup // Tracks background goroutines for proper cleanup
	closed            atomic.Bool
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

// snapshotSequenceBuffer tracks sequence numbers seen during snapshot for deduplication.
//
// Architecture: Lock-free sharded hash table design
//
// Instead of a single map[string]string with one lock (which would cause severe contention
// with parallel snapshot segment readers), this uses 32 independent shards, each with its
// own lock. Keys are distributed across shards using FNV-1a hash.
//
// Concurrency improvement: 10-30x less lock contention on high-core machines
//
// Example: On a 64-core machine scanning a 100M row table with 10 parallel segments:
//   - Single lock: All 10 goroutines fight for 1 lock = ~90% time waiting
//   - 32 shards:   Each goroutine gets its own shard 97% of the time = ~3% time waiting
//
// Why 32 shards? Power-of-2 for fast modulo (hash%32), and matches typical core counts.
type snapshotSequenceBuffer struct {
	shards           [32]bufferShard // 32 independent shards with separate locks
	maxSize          int
	totalCount       atomic.Int64 // Track total size across all shards (lock-free)
	overflow         atomic.Bool  // true if buffer exceeded maxSize
	overflowReported atomic.Bool  // true if overflow has been reported to metrics (emit once)
}

// bufferShard is a single shard of the buffer with its own lock.
// Each shard handles ~1/32 of all keys (on average, due to FNV-1a distribution).
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

// getShard returns the shard for a given key using FNV-1a hash.
//
// Performance rationale: This function is called millions of times during snapshot scans
// and is a hot path. The inline FNV-1a implementation provides:
//
//  1. Zero allocations (vs hash/fnv.New32a which allocates)
//  2. ~2-3x faster than the standard library version
//  3. Excellent key distribution across 32 shards
//
// The sharded design provides 10-30x better concurrency on high-core machines by
// reducing lock contention. With 32 shards and FNV-1a's good distribution, most
// goroutines access different shards simultaneously rather than fighting over one lock.
//
// FNV-1a algorithm: https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
func (s *snapshotSequenceBuffer) getShard(key string) *bufferShard {
	// FNV-1a constants (32-bit version)
	const offset32 = 2166136261 // FNV offset basis
	const prime32 = 16777619    // FNV prime

	hash := uint32(offset32)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i]) // XOR with byte
		hash *= prime32        // Multiply by FNV prime
	}
	return &s.shards[hash%32] // Map to one of 32 shards
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

// validateDynamoDBCDCConfig validates the configuration for consistency
func validateDynamoDBCDCConfig(conf dynamoDBCDCConfig) error {
	// Validate tag discovery mode requirements
	if conf.tableDiscoveryMode == "tag" {
		if conf.tableTagKey == "" {
			return errors.New("table_tag_key is required when table_discovery_mode is 'tag'")
		}
	}

	// Validate tables list for non-tag modes
	if conf.tableDiscoveryMode != "tag" && len(conf.tables) == 0 {
		return errors.New("tables list cannot be empty when table_discovery_mode is 'single' or 'includelist'")
	}

	// Validate snapshot configuration
	if conf.snapshot.segments < 1 || conf.snapshot.segments > 10 {
		return errors.New("snapshot_segments must be between 1 and 10")
	}

	if conf.snapshot.batchSize < 1 || conf.snapshot.batchSize > 1000 {
		return errors.New("snapshot_batch_size must be between 1 and 1000")
	}

	return nil
}

func dynamoCDCInputConfigFromParsed(pConf *service.ParsedConfig) (conf dynamoDBCDCConfig, err error) {
	if conf.tables, err = pConf.FieldStringList("tables"); err != nil {
		return
	}
	if conf.tableDiscoveryMode, err = pConf.FieldString("table_discovery_mode"); err != nil {
		return
	}
	if conf.tableTagKey, err = pConf.FieldString("table_tag_key"); err != nil {
		return
	}
	if conf.tableTagValue, err = pConf.FieldString("table_tag_value"); err != nil {
		return
	}
	if conf.tableDiscoveryInterval, err = pConf.FieldDuration("table_discovery_interval"); err != nil {
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

	// Validate configuration
	if err := validateDynamoDBCDCConfig(conf); err != nil {
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
		tableStreams: make(map[string]*tableStream),
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

// discoverTables discovers tables based on the configured discovery mode
func (d *dynamoDBCDCInput) discoverTables(ctx context.Context) ([]string, error) {
	switch d.conf.tableDiscoveryMode {
	case "single", "includelist":
		if len(d.conf.tables) == 0 {
			return nil, errors.New("tables list cannot be empty when table_discovery_mode is single or includelist")
		}
		return d.conf.tables, nil

	case "tag":
		if d.conf.tableTagKey == "" {
			return nil, errors.New("table_tag_key cannot be empty when table_discovery_mode is tag")
		}
		return d.discoverTablesByTag(ctx)

	default:
		return nil, fmt.Errorf("unsupported table_discovery_mode: %s", d.conf.tableDiscoveryMode)
	}
}

// discoverTablesByTag discovers tables that match the configured tag key/value
func (d *dynamoDBCDCInput) discoverTablesByTag(ctx context.Context) ([]string, error) {
	var matchingTables []string
	var lastEvaluatedTableName *string

	// List all tables (paginated)
	for {
		listInput := &dynamodb.ListTablesInput{
			Limit: aws.Int32(100),
		}
		if lastEvaluatedTableName != nil {
			listInput.ExclusiveStartTableName = lastEvaluatedTableName
		}

		listOutput, err := d.dynamoClient.ListTables(ctx, listInput)
		if err != nil {
			return nil, fmt.Errorf("failed to list tables: %w", err)
		}

		// Check each table for matching tags
		for _, tableName := range listOutput.TableNames {
			// Get table ARN first (with timeout)
			descCtx, descCancel := context.WithTimeout(ctx, defaultAPICallTimeout)
			descOutput, err := d.dynamoClient.DescribeTable(descCtx, &dynamodb.DescribeTableInput{
				TableName: aws.String(tableName),
			})
			descCancel()
			if err != nil {
				d.log.Warnf("Failed to describe table %s: %v", tableName, err)
				continue
			}

			if descOutput.Table.TableArn == nil {
				d.log.Warnf("Table %s has no ARN, skipping", tableName)
				continue
			}

			// List tags for the table (with pagination and timeout)
			var nextToken *string
			foundMatch := false
			for {
				tagsCtx, tagsCancel := context.WithTimeout(ctx, defaultAPICallTimeout)
				tagsOutput, err := d.dynamoClient.ListTagsOfResource(tagsCtx, &dynamodb.ListTagsOfResourceInput{
					ResourceArn: descOutput.Table.TableArn,
					NextToken:   nextToken,
				})
				tagsCancel()
				if err != nil {
					d.log.Warnf("Failed to list tags for table %s: %v", tableName, err)
					break
				}

				// Check if table has matching tag
				for _, tag := range tagsOutput.Tags {
					if tag.Key != nil && *tag.Key == d.conf.tableTagKey {
						// If tag value is specified, check for match
						if d.conf.tableTagValue == "" || (tag.Value != nil && *tag.Value == d.conf.tableTagValue) {
							matchingTables = append(matchingTables, tableName)
							d.log.Infof("Discovered table %s with tag %s=%s", tableName, d.conf.tableTagKey,
								aws.ToString(tag.Value))
							foundMatch = true
							break
						}
					}
				}

				if foundMatch || tagsOutput.NextToken == nil {
					break
				}
				nextToken = tagsOutput.NextToken
			}
		}

		lastEvaluatedTableName = listOutput.LastEvaluatedTableName
		if lastEvaluatedTableName == nil {
			break
		}
	}

	if len(matchingTables) == 0 {
		d.log.Warnf("No tables found with tag %s=%s", d.conf.tableTagKey, d.conf.tableTagValue)
	}

	return matchingTables, nil
}

func (d *dynamoDBCDCInput) Connect(ctx context.Context) error {
	d.dynamoClient = dynamodb.NewFromConfig(d.awsConf)
	d.streamsClient = dynamodbstreams.NewFromConfig(d.awsConf)

	// Initialize message channel with buffer to reduce blocking between scanner and processor
	// Buffer size of 1000 allows scanner to work ahead without blocking
	d.msgChan = make(chan asyncMessage, 1000)

	// Discover tables based on configured mode
	tables, err := d.discoverTables(ctx)
	if err != nil {
		return fmt.Errorf("failed to discover tables: %w", err)
	}

	if len(tables) == 0 {
		return errors.New("no tables found to stream from")
	}

	d.log.Infof("Discovered %d table(s) to stream: %v", len(tables), tables)

	// Use optimized single-table code path when there is exactly one table
	// This covers both "single" mode and "includelist" mode with one table
	if len(tables) == 1 {
		return d.connectSingleTable(ctx, tables[0])
	}

	// Multi-table mode (includelist with >1 table, or tag discovery)
	return d.connectMultipleTables(ctx, tables)
}

// connectSingleTable handles the single table mode (legacy behavior)
func (d *dynamoDBCDCInput) connectSingleTable(ctx context.Context, tableName string) error {
	// Get stream ARN
	descTable, err := d.dynamoClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &tableName,
	})
	if err != nil {
		var aerr *types.ResourceNotFoundException
		if errors.As(err, &aerr) {
			return fmt.Errorf("table %s does not exist", tableName)
		}
		return fmt.Errorf("failed to describe table %s: %w", tableName, err)
	}

	d.streamArn = descTable.Table.LatestStreamArn
	if d.streamArn == nil {
		return fmt.Errorf("no stream enabled on table %s", tableName)
	}

	// Store key schema for snapshot deduplication
	d.keySchema = descTable.Table.KeySchema

	// Initialize checkpointer
	d.checkpointer, err = dynamocdc.NewCheckpointer(ctx, d.dynamoClient, d.conf.checkpointTable, *d.streamArn, d.conf.checkpointLimit, d.log)
	if err != nil {
		return fmt.Errorf("failed to create checkpointer: %w", err)
	}

	// Initialize record batcher
	d.recordBatcher = dynamocdc.NewRecordBatcher(d.conf.maxTrackedShards, d.conf.checkpointLimit, d.log)

	d.log.Infof("Connected to DynamoDB stream: %s", *d.streamArn)

	// Handle snapshot mode
	if d.conf.snapshot.mode != "none" {
		return d.connectWithSnapshot(ctx, tableName)
	}

	// CDC-only mode (existing behavior)
	return d.connectCDCOnly(ctx)
}

// connectMultipleTables handles streaming from multiple tables simultaneously
func (d *dynamoDBCDCInput) connectMultipleTables(ctx context.Context, tables []string) error {
	// Initialize each table stream
	for _, tableName := range tables {
		if _, err := d.initializeTableStream(ctx, tableName); err != nil {
			d.log.Errorf("Failed to initialize table stream for %s: %v", tableName, err)
			// Continue with other tables rather than failing completely
			continue
		}
	}

	d.mu.RLock()
	tableCount := len(d.tableStreams)
	d.mu.RUnlock()

	if tableCount == 0 {
		return errors.New("failed to initialize any table streams")
	}

	d.log.Infof("Successfully initialized %d table stream(s)", tableCount)

	// Start coordinators for all tables
	d.mu.RLock()
	for tableName, ts := range d.tableStreams {
		d.startTableCoordinator(tableName, ts)
	}
	d.mu.RUnlock()

	// Start periodic table discovery if enabled
	if d.conf.tableDiscoveryInterval > 0 && d.conf.tableDiscoveryMode != "single" {
		d.startBackgroundWorker("periodic table discovery", d.periodicTableDiscovery)
	}

	return nil
}

// initializeTableStream creates and initializes a tableStream for a given table.
// Returns (true, nil) if a new stream was created, (false, nil) if it already existed.
func (d *dynamoDBCDCInput) initializeTableStream(ctx context.Context, tableName string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if already initialized
	if _, exists := d.tableStreams[tableName]; exists {
		d.log.Debugf("Table stream for %s already initialized", tableName)
		return false, nil
	}

	// Get stream ARN (with timeout)
	descCtx, descCancel := context.WithTimeout(ctx, defaultAPICallTimeout)
	descTable, err := d.dynamoClient.DescribeTable(descCtx, &dynamodb.DescribeTableInput{
		TableName: &tableName,
	})
	descCancel()
	if err != nil {
		return false, fmt.Errorf("failed to describe table %s: %w", tableName, err)
	}

	if descTable.Table.LatestStreamArn == nil {
		return false, fmt.Errorf("no stream enabled on table %s", tableName)
	}

	streamArn := *descTable.Table.LatestStreamArn

	// Initialize checkpointer for this table
	checkpointer, err := dynamocdc.NewCheckpointer(ctx, d.dynamoClient, d.conf.checkpointTable, streamArn, d.conf.checkpointLimit, d.log)
	if err != nil {
		return false, fmt.Errorf("failed to create checkpointer for table %s: %w", tableName, err)
	}

	// Initialize record batcher for this table
	recordBatcher := dynamocdc.NewRecordBatcher(d.conf.maxTrackedShards, d.conf.checkpointLimit, d.log)

	// Initialize snapshot state for this table
	var snapshot *snapshotState
	if d.conf.snapshot.mode != "none" {
		snapshot = &snapshotState{
			segmentsTotal: d.conf.snapshot.segments,
		}
		if d.conf.snapshot.dedupe {
			snapshot.seqBuffer = newSnapshotSequenceBuffer(d.conf.snapshot.bufferSize)
		}
	}

	// Create table stream
	ts := &tableStream{
		tableName:     tableName,
		streamArn:     streamArn,
		keySchema:     descTable.Table.KeySchema,
		checkpointer:  checkpointer,
		recordBatcher: recordBatcher,
		shardReaders:  make(map[string]*dynamoDBShardReader),
		snapshot:      snapshot,
	}

	d.tableStreams[tableName] = ts
	d.log.Infof("Initialized table stream for %s (stream ARN: %s)", tableName, streamArn)

	return true, nil
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
	d.backgroundWorkers.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				d.log.Errorf("Shard coordinator panicked: %v", r)
			}
			d.backgroundWorkers.Done()
		}()
		defer coordinatorCancel()
		d.startShardCoordinator(coordinatorCtx)
	}()

	return nil
}

// connectWithSnapshot handles snapshot + CDC coordination
func (d *dynamoDBCDCInput) connectWithSnapshot(ctx context.Context, tableName string) error {
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
		d.backgroundWorkers.Add(1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					d.log.Errorf("CDC shard coordinator panicked during snapshot: %v", r)
				}
				d.backgroundWorkers.Done()
			}()
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
		Table:              tableName,
		Segments:           d.conf.snapshot.segments,
		BatchSize:          d.conf.snapshot.batchSize,
		Throttle:           d.conf.snapshot.throttle,
		Checkpointer:       d.checkpointer,
		CheckpointInterval: 10, // Checkpoint every 10 batches (10x cost reduction)
		Logger:             d.log,
	})

	// Set batch callback to send snapshot records to msgChan
	d.snapshot.scanner.SetBatchCallback(func(ctx context.Context, items []map[string]dynamodbtypes.AttributeValue, segment int) error {
		return d.handleSnapshotBatch(ctx, items, segment, tableName)
	})

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
	d.backgroundWorkers.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				d.log.Errorf("Snapshot scanner panicked: %v", r)
				d.snapshot.errOnce.Do(func() {
					d.snapshot.err = fmt.Errorf("snapshot scanner panicked: %v", r)
				})
				d.snapshot.state.Store(3) // failed
				d.metrics.snapshotState.Set(3)
			}
			d.backgroundWorkers.Done()
		}()
		defer scanCancel()
		d.log.Info("Starting snapshot scan")
		if err := d.snapshot.scanner.Scan(scanCtx, snapshotCheckpoint); err != nil {
			if !errors.Is(err, context.Canceled) {
				wrappedErr := fmt.Errorf("snapshot scan failed for table %s: %w", tableName, err)
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

	refreshTicker := time.NewTicker(shardRefreshInterval)
	defer refreshTicker.Stop()

	cleanupTicker := time.NewTicker(shardCleanupInterval)
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

		// Update active shards metric (acquire lock once instead of per-shard)
		activeCount := 0
		for shardID := range activeShards {
			if reader, exists := currentReaders[shardID]; exists && !reader.exhausted {
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
			refreshCtx, refreshCancel := context.WithTimeout(ctx, defaultAPICallTimeout)
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

// periodicTableDiscovery periodically rediscovers tables and initializes new ones
func (d *dynamoDBCDCInput) periodicTableDiscovery(ctx context.Context) {
	ticker := time.NewTicker(d.conf.tableDiscoveryInterval)
	defer ticker.Stop()

	d.log.Infof("Starting periodic table discovery every %v", d.conf.tableDiscoveryInterval)

	for {
		select {
		case <-ctx.Done():
			d.log.Info("Stopping periodic table discovery")
			return
		case <-ticker.C:
			tables, err := d.discoverTables(ctx)
			if err != nil {
				d.log.Errorf("Failed to discover tables: %v", err)
				continue
			}

			// Initialize any new tables
			for _, tableName := range tables {
				isNew, err := d.initializeTableStream(ctx, tableName)
				if err != nil {
					d.log.Errorf("Failed to initialize new table stream for %s: %v", tableName, err)
					continue
				}

				// Only start a coordinator for newly discovered tables
				if !isNew {
					continue
				}

				d.mu.RLock()
				ts, exists := d.tableStreams[tableName]
				d.mu.RUnlock()

				if exists && ts != nil {
					d.startTableCoordinator(tableName, ts)
				}
			}
		}
	}
}

// startTableStreamCoordinator manages shard readers for a specific table stream
func (d *dynamoDBCDCInput) startTableStreamCoordinator(ctx context.Context, tableName string, ts *tableStream) {
	d.log.Infof("Starting coordinator for table stream: %s", tableName)
	defer d.log.Infof("Stopped coordinator for table stream: %s", tableName)

	// Initialize shards for this table
	if err := d.refreshTableShards(ctx, tableName, ts); err != nil {
		d.log.Errorf("Failed to initialize shards for table %s: %v", tableName, err)
		return
	}

	// Track running shard readers for this table
	activeShards := make(map[string]context.CancelFunc)
	defer func() {
		// Cancel all active shard readers on shutdown
		for _, cancelFn := range activeShards {
			cancelFn()
		}
	}()

	refreshTicker := time.NewTicker(shardRefreshInterval)
	defer refreshTicker.Stop()

	cleanupTicker := time.NewTicker(shardCleanupInterval)
	defer cleanupTicker.Stop()

	for {
		// Start new shard readers for any new shards
		ts.mu.RLock()
		for shardID, reader := range ts.shardReaders {
			if _, exists := activeShards[shardID]; !exists && !reader.exhausted {
				shardCtx, shardCancel := context.WithCancel(ctx)
				activeShards[shardID] = shardCancel
				go d.startTableShardReader(shardCtx, tableName, ts, shardID)
			}
		}
		ts.mu.RUnlock()

		// Update active shards metric
		activeCount := 0
		ts.mu.RLock()
		for shardID := range activeShards {
			reader, exists := ts.shardReaders[shardID]
			if exists && !reader.exhausted {
				activeCount++
			}
		}
		ts.mu.RUnlock()

		select {
		case <-ctx.Done():
			return
		case <-refreshTicker.C:
			// Refresh shards periodically to discover new shards
			refreshCtx, refreshCancel := context.WithTimeout(ctx, defaultAPICallTimeout)
			if err := d.refreshTableShards(refreshCtx, tableName, ts); err != nil && !errors.Is(err, context.Canceled) {
				d.log.Warnf("Failed to refresh shards for table %s: %v", tableName, err)
			}
			refreshCancel()
		case <-cleanupTicker.C:
			// Clean up exhausted shards
			d.cleanupTableExhaustedShards(tableName, ts, activeShards)
		}
	}
}

// refreshTableShards refreshes shard information for a specific table
func (d *dynamoDBCDCInput) refreshTableShards(ctx context.Context, tableName string, ts *tableStream) error {
	streamDesc, err := d.streamsClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: &ts.streamArn,
	})
	if err != nil {
		return err
	}

	// Collect new shards to add
	type shardToAdd struct {
		shardID  string
		iterator *string
	}
	var newShards []shardToAdd

	for _, shard := range streamDesc.StreamDescription.Shards {
		shardID := *shard.ShardId

		// Check if shard already exists
		ts.mu.RLock()
		_, exists := ts.shardReaders[shardID]
		ts.mu.RUnlock()
		if exists {
			continue
		}

		// Check checkpoint
		checkpoint, err := ts.checkpointer.Get(ctx, shardID)
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
			d.log.Infof("Resuming shard %s (table %s) from checkpoint: %s", shardID, tableName, checkpoint)
		} else {
			if d.conf.startFrom == "latest" {
				iteratorType = types.ShardIteratorTypeLatest
			} else {
				iteratorType = types.ShardIteratorTypeTrimHorizon
			}
			d.log.Infof("Starting shard %s (table %s) from %s", shardID, tableName, d.conf.startFrom)
		}

		// Get shard iterator
		iter, err := d.streamsClient.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
			StreamArn:         &ts.streamArn,
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

	// Add all new shard readers
	if len(newShards) > 0 {
		ts.mu.Lock()
		for _, s := range newShards {
			if _, exists := ts.shardReaders[s.shardID]; !exists {
				ts.shardReaders[s.shardID] = &dynamoDBShardReader{
					shardID:   s.shardID,
					iterator:  s.iterator,
					exhausted: false,
				}
			}
		}
		shardCount := len(ts.shardReaders)
		ts.mu.Unlock()

		d.log.Infof("Table %s: tracking %d shards", tableName, shardCount)
		d.updateTotalShardsMetric()
	}

	return nil
}

// startTableShardReader reads from a single shard for a specific table
func (d *dynamoDBCDCInput) startTableShardReader(ctx context.Context, tableName string, ts *tableStream, shardID string) {
	d.log.Debugf("Starting reader for shard %s (table %s)", shardID, tableName)
	defer d.log.Debugf("Stopped reader for shard %s (table %s)", shardID, tableName)

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
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Apply backpressure if too many messages are in flight
			for ts.recordBatcher != nil && ts.recordBatcher.ShouldThrottle() {
				d.log.Debugf("Throttling shard %s (table %s) due to too many in-flight messages", shardID, tableName)
				select {
				case <-ctx.Done():
					return
				case <-time.After(d.conf.throttleBackoff):
				}
			}

			// Get current reader state
			ts.mu.RLock()
			reader, exists := ts.shardReaders[shardID]
			if !exists {
				ts.mu.RUnlock()
				d.log.Errorf("BUG: shard reader for %s (table %s) not found in map", shardID, tableName)
				return
			}
			if reader.exhausted || reader.iterator == nil {
				ts.mu.RUnlock()
				return
			}
			iterator := reader.iterator
			ts.mu.RUnlock()

			// Read records from the shard
			getRecords, err := d.streamsClient.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
				ShardIterator: iterator,
				Limit:         aws.Int32(int32(d.conf.batchSize)),
			})
			if err != nil {
				if isThrottlingError(err) {
					wait := boff.NextBackOff()
					d.log.Debugf("Throttled on shard %s (table %s), backing off for %v", shardID, tableName, wait)
					time.Sleep(wait)
					continue
				}
				d.log.Errorf("Failed to get records from shard %s (table %s): %v", shardID, tableName, err)
				continue
			}

			// Success - reset backoff
			boff.Reset()

			// Update iterator
			ts.mu.Lock()
			reader.iterator = getRecords.NextShardIterator
			if reader.iterator == nil {
				reader.exhausted = true
				d.log.Infof("Shard %s (table %s) exhausted", shardID, tableName)
				ts.mu.Unlock()
				return
			}
			ts.mu.Unlock()

			if len(getRecords.Records) == 0 {
				continue
			}

			// Convert records to messages
			batch := convertTableRecordsToBatch(getRecords.Records, tableName, shardID)
			if len(batch) == 0 {
				continue
			}

			// Track messages in batcher
			batch = ts.recordBatcher.AddMessages(batch, shardID)

			// Track pending ack
			d.pendingAcks.Add(1)

			// Create ack function
			checkpointer := ts.checkpointer
			recordBatcher := ts.recordBatcher
			ackFunc := func(ackCtx context.Context, err error) error {
				defer d.pendingAcks.Done()

				if d.closed.Load() {
					d.log.Warn("Received ack after close, dropping")
					if err == nil && recordBatcher != nil {
						recordBatcher.RemoveMessages(batch)
					}
					return nil
				}

				if err != nil {
					d.log.Warnf("Batch nacked from shard %s (table %s): %v", shardID, tableName, err)
					if recordBatcher != nil {
						recordBatcher.RemoveMessages(batch)
					}
					return err
				}

				// Mark messages as acked and checkpoint if needed
				if recordBatcher != nil && checkpointer != nil {
					if ackErr := recordBatcher.AckMessages(ackCtx, checkpointer, batch); ackErr != nil {
						d.log.Errorf("Failed to checkpoint shard %s (table %s) after ack: %v", shardID, tableName, ackErr)
						return ackErr
					}
					d.log.Debugf("Successfully checkpointed %d messages from shard %s (table %s)", len(batch), shardID, tableName)
				}
				return nil
			}

			// Send to channel
			select {
			case <-ctx.Done():
				return
			case d.msgChan <- asyncMessage{msg: batch, ackFn: ackFunc}:
				d.log.Debugf("Sent batch of %d records from shard %s (table %s)", len(batch), shardID, tableName)
			}
		}
	}
}

// convertTableRecordsToBatch converts DynamoDB Stream records to Benthos messages for a specific table
func convertTableRecordsToBatch(records []types.Record, tableName, shardID string) service.MessageBatch {
	batch := make(service.MessageBatch, 0, len(records))

	for _, record := range records {
		msg := service.NewMessage(nil)

		// Structure similar to Kinesis format for consistency
		recordData := map[string]any{
			"tableName":    tableName,
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
		msg.MetaSetMut("dynamodb_table", tableName)

		batch = append(batch, msg)
	}

	return batch
}

// flushCheckpoint flushes pending checkpoints for a given checkpointer/batcher pair.
// Returns true if any error occurred during flush.
func (d *dynamoDBCDCInput) flushCheckpoint(ctx context.Context, cp *dynamocdc.Checkpointer, batcher *dynamocdc.RecordBatcher, label string) bool {
	if cp == nil || batcher == nil {
		return false
	}

	pending := batcher.GetPendingCheckpoints()
	if len(pending) == 0 {
		return false
	}

	d.log.Infof("Flushing %d pending checkpoints for %s on close", len(pending), label)
	if err := cp.FlushCheckpoints(ctx, pending); err != nil {
		d.log.Errorf("Failed to flush checkpoints for %s: %v", label, err)
		d.metrics.checkpointFailures.Incr(1)
		return true
	}
	return false
}

// startBackgroundWorker launches a goroutine with proper panic recovery,
// shutdown signaling, and waitgroup tracking. Use this for all background goroutines.
func (d *dynamoDBCDCInput) startBackgroundWorker(name string, fn func(context.Context)) {
	workerCtx, workerCancel := d.shutSig.SoftStopCtx(context.Background())
	d.backgroundWorkers.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				d.log.Errorf("Background worker %s panicked: %v", name, r)
			}
			d.backgroundWorkers.Done()
		}()
		defer workerCancel()
		fn(workerCtx)
	}()
}

// startTableCoordinator launches a table stream coordinator goroutine.
func (d *dynamoDBCDCInput) startTableCoordinator(tableName string, ts *tableStream) {
	d.startBackgroundWorker(
		fmt.Sprintf("coordinator for table %s", tableName),
		func(ctx context.Context) {
			d.startTableStreamCoordinator(ctx, tableName, ts)
		},
	)
}

// updateTotalShardsMetric aggregates shard counts across all table streams and
// updates the shardsTracked gauge. This prevents multi-table mode from overwriting
// the gauge with a single table's count.
func (d *dynamoDBCDCInput) updateTotalShardsMetric() {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var total int64
	for _, ts := range d.tableStreams {
		ts.mu.RLock()
		total += int64(len(ts.shardReaders))
		ts.mu.RUnlock()
	}
	// Also include single-table mode shards
	total += int64(len(d.shardReaders))
	d.metrics.shardsTracked.Set(total)
}

// cleanupTableExhaustedShards removes exhausted shards for a specific table
func (d *dynamoDBCDCInput) cleanupTableExhaustedShards(tableName string, ts *tableStream, activeShards map[string]context.CancelFunc) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	var cleaned []string
	for shardID, reader := range ts.shardReaders {
		if reader.exhausted {
			if cancelFn, isActive := activeShards[shardID]; isActive {
				cancelFn()
				delete(activeShards, shardID)
			}
			delete(ts.shardReaders, shardID)
			cleaned = append(cleaned, shardID)
		}
	}

	if len(cleaned) > 0 {
		d.log.Infof("Table %s: cleaned up %d exhausted shards: %v", tableName, len(cleaned), cleaned)
		d.updateTotalShardsMetric()
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
func (d *dynamoDBCDCInput) handleSnapshotBatch(ctx context.Context, items []map[string]dynamodbtypes.AttributeValue, segment int, tableName string) error {
	if len(items) == 0 {
		return nil
	}

	// Read immutable fields once before loop (not once per item)
	d.mu.RLock()
	buffer := d.snapshot.seqBuffer
	startTime := d.snapshot.startTime
	keySchema := d.keySchema
	d.mu.RUnlock()

	batch := make(service.MessageBatch, 0, len(items))

	for _, item := range items {
		msg := service.NewMessage(nil)

		// Structure the snapshot record similar to CDC events
		recordData := map[string]any{
			"tableName": tableName,
			"eventName": "READ", // Distinguish snapshot reads from CDC events
		}

		// Add the full item as newImage (similar to CDC INSERT events)
		dynamoData := map[string]any{
			"newImage": convertDynamoDBAttributeMap(item),
		}
		if buffer != nil {
			keyStr := buildItemKeyString(item, keySchema)
			if keyStr != "" {
				// Record this item in the snapshot buffer (with timestamp as sequence for deduplication)
				buffer.RecordSnapshotItem(keyStr, startTime.Format(time.RFC3339Nano))
			}
		}

		recordData["dynamodb"] = dynamoData
		msg.SetStructured(recordData)

		// Set metadata - note these are different from CDC events
		msg.MetaSetMut("dynamodb_event_name", "READ")
		msg.MetaSetMut("dynamodb_table", tableName)
		msg.MetaSetMut("dynamodb_snapshot_segment", strconv.Itoa(segment))

		batch = append(batch, msg)
	}

	// Update metrics
	d.snapshot.recordsRead.Add(int64(len(batch)))
	d.metrics.snapshotRecordsRead.Incr(int64(len(batch)))

	// Check and report buffer overflow (only once - buffer already read at function start)
	if buffer != nil && buffer.IsOverflow() && buffer.overflowReported.CompareAndSwap(false, true) {
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

// buildItemKeyString creates a string representation of an item's primary key for deduplication.
// Uses the table's actual key schema to extract primary key attributes reliably.
func buildItemKeyString(item map[string]dynamodbtypes.AttributeValue, keySchema []dynamodbtypes.KeySchemaElement) string {
	if len(keySchema) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.Grow(64) // Pre-allocate reasonable capacity

	first := true
	for _, keyElem := range keySchema {
		keyName := aws.ToString(keyElem.AttributeName)
		v, ok := item[keyName]
		if !ok {
			// Item missing a key attribute - can't build reliable key
			return ""
		}
		if !first {
			sb.WriteByte(';')
		}
		sb.WriteString(keyName)
		sb.WriteByte('=')
		writeAttributeValueString(&sb, v)
		first = false
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

	// Get table name (for single-table mode, it's the first element)
	tableName := ""
	if len(d.conf.tables) > 0 {
		tableName = d.conf.tables[0]
	}

	for _, record := range records {
		msg := service.NewMessage(nil)

		// Structure similar to Kinesis format for consistency
		recordData := map[string]any{
			"tableName":    tableName,
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
		msg.MetaSetMut("dynamodb_table", tableName)

		batch = append(batch, msg)
	}

	return batch
}

func (d *dynamoDBCDCInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	// msgChan and shutSig are immutable after Connect(), no lock needed
	if d.msgChan == nil || d.shutSig == nil {
		return nil, nil, service.ErrNotConnected
	}

	// Check if snapshot failed and propagate the error
	if d.snapshot != nil && d.snapshot.state.Load() == 3 { // failed
		if d.snapshot.err != nil {
			return nil, nil, d.snapshot.err
		}
		// Get table name for error message
		tableName := "unknown"
		if len(d.conf.tables) > 0 {
			tableName = d.conf.tables[0]
		}
		return nil, nil, fmt.Errorf("snapshot scan failed for table %s", tableName)
	}

	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-d.shutSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case am, open := <-d.msgChan:
		if !open {
			return nil, nil, service.ErrNotConnected
		}
		return am.msg, am.ackFn, nil
	}
}

func (d *dynamoDBCDCInput) Close(ctx context.Context) error {
	// Mark as closed to reject new acks
	d.closed.Store(true)

	// Trigger graceful shutdown (shutSig is immutable after Connect())
	d.log.Debug("Initiating graceful shutdown")
	d.shutSig.TriggerSoftStop()

	// Wait for background goroutines to stop
	select {
	case <-d.shutSig.HasStoppedChan():
		d.log.Debug("Background goroutines stopped")
	case <-time.After(defaultShutdownTimeout):
		d.log.Warn("Timeout waiting for background goroutines to stop")
		// Trigger hard stop if graceful shutdown times out
		d.shutSig.TriggerHardStop()
	}

	// Wait for all tracked background workers to finish
	d.log.Debug("Waiting for background workers")
	workersDone := make(chan struct{})
	go func() {
		d.backgroundWorkers.Wait()
		close(workersDone)
	}()

	select {
	case <-workersDone:
		d.log.Debug("All background workers stopped")
	case <-time.After(defaultShutdownTimeout):
		d.log.Warn("Timeout waiting for background workers")
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

	// Flush single-table mode checkpoints (fields immutable after Connect())
	d.flushCheckpoint(ctx, d.checkpointer, d.recordBatcher, "single-table")

	// Flush multi-table mode checkpoints
	d.mu.RLock()
	tableStreamsCopy := make(map[string]*tableStream, len(d.tableStreams))
	for k, v := range d.tableStreams {
		tableStreamsCopy[k] = v
	}
	d.mu.RUnlock()

	for tableName, ts := range tableStreamsCopy {
		d.flushCheckpoint(ctx, ts.checkpointer, ts.recordBatcher, fmt.Sprintf("table %s", tableName))
	}

	// Clear references to help GC
	d.mu.Lock()
	d.dynamoClient = nil
	d.streamsClient = nil
	d.shardReaders = nil
	d.keySchema = nil
	d.checkpointer = nil
	d.recordBatcher = nil
	d.msgChan = nil
	d.shutSig = nil
	d.tableStreams = nil
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
