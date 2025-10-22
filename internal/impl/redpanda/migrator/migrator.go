// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migrator

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Jeffail/shutdown"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
)

const (
	rmoFieldTopic                  = "topic"
	rmoFieldTopicReplicationFactor = "topic_replication_factor"
	rmoFieldSyncTopicACLs          = "sync_topic_acls"
	rmoFieldServerless             = "serverless"
)

func migratorInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("4.67.0").
		Summary("Kafka consumer for migration pipelines. All migration logic is handled by the redpanda_migrator output.").
		Description(`
The ` + "`redpanda_migrator`" + ` input simply consumes records from the source cluster and forwards them downstream. 
It does not perform topic/schema/group synchronisation. 
All migration features and coordination live in the paired ` + "`redpanda_migrator`" + ` output.

**IMPORTANT:** This input requires a corresponding ` + "`redpanda_migrator`" + ` output in the same pipeline. 
Each pipeline must have both input and output components configured.
For capabilities, guarantees, scheduling, and examples, see the output documentation.

**Multiple migrator pairs:** When using multiple migrator pairs in a single pipeline, 
the mapping between input and output components is done based on the label field. 
The label of the input and output must match exactly for proper coordination.

**Performance tuning for high throughput:** For workloads with high message rates or large messages, 
adjust the following fields to increase buffer sizes and batch processing:
- ` + "`partition_buffer_bytes`" + `: Increase to 10MB or higher (default: 1MB)
- ` + "`max_yield_batch_bytes`" + `: Increase to 100MB or higher (default: 10MB)

These settings allow the consumer to buffer more data per partition and yield larger batches, 
reducing overhead and improving throughput at the cost of higher memory usage.`).
		// Kafka fields
		Fields(kafka.FranzConnectionFields()...).
		Fields(kafka.FranzConsumerFields()...).
		Fields(kafka.FranzReaderOrderedConfigFields()...).
		LintRule(kafka.FranzConsumerFieldLintRules).
		// Schema registry fields
		Field(schemaRegistryField().Optional()).
		// Other fields
		Field(service.NewAutoRetryNacksToggleField())
}

func migratorOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("4.67.0").
		Summary("A specialised Kafka producer for comprehensive data migration between Apache Kafka and Redpanda clusters.").
		Description(`
The `+"`redpanda_migrator`"+` output performs all migration work. 
It coordinates topics, schema registry, and consumer groups to migrate data from a source Kafka/Redpanda cluster to a destination cluster.

**IMPORTANT:** This output requires a corresponding `+"`redpanda_migrator`"+` input in the same pipeline. 
Each pipeline must have both input and output components configured.

**Multiple migrator pairs:** When using multiple migrator pairs in a single pipeline, 
the mapping between input and output components is done based on the label field. 
The label of the input and output must match exactly for proper coordination.

**Performance tuning for high throughput:** For workloads with high message rates or large messages, 
adjust buffer sizes on the paired input component to improve throughput. See the input documentation for details.

What gets synchronised:

- Topics
  - Name resolution with interpolation (default: preserve source name)
  - Automatic creation with mirrored partition counts
  - Selectable replication factor (default: inherit from source)
  - Copy of supported topic configuration keys (serverless-aware subset)
  - Optional ACL replication with safe transforms:
    - Excludes `+"`ALLOW WRITE`"+` entries
    - Downgrades `+"`ALLOW ALL`"+` to `+"`READ`"+`
    - Preserves resource pattern type and host filters

- Schema Registry
  - One-shot or periodic syncing
  - Subject selection via include/exclude regex
  - Subject renaming with interpolation
  - Versions: `+"`latest`"+` or `+"`all`"+` (default: `+"`all`"+`)
  - Optional include of soft-deleted subjects
  - ID handling: translate IDs (create-or-reuse) or keep fixed IDs and versions
  - Optional schema normalisation on create
  - Optional per-subject compatibility propagation when explicitly set on source (global mode is not forced)
  - Serverless note: schema metadata and rule sets are not copied in serverless mode

- Consumer Groups
  - Periodic syncing
  - Group selection via include/exclude regex
  - Only groups in `+"`Empty`"+` state are migrated (active groups are skipped)
  - Timestamp-based offset translation (approximate) per partition using previous-record timestamp and `+"`ListOffsetsAfterMilli`"+`
  - No rewind guarantee: destination offsets are never moved backwards
  - Commit performed in parallel with per-group metrics
  - Requires matching partition counts between source and destination topics

How it runs:

- Topics: synced on demand. The first write triggers discovery and creation; subsequent writes create on first encounter per topic.
- Schema Registry: one sync at connect, then triggered when topic record has unknown schema; optional background loop controlled by `+"`schema_registry.interval`"+`.
- Consumer Groups: background loop controlled by `+"`consumer_groups.interval`"+` and filtered by the current topic mappings.

Guarantees:

- Topics are created with the intended partitioning and configured replication factor. Existing topics are respected; partition mismatches are logged and consumer group migration for mismatched topics is skipped.
- Consumer group offsets are never rewound. Only translated forward positions are committed.
- ACL replication excludes `+"`ALLOW WRITE`"+` operations and downgrades `+"`ALLOW ALL`"+` to `+"`READ`"+` to avoid unsafe grants.

Limitations and requirements:

- Destination Schema Registry must be in `+"`READWRITE`"+` or `+"`IMPORT`"+` mode.
- Offset translation is best-effort: if the previous-offset timestamp cannot be read, or no destination offset exists after the timestamp, that partition is skipped.
- Consumer group migration requires identical partition counts for source and destination topics.

Metrics:

The component exposes comprehensive metrics for monitoring migration operations:

Topic Migration Metrics:
- `+"`redpanda_migrator_topics_created_total`"+` (counter): Total number of topics successfully created on the destination cluster
- `+"`redpanda_migrator_topic_create_errors_total`"+` (counter): Total number of errors encountered when creating topics
- `+"`redpanda_migrator_topic_create_latency_ns`"+` (timer): Latency in nanoseconds for topic creation operations

Schema Registry Migration Metrics:
- `+"`redpanda_migrator_sr_schemas_created_total`"+` (counter): Total number of schemas successfully created in the destination schema registry
- `+"`redpanda_migrator_sr_schema_create_errors_total`"+` (counter): Total number of errors encountered when creating schemas
- `+"`redpanda_migrator_sr_schema_create_latency_ns`"+` (timer): Latency in nanoseconds for schema creation operations
- `+"`redpanda_migrator_sr_compatibility_updates_total`"+` (counter): Total number of compatibility level updates applied to subjects
- `+"`redpanda_migrator_sr_compatibility_update_errors_total`"+` (counter): Total number of errors encountered when updating compatibility levels
- `+"`redpanda_migrator_sr_compatibility_update_latency_ns`"+` (timer): Latency in nanoseconds for compatibility level update operations

Consumer Group Migration Metrics (with group label):
- `+"`redpanda_migrator_cg_offsets_translated_total`"+` (counter): Total number of offsets successfully translated per consumer group
- `+"`redpanda_migrator_cg_offset_translation_errors_total`"+` (counter): Total number of errors encountered when translating offsets per consumer group
- `+"`redpanda_migrator_cg_offset_translation_latency_ns`"+` (timer): Latency in nanoseconds for offset translation operations per consumer group
- `+"`redpanda_migrator_cg_offsets_committed_total`"+` (counter): Total number of offsets successfully committed per consumer group
- `+"`redpanda_migrator_cg_offset_commit_errors_total`"+` (counter): Total number of errors encountered when committing offsets per consumer group
- `+"`redpanda_migrator_cg_offset_commit_latency_ns`"+` (timer): Latency in nanoseconds for offset commit operations per consumer group

Consumer Lag Metrics (with topic and partition labels):
- `+"`redpanda_lag`"+` (gauge): Current consumer lag in messages for each topic partition being consumed by the migrator input. This metric shows the difference between the high water mark and the current consumer position, providing visibility into how far behind the consumer is on each partition. The metric includes labels for topic name and partition number to enable per-partition monitoring.

This component must be paired with the `+"`redpanda_migrator`"+` input in the same pipeline.`).
		Example(
			"Basic migration",
			"Migrate topics, schemas and consumer groups from source to destination.",
			`input:
  redpanda_migrator:
    seed_brokers: ["source:9092"]
    topics: ["orders", "payments"]
    consumer_group: "migration"

output:
  redpanda_migrator:
    seed_brokers: ["destination:9092"]
    # Write to the same topic name
    topic: ${! metadata("kafka_topic") }
    schema_registry:
      url: "http://dest-registry:8081"
      translate_ids: true
    consumer_groups:
      interval: 1m
`).
		Example(
			"Migration to Redpanda Serverless",
			"Migrate from Confluent/Kafka to Redpanda Cloud serverless cluster with authentication.",
			`input:
  redpanda_migrator:
    seed_brokers: ["source-kafka:9092"]
    topics:
      - '^[^_]'  # All topics not starting with underscore
    regexp_topics: true
    consumer_group: "migrator_cg"
    schema_registry:
      url: "http://source-registry:8081"

output:
  redpanda_migrator:
    seed_brokers: ["serverless-cluster.redpanda.com:9092"]
    tls:
      enabled: true
    sasl:
      - mechanism: SCRAM-SHA-256
        username: "migrator"
        password: "migrator"
    schema_registry:
      url: "https://serverless-cluster.redpanda.com:8081"
      basic_auth:
        enabled: true
        username: "migrator"
        password: "migrator"
      translate_ids: true
    consumer_groups:
      exclude:
        - "migrator_cg"  # Exclude the migration consumer group itself
    serverless: true  # Enable serverless mode for restricted configurations
`).
		// Kafka fields
		Fields(kafka.FranzConnectionFields()...).
		Fields(kafka.FranzProducerFields()...).
		// Schema registry fields
		Field(schemaRegistryField(schemaRegistryMigratorFields()...).Optional()).
		// Consumer groups fields
		Field(service.NewObjectField(groupsObjectField, groupsMigratorFields()...).Optional()).
		// Topic fields
		Field(service.NewInterpolatedStringField(rmoFieldTopic).
			Description("The topic to write messages to. Use interpolation to derive destination topic names from source topics. The source topic name is available as 'kafka_topic' metadata.").
			Default("${! @kafka_topic }").
			Example("prod_${! @kafka_topic }")).
		Field(service.NewIntField(rmoFieldTopicReplicationFactor).
			Description("The replication factor for created topics. If not specified, inherits the replication factor from source topics. Useful when migrating to clusters with different sizes.").
			Example("3").
			Example("1  # For single-node clusters").
			Optional()).
		// ACL fields
		Field(service.NewBoolField(rmoFieldSyncTopicACLs).
			Description("Whether to synchronise topic ACLs from source to destination cluster. ACLs are transformed safely: ALLOW WRITE permissions are excluded, and ALLOW ALL is downgraded to ALLOW READ to prevent conflicts.").
			Default(false)).
		Field(service.NewBoolField(rmoFieldServerless).
			Description("Enable serverless mode for Redpanda Cloud serverless clusters. This restricts topic configurations and schema features to those supported by serverless environments.").
			Default(false).
			Advanced()).
		LintRule(`
root = [
  if this.key.or("") != "" {
    "key field is not supported by migrator, setting it could break consumer group migration"
  },
  if this.partitioner.or("") != "" {
    "partitioner field is not supported by migrator, setting it could break consumer group migration"
  },
  if this.partition.or("") != "" {
    "partition field is not supported by migrator, setting it could break consumer group migration"
  },
  if this.timestamp.or("") != "" {
    "timestamp field is not supported by migrator, setting it could break consumer group migration"
  },
  if this.timestamp_ms.or("") != "" {
    "timestamp_ms field is not supported by migrator, setting it could break consumer group migration"
  }
]
`)
}

type migratorResKey string

func migratorKey(label string) migratorResKey {
	if label == "" {
		label = "default"
	}
	return migratorResKey("migrator_" + label)
}

func newMigratorFrom(mgr *service.Resources) *Migrator {
	m, _ := mgr.GetOrSetGeneric(migratorKey(mgr.Label()), NewMigrator(mgr))
	return m.(*Migrator)
}

type migratorBatchInput struct {
	service.BatchInput
	m *Migrator
}

func (w migratorBatchInput) Connect(ctx context.Context) error {
	if err := w.BatchInput.Connect(ctx); err != nil {
		return err
	}
	return w.m.onInputConnected(ctx, w.BatchInput.(*kafka.FranzReaderOrdered))
}

type migratorBatchOutput struct {
	service.BatchOutput
	m *Migrator
}

func (w migratorBatchOutput) Connect(ctx context.Context) error {
	if err := w.BatchOutput.Connect(ctx); err != nil {
		return err
	}
	return w.m.onOutputConnected(ctx, w.BatchOutput.(franzWriter))
}

func (w migratorBatchOutput) Close(ctx context.Context) error {
	err := w.BatchOutput.Close(ctx)
	w.m.stopSig.TriggerHardStop()
	return err
}

func init() {
	service.MustRegisterBatchInput("redpanda_migrator", migratorInputConfig(),
		func(pConf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			m := newMigratorFrom(mgr)
			if err := m.initInputFromParsed(pConf, mgr); err != nil {
				return nil, err
			}

			fr, err := newFranzReaderOrdered(pConf, mgr)
			if err != nil {
				return nil, err
			}
			m.srcAdm = kadm.NewClient(fr.Client)

			return service.AutoRetryNacksBatchedToggled(pConf, migratorBatchInput{fr, m})
		})

	service.MustRegisterBatchOutput("redpanda_migrator", migratorOutputConfig(),
		func(pConf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			m := newMigratorFrom(mgr)

			err = m.initOutputFromParsed(pConf, mgr)
			if err != nil {
				return
			}

			fw, err := newFranzWriter(pConf, mgr)
			if err != nil {
				return
			}
			fw.OnWrite = m.onWrite
			out = migratorBatchOutput{fw, m}

			// Force single in-flight batch message to ensure data ordering
			maxInFlight = 1

			return
		})
}

//------------------------------------------------------------------------------

// Migrator orchestrates comprehensive data migration between Kafka clusters.
// It coordinates the migration of messages, topics, schemas, consumer groups,
// and ACLs between source and destination Kafka/Redpanda clusters.
//
// The Migrator operates as a stateful coordinator that:
//   - Manages topic creation and synchronisation on the destination cluster
//   - Handles schema registry migration with ID translation
//   - Migrates consumer group offsets using timestamp-based correlation
//   - Synchronises topic ACLs with appropriate security transformations
//   - Provides metrics and monitoring for all migration operations
type Migrator struct {
	topic  topicMigrator
	sr     schemaRegistryMigrator
	groups groupsMigrator
	log    *service.Logger

	plumbing uint8
	stopSig  *shutdown.Signaller

	mu     sync.RWMutex
	src    *kgo.Client
	srcAdm *kadm.Client
}

// NewMigrator creates a new Migrator instance with the provided logger.
func NewMigrator(mgr *service.Resources) *Migrator {
	log := mgr.Logger()
	return &Migrator{
		topic: topicMigrator{
			metrics:     newTopicMetrics(mgr.Metrics()),
			log:         log,
			knownTopics: make(map[string]TopicMapping),
		},
		sr: schemaRegistryMigrator{
			metrics:      newSchemaRegistryMetrics(mgr.Metrics()),
			log:          log,
			knownSchemas: make(map[int]schemaInfo),
			compatSet:    make(map[string]struct{}),
		},
		groups: groupsMigrator{
			metrics:         newGroupsMetrics(mgr.Metrics()),
			log:             log,
			topicIDs:        make(map[string]kadm.TopicID),
			commitedOffsets: make(map[string]map[string]map[int32][2]int64),
		},
		log:     log,
		stopSig: shutdown.NewSignaller(),
	}
}

func (m *Migrator) initInputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) error {
	var err error

	m.sr.src, err = schemaRegistryClientFromParsed(pConf, mgr)
	if err != nil {
		return err
	}

	if err := m.groups.conf.initFromParsedInput(pConf); err != nil {
		return err
	}

	m.plumbing |= inputInitialized
	return nil
}

func (m *Migrator) initOutputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) error {
	var err error

	if err := m.topic.conf.initFromParsed(pConf); err != nil {
		return err
	}

	m.sr.dst, err = schemaRegistryClientFromParsed(pConf, mgr)
	if err != nil {
		return err
	}
	if err := m.sr.conf.initFromParsed(pConf); err != nil {
		return err
	}

	if err := m.groups.conf.initFromParsed(pConf); err != nil {
		return err
	}

	m.plumbing |= outputInitialized
	return nil
}

func (m *Migrator) onInputConnected(_ context.Context, fr *kafka.FranzReaderOrdered) error { //nolint:revive
	if err := m.validateInitialized(); err != nil {
		return err
	}

	m.mu.Lock()
	m.src = fr.Client
	m.srcAdm = kadm.NewClient(fr.Client)
	m.groups.src = fr.Client
	m.groups.srcAdm = m.srcAdm
	m.mu.Unlock()

	return nil
}

func (m *Migrator) onOutputConnected(_ context.Context, fw franzWriter) error {
	if err := m.validateInitialized(); err != nil {
		return err
	}

	ctx, cancel := m.stopSig.SoftStopCtx(context.Background())

	// Set up destination admin client for groups migrator
	clientInfo, err := fw.GetClient(ctx)
	if err != nil {
		cancel()
		return fmt.Errorf("get franz client: %w", err)
	}
	m.mu.Lock()
	m.groups.dstAdm = kadm.NewClient(clientInfo.Client)
	m.mu.Unlock()

	// Syncing topics is deferred until the first message is received because
	// we use GetConsumeTopics, which is only available after the first message
	// is received.

	// Sync the schema registry once
	if err := m.sr.Sync(ctx); err != nil {
		cancel()
		return err
	}
	go m.sr.SyncLoop(ctx)

	// Start groups sync loop - there is no point in syncing groups before
	// syncing topics
	go m.groups.SyncLoop(ctx, m.topic.TopicMapping)

	return nil
}

func (m *Migrator) validateInitialized() error {
	if m.plumbing&inputInitialized == 0 {
		return errors.New("input not initialized")
	}
	if m.plumbing&outputInitialized == 0 {
		return errors.New("output not initialized")
	}
	// If schema registry migration is disabled, allow client mismatch.
	if !m.sr.conf.Enabled {
		return nil
	}
	if m.sr.src != nil && m.sr.dst == nil || m.sr.dst != nil && m.sr.src == nil {
		return errors.New("schema registry mismatch: both input and output must be set")
	}
	return nil
}

func (m *Migrator) onWrite(
	ctx context.Context,
	dst *kgo.Client,
	records []*kgo.Record,
) error {
	m.mu.RLock()
	src := m.src
	srcAdm := m.srcAdm
	m.mu.RUnlock()

	dstAdm := kadm.NewClient(dst)

	if err := m.topic.SyncOnce(ctx, srcAdm, dstAdm, src.GetConsumeTopics); err != nil {
		return fmt.Errorf("sync topics: %w", err)
	}

	var (
		lastTopic       string
		lastDstTopic    string
		lastSchemaID    int
		lastDstSchemaID int
	)
	for _, record := range records {
		if record == nil {
			continue
		}

		if record.Topic != lastTopic {
			topic, err := m.topic.CreateTopicIfNeeded(ctx, srcAdm, dstAdm, record.Topic)
			if err != nil {
				return err
			}
			lastTopic, lastDstTopic = record.Topic, topic
		}
		record.Topic = lastDstTopic

		// Update schema ID
		if m.sr.enabled() && m.sr.conf.TranslateIDs {
			schemaID, err := parseSchemaID(record.Value)
			if err != nil {
				return fmt.Errorf("parse schema ID: %w", err)
			}
			if schemaID != 0 {
				if schemaID != lastSchemaID {
					id, err := m.sr.DestinationSchemaID(schemaID)
					if err != nil {
						return fmt.Errorf("resolve destination schema ID: %w", err)
					}
					lastSchemaID, lastDstSchemaID = schemaID, id
				}
				if err := updateSchemaID(record.Value, lastDstSchemaID); err != nil {
					return fmt.Errorf("update schema ID: %w", err)
				}
			}
		}
	}

	return nil
}

func parseSchemaID(b []byte) (int, error) {
	if b == nil {
		return 0, nil
	}

	var ch sr.ConfluentHeader
	schemaID, _, err := ch.DecodeID(b)
	if err != nil && !errors.Is(err, sr.ErrBadHeader) {
		return 0, fmt.Errorf("decode schema ID: %w", err)
	}
	return schemaID, nil
}

func updateSchemaID(b []byte, schemaID int) error {
	var ch sr.ConfluentHeader
	return ch.UpdateID(b, uint32(schemaID))
}
