# Integration Tests

This document contains a list of integration tests for the Redpanda Migrator component.

## Core Migration Tests (`integration_test.go`)

### `TestIntegrationMigratorSinglePartition`

Verifies basic single-partition migration functionality.
- Creates source and destination Redpanda clusters without Schema Registry
- Produces 100 messages to partition 0 of source cluster
- Starts migrator and waits for messages to transfer
- Validates all messages arrive at destination in correct order
- Confirms message keys and values match exactly

### `TestIntegrationMigratorSinglePartitionMalformedSchemaID`

Tests graceful handling of messages with malformed schema ID headers.
- Creates source and destination clusters with Schema Registry enabled
- Registers a schema in source Schema Registry
- Produces 100 messages with malformed 5-byte schema ID headers (non-conformant to wire format)
- Starts migrator and waits for message transfer
- Validates:
  - All messages arrive at destination without migration failure
  - Malformed schema ID headers are preserved unchanged
  - Message values remain intact

### `TestIntegrationMigratorMultiPartitionSchemaAwareWithConsumerGroups`

Tests multi-partition migration with Schema Registry and consumer group synchronization.
- Creates source and destination clusters with Schema Registry enabled
- Registers an Avro schema in source Schema Registry
- Produces 10,000 schema-encoded messages across 2 partitions with specific timestamps
- Commits consumer group offsets in source cluster
- Starts migrator and waits for message transfer
- Validates:
  - Schema is correctly migrated to destination Schema Registry
  - All messages contain correct schema ID headers
  - Messages maintain correct partition assignment
  - Message timestamps are preserved
  - Consumer group offsets are synchronized to destination
  - Metrics endpoint is functional

### `TestIntegrationMigratorInputKafkaFranzConsumerGroup`

Verifies consumer group migration when separate consumers read from the cluster.
- Creates source and destination clusters without Schema Registry
- Produces first message to source cluster
- Starts migrator to begin migration
- Uses `kafka_franz` input component to consume from source cluster
- Produces second message to source cluster
- Validates:
  - Both messages are migrated to destination
  - Consumer group offsets are synchronized to destination
  - Second consumer reading from destination sees correct offset

### `TestIntegrationRealMigratorConfluentToServerless`

End-to-end test for Confluent Platform to Redpanda Serverless migration.
- **Manual setup required**: Needs real Redpanda Serverless cluster credentials
- Starts Confluent Platform in Docker (Kafka, Schema Registry, Connect)
- Configures RPCN pipeline to produce test data
- Migrates topics, schemas, and consumer groups to Serverless
- Validates complete migration including:
  - Topic metadata and configurations
  - Schema Registry subjects and schemas
  - Consumer group offsets
  - Message content and ordering

## Soak Test (`integration_soak_test.go`)

### `TestIntegrationMigratorSoak`

Long-running stability test with configurable timing parameters.
- Starts Confluent Platform cluster with Schema Registry
- Launches datagen Kafka Connec connectors producing continuous data streams
- Runs data generation for configurable duration (default: 20-60 seconds)
- Starts migrator and runs for configurable duration (default: 20-30 seconds)
- Waits for post-migration stabilization (default: 20-30 seconds)
- Validates:
  - Topic lists match between source and destination
  - Partition counts match for pageviews topic
  - Consumer group offsets and data are synchronized
  - System remains stable under continuous load

## Consumer Groups Tests (`migrator_groups_integration_test.go`)

### `TestIntegrationListGroupOffsets`

Tests consumer group offset listing with various filtering options.
- Creates multiple topics and consumer groups in source cluster
- Commits offsets for various group/topic/partition combinations
- Tests filtering by:
  - All groups (default behaviour)
  - Include pattern (regex matching group names)
  - Exclude pattern (regex excluding group names)
  - Combination of include and exclude patterns
- Validates deleted groups are excluded from results

### `TestIntegrationReadRecordTimestamp`

Verifies correct extraction of record timestamps during migration.
- Produces messages with specific timestamps to source cluster
- Uses migrator to read and translate timestamps
- Validates timestamp preservation across migration
- Tests edge cases with various timestamps

### `TestIntegrationGroupsOffsetSync`

Tests consumer group offset synchronization between clusters.
- Creates source and destination clusters
- Produces messages to multiple partitions
- Commits consumer group offsets in source cluster
- Runs offset synchronization
- Validates:
  - Offsets are correctly translated based on destination cluster state
  - Synchronization is idempotent (repeated calls produce same result)
  - Multiple consumer groups are handled correctly
  - Partition-specific offsets are maintained

## Schema Registry Tests (`migrator_schema_registry_integration_test.go`)

### `TestIntegrationSchemaRegistryMigratorListSubjectSchemas`

Tests listing schemas from Schema Registry with various filters.
- Creates multiple subjects with different schemas in source registry
- Tests soft-deleted subjects and schema versions
- Creates subject with multiple schema versions
- Tests filtering by:
  - All subjects (default)
  - Include pattern (regex matching subject names)
  - Exclude pattern (regex excluding subject names)
  - Combination of include and exclude patterns
- Validates deleted subjects/versions are handled correctly

### `TestIntegrationSchemaRegistryMigratorSyncNameResolver`

Verifies schema subject name resolution and transformation.
- Tests topic-to-subject name mapping
- Validates name resolver correctly transforms subject names
- Ensures compatibility with various naming conventions

### `TestIntegrationSchemaRegistryMigratorSyncVersionsAll`

Tests synchronization of all schema versions for each subject.
- Creates subject with multiple schema versions
- Syncs from source to destination
- Validates all versions are migrated in correct order
- Confirms schema IDs are properly handled

### `TestIntegrationSchemaRegistryMigratorSyncTranslateIDs`

Verifies schema ID translation between source and destination registries.
- Creates schemas with specific IDs in source registry
- Migrates to destination registry
- Validates ID mapping is maintained
- Tests messages referencing old IDs work with new IDs

### `TestIntegrationSchemaRegistryMigratorSyncNormalize`

Tests schema normalization during migration.
- Creates schemas with different formatting/whitespace
- Syncs to destination registry
- Validates schemas are normalized correctly
- Ensures functionally equivalent schemas are treated as identical

### `TestIntegrationSchemaRegistryMigratorSyncIdempotence`

Verifies schema synchronization is idempotent.
- Syncs schemas from source to destination
- Runs sync operation multiple times
- Validates:
  - Repeated syncs produce identical results
  - No duplicate schemas are created
  - Schema versions remain consistent

### `TestIntegrationSchemaRegistryMigratorCompatibilityFromSource`

Tests migration of compatibility mode settings.
- Sets specific compatibility mode in source registry
- Syncs to destination registry
- Validates compatibility mode is preserved
- Tests various compatibility levels (BACKWARD, FORWARD, FULL, etc.)

## Topic Migration Tests (`migrator_topic_integration_test.go`)

### `TestIntegrationTopicMigratorSyncConfig`

Verifies topic configuration synchronization.
- Creates topic with custom configurations in source cluster
- Syncs to destination cluster
- Validates configurations are correctly migrated
- Tests various config options (retention.ms, cleanup.policy, etc.)

### `TestIntegrationTopicMigratorSyncACLs`

Tests ACL (Access Control List) migration for topics.
- Creates topics with various ACL permissions in source
- Tests ACL transformations:
  - `ALLOW DESCRIBE` - migrated as-is
  - `ALLOW ALL` - downgraded to `ALLOW READ` for safety
  - `ALLOW WRITE` - skipped (not migrated)
- Validates ACLs are correctly applied to destination topics
- Ensures security model is maintained during migration

### `TestIntegrationTopicMigratorIdempotentSyncIdempotence`

Confirms topic synchronization is idempotent.
- Syncs topic from source to destination
- Runs sync operation multiple times
- Validates:
  - Repeated syncs succeed without errors
  - Topic configurations remain unchanged
  - No duplicate topics are created
