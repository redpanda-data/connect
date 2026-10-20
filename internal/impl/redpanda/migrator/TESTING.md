# Integration Tests

This document contains a list of integration tests for the Redpanda Migrator component.

## Performance Benchmarks

The migrator has been benchmarked to handle high-throughput scenarios, demonstrating stable 1GB/s+ throughput in production-like conditions. See the `bench/` directory for configuration details and test setup.

Example benchmark output showing 1GB/s+ throughput:
```
[output.processors.0] time="2025-10-10T11:56:50Z" level=info msg="rolling stats: 1035873 msg/sec, 1.0 GB/sec"
[output.processors.0] time="2025-10-10T11:57:10Z" level=info msg="rolling stats: 1035211.5 msg/sec, 1.0 GB/sec"
[output.processors.0] time="2025-10-10T11:57:12Z" level=info msg="rolling stats: 1037427.5 msg/sec, 1.0 GB/sec"
```

## Core Migration Tests

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

### `TestIntegrationMigratorIncompatibleSubjectDoesNotBlockTopics`

Regression test for CON-530: a schema subject that cannot be registered at the destination must not fail the output connect and block topic migration.
- Creates source and destination clusters with Schema Registry enabled
- Registers two incompatible versions of one subject at source (source compatibility set to `NONE`), plus a healthy subject
- Pre-registers only the first version of that subject at destination, where the default `BACKWARD` compatibility rejects the second version
- Creates a populated topic (3 messages) and an empty topic at source
- Starts migrator with a one-shot schema sync (`interval: 0s`, `versions: all`, `translate_ids: true`)
- Validates:
  - Both topics are created at destination
  - All messages are copied in order
  - The healthy subject is registered at destination
  - The incompatible version is not registered at destination

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

### `TestIntegrationSchemaRegistryMigratorSyncIncompatibleSubject`

Regression test for CON-530: a subject that cannot be registered at the destination must not abort the sync of the remaining subjects.
- Registers two incompatible versions of one subject at source (source compatibility set to `NONE`), plus 20 healthy subjects
- Pre-registers only the first version of that subject at destination, where the default `BACKWARD` compatibility rejects the second version
- Syncs with `versions: all` and `translate_ids: true`
- Validates:
  - Sync returns a partial sync error naming only the incompatible subject version
  - Every healthy subject is registered at destination
  - Records encoded with the failed schema are rejected rather than written with the untranslated source ID
  - A second sync retries the failed subject and reports it failed again
  - After relaxing the destination subject compatibility to `NONE`, a further sync succeeds and the subject is fully synced
  - Records encoded with the schema then translate to its destination ID

### `TestIntegrationSchemaRegistryMigratorSyncFailedVersionBlocksLaterVersions`

Verifies a later version of a subject is not synced after an earlier version failed, which would shift destination version numbers.
- Registers three versions of one subject at source, where v2 is incompatible with v1 and v3 is compatible with v1
- Pre-registers only v1 at destination under the default `BACKWARD` compatibility
- Syncs with `versions: all` and `translate_ids: true`
- Validates:
  - Sync reports v2 as failed and v3 as skipped
  - Destination still holds only v1

### `TestIntegrationSchemaRegistryMigratorSyncReadOnlyDestination`

Verifies registry misconfiguration still fails the sync outright, so that it keeps failing the output connect.
- Registers a subject at source
- Sets destination registry mode to `READONLY`
- Syncs with `versions: latest`
- Validates:
  - Sync fails with an error that is not a partial sync error
  - Error reports that the destination must be in `READWRITE or IMPORT` mode

### `TestIntegrationSchemaRegistryMigratorSyncFixedIDCollision`

Verifies that, with fixed IDs, a schema whose source ID already holds a different schema at the destination fails the sync outright, since records copied with that ID would resolve to the wrong schema.
- Sets source and destination registry mode to `IMPORT`
- Registers a schema at source with ID 100, and a different schema at destination with ID 100
- Syncs with `versions: latest` and `translate_ids: false`
- Validates:
  - Sync fails with an error that is not a partial sync error
  - Error suggests enabling `translate_ids`

### `TestIntegrationSchemaRegistryMigratorSyncFailedReferenceBlocksReferrer`

Verifies a schema is not synced when a schema it references failed, which could otherwise bind it to a different schema at the destination.
- Registers two incompatible versions of a subject at source, and a second subject that references v2
- Pre-registers v1 and a different, compatible v2 of the referenced subject at destination under the default `BACKWARD` compatibility
- Syncs with `versions: latest` and `translate_ids: true`
- Validates:
  - Sync reports the referenced v2 as failed and the referrer as skipped
  - The referrer is not registered at destination
  - Records encoded with the referrer are rejected

### `TestIntegrationSchemaRegistryMigratorSyncLoopRetriesFailedAtZeroInterval`

Verifies that with `interval: 0s` the subjects that failed the initial sync are retried until they sync.
- Registers two incompatible versions of one subject at source, and pre-registers only v1 at destination under the default `BACKWARD` compatibility
- Runs an initial sync with `versions: all` and `translate_ids: true`, which partially fails
- Registers a new subject at source, then starts the sync loop with `interval: 0s`
- Relaxes the destination subject compatibility to `NONE`
- Validates:
  - The failed subject is synced without another explicit sync, and its records translate
  - The loop stops once nothing is left to retry
  - The subject added after the initial sync is not migrated

### `TestIntegrationSchemaRegistryMigratorSyncFailedVersionDoesNotBlockEarlierReference`

Verifies that a failed later version of a subject does not block an earlier version that another subject references, nor the referrer.
- Registers three versions of a subject at source, the third incompatible, and a second subject that references v2
- Pre-registers v1 at destination under the default `BACKWARD` compatibility, so only v3 is rejected
- Runs for both `versions: latest` and `versions: all`, with `translate_ids: true` and a single worker, 8 syncs each to cover both subject orders
- Validates:
  - Sync reports only v3 as failed
  - The referenced v2 and the referrer are synced, and their records translate
  - Records encoded with v3 are rejected

### `TestIntegrationSchemaRegistryMigratorSyncSubjectDeletedAfterListing`

Verifies that a subject deleted at the source after it was listed fails only that subject.
- Registers two subjects at source, behind a proxy that returns 404 when one of them is fetched
- Syncs with `versions: latest` and `translate_ids: true`
- Validates:
  - Sync completes and reports only the deleted subject as failed
  - The other subject is synced

### `TestIntegrationSchemaRegistryMigratorSyncLoopSingleRetryLoop`

Verifies that with `interval: 0s` only one retry loop runs, as `SyncLoop` is started on every output connect.
- Registers a subject at source that the destination rejects, and runs an initial sync that partially fails
- Starts the sync loop twice
- Relaxes the destination subject compatibility to `NONE`, then repeats with a second failing subject after the loop stops
- Validates:
  - One call returns immediately while the other keeps retrying
  - The running loop stops once the subject syncs
  - A later call starts a new loop after the previous one stopped

### `TestIntegrationSchemaRegistryMigratorSyncCompatibilityFailureKeepsMapping`

Verifies that a registered schema keeps its ID mapping when syncing its subject compatibility fails.
- Registers a subject with an explicit `FULL` compatibility at source
- Puts a proxy in front of the destination that rejects setting the subject compatibility with 422
- Syncs with `versions: latest` and `translate_ids: true`
- Validates:
  - Sync reports only the compatibility sync as failed
  - Records encoded with the schema translate to its destination ID

### `TestIntegrationSchemaRegistryMigratorSyncVersionsGoneDuringTraversal`

Verifies that a subject whose versions disappear at the source during its traversal fails only that root.
- Registers a subject with two versions and another subject at source, behind a proxy that returns 404 when the versions of the first are listed
- Syncs with `versions: all` and `translate_ids: true`
- Validates:
  - Sync completes and reports only that root as failed, and it is the only root to retry
  - The other subject is synced

### `TestIntegrationSchemaRegistryMigratorSyncPrunesFailureDeletedAtSource`

Verifies that a failed schema stops being rejected once the source no longer has it.
- Registers two incompatible versions of a subject at source, and pre-registers only v1 at destination under the default `BACKWARD` compatibility
- Syncs with `versions: all` and `translate_ids: true`, then soft-deletes v2 at source and syncs again
- Validates:
  - Records encoded with v2 are rejected after the first sync
  - After the second sync, v2's ID is handled as an unknown ID and no subject is left to retry

### `TestIntegrationSchemaRegistryMigratorSyncKeepsFailureStillAtSource`

Verifies that a failed schema that no sync visits any more stays rejected while the source still has it.
- Registers a subject whose v1 the destination rejects, and a second subject that references v1
- Syncs with `versions: latest` and `translate_ids: true`, then deletes the referrer at source and syncs again
- Validates:
  - Records encoded with the referenced v1 are still rejected
  - Records encoded with the deleted referrer are no longer rejected

### `TestIntegrationSchemaRegistryMigratorSyncRetryKeepsOtherFailedRoots`

Verifies that a retry of some failed roots keeps the failed roots outside it.
- Registers two subjects that the destination rejects, and runs a full sync that fails both
- Retries one subject, then relaxes its destination compatibility and retries it again
- Validates:
  - Both subjects are still to retry after the first retry
  - Only the other subject is left to retry once the retried one syncs

## Schema Registry Fan-out Test (`migrator_schema_registry_fanout_integration_test.go`)

### `TestIntegrationSchemaRegistryMigratorSyncSharedSchemaFanout`

Guards against O(N^2) destination-registry traffic when syncing subjects that share identical schema bodies, in both ID-translation modes.
- Creates source and destination clusters with Schema Registry (per subtest: `translate_ids: true` with READWRITE destination, `translate_ids: false` with IMPORT destination)
- Places a counting reverse proxy in front of the destination Schema Registry, recording request counts by endpoint and peak concurrent in-flight requests
- Registers 40 source subjects sharing one identical schema body (which deduplicate to a single destination schema ID)
- Syncs with `max_parallel_http_requests: 2`
- Validates:
  - At least one destination registration per subject occurred (guards against a vacuous pass)
  - Zero requests to the schema-usage endpoints (no per-registration fan-out to the subject-versions sharing the destination schema ID)
  - Peak destination request concurrency respects `max_parallel_http_requests`

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
