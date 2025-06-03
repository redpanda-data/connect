# Redpanda Migrator

Redpanda Migrator is a tool for migrating data (records, consumer group updates and schemas) between various Kafka clusters.

It guarantees at-least-once delivery semantics for Kafka records, so the destination cluster can end up receiving duplicate records.

Given this delivery guarantee model, record offsets in the destination cluster will differ from the source offsets. This forces us to perform record offset translation when migrating consumer group updates.

While more sophisticated approaches have been implemented (see [here](https://web.archive.org/web/20250112205959/https://blog.cloudera.com/a-look-inside-kafka-mirrormaker-2/) and [here](https://current.confluent.io/2024-sessions/mirrormaker-2s-offset-translation-isnt-exactly-once-and-thats-okay)), they are difficult to reason about and troubleshoot.

In Redpanda Migrator, we chose a simple record timestamp-based approach, where, each time we get an update on the `__consumer_offsets` topic in the source cluster, we first try to determine the timestamp of the consumed record which triggered this update and then use this timestamp to do another lookup in the destination cluster to determine the offset of the corresponding migrated record.

This approach requires that all topics which need to be migrated contain records which have monotonically-increasing timestamps, including duplicates.

## Sequence diagrams

### Consumer group offsets migration

Consumer group offsets are migrated independently from records and schemas.

1. Migrate a consumer group update for a record in topic `T` partition `P` located at an arbitrary offset `O` in the `[start, end)` interval in the Source cluster which translates to offset `O'` in the Destination cluster.

```mermaid
sequenceDiagram

participant Source
participant Offsets Input
participant Offsets Output
participant Destination

Source->>Offsets Input: O = OffsetFetch()
Source->>Offsets Input: X = ListEndOffsets(T, P)
Source->>Offsets Input: Check X < O
Source->>Offsets Input: TS = ReadTimestamp(T, P, O)
Offsets Input->>Offsets Output: (T, P, TS)
Offsets Output->>Destination: O' = ListOffsetsAfterMilli(T, P, TS)
Offsets Output->>Destination: CommitOffsets(T, P, O')
```

2. Migrate a consumer group update for the record at the end of topic `T` partition `P` with offset `O` in the Source cluster which translates to offset `O'` in the Destination cluster.

Note: `-1` is used to retrieve the last record offset from a topic.

```mermaid
sequenceDiagram

participant Source
participant Offsets Input
participant Offsets Output
participant Destination

Source->>Offsets Input: O = OffsetFetch()
Source->>Offsets Input: X = ListEndOffsets(T, P)
Source->>Offsets Input: Check X == O
Source->>Offsets Input: TS = ReadTimestamp(T, P, -1)
Offsets Input->>Offsets Output: (T, P, TS)
Offsets Output->>Destination: O', TS' = ListOffsetsAfterMilli(T, P, TS)
Offsets Output->>Destination: If TS' != -1 then O'' = ListEndOffsets(T, P)
Offsets Output->>Destination: CommitOffsets(T, P, O' + 1 ==  O'' ? O'' : O')
```

### Record and schema migration

1. End to end flow.

```mermaid
sequenceDiagram

participant Source SR
participant SR Input
participant SR Output
participant Destination SR
participant Source
participant Migrator Input
participant Migrator Output
participant Destination

Source SR->>SR Input: Read all via REST API
SR Input->>SR Output: Stream schemas
SR Output->>Destination SR: POST via REST API
Source->>Migrator Output: Read all topics on startup
Migrator Output->>Destination: Create all topics & ACLs
Source->>Migrator Input: Record batch
Migrator Input->>Migrator Output: Record batch
Migrator Output->>Migrator Output: Lookup topic in local cache
Migrator Output->>Source: Fetch topic config
Migrator Output->>Destination: Create topic & ACLs and update local cache
Migrator Output->>Migrator Output: R = Foreach record
Migrator Output->>Migrator Output: Lookup schema ID X in local cache
Migrator Output->>SR Output: GetDestinationSchemaID(X)
SR Output->>SR Output: Lookup X in local cache
SR Output->>Source SR: S = GetSchemaByID(X)
SR Output->>Destination SR: Y = getOrCreateSchemaID(S)
SR Output->>Migrator Output: Y
Migrator Output->>Migrator Output: UpdateID(R, Y)
Migrator Output->>Destination: Record batch
```

2. Detailed view of schema creation when given ID `X`, subject `S` and version `V` along with the schema body `B` as input.

```mermaid
sequenceDiagram

participant Source SR
participant SR Output
participant Destination SR

SR Output->>SR Output: Lookup ID Y = (X,S,V) in local cache
SR Output->>Source SR: Fetch B.references recursively
SR Output->>Destination SR: Backfill references and cache new IDs locally
SR Output->>Source SR: Fetch previous versions for S
SR Output->>Destination SR: Backfill previous versions and cache new IDs locally
SR Output->>Destination SR: Create Y from (S,V,B)
SR Output->>SR Output: Cache Y = (X,S,V) in the local cache

```
