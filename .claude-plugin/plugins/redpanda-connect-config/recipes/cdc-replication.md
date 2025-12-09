# Change Data Capture (CDC) Replication

**Pattern**: Kafka Patterns - Database CDC Replication
**Difficulty**: Advanced
**Components**: postgres_cdc, sql_raw, switch, batching
**Use Case**: Replicate database changes in real-time using Postgres logical replication to keep databases synchronized

## Overview

This recipe demonstrates Change Data Capture (CDC) for replicating database changes. It streams changes from a Postgres database using logical replication, groups them by transaction, and applies them to a destination database using MERGE (upsert) and DELETE operations. This pattern is essential for building real-time data synchronization pipelines.

## Configuration

See [`cdc-replication.yaml`](./cdc-replication.yaml) for the complete configuration.

## Key Concepts

### 1. Postgres CDC Input

The `postgres_cdc` input streams database changes using Postgres logical replication:
- **Replication Slot**: Named slot for tracking position
- **Snapshot**: Initial table snapshot before streaming changes
- **Transaction Markers**: Begin/commit messages for grouping
- **Operations**: Insert, update, delete with full row data

### 2. Transaction-Based Batching

Changes are grouped by transaction to maintain consistency:
```yaml
batching:
  check: '@operation == "commit"'
  period: 10s
```

All changes in a transaction are batched together before being applied. This preserves foreign key constraints and data consistency.

### 3. Switch Output for Operation Types

Different operations require different SQL:
- **Insert/Update** → SQL MERGE (upsert)
- **Delete** → SQL DELETE

The switch routes based on `@operation` metadata.

### 4. SQL MERGE for Upserts

The MERGE statement handles both inserts and updates atomically:
```sql
MERGE INTO dst_table AS old
USING (SELECT $1 id, $2 foo, $3 bar) AS new
ON new.id = old.id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

This ensures idempotency - replaying the same change is safe.

## Important Details

- **Security**: Use environment variables for DSN (`${POSTGRES_DSN}`)
- **Performance**:
  - Transaction batching reduces round-trips
  - Replication slot prevents data loss
  - Window period (10s) must accommodate largest transaction
- **Error handling**: `strict_mode: true` ensures all messages match a case
- **Idempotency**: MERGE operations can be safely retried

## Testing

```bash
# Set environment variables
export SOURCE_DSN="postgres://user:pass@source:5432/db?sslmode=disable"
export DEST_DSN="postgres://user:pass@dest:5432/db?sslmode=disable"

# Create replication slot on source database
psql $SOURCE_DSN -c "SELECT pg_create_logical_replication_slot('test_slot', 'pgoutput');"

# Run the pipeline
rpk connect run cdc-replication.yaml

# In another terminal, make changes to source database
psql $SOURCE_DSN -c "INSERT INTO my_src_table (id, foo, bar) VALUES (1, 'test', 'data');"
psql $SOURCE_DSN -c "UPDATE my_src_table SET foo='updated' WHERE id=1;"
psql $SOURCE_DSN -c "DELETE FROM my_src_table WHERE id=1;"

# Check destination database
psql $DEST_DSN -c "SELECT * FROM my_dst_table;"
```

## Variations

**Kafka as Destination:**
```yaml
output:
  switch:
    cases:
      - check: '@operation == "delete"'
        output:
          kafka_franz:
            topic: deletes
      - output:
          kafka_franz:
            topic: upserts
```

**Multi-Table Replication:**
```yaml
input:
  postgres_cdc:
    tables: [table1, table2, table3]

output:
  switch:
    cases:
      - check: '@table == "table1"'
        output:
          sql_raw:
            query: |
              MERGE INTO dst_table1 ...
```

## Related Recipes

- [Content-Based Router](./content-based-router.md) - Similar switch-based routing pattern
- [Stateful Counter](../stateful/stateful-counter.md) - Track CDC metrics

## References

- [Postgres CDC Input Documentation](https://github.com/redpanda-data/connect/blob/main/docs/modules/components/pages/inputs/postgres_cdc.adoc)
- [SQL Raw Output Documentation](https://github.com/redpanda-data/connect/blob/main/docs/modules/components/pages/outputs/sql_raw.adoc)
- [Postgres Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
