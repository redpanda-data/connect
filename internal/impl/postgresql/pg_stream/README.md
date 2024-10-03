# PostgreSQL Logical Replication Streaming Plugin for Benthos

Welcome to the PostgreSQL Logical Replication Streaming Plugin for Benthos! This plugin allows you to seamlessly stream data changes from your PostgreSQL database using Benthos, a versatile stream processor.

## Features

- **Real-time Data Streaming:** Capture data changes in real-time as they happen in your PostgreSQL database.

- **Flexible Configuration:** Easily configure the plugin to specify the database connection details, replication slot, and table filtering rules.

- **Checkpoints:** Store your replication consuming progress in Redis

## Prerequisites

Before you begin, make sure you have the following prerequisites:

- [PostgreSQL](https://www.postgresql.org/): Ensure you have a PostgreSQL database instance that supports logical replication.

### Create benthos configuration with plugin

```yaml
input:
  label: postgres_cdc_input
  # register new plugin
  pg_stream:
    host: datbase hoat
    slot_name: reqplication slot name
    user: postgres username with replication permissions
    password: password
    port: 5432
    schema: schema you want to replicate tables from
    stream_snapshot: set true if you want to stream existing data. If set to false only a new data will be streamed
    database: name of the database
    checkpoint_storage: redis uri if you want to store checkpoints
    tables: ## list of tables you want to replicate
      - table_name
```

### Register processor to pretty format your data
By default, plugins exports raw `wal2json` message. If you want to receive your data as json structure 
without metadata to transform it with benthos - you can register `pg_stream_schemaless` plugin to transform it

```yaml
pipeline:
  processors:
    - label: pretty_changes_processor
      pg_stream_schemaless: { }
```
