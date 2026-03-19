CDC Schema Registry
===================

Demonstrates a full CDC pipeline: capturing changes from PostgreSQL, encoding them as Avro via the Schema Registry (using the common schema metadata from the CDC input), and consuming the Avro-encoded messages from a Redpanda topic.

The schema is **auto-registered** with the Schema Registry — no manual schema management required. The `postgres_cdc` input attaches a common schema to each message's metadata, and the `schema_registry_encode` processor converts it to Avro and registers it automatically.

## Architecture

```
┌─────────────┐     ┌──────────┐     ┌────────────────────┐     ┌──────────┐     ┌──────────────┐
│  generate   │────>│ postgres │────>│  cdc + avro encode │────>│ redpanda │────>│ avro decode  │
│ (sample data)│     │          │     │  (schema registry) │     │  topic   │     │  + stdout    │
└─────────────┘     └──────────┘     └────────────────────┘     └──────────┘     └──────────────┘
```

**Three pipelines:**

1. **generate.yaml** — Produces random product data and inserts it into PostgreSQL every 2 seconds.
2. **cdc.yaml** — Streams CDC events from PostgreSQL, encodes them as Avro using the Schema Registry, and writes to a Redpanda topic.
3. **consume.yaml** — Reads from the Redpanda topic, decodes the Avro back to JSON, enriches with CDC metadata, and prints to stdout.

## Run

```sh
docker compose up -d
```

## See output

```sh
docker compose logs -f connect-consume
```

You should see JSON messages with the CDC operation, table name, and decoded row data:

```json
{"data":{"category":"electronics","created_at":"...","id":1,"in_stock":true,"name":"premium widget","price":"29.99"},"operation":"read","table":"products"}
```

## Clean up

```sh
docker compose down -v
```
