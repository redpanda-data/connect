---
name: cdc-operations
description: This skill should be used when users run, start, backfill, or troubleshoot a Redpanda Connect change data capture (CDC) input on a live database. Trigger when users mention "postgres_cdc", "mysql_cdc", "oracledb_cdc", "microsoft_sql_server_cdc", "salesforce_cdc", a snapshot or backfill signal, a checkpoint or replay position, or a CDC pipeline that won't start, stopped streaming, or found no tables.
---

# Redpanda Connect CDC Operations

Run and repair CDC pipelines without stalling the source database or losing changes.

## Objective

CDC inputs act on production databases and keep a stream position that cannot be rebuilt.
This skill holds only the checks to make before acting and the actions that need the user's approval.
For config fields, defaults, and database setup, use the `component-search` skill's `Online Component Documentation` tool.

## General Rules

- Never kill another database session, alter a user's table, or delete, reset, or rewrite a checkpoint yourself. Explain the effect and ask first.
- A schema change (a new key, a table owner, replica identity) belongs to the user. Name the change and leave it to them.

## postgres_cdc

**Backfill a table with a signal, after checking the table.**
When a backfill signal was rejected, or before you insert one, check the table in this order.
A rejected signal is only logged: replication continues and nothing else reports it.

1. It is in `tables`, or `tables` is empty.
2. It has a primary key, and no key column is `bytea`, `interval`, `bit`, `bit varying`, or a range or multirange type. Check the type of each key column (for example with `format_type`), not just that a key exists. No connector setting works around a rejected key type, including `stream_snapshot`. Name the column as the cause and leave changing the key to the user.
3. If it is partitioned, the publication `pglog_stream_<slot_name>` has `publish_via_partition_root = true`.
4. If it has large (TOASTable) columns, it needs `REPLICA IDENTITY FULL` while it backfills. Without it, an update during the backfill can deliver only the `unchanged_toast_value` placeholder for an unchanged large column. Tell the user that risk and the cost of the fix (`ALTER TABLE ... REPLICA IDENTITY FULL` takes a lock, and every later update and delete logs the full old row), and ask before you run it.

## mysql_cdc

**Before starting a pipeline that will snapshot, check for open transactions on its tables.**
The snapshot runs when `stream_snapshot` is `true` and the checkpoint cache holds no position.
It starts with `FLUSH TABLES <tables> WITH READ LOCK`, which waits for every transaction that has one of those tables open. While it waits, other sessions' queries on those tables queue behind it.

1. On the primary, look for long-running transactions and the locks they hold (`information_schema.innodb_trx`, `performance_schema.metadata_locks`, the process list).
2. If one touches a listed table, don't start the pipeline. Tell the user which session holds it and let them choose: wait for it to finish, approve ending that session, or start at a quieter time.

## oracledb_cdc

**When the input finds no user tables, check who owns the tables before grants or patterns.**
Table discovery skips every owner whose name starts with `C##`.
No grant, `include` pattern, or `pdb_name` change makes a common user's tables capturable.
The tables must be owned by a local (non-`C##`) user inside the PDB, with `include` updated to match.
Moving them is a schema change: explain it and leave it to the user.

## microsoft_sql_server_cdc

**Give each pipeline's built-in checkpoint its own schema.**
Without `checkpoint_cache`, the input keeps its checkpoint in one fixed row of `checkpoint_cache_table_name` and writes it through a stored procedure, `<schema>.CdcCheckpointCacheUpdate`, that every start re-creates to point at its own table.
Two pipelines whose checkpoint tables share a schema (the default is `rpcn`) overwrite each other's checkpoints, even with different table names. `checkpoint_cache_key` does not help: the built-in cache ignores it.

For a second pipeline on the same database, use one of:
- A checkpoint table in a different schema (create the schema first and grant the login rights in it)
- A checkpoint table in another database, through `checkpoint_cache_connection_string`
- An external `checkpoint_cache` resource with its own `checkpoint_cache_key`

Leave the existing pipeline's checkpoint where it is: moving it discards that pipeline's position.

## salesforce_cdc

**Never delete, reset, or rewrite a running pipeline's checkpoint, or change its `checkpoint_cache_key`, without asking the user first.**
The checkpoint holds every topic's replay position.
Without it, each topic resubscribes from `replay_preset`, and with `latest` the changes made in between are never delivered.

- When a topic stopped on an event it cannot decode, keep the checkpoint. Explain that skipping the event loses every change since the checkpoint, and ask the user or escalate the event (to the Salesforce admin or Redpanda support).
- To backfill an sObject added to `topics` later (the snapshot runs once per checkpoint), read it with a separate one-off pipeline.
