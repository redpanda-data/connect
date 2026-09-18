# Snowflake integration tests

This package has two real-Snowflake-account integration suites:

- **`snowflake_streaming_pipe`** (`streaming_pipe_*_test.go`) — covered below.
- **`snowflake_streaming`**, the older table-based output (`integration_test.go`) — has its
  own, separate, not-yet-unified env vars; see that file and
  [`streaming/README.md`](streaming/README.md). Converging it onto the same `SNOWFLAKE_*`
  set documented here is a tracked follow-up, not done yet — until then, running both suites
  means configuring both naming schemes side by side against the same account.

Every variable below, what it's for, and where to find your own value is documented in full on
`loadSnowflakeITEnv` in [`streaming_pipe_ithelper_test.go`](streaming_pipe_ithelper_test.go) —
that's the canonical reference. This file is the walkthrough for getting from "nothing" to a
working set of values.

## 1. Get a Snowflake account

Any edition works, including a free trial. A trial account already comes with a default
warehouse (`COMPUTE_WH`) and the `ACCOUNTADMIN` role, which is the simplest path for a personal,
disposable test setup. For a shared account, use a scoped custom role instead (step 3).

## 2. Pick a database, schema, and warehouse

```sql
SHOW DATABASES;
SHOW SCHEMAS IN DATABASE <database>;
SHOW WAREHOUSES;
```

Or create a dedicated throwaway pair:

```sql
CREATE DATABASE IF NOT EXISTS RPCN_IT;
CREATE SCHEMA IF NOT EXISTS RPCN_IT.PUBLIC;
```

## 3. Pick (or create) a role

The suite creates, drops, and recreates tables and pipes in your schema, and needs the same
Snowpipe Streaming ingest privileges the production output's own `role` field requires — see
[Snowflake's required-privileges doc](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#required-access-privileges).
For a personal trial account, just use `ACCOUNTADMIN` and skip the grants below. For anything
shared:

```sql
CREATE ROLE IF NOT EXISTS RPCN_IT_ROLE;
GRANT USAGE ON DATABASE RPCN_IT TO ROLE RPCN_IT_ROLE;
GRANT USAGE, CREATE TABLE, CREATE PIPE ON SCHEMA RPCN_IT.PUBLIC TO ROLE RPCN_IT_ROLE;
GRANT USAGE ON WAREHOUSE <warehouse> TO ROLE RPCN_IT_ROLE;
GRANT ROLE RPCN_IT_ROLE TO USER <user>;
```

## 4. Set up key-pair auth for the user

If you don't already know `<user>` (the username you're logged in as, needed below and for
`SNOWFLAKE_USER` later): run `SELECT CURRENT_USER();`, or check the profile icon/avatar in
Snowsight's UI (usually bottom-left) if you can't run SQL yet.

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Take `rsa_key.pub`'s contents, strip the `-----BEGIN/END PUBLIC KEY-----` lines and newlines,
and register it:

```sql
ALTER USER <user> SET RSA_PUBLIC_KEY='<the stripped base64 from rsa_key.pub>';
```

Full walkthrough (encrypted-key and key-rotation forms included) at
[Snowflake's key-pair auth doc](https://docs.snowflake.com/en/user-guide/key-pair-auth).

## 5. Find your account identifier

```sql
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();
```

That's `SNOWFLAKE_ACCOUNT` (e.g. `MYORG-MYACCOUNT`). Leave `SNOWFLAKE_ACCOUNT_HOST` unset
unless you're on PrivateLink or another non-standard hostname.

## 6. Export everything

```bash
export SNOWFLAKE_ACCOUNT="MYORG-MYACCOUNT"
export SNOWFLAKE_USER="<user>"
export SNOWFLAKE_ROLE="RPCN_IT_ROLE"          # or ACCOUNTADMIN
export SNOWFLAKE_PRIVATE_KEY_FILE="$(pwd)/rsa_key.p8"
export SNOWFLAKE_DATABASE="RPCN_IT"
export SNOWFLAKE_SCHEMA="PUBLIC"
export SNOWFLAKE_WAREHOUSE="<warehouse>"      # e.g. COMPUTE_WH
export SNOWFLAKE_PIPE="RPCN_IT_PIPE"          # a name you're choosing, not a pre-existing object
export SNOWFLAKE_TABLE="RPCN_IT_TABLE"        # same -- created/dropped fresh on every run
```

(`SNOWFLAKE_PRIVATE_KEY_PASS` only if you encrypted the key.)

## 7. Run the suite

`integration.CheckSkip` only runs a test when `-run` matches it — nothing else gates it. Use a
pattern scoped to just these tests, not the broader `task test:integration -- snowflake`: that
task's default `-run` pattern matches any integration test in this package, including
`snowflake_streaming`'s (which don't skip cleanly on missing env vars — they fall back to
hardcoded defaults referencing a different account entirely, and just fail).

```bash
export GOTOOLCHAIN=go1.26.6   # matches go.mod; this repo pins its toolchain for gates
go test ./internal/impl/snowflake/ -run '^TestIntegrationSnowflakeStreamingPipe' -v
```

Runs every test in the suite: the main tier 1/2/2b suite, the max-in-flight/reorder regression,
the OAuth scope probe, the cross-restart redelivery test, the inline/encrypted private-key auth
tests, and the smaller edge-case tests (missing pipe, unset offset_token, multi-channel batches,
row rejection). Expect a few minutes — several assertions poll for up to 90s (tier 1/2/2b) or 3
minutes (max-in-flight) waiting for Snowflake's asynchronous commit.

## 8. Tier 2 and 2b will report as skipped on most accounts

That's expected, not broken. They need Snowflake's `DP_STREAMING_TRANSFORMATIONS`
private-preview flag, which even `ACCOUNTADMIN` can't self-enable — only Snowflake support can
turn it on for your account. Ask them (or your account team) if you need those tiers to
actually execute rather than skip.

## 9. Re-running / cleanup

Nothing to tear down between runs — `SNOWFLAKE_PIPE`/`SNOWFLAKE_TABLE` (and the tier 2/2b
`_DIM`/`_ENRICHED`/`_FILTERED` siblings derived from them) get `create or replace`'d fresh each
time. To remove them afterward: `DROP TABLE`/`DROP PIPE` for each, or drop the whole `RPCN_IT`
database if you created one solely for this.
