# Snowflake integration tests

This package has two real-account integration suites:

- **`snowflake_streaming_pipe`** (`streaming_pipe_*_test.go`), covered below.
- **`snowflake_streaming`** (`integration_test.go`), which uses its own env vars; see that file
  and [`streaming/README.md`](streaming/README.md).

Each variable below is documented on `loadSnowflakeITEnv` in
[`streaming_pipe_ithelper_test.go`](streaming_pipe_ithelper_test.go).

## 1. Get a Snowflake account

Any edition works, including a free trial (which comes with `COMPUTE_WH` and `ACCOUNTADMIN`).
For a shared account, use a scoped role (step 3).

## 2. Pick a database, schema, and warehouse

```sql
SHOW DATABASES;
SHOW SCHEMAS IN DATABASE <database>;
SHOW WAREHOUSES;
```

Or create a throwaway pair:

```sql
CREATE DATABASE IF NOT EXISTS RPCN_IT;
CREATE SCHEMA IF NOT EXISTS RPCN_IT.PUBLIC;
```

## 3. Pick (or create) a role

The suite creates and drops tables and pipes in your schema and needs the
[Snowpipe Streaming ingest privileges](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#required-access-privileges).
On a personal trial account, `ACCOUNTADMIN` is fine. Otherwise:

```sql
CREATE ROLE IF NOT EXISTS RPCN_IT_ROLE;
GRANT USAGE ON DATABASE RPCN_IT TO ROLE RPCN_IT_ROLE;
GRANT USAGE, CREATE TABLE, CREATE PIPE ON SCHEMA RPCN_IT.PUBLIC TO ROLE RPCN_IT_ROLE;
GRANT USAGE ON WAREHOUSE <warehouse> TO ROLE RPCN_IT_ROLE;
GRANT ROLE RPCN_IT_ROLE TO USER <user>;
```

## 4. Set up key-pair auth for the user

`<user>` is your login name (`SELECT CURRENT_USER();`).

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Register `rsa_key.pub`'s contents without the `BEGIN/END` lines or newlines:

```sql
ALTER USER <user> SET RSA_PUBLIC_KEY='<the stripped base64 from rsa_key.pub>';
```

See [Snowflake's key-pair auth doc](https://docs.snowflake.com/en/user-guide/key-pair-auth)
for encrypted keys and rotation.

## 5. Find your account identifier

```sql
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();
```

That is `SNOWFLAKE_ACCOUNT` (e.g. `MYORG-MYACCOUNT`). Leave `SNOWFLAKE_ACCOUNT_HOST` unset
unless you are on PrivateLink or another non-standard hostname.

## 6. Export everything

```bash
export SNOWFLAKE_ACCOUNT="MYORG-MYACCOUNT"
export SNOWFLAKE_USER="<user>"
export SNOWFLAKE_ROLE="RPCN_IT_ROLE"          # or ACCOUNTADMIN
export SNOWFLAKE_PRIVATE_KEY_FILE="$(pwd)/rsa_key.p8"
export SNOWFLAKE_DATABASE="RPCN_IT"
export SNOWFLAKE_SCHEMA="PUBLIC"
export SNOWFLAKE_WAREHOUSE="<warehouse>"      # e.g. COMPUTE_WH
export SNOWFLAKE_PIPE="RPCN_IT_PIPE"          # a name you choose; created and dropped by the suite
export SNOWFLAKE_TABLE="RPCN_IT_TABLE"        # same
```

Add `SNOWFLAKE_PRIVATE_KEY_PASS` only if you encrypted the key.

## 7. Run the suite

Scope `-run` to these tests: the broader `task test:integration -- snowflake` also matches
`snowflake_streaming`'s tests, which use different env vars.

```bash
export GOTOOLCHAIN=go1.26.6   # matches go.mod
go test ./internal/impl/snowflake/ -run '^TestIntegrationSnowflakeStreamingPipe' -v
```

Expect a few minutes; several tests poll for up to 90s waiting for Snowflake's asynchronous
commit.

## 8. Tier 2 and 2b will report as skipped on most accounts

They need Snowflake's `DP_STREAMING_TRANSFORMATIONS` private-preview flag, which only
Snowflake support can enable for an account.

## 9. Re-running / cleanup

Nothing to tear down between runs: `SNOWFLAKE_PIPE`/`SNOWFLAKE_TABLE` and their derived
siblings are `create or replace`'d each time. To remove them, `DROP TABLE`/`DROP PIPE` each, or
drop the `RPCN_IT` database if you created it for this.
