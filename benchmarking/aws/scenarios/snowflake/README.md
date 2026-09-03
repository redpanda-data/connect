# Snowflake sink bench — one-time setup

The snowflake stack provisions nothing in Snowflake: it expects an existing
account (on AWS), a bench user with key-pair auth, and six SSM parameters in
the bench AWS account. Do this once; every run reads from here.

## 1. Generate an RSA key pair

`snowflake_streaming` and `snowflake-tablegen` both authenticate with an
**unencrypted** PKCS#8 key:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

## 2. Create the Snowflake objects and register the key

As `ACCOUNTADMIN` (or equivalent), with the public key body pasted **without**
the `-----BEGIN/END PUBLIC KEY-----` lines:

```sql
CREATE DATABASE IF NOT EXISTS BENCH_DB;
CREATE SCHEMA IF NOT EXISTS BENCH_DB.PUBLIC;

CREATE ROLE IF NOT EXISTS BENCH_ROLE;
GRANT USAGE ON DATABASE BENCH_DB TO ROLE BENCH_ROLE;
GRANT USAGE, CREATE TABLE ON SCHEMA BENCH_DB.PUBLIC TO ROLE BENCH_ROLE;
-- Snowpipe Streaming writes + the reset's CREATE OR REPLACE:
GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA BENCH_DB.PUBLIC TO ROLE BENCH_ROLE;

CREATE USER IF NOT EXISTS BENCH_USER
  DEFAULT_ROLE = BENCH_ROLE
  RSA_PUBLIC_KEY = 'MIIBIjANBgkq...';
GRANT ROLE BENCH_ROLE TO USER BENCH_USER;
```

No warehouse is needed: the bench's DDL and `SHOW TABLES` polling are
metadata-only, and Snowpipe Streaming ingest bills serverless credits, not a
warehouse.

## 3. Store the connection facts in SSM

In the bench AWS account/region (`us-east-2` unless overridden). The account
identifier is `<orgname>-<account_name>`:

```bash
aws ssm put-parameter --name /bench/snowflake/account  --type String --value 'MYORG-MYACCT'
aws ssm put-parameter --name /bench/snowflake/user     --type String --value 'BENCH_USER'
aws ssm put-parameter --name /bench/snowflake/role     --type String --value 'BENCH_ROLE'
aws ssm put-parameter --name /bench/snowflake/database --type String --value 'BENCH_DB'
aws ssm put-parameter --name /bench/snowflake/schema   --type String --value 'PUBLIC'
aws ssm put-parameter --name /bench/snowflake/private_key --type SecureString \
  --value "$(cat rsa_key.p8)"
```

Then delete the local `rsa_key.p8` (or keep it somewhere safe outside the
repo). The five plain parameters surface as TF outputs of the snowflake
stack; the SecureString never enters TF state — the runner host fetches it at
reset time with the `ssm:GetParameter` its `AmazonSSMManagedInstanceCore`
role policy already grants (the default `aws/ssm` KMS key decrypts for any
principal the parameter API admits).

## 4. Run

```bash
# smoke first on a fresh setup
aws-vault exec <profile> -- go run ./benchmarking/aws/runner \
  --scenario benchmarking/aws/scenarios/snowflake/orders-sink-smoke.yaml

# then the sweep
aws-vault exec <profile> -- go run ./benchmarking/aws/runner \
  --scenario benchmarking/aws/scenarios/snowflake/orders-sink.yaml
```

## Known measurement caveat

The sidecar polls `SHOW TABLES` for `bytes` and `rows`. `BYTES` reflects
compressed micro-partition storage and can lag streaming commits;
`ROW_COUNT` is the dependable signal. If the smoke shows the bytes series
lagging or plateauing while rows climb, compute MB/s as
`rows x row_size_bytes` from the `total_records` series instead of trusting
`total_files_size_bytes`.
