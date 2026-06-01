# Bootstrap: one-time AWS account setup

You're using this skill against your own AWS account (per-teammate model). This page is the one-time setup. After it's done you can ignore it; the rest of the skill is account-neutral.

> **Why per-teammate?** Cleaner spend attribution, no TF-state-lock concurrency when two people run at once, no risk of one teammate's destroy clobbering another's infra. Cost: ~50 lines of one-time setup.

## Step 1: AWS account access

You need an AWS account (Redpanda-owned sub-account or your own) with permissions for: EC2, RDS, S3, IAM, SSM, CloudWatch Logs, Lambda, EventBridge, SNS, DynamoDB, Cost Explorer. `AdministratorAccess` is sufficient.

Set up an aws-vault profile pointing at it. Conventional name: `bench-<initials>`. Example `~/.aws/config` entry:

```ini
[profile bench-<initials>]
sso_session = redpanda
sso_account_id = <your-account-id>
sso_role_name = AdministratorAccess
region = us-east-2
```

(Or whatever your SSO config dictates — the only requirement is that `aws-vault exec bench-<initials>` works.)

Verify:

```bash
aws-vault exec bench-<initials> -- aws sts get-caller-identity
```

## Step 2: Create the S3 state bucket

S3 bucket names are globally unique. Use a name that includes your initials or similar:

```bash
aws-vault exec bench-<initials> -- \
  aws s3 mb s3://redpanda-connect-bench-tfstate-<initials> --region us-east-2

aws-vault exec bench-<initials> -- \
  aws s3api put-bucket-versioning \
    --bucket redpanda-connect-bench-tfstate-<initials> \
    --versioning-configuration Status=Enabled

aws-vault exec bench-<initials> -- \
  aws s3api put-public-access-block \
    --bucket redpanda-connect-bench-tfstate-<initials> \
    --public-access-block-configuration \
      BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

aws-vault exec bench-<initials> -- \
  aws s3api put-bucket-encryption \
    --bucket redpanda-connect-bench-tfstate-<initials> \
    --server-side-encryption-configuration \
      '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
```

## Step 3: Create the DynamoDB lock table

```bash
aws-vault exec bench-<initials> -- \
  aws dynamodb create-table \
    --table-name redpanda-connect-bench-tflocks-<initials> \
    --attribute-definitions AttributeName=LockID,AttributeType=S \
    --key-schema AttributeName=LockID,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region us-east-2
```

Wait ~15s for it to become ACTIVE:

```bash
aws-vault exec bench-<initials> -- \
  aws dynamodb describe-table \
    --table-name redpanda-connect-bench-tflocks-<initials> \
    --query 'Table.TableStatus' --region us-east-2
```

## Step 4: Edit backend config locally

The framework currently hardcodes the bucket + DDB table in several files. Edit each to match what you created in Steps 2 + 3.

**For the existing stacks (run once):**

```bash
# 1. benchmarking/aws/terraform/backend.hcl
sed -i '' "s/redpanda-connect-bench-tfstate/redpanda-connect-bench-tfstate-<initials>/" \
  benchmarking/aws/terraform/backend.hcl
sed -i '' "s/redpanda-connect-bench-tflocks/redpanda-connect-bench-tflocks-<initials>/" \
  benchmarking/aws/terraform/backend.hcl

# 2. benchmarking/aws/terraform/stacks/postgres/main.tf (backend block, line ~24)
sed -i '' "s/redpanda-connect-bench-tfstate/redpanda-connect-bench-tfstate-<initials>/" \
  benchmarking/aws/terraform/stacks/postgres/main.tf

# 3. benchmarking/aws/terraform/stacks/mysql/main.tf (backend block, line ~24)
sed -i '' "s/redpanda-connect-bench-tfstate/redpanda-connect-bench-tfstate-<initials>/" \
  benchmarking/aws/terraform/stacks/mysql/main.tf
```

(On Linux, drop the `''` after `-i`.)

**If you are adding a NEW stack** (e.g. `stacks/sqlserver/`), the same substitution applies to its `main.tf` as well. Run:

```bash
sed -i '' "s/redpanda-connect-bench-tfstate/redpanda-connect-bench-tfstate-<initials>/" \
  benchmarking/aws/terraform/stacks/<your-engine>/main.tf
```

Do this BEFORE the first bench run on the new stack, otherwise `terraform init` will try to connect to the shared bucket name and fail.

**Important:** Do NOT commit these edits — they're personal to your account. Either:
- Keep them in a local `bench-<initials>` branch you never push, OR
- `git stash` before pushing to the main bench branch and `git stash pop` after

A follow-up item is to parameterize this via `BENCH_TFSTATE_BUCKET` env var so local edits aren't needed. See spec's Open Items.

## Step 5: Connect Enterprise license

Place your license file at `<repo-root>/rpcn.license`. The `.gitignore` covers `*.license` so it won't be committed accidentally.

```bash
cp /path/to/your/rpcn.license /Users/<you>/Documents/connect_prakhar/rpcn.license
```

**Do NOT** place it in `~/Downloads/`, `~/Documents/`, or `~/Desktop/` — macOS TCC blocks reads from those dirs for sandboxed processes. See [traps.md#license-location](traps.md#license-location).

## Step 6: Build the cleanup Lambda zip

The orphan-cleanup Lambda needs `bootstrap.zip` built before `terraform apply`:

```bash
cd benchmarking/aws/cleanup-lambda && make zip && cd -
```

The zip is gitignored. Rebuild whenever the lambda source changes.

## Step 7: Verify with a 1-vCPU smoke

Run the cheapest possible smoke (~$1.50, ~25 min) to confirm everything wires up:

The Taskfile takes a bare relative path (prepends `benchmarking/aws/scenarios/` and appends `.yaml`). The Taskfile does NOT forward arbitrary flags, so to narrow to Connect-only you need the direct runner invocation:

`REDPANDA_LICENSE_FILEPATH` is mandatory — the runner refuses to start enterprise connectors without it.

```bash
# Pin the scenario to cpu_points: [1] temporarily.
# Option A: both engines (Taskfile default — KC sequential after Connect, ~$3 total):
aws-vault exec bench-<initials> -- \
  env REDPANDA_LICENSE_FILEPATH=$PWD/rpcn.license \
  task aws:bench scenario=postgres/orders-cdc

# Option B: Connect only (cheaper, ~$1.50). Bypass the Taskfile:
aws-vault exec bench-<initials> -- \
  env REDPANDA_LICENSE_FILEPATH=$PWD/rpcn.license \
  go run ./benchmarking/aws/runner bench \
  --scenario=benchmarking/aws/scenarios/postgres/orders-cdc.yaml \
  --repo-root=. \
  --engines=connect
```

Acceptance:
- Apply succeeds (no `Backend configuration changed` errors → your edits in Step 4 are consistent)
- Connect produces a non-zero throughput JSON
- `defer destroy` runs cleanly at the end
- Total spend ≈ $1.50

If anything fails, see [debugging-playbook.md](debugging-playbook.md).

## Done

You're set up. Future benches just need `aws-vault exec bench-<initials> -- task aws:bench ...`. Skip back to the main [SKILL.md](../SKILL.md) flow.
