# Shared S3 backend for all benchmarking/aws/terraform/{shared,stacks/*} state.
# Region and bucket below are the dedicated benchmarking AWS account; change in
# a private fork if you run this in another account.
#
# Locking is S3-native (a .tflock object next to the state, Terraform >=
# 1.10 — the required_version floor in every root), NOT DynamoDB:
# the old redpanda-connect-bench-tflocks table was deleted out from under us
# four times (most recently failing the 2026-08-27 nightly soak before it
# provisioned anything), and a lock that depends on a second service is a
# lock that fails when that service's resource vanishes.
bucket       = "redpanda-connect-bench-tfstate"
region       = "us-east-2"
use_lockfile = true
encrypt      = true
