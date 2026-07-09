# Shared S3 backend for all benchmarking/aws/terraform/{shared,stacks/*} state.
# Region and bucket below are the dedicated benchmarking AWS account; change in
# a private fork if you run this in another account.
bucket         = "redpanda-connect-bench-tfstate"
region         = "us-east-2"
dynamodb_table = "redpanda-connect-bench-tflocks"
encrypt        = true
