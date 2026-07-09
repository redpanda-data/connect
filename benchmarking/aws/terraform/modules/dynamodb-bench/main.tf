# Source DynamoDB table for the CDC bench. PROVISIONED billing keeps producer-side
# throughput predictable — PAY_PER_REQUEST has burst-credit dynamics analogous to
# gp3 IOPS bursting and would muddle the comparison. Tune read_capacity /
# write_capacity to match the workload rate at the scenario layer.
#
# Streams are enabled NEW_AND_OLD_IMAGES so both the Connect input and any future
# KC counterpart see the full before/after payload.
resource "aws_dynamodb_table" "bench" {
  name           = var.table_name
  billing_mode   = "PROVISIONED"
  read_capacity  = var.read_capacity
  write_capacity = var.write_capacity
  hash_key       = "id"

  attribute {
    name = "id"
    type = "S"
  }

  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

  # AWS's default delete timeout (10m) is too short for a 40K+ WCU table —
  # delete can take 15-20m even when the table is idle. Without this override,
  # `terraform destroy` times out and leaves the table in DELETING state with
  # a stale entry in TF state, breaking the next bench's apply. Bumped to 30m
  # to cover the 100K WCU ceiling case too.
  timeouts {
    create = "20m"
    delete = "30m"
  }
}
