# Comma-joined list of source table names. Kept as a single string (not a list)
# so the runner's string-keyed terraform-output map and the DDB_TABLE
# ExtraEnvVar contract stay intact. The seeder gets its table list from the
# scenario's dataset.tables, not from this output, so the value is informational
# / for the (vestigial) DDB_TABLE env only.
output "dynamodb_table_name" {
  value = join(",", var.table_names)
}

# Informational only — the Connect input discovers each stream ARN via
# DescribeTable, so callers don't wire these into the connector config.
output "dynamodb_stream_arn" {
  value = join(",", [for t in aws_dynamodb_table.bench : t.stream_arn])
}
