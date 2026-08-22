output "dynamodb_table_name" {
  value = aws_dynamodb_table.bench.name
}

# Informational only — the Connect input discovers the current stream ARN via
# DescribeTable, so callers don't need to wire this into the connector config.
# It changes whenever the reset hook drops + recreates the table.
output "dynamodb_stream_arn" {
  value = aws_dynamodb_table.bench.stream_arn
}
