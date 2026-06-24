output "dynamodb_table_name" {
  value = module.dynamodb.dynamodb_table_name
}

output "dynamodb_stream_arn" {
  value = module.dynamodb.dynamodb_stream_arn
}

# The runner reads this when rendering the connector config and when emitting
# AWS_REGION into the seeder env. Wiring the region as a TF output (rather than
# relying on AWS_REGION on the runner's environment) keeps the bench
# reproducible across operators.
output "aws_region" {
  value = var.region
}

# Capacity outputs feed the reset bash via engineSpec.ExtraEnvVars. The bash
# block drops + recreates the DDB table between sweep points; without these
# outputs it would have to hardcode the WCU/RCU values, decoupling the
# recreate from whatever was provisioned by terraform.
output "read_capacity" {
  value = tostring(var.read_capacity)
}

output "write_capacity" {
  value = tostring(var.write_capacity)
}
