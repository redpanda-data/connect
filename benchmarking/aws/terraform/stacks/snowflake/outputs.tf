# sensitive = true because aws_ssm_parameter data-source values are always
# sensitive-flagged in Terraform; the runner's Outputs() reads them from
# `terraform output -json` regardless. None of these five is actually secret.
output "snowflake_account" {
  value     = data.aws_ssm_parameter.account.value
  sensitive = true
}
output "snowflake_user" {
  value     = data.aws_ssm_parameter.user.value
  sensitive = true
}
output "snowflake_role" {
  value     = data.aws_ssm_parameter.role.value
  sensitive = true
}
output "snowflake_database" {
  value     = data.aws_ssm_parameter.database.value
  sensitive = true
}
output "snowflake_schema" {
  value     = data.aws_ssm_parameter.schema.value
  sensitive = true
}
