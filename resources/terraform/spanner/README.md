# GCP Spanner

Create and destroy a Spanner instance and database for testing.

## Usage

* Run `terraform init` and `terraform apply` to create the resources.
* Run `terraform destroy` to destroy the resources.

## Running tests

Given the default configuration, you can run tests as follows:

```bash
go test -v -run TestIntegrationReal ./internal/impl/gcp/enterprise/... \
  -spanner.project_id=sandbox-rpcn-457914 \
  -spanner.instance_id=rpcn-tests-spanner \
  -spanner.database_id=rpcn-tests
```
