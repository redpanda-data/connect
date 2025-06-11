# GCP Spanner

## Terraform

To run tests, create a Spanner instance and database for testing.

Inside the `terraform` directory, run the following commands:

* Run `terraform init` and `terraform apply` to create the resources.
* Run `terraform destroy` to destroy the resources.

## Running tests

Given the default configuration, you can run tests as follows from the root of the repository:

```bash
go test -v -run TestIntegrationReal ./internal/impl/gcp/enterprise/... \
  -spanner.project_id=sandbox-rpcn-457914 \
  -spanner.instance_id=rpcn-tests-spanner \
  -spanner.database_id=rpcn-tests
```

This runs the integration tests that require a real Spanner environment.
