# GCP Spanner

Manage a Spanner instance for integration tests.

## Running tests

Procedure:
 
* Run `task terraform:create` to create the resources. 
* Run `task test` to run the integration tests.
* Run `task terraform:destroy` to destroy the resources.
