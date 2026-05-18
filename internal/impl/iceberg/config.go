// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
)

const (
	// Catalog fields
	ioFieldCatalog            = "catalog"
	ioFieldCatalogWarehouse   = "warehouse"
	ioFieldCatalogURL         = "url"
	ioFieldCatalogAuth        = "auth"
	ioFieldCatalogAuthOAuth2  = "oauth2"
	ioFieldCatalogAuthBearer  = "bearer"
	ioFieldCatalogAuthSigV4   = "aws_sigv4"
	ioFieldOAuth2ServerURI    = "server_uri"
	ioFieldOAuth2ClientID     = "client_id"
	ioFieldOAuth2ClientSecret = "client_secret"
	ioFieldOAuth2Scope        = "scope"
	ioFieldSigV4Region        = "region"
	ioFieldSigV4Service       = "service"
	ioFieldCatalogHeaders     = "headers"
	ioFieldCatalogTLSSkipVer  = "tls_skip_verify"

	// Table fields
	ioFieldNamespace            = "namespace"
	ioFieldTable                = "table"
	ioFieldCaseSensitiveColumns = "case_sensitive_columns"

	// Storage fields - common
	ioFieldStorage = "storage"

	// S3 storage fields
	ioFieldStorageS3            = "aws_s3"
	ioFieldS3Bucket             = "bucket"
	ioFieldS3Region             = "region"
	ioFieldS3Endpoint           = "endpoint"
	ioFieldS3ForcePathStyleURLs = "force_path_style_urls"
	ioFieldS3Credentials        = "credentials"
	ioFieldS3CredID             = "id"
	ioFieldS3CredSecret         = "secret"
	ioFieldS3CredToken          = "token"

	// GCS storage fields
	ioFieldStorageGCS  = "gcp_cloud_storage"
	ioFieldGCSBucket   = "bucket"
	ioFieldGCSEndpoint = "endpoint"
	ioFieldGCSCredType = "credentials_type"
	ioFieldGCSKeyPath  = "credentials_file"
	ioFieldGCSJSONKey  = "credentials_json"

	// Azure storage fields
	ioFieldStorageAzure          = "azure_blob_storage"
	ioFieldAzureStorageAccount   = "storage_account"
	ioFieldAzureContainer        = "container"
	ioFieldAzureEndpoint         = "endpoint"
	ioFieldAzureSASToken         = "storage_sas_token"
	ioFieldAzureConnectionString = "storage_connection_string"
	ioFieldAzureAccessKey        = "storage_access_key"

	// Schema evolution fields
	ioFieldSchemaEvolution                     = "schema_evolution"
	ioFieldSchemaEvolutionEnabled              = "enabled"
	ioFieldSchemaEvolutionPartitionSpec        = "partition_spec"
	ioFieldSchemaEvolutionTableLoc             = "table_location"
	ioFieldSchemaEvolutionSchemaMetadata       = "schema_metadata"
	ioFieldSchemaEvolutionNewColumnTypeMapping = "new_column_type_mapping"

	// Commit fields
	ioFieldCommit               = "commit"
	ioFieldManifestMergeEnabled = "manifest_merge_enabled"
	ioFieldMaxSnapshotAge       = "max_snapshot_age"
	ioFieldMaxCommitRetries     = "max_retries"

	// Performance fields
	ioFieldBatching    = "batching"
	ioFieldMaxInFlight = "max_in_flight"

	// Parquet writer fields
	ioFieldParquet               = "parquet"
	ioFieldParquetStringEncoding = "string_encoding"
)

// icebergOutputConfig returns the configuration spec for the Iceberg output.
func icebergOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Version("4.80.0").
		Summary("Write data to Apache Iceberg tables via REST catalog.").
		Description(`
Write streaming data to Apache Iceberg tables using the REST catalog API. This output supports:

* Multiple storage backends (S3, GCS, Azure)
* Automatic table creation with schema detection
* Partition transforms (year, month, day, hour, bucket, truncate)
* Schema evolution (automatic column addition)
* Transaction retry logic for concurrent writes

This output is designed to work with REST catalog implementations like Apache Polaris, AWS Glue Data Catalog, and the Databricks Unity Catalog.

Currently only version 2 of the Iceberg specification is supported. Any pre-existing version 1 tables will be upgraded to version 2 automatically.

=== Apache Polaris

To use with https://polaris.apache.org[Apache Polaris^]:

* Set `+"`catalog.url`"+` to the Polaris REST endpoint (e.g., `+"`http://localhost:8181/api/catalog`"+`).
* Set `+"`catalog.warehouse`"+` to the catalog name configured in Polaris.
* Configure `+"`catalog.auth.oauth2`"+` with client credentials granted access to the catalog.

=== AWS Glue Data Catalog

To use with AWS Glue Data Catalog:

* Set `+"`catalog.url`"+` to `+"`https://glue.<region>.amazonaws.com/iceberg`"+` (the REST client appends the API version automatically).
* Set `+"`catalog.warehouse`"+` to your AWS account ID (the Glue catalog identifier).
* Set `+"`schema_evolution.table_location`"+` to an S3 prefix (e.g., `+"`s3://my-bucket/`"+`) since Glue does not automatically assign table locations.
* Configure `+"`catalog.auth.aws_sigv4`"+` with the appropriate region and set `+"`service`"+` to `+"`glue`"+`.
* Configure `+"`storage.aws_s3`"+` with the same bucket and region.

=== Azure Blob Storage (ADLS Gen2)

To use with Azure Data Lake Storage Gen2:

* Configure `+"`storage.azure_blob_storage`"+` with your storage account name and container.
* Authenticate using one of: `+"`storage_access_key`"+` (shared key), `+"`storage_sas_token`"+`, or `+"`storage_connection_string`"+`.
* The storage account must have hierarchical namespace (HNS) enabled for ADLS Gen2 compatibility.

[%header,format=dsv]
|===
Bloblang type:Iceberg type
string:string
bytes:binary
bool:boolean
number:double
timestamp:timestamp (with timezone)
object:struct
array:list
|===

`+service.OutputPerformanceDocs(true, true)).
		Fields(
			// Catalog configuration
			service.NewObjectField(ioFieldCatalog,
				service.NewStringField(ioFieldCatalogURL).
					Description("The REST catalog endpoint URL.").
					Example("http://localhost:8181/api/catalog").
					Example("https://polaris.example.com/api/catalog").
					Example("https://glue.us-east-1.amazonaws.com/iceberg"),
				service.NewStringField(ioFieldCatalogWarehouse).
					Description("The REST catalog warehouse.").
					Optional().
					Example("redpanda-catalog"),
				service.NewObjectField(ioFieldCatalogAuth,
					service.NewObjectField(ioFieldCatalogAuthOAuth2,
						service.NewStringField(ioFieldOAuth2ServerURI).
							Description("OAuth2 token endpoint URI.").
							Default("/v1/oauth/tokens"),
						service.NewStringField(ioFieldOAuth2ClientID).
							Description("OAuth2 client identifier."),
						service.NewStringField(ioFieldOAuth2ClientSecret).
							Description("OAuth2 client secret.").
							Secret(),
						service.NewStringField(ioFieldOAuth2Scope).
							Description("OAuth2 scope to request.").
							Optional(),
					).Description("OAuth2 authentication configuration.").
						Optional(),
					service.NewStringField(ioFieldCatalogAuthBearer).
						Description("Static bearer token for authentication. For testing only, not recommended for production.").
						Optional().
						Secret(),
					service.NewObjectField(ioFieldCatalogAuthSigV4,
						append(config.SessionFields(),
							service.NewStringField(ioFieldSigV4Service).
								Description("AWS service name for SigV4 signing.").
								Advanced().
								Optional())...,
					).Description("AWS SigV4 authentication (for AWS Glue Data Catalog or API Gateway).").
						Optional(),
				).Description("Authentication configuration for the REST catalog. Only one authentication method can be active at a time.").
					Optional(),
				service.NewStringMapField(ioFieldCatalogHeaders).
					Description("Custom HTTP headers to include in all requests to the catalog.").
					Example(map[string]string{"X-Api-Key": "your-api-key"}).
					Optional().
					Advanced(),
				service.NewBoolField(ioFieldCatalogTLSSkipVer).
					Description("Skip TLS certificate verification. Not recommended for production.").
					Default(false).
					Advanced(),
			).Description("REST catalog configuration."),

			// Table identification
			service.NewInterpolatedStringField(ioFieldNamespace).
				Description("The Iceberg namespace for the table, dot delimiters are split as nested namespaces.").
				Example("analytics.events").
				Example("production"),

			service.NewInterpolatedStringField(ioFieldTable).
				Description("The Iceberg table name. Supports interpolation functions for dynamic table names.").
				Example("user_events").
				Example(`events_${!meta("topic")}`),

			service.NewBoolField(ioFieldCaseSensitiveColumns).
				Description("Controls how message field names are matched against table column names, and how column references in the partition spec are resolved. When `true` (the default), names must match exactly. When `false`, matching is case-insensitive — set this when your downstream catalog or query engine treats column names as case-insensitive (the iceberg specification's recommended convention) so that, for example, a message keyed `\"COLUMN\"` lands in an existing `column` rather than triggering schema evolution. Ambiguous case-only duplicates in the input are rejected.").
				Default(true).
				Advanced(),

			// Storage configuration - one of s3, gcs, or azure must be specified
			service.NewObjectField(ioFieldStorage,
				// S3 storage configuration
				service.NewObjectField(ioFieldStorageS3,
					service.NewStringField(ioFieldS3Bucket).
						Description("The S3 bucket name.").
						Example("my-iceberg-data"),
					service.NewStringField(ioFieldS3Region).
						Description("The AWS region.").
						Optional().
						Example("us-west-2"),
					service.NewStringField(ioFieldS3Endpoint).
						Description("Custom endpoint for S3-compatible storage (e.g., MinIO).").
						Optional().
						Example("http://localhost:9000"),
					service.NewBoolField(ioFieldS3ForcePathStyleURLs).
						Description("Forces the client API to use path style URLs, which is often required when connecting to custom endpoints.").
						Default(false).
						Advanced(),
					service.NewObjectField(ioFieldS3Credentials,
						service.NewStringField(ioFieldS3CredID).
							Description("The AWS access key ID.").
							Optional(),
						service.NewStringField(ioFieldS3CredSecret).
							Description("The AWS secret access key.").
							Optional().Secret(),
						service.NewStringField(ioFieldS3CredToken).
							Description("The AWS session token, required when using short term credentials.").
							Optional(),
					).Description("Static AWS credentials for S3 access. When not specified, credentials are loaded from the default AWS credential chain.").
						Advanced().
						Optional(),
				).Description("S3 storage configuration.").
					Optional(),

				// GCS storage configuration
				service.NewObjectField(ioFieldStorageGCS,
					service.NewStringField(ioFieldGCSBucket).
						Description("The GCS bucket name.").
						Example("my-iceberg-data"),
					service.NewStringField(ioFieldGCSEndpoint).
						Description("Custom endpoint for GCS-compatible storage.").
						Optional().
						Advanced(),
					service.NewStringField(ioFieldGCSCredType).
						Description("The type of credentials to use. Valid values: `service_account`, `authorized_user`, `impersonated_service_account`, `external_account`.").
						Optional().
						Example("service_account"),
					service.NewStringField(ioFieldGCSKeyPath).
						Description("Path to a GCP credentials JSON file.").
						Optional(),
					service.NewStringField(ioFieldGCSJSONKey).
						Description("GCP credentials JSON content. Use this or `credentials_file`, not both.").
						Optional().
						Secret(),
				).Description("Google Cloud Storage configuration.").
					Optional(),

				// Azure storage configuration
				service.NewObjectField(ioFieldStorageAzure,
					service.NewStringField(ioFieldAzureStorageAccount).
						Description("The Azure storage account name.").
						Example("mystorageaccount"),
					service.NewStringField(ioFieldAzureContainer).
						Description("The Azure blob container name.").
						Example("iceberg-data"),
					service.NewStringField(ioFieldAzureEndpoint).
						Description("Custom endpoint for Azure-compatible storage.").
						Optional().
						Advanced(),
					service.NewStringField(ioFieldAzureSASToken).
						Description("SAS token for authentication. Prefix with the container name followed by a dot if container-specific.").
						Optional().
						Secret(),
					service.NewStringField(ioFieldAzureConnectionString).
						Description("Azure storage connection string. Use this or other auth methods, not both.").
						Optional().
						Secret(),
					service.NewStringField(ioFieldAzureAccessKey).
						Description("Azure storage access key for shared key authentication.").
						Optional().
						Secret(),
				).Description("Azure Blob Storage (ADLS Gen2) configuration.").
					Optional(),
			).Description("Storage backend configuration for data files. Exactly one of `aws_s3`, `gcp_cloud_storage`, or `azure_blob_storage` must be specified."),

			// Schema evolution
			service.NewObjectField(ioFieldSchemaEvolution,
				service.NewBoolField(ioFieldSchemaEvolutionEnabled).
					Description("Enable automatic schema evolution. When enabled, new columns will be automatically added to the table.").
					Default(false),
				service.NewInterpolatedStringField(ioFieldSchemaEvolutionPartitionSpec).
					Description("A bloblang expression to evaluate when a new table is created to determine the table's partition spec. The result of the mapping should be an iceberg partition spec in the same string format as the https://docs.redpanda.com/current/manage/iceberg/about-iceberg-topics/#use-custom-partitioning[^Redpanda Streaming Topic Property]").
					Example(`(col1)`).
					Example(`(nested.col)`).
					Example(`(year(my_ts_col))`).
					Example(`(year(my_ts_col), col2)`).
					Example(`(hour(my_ts_col), truncate(42, col2))`).
					Example(`(day(my_ts_col), bucket(4, nested.col))`).
					Example("(day(my_ts_col), void(`non.nested column.with.dots`), identity(nested.column))").
					Default("()"),
				service.NewStringField(ioFieldSchemaEvolutionTableLoc).
					Description("A prefix used as the location for new tables when the catalog does not automatically assign one. For example, AWS Glue requires explicit table locations. When set, table locations are derived as `{prefix}{namespace}/{table}`.").
					Example("s3://my-iceberg-bucket/").
					Optional(),
				service.NewStringField(ioFieldSchemaEvolutionSchemaMetadata).
					Description("The name of a message metadata field containing a schema definition. When set, the schema is used to determine column types during schema evolution and table creation instead of inferring types from values. The schema must be in the standard common schema format (the same format used by the `parquet_encode` processor's `schema_metadata` field). For batches of messages, the first message's schema is used. Record presence drives schema shape: fields declared in the schema metadata that are absent from the record are not added to the table, while the metadata controls column ordering, naming, and types for fields that are present. In case-insensitive mode, top-level column names use the metadata's casing — record keys are matched by case-folding and the metadata's name is what lands in the table.").
					Default("").
					Optional().
					Advanced(),
				service.NewBloblangField(ioFieldSchemaEvolutionNewColumnTypeMapping).
					Description("An optional Bloblang mapping to customize column types during schema evolution. This mapping is executed for each new column and can override the inferred or schema-metadata-derived type. The mapping receives an object with fields `name` (column name), `path` (dot-separated path), `value` (sample value), `inferred_type` (the type that would be used without this mapping), `message` (the full message body), `namespace`, and `table`. It must return a string with a valid Iceberg type name: `boolean`, `int`, `long`, `float`, `double`, `string`, `binary`, `date`, `time`, `timestamp`, `timestamptz`, `uuid`, `decimal(p,s)`, or `fixed[n]`.").
					Optional().
					Advanced(),
			).Description("Schema evolution configuration.").
				Optional().
				Advanced(),

			// Commit behavior
			service.NewObjectField(ioFieldCommit,
				service.NewBoolField(ioFieldManifestMergeEnabled).
					Description("Merge small manifest files during commits to reduce metadata overhead.").
					Default(true),
				service.NewDurationField(ioFieldMaxSnapshotAge).
					Description("Maximum age of snapshots to retain for time-travel queries. Set to zero to disable removing old snapshots.").
					Default("24h"),
				service.NewIntField(ioFieldMaxCommitRetries).
					Description("Maximum number of times to retry a failed transaction commit.").
					Default(3),
			).Description("Commit behavior configuration.").
				Advanced().
				Optional(),

			// Parquet writer configuration
			service.NewObjectField(ioFieldParquet,
				service.NewStringEnumField(ioFieldParquetStringEncoding, "plain", "delta_length_byte_array").
					Description("The encoding to use for string and binary columns. Use `plain` for compatibility with readers that do not support `DELTA_LENGTH_BYTE_ARRAY` encoding, such as AWS Redshift Spectrum.").
					Default("delta_length_byte_array"),
			).Description("Parquet writer configuration.").
				Advanced().
				Optional(),

			// Batching
			service.NewBatchPolicyField(ioFieldBatching),
			service.NewOutputMaxInFlightField().Default(4),
		)
}
