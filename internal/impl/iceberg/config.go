// Copyright 2026 Redpanda Data, Inc.
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

	// Row-operation fields for row-level mutations (insert / upsert / delete).
	// `row_operation` is named to avoid colliding with Iceberg's snapshot-level
	// "operation" (append/replace/overwrite/delete); `identifier_fields` matches
	// the Iceberg spec's identifier-field-ids (primary-key columns).
	ioFieldRowOperation     = "row_operation"
	ioFieldIdentifierFields = "identifier_fields"

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
	ioFieldSchemaEvolution                      = "schema_evolution"
	ioFieldSchemaEvolutionEnabled               = "enabled"
	ioFieldSchemaEvolutionPartitionSpec         = "partition_spec"
	ioFieldSchemaEvolutionTableLoc              = "table_location"
	ioFieldSchemaEvolutionSchemaMetadata        = "schema_metadata"
	ioFieldSchemaEvolutionNewColumnTypeMapping  = "new_column_type_mapping"
	ioFieldSchemaEvolutionRequireSchemaMetadata = "require_schema_metadata"

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

// rowOperationDocs is the long-form documentation for the row-level operation
// feature. It lives in the component description rather than inline in the
// `row_operation` / `identifier_fields` field descriptions because the field
// table on the docs site renders long, multi-paragraph cells poorly; those
// field descriptions stay short and link here via the `row-level-operations`
// anchor. Written as a double-quoted string so inline `code` spans can use
// literal backticks.
const rowOperationDocs = "\n" +
	"== Row-level operations\n" +
	"\n" +
	"By default this output is append-only — every message becomes a new row (`row_operation: insert`), and existing configurations are unaffected. Set `row_operation` to apply a per-message operation, with `identifier_fields` defining the row identity:\n" +
	"\n" +
	"* `insert` — append the row. This is an unconditional append: it is *not* keyed or de-duplicated.\n" +
	"* `upsert` — replace any existing rows matching `identifier_fields`, then append this row (equivalent to Iceberg's Flink UPSERT mode).\n" +
	"* `delete` — remove rows matching `identifier_fields`.\n" +
	"\n" +
	"`row_operation` supports interpolation, so the operation can be driven by the data itself — for example by mapping a change-data-capture stream's operation field — but no CDC-specific format is assumed (see the change-data-capture example below). It is named `row_operation` to distinguish it from Iceberg's snapshot-level operation.\n" +
	"\n" +
	"`upsert` and `delete` require `identifier_fields` and use Iceberg merge-on-read equality deletes, which require table format version 2. A version-1 table is automatically upgraded to version 2 on the first `upsert`/`delete`; *this upgrade is irreversible*.\n" +
	"\n" +
	"*Identifier fields.* `identifier_fields` must reference existing table columns of a primitive, non-floating-point type. A static `upsert`/`delete` is validated at startup; an interpolated `row_operation` is validated per message at write time, so an empty `identifier_fields` is not caught until the first `upsert`/`delete` message arrives. Identifier columns of a temporal type (`timestamp`, `timestamptz`, `date`, `time`) must arrive as time values, not bare numbers — a numeric epoch is ambiguous as a delete key and is rejected at write time; convert it to a timestamp upstream. If the table is partitioned, every partition source column must be one of the `identifier_fields`, since equality deletes are partition-scoped.\n" +
	"\n" +
	"When this output auto-creates a table (via `schema_evolution`), the `identifier_fields` columns are created as *required* and registered as the table's Iceberg identifier-field-ids, so downstream engines and other writers see the primary key. A consequence is that a null or missing value in an identifier column is rejected on write, even for `insert`. Identifier columns must therefore be present at creation — in the first message or declared via `schema_metadata`. Pre-existing tables are never modified.\n" +
	"\n" +
	"*Batching and ordering.* Within a single batch the last `upsert`/`delete` per `identifier_fields` key wins. Each batch containing an `upsert`/`delete` is committed as its own snapshot (these commits are never coalesced, which is required for correctness), so a high-throughput mutation workload produces one snapshot per batch. Size batches accordingly and run regular table maintenance (snapshot expiry and compaction) to keep metadata manageable. Pure `insert`-only batches keep the original append fast path, which does coalesce commits.\n" +
	"\n" +
	"Ordering only holds *within* a batch. With more than one batch in flight, concurrent batches can commit out of order, so a stale `upsert` may overwrite a newer one for the same key. Set `max_in_flight: 1` for keyed (change-data-capture) workloads to preserve per-key order — this is enforced by config linting whenever `row_operation` is anything other than a static `insert`.\n" +
	"\n" +
	"[CAUTION]\n" +
	"====\n" +
	"`insert` is an unconditional append and is *not* keyed or de-duplicated. For keyed data (including change-data-capture), map create/read events to `upsert`, never `insert` — mixing `insert` with `upsert`/`delete` on the same key in one batch produces duplicate rows.\n" +
	"====\n"

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

`+rowOperationDocs+service.OutputPerformanceDocs(true, true)).
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

			// Row-level operation mapping (insert / upsert / delete).
			service.NewInterpolatedStringField(ioFieldRowOperation).
				Description("The row-level operation to apply for each message: `insert` (append), `upsert` (replace rows matching `identifier_fields`, then append), or `delete` (remove rows matching `identifier_fields`). Supports interpolation so the operation can be driven by the data — e.g. a change-data-capture stream's operation field. Defaults to `insert`, preserving the original append-only behaviour.\n\nSee the <<row-level-operations,Row-level operations>> section above for the full semantics, the format-version-2 upgrade, batching behaviour, and important caveats.").
				Example("insert").
				Example(`${! metadata("op") }`).
				Example(`${! this.op == "d" ? "delete" : "upsert" }`).
				Default("insert").
				Advanced(),

			service.NewStringListField(ioFieldIdentifierFields).
				Description("The columns forming the row identity (the Iceberg identifier fields / equality-delete key) used by `upsert` and `delete`. Required when `row_operation` can evaluate to `upsert` or `delete`, and must reference existing table columns of a primitive, non-floating-point type.\n\nSee the <<row-level-operations,Row-level operations>> section above for the full constraints, including the temporal-type and partitioning rules and when the requirement is enforced.").
				Example([]string{"id"}).
				Example([]string{"tenant_id", "user_id"}).
				Default([]string{}).
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
				service.NewBoolField(ioFieldSchemaEvolutionRequireSchemaMetadata).
					Description("When `true`, writing a numeric value into a `timestamp`, `timestamptz`, `date`, or `time` column without `schema_metadata` registered for that column is a hard error. The default `false` permits a fallback path that interprets bare numeric timestamps as Unix seconds and bare numeric times as already-microseconds — convenient, but silently wrong if upstream produced milliseconds. Enable this when you cannot guarantee the upstream attaches schema metadata and want to fail loudly rather than corrupt dates by ~50,000 years. No effect on time-typed columns receiving `time.Time`/`time.Duration` Go values, which carry their own unit unambiguously, and no effect on non-time columns. Requires `schema_metadata` to be set.").
					Default(false).
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
		).
		// Keyed (upsert/delete) writes rely on per-key ordering. With more than
		// one batch in flight, concurrent batches can commit out of order and
		// corrupt the last-writer-wins result, so require max_in_flight: 1 for
		// any non-insert row_operation. A static `insert` (the default) is
		// append-only and unaffected; an interpolated row_operation is treated
		// conservatively as potentially mutating.
		LintRule(`root = if this.row_operation.or("insert") != "insert" && this.max_in_flight.or(4) > 1 {
  [ "row_operation can resolve to upsert/delete, which rely on per-key write ordering; with max_in_flight > 1 concurrent batches may commit out of order and corrupt the last-writer-wins result. Set max_in_flight: 1 for keyed (change-data-capture) workloads, or use a static row_operation: insert for append-only writes." ]
}`).
		Example(
			"Change-data-capture upsert/delete",
			"Materialize a change-data-capture stream into an Iceberg table. A mapping derives the row operation from the source's operation field (here Debezium's `op`: `c`reate / `r`ead / `u`pdate map to `upsert`, `d`elete maps to `delete`) and selects the row image, while `identifier_fields` is the primary key. Note that creates map to `upsert`, never `insert`, so re-delivered or snapshot rows do not duplicate.",
			`
input:
  redpanda:
    seed_brokers: [ localhost:9092 ]
    topics: [ dbserver.inventory.customers ]
    consumer_group: iceberg_sink

pipeline:
  processors:
    - mapping: |
        meta op = match this.op {
          "d" => "delete",
          _   => "upsert",
        }
        # Debezium puts the row image in 'after' (or 'before' for deletes).
        root = this.after | this.before

output:
  iceberg:
    catalog:
      url: http://localhost:8181/api/catalog
    namespace: inventory
    table: customers
    row_operation: ${! metadata("op") }
    identifier_fields: [ id ]
    # Keyed writes must stay ordered: a single batch in flight prevents
    # concurrent batches from committing a stale update over a newer one.
    max_in_flight: 1
    storage:
      aws_s3:
        bucket: my-iceberg-data
        region: us-east-1
`,
		)
}
