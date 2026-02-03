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

	// Table fields
	ioFieldNamespace = "namespace"
	ioFieldTable     = "table"

	// Storage fields
	ioFieldStorage         = "storage"
	ioFieldStorageType     = "type"
	ioFieldStorageBucket   = "bucket"
	ioFieldStorageRegion   = "region"
	ioFieldStorageEndpoint = "endpoint"

	// Schema evolution fields
	ioFieldSchemaEvolution              = "schema_evolution"
	ioFieldSchemaEvolutionEnabled       = "enabled"
	ioFieldSchemaEvolutionPartitionSpec = "partition_spec"

	// Performance fields
	ioFieldBatching      = "batching"
	ioFieldMaxInFlight   = "max_in_flight"
	ioFieldCommitTimeout = "commit_timeout"
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

This output is designed to work with REST catalog implementations like Apache Polaris, AWS Glue Data Catalog, and Snowflake.

[%header,format=dsv]
|===
Go type:Iceberg type
string:string
[]byte:binary
bool:boolean
int, int32:int
int64:long
float32:float
float64:double
time.Time:timestamp (with timezone)
map[string]any:struct
[]any:list
|===

`+service.OutputPerformanceDocs(true, true)).
		Fields(
			// Catalog configuration
			service.NewObjectField(ioFieldCatalog,
				service.NewStringField(ioFieldCatalogURL).
					Description("The REST catalog endpoint URL.").
					Example("http://localhost:8181/api/catalog").
					Example("https://polaris.example.com/api/catalog"),
				service.NewStringField(ioFieldCatalogWarehouse).
					Description("The REST catalog warehouse.").
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
					).Description("OAuth2 authentication configuration.").
						Optional(),
					service.NewStringField(ioFieldCatalogAuthBearer).
						Description("Static bearer token for authentication. For testing only, not recommended for production.").
						Optional().
						Secret(),
					service.NewBoolField(ioFieldCatalogAuthSigV4).
						Description("Use AWS SigV4 authentication (for AWS Glue Data Catalog). Uses the same credentials as the storage configuration.").
						Optional().
						Default(false),
				).Description("Authentication configuration for the REST catalog. Only one authentication method can be active at a time.").
					Optional(),
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

			// Storage configuration
			service.NewObjectField(ioFieldStorage,
				append([]*service.ConfigField{
					service.NewStringField(ioFieldStorageType).
						Description("Storage backend type.").
						Example("s3").
						Example("gcs").
						Example("azure"),
					service.NewStringField(ioFieldStorageBucket).
						Description("The storage bucket name (S3/GCS) or storage account name (Azure).").
						Example("my-iceberg-data"),
					service.NewStringField(ioFieldStorageRegion).
						Description("The AWS region (for S3).").
						Optional().
						Example("us-west-2"),
					service.NewStringField(ioFieldStorageEndpoint).
						Description("Custom endpoint for S3-compatible storage (e.g., MinIO).").
						Optional().
						Example("http://localhost:9000"),
				}, config.SessionFields()...)...,
			).Description("Storage backend configuration for data files."),

			// Schema evolution
			service.NewObjectField(ioFieldSchemaEvolution,
				service.NewBoolField(ioFieldSchemaEvolutionEnabled).
					Description("Enable automatic schema evolution. When enabled, new columns will be automatically added to the table.").
					Default(false),
				service.NewInterpolatedStringField(ioFieldSchemaEvolutionPartitionSpec).
					Description("A bloblang expression to evalutate when a new table is created to determine the table's partition spec. The result of the mapping should be an iceberg partition spec in the same string format as the https://docs.redpanda.com/current/manage/iceberg/about-iceberg-topics/#use-custom-partitioning[^Redpanda Streaming Topic Property]").
					Example(`(col1)`).
					Example(`(nested.col)`).
					Example(`(year(my_ts_col))`).
					Example(`(year(my_ts_col), col2)`).
					Example(`(hour(my_ts_col), truncate(42, col2))`).
					Example(`(day(my_ts_col), bucket(4, nested.col))`).
					Example("(day(my_ts_col), void(`non.nested column.with.dots`), identity(nested.column))").
					Default("()"),
			).Description("Schema evolution configuration.").
				Optional().
				Advanced(),

			// Batching
			service.NewBatchPolicyField(ioFieldBatching),
			service.NewOutputMaxInFlightField().Default(4),

			// Performance tuning
			service.NewDurationField(ioFieldCommitTimeout).
				Description("Maximum time to wait for catalog transaction commit.").
				Default("30s").
				Advanced(),
		)
}
