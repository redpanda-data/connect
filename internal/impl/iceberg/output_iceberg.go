// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/catalogx"
	"github.com/redpanda-data/connect/v4/internal/license"
)

func init() {
	err := service.RegisterBatchOutput(
		"iceberg",
		icebergOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			// Check enterprise license
			if err = license.CheckRunningEnterprise(mgr); err != nil {
				return
			}

			// Parse configuration
			output, err = newIcebergOutputFromConfig(conf, mgr)
			if err != nil {
				return
			}

			// Get batch policy
			if batchPolicy, err = conf.FieldBatchPolicy(ioFieldBatching); err != nil {
				return
			}

			// Get max in flight
			if maxInFlight, err = conf.FieldInt(ioFieldMaxInFlight); err != nil {
				return
			}

			return
		})
	if err != nil {
		panic(err)
	}
}

// icebergOutput implements service.BatchOutput for Iceberg tables.
type icebergOutput struct {
	router *Router
	logger *service.Logger
}

// newIcebergOutputFromConfig creates a new Iceberg output from parsed configuration.
func newIcebergOutputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*icebergOutput, error) {
	// Parse catalog configuration
	catalogCfg, err := parseCatalogConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("parsing catalog config: %w", err)
	}

	// Parse table identification
	namespaceStr, err := conf.FieldInterpolatedString(ioFieldNamespace)
	if err != nil {
		return nil, fmt.Errorf("parsing namespace: %w", err)
	}

	tableStr, err := conf.FieldInterpolatedString(ioFieldTable)
	if err != nil {
		return nil, fmt.Errorf("parsing table name: %w", err)
	}

	// Parse schema evolution config
	schemaEvoCfg, err := parseSchemaEvolutionConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("parsing schema evolution config: %w", err)
	}

	// Parse commit config
	commitCfg, err := parseCommitConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("parsing commit config: %w", err)
	}

	// Create router
	rtr := NewRouter(catalogCfg, namespaceStr, tableStr, schemaEvoCfg, commitCfg, mgr.Logger())

	return &icebergOutput{
		router: rtr,
		logger: mgr.Logger(),
	}, nil
}

// parseCatalogConfig parses the catalog configuration.
func parseCatalogConfig(conf *service.ParsedConfig) (catalogx.Config, error) {
	cfg := catalogx.Config{
		AuthType: "none", // Default to no auth
	}

	// Parse catalog URL
	var err error
	cfg.URL, err = conf.FieldString(ioFieldCatalog, ioFieldCatalogURL)
	if err != nil {
		return cfg, fmt.Errorf("catalog.url is required: %w", err)
	}

	// Parse warehouse (optional)
	if conf.Contains(ioFieldCatalog, ioFieldCatalogWarehouse) {
		cfg.Warehouse, err = conf.FieldString(ioFieldCatalog, ioFieldCatalogWarehouse)
		if err != nil {
			return cfg, err
		}
	}

	// Parse storage configuration for AdditionalProps
	cfg.AdditionalProps, err = parseStorageProps(conf)
	if err != nil {
		return cfg, err
	}

	// Parse custom headers (optional)
	if conf.Contains(ioFieldCatalog, ioFieldCatalogHeaders) {
		cfg.Headers, err = conf.FieldStringMap(ioFieldCatalog, ioFieldCatalogHeaders)
		if err != nil {
			return cfg, err
		}
	}

	// Parse TLS skip verify (optional)
	if conf.Contains(ioFieldCatalog, ioFieldCatalogTLSSkipVer) {
		cfg.TLSSkipVerify, err = conf.FieldBool(ioFieldCatalog, ioFieldCatalogTLSSkipVer)
		if err != nil {
			return cfg, err
		}
	}

	// Parse authentication (if present)
	if !conf.Contains(ioFieldCatalog, ioFieldCatalogAuth) {
		return cfg, nil // No auth configured
	}

	// Check for OAuth2
	if conf.Contains(ioFieldCatalog, ioFieldCatalogAuth, ioFieldCatalogAuthOAuth2) {
		cfg.AuthType = "oauth2"
		cfg.OAuth2ClientID, err = conf.FieldString(ioFieldCatalog, ioFieldCatalogAuth, ioFieldCatalogAuthOAuth2, ioFieldOAuth2ClientID)
		if err != nil {
			return cfg, err
		}
		cfg.OAuth2ClientSecret, err = conf.FieldString(ioFieldCatalog, ioFieldCatalogAuth, ioFieldCatalogAuthOAuth2, ioFieldOAuth2ClientSecret)
		if err != nil {
			return cfg, err
		}
		serverURI, _ := conf.FieldString(ioFieldCatalog, ioFieldCatalogAuth, ioFieldCatalogAuthOAuth2, ioFieldOAuth2ServerURI)
		if serverURI != "" {
			cfg.OAuth2ServerURI, err = url.Parse(serverURI)
			if err != nil {
				return cfg, fmt.Errorf("parsing oauth2 server URI: %w", err)
			}
		}
		// Parse OAuth2 scope (optional)
		if conf.Contains(ioFieldCatalog, ioFieldCatalogAuth, ioFieldCatalogAuthOAuth2, ioFieldOAuth2Scope) {
			cfg.OAuth2Scope, _ = conf.FieldString(ioFieldCatalog, ioFieldCatalogAuth, ioFieldCatalogAuthOAuth2, ioFieldOAuth2Scope)
		}
		return cfg, nil
	}

	// Check for Bearer token
	if conf.Contains(ioFieldCatalog, ioFieldCatalogAuth, ioFieldCatalogAuthBearer) {
		cfg.AuthType = "bearer"
		cfg.BearerToken, err = conf.FieldString(ioFieldCatalog, ioFieldCatalogAuth, ioFieldCatalogAuthBearer)
		if err != nil {
			return cfg, err
		}
		return cfg, nil
	}

	// Check for AWS SigV4
	if conf.Contains(ioFieldCatalog, ioFieldCatalogAuth, ioFieldCatalogAuthSigV4) {
		cfg.AuthType = "sigv4"
		// Parse optional region
		if conf.Contains(ioFieldCatalog, ioFieldCatalogAuth, ioFieldCatalogAuthSigV4, ioFieldSigV4Region) {
			cfg.SigV4Region, err = conf.FieldString(ioFieldCatalog, ioFieldCatalogAuth, ioFieldCatalogAuthSigV4, ioFieldSigV4Region)
			if err != nil {
				return cfg, err
			}
		}
		// Parse service
		if conf.Contains(ioFieldCatalog, ioFieldCatalogAuth, ioFieldCatalogAuthSigV4, ioFieldSigV4Service) {
			cfg.SigV4Service, err = conf.FieldString(ioFieldCatalog, ioFieldCatalogAuth, ioFieldCatalogAuthSigV4, ioFieldSigV4Service)
			if err != nil {
				return cfg, err
			}
		}
	}

	return cfg, nil
}

// parseStorageProps extracts storage properties from config and returns them as iceberg.Properties.
func parseStorageProps(conf *service.ParsedConfig) (iceberg.Properties, error) {
	props := make(iceberg.Properties)

	// Check if storage config exists
	if !conf.Contains(ioFieldStorage) {
		return props, nil
	}

	// Check for S3 configuration
	if conf.Contains(ioFieldStorage, ioFieldStorageS3) {
		return parseS3Props(conf)
	}

	// Check for GCS configuration
	if conf.Contains(ioFieldStorage, ioFieldStorageGCS) {
		return parseGCSProps(conf)
	}

	// Check for Azure configuration
	if conf.Contains(ioFieldStorage, ioFieldStorageAzure) {
		return parseAzureProps(conf)
	}

	return props, nil
}

// parseS3Props extracts S3 storage properties from the nested s3 config.
func parseS3Props(conf *service.ParsedConfig) (iceberg.Properties, error) {
	props := make(iceberg.Properties)

	// Get region
	if conf.Contains(ioFieldStorage, ioFieldStorageS3, ioFieldS3Region) {
		region, err := conf.FieldString(ioFieldStorage, ioFieldStorageS3, ioFieldS3Region)
		if err != nil {
			return nil, err
		}
		props[io.S3Region] = region
	}

	// Get endpoint
	if conf.Contains(ioFieldStorage, ioFieldStorageS3, ioFieldS3Endpoint) {
		endpoint, err := conf.FieldString(ioFieldStorage, ioFieldStorageS3, ioFieldS3Endpoint)
		if err != nil {
			return nil, err
		}
		props[io.S3EndpointURL] = endpoint
	}

	// Get force_path_style_urls - explicit setting like the standard S3 connector.
	// iceberg-go uses S3ForceVirtualAddressing which is the inverse:
	// - force_path_style_urls=true  → S3ForceVirtualAddressing="false" (path-style)
	// - force_path_style_urls=false → S3ForceVirtualAddressing="true"  (virtual-hosted, AWS default)
	forcePathStyle, err := conf.FieldBool(ioFieldStorage, ioFieldStorageS3, ioFieldS3ForcePathStyleURLs)
	if err != nil {
		return nil, err
	}
	if forcePathStyle {
		props[io.S3ForceVirtualAddressing] = "false"
	} else {
		props[io.S3ForceVirtualAddressing] = "true"
	}

	// Get credentials - check for static credentials first
	// The credentials field comes from config.SessionFields() which uses "credentials.id" and "credentials.secret"
	if conf.Contains(ioFieldStorage, ioFieldStorageS3, "credentials", "id") {
		accessKeyID, err := conf.FieldString(ioFieldStorage, ioFieldStorageS3, "credentials", "id")
		if err != nil {
			return nil, err
		}
		props[io.S3AccessKeyID] = accessKeyID
	}
	if conf.Contains(ioFieldStorage, ioFieldStorageS3, "credentials", "secret") {
		secretAccessKey, err := conf.FieldString(ioFieldStorage, ioFieldStorageS3, "credentials", "secret")
		if err != nil {
			return nil, err
		}
		props[io.S3SecretAccessKey] = secretAccessKey
	}

	return props, nil
}

// parseGCSProps extracts GCS storage properties from the nested gcs config.
func parseGCSProps(conf *service.ParsedConfig) (iceberg.Properties, error) {
	props := make(iceberg.Properties)

	// Get endpoint
	if conf.Contains(ioFieldStorage, ioFieldStorageGCS, ioFieldGCSEndpoint) {
		endpoint, err := conf.FieldString(ioFieldStorage, ioFieldStorageGCS, ioFieldGCSEndpoint)
		if err != nil {
			return nil, err
		}
		props[io.GCSEndpoint] = endpoint
	}

	// Get credentials type
	if conf.Contains(ioFieldStorage, ioFieldStorageGCS, ioFieldGCSCredType) {
		credType, err := conf.FieldString(ioFieldStorage, ioFieldStorageGCS, ioFieldGCSCredType)
		if err != nil {
			return nil, err
		}
		props[io.GCSCredType] = credType
	}

	// Get credentials file path
	if conf.Contains(ioFieldStorage, ioFieldStorageGCS, ioFieldGCSKeyPath) {
		keyPath, err := conf.FieldString(ioFieldStorage, ioFieldStorageGCS, ioFieldGCSKeyPath)
		if err != nil {
			return nil, err
		}
		props[io.GCSKeyPath] = keyPath
	}

	// Get credentials JSON
	if conf.Contains(ioFieldStorage, ioFieldStorageGCS, ioFieldGCSJSONKey) {
		jsonKey, err := conf.FieldString(ioFieldStorage, ioFieldStorageGCS, ioFieldGCSJSONKey)
		if err != nil {
			return nil, err
		}
		props[io.GCSJSONKey] = jsonKey
	}

	return props, nil
}

// parseAzureProps extracts Azure storage properties from the nested azure config.
func parseAzureProps(conf *service.ParsedConfig) (iceberg.Properties, error) {
	props := make(iceberg.Properties)

	// Get storage account name for SAS token prefix
	storageAccount := ""
	if conf.Contains(ioFieldStorage, ioFieldStorageAzure, ioFieldAzureStorageAccount) {
		var err error
		storageAccount, err = conf.FieldString(ioFieldStorage, ioFieldStorageAzure, ioFieldAzureStorageAccount)
		if err != nil {
			return nil, err
		}
	}

	// Get container name for SAS token prefix
	container := ""
	if conf.Contains(ioFieldStorage, ioFieldStorageAzure, ioFieldAzureContainer) {
		var err error
		container, err = conf.FieldString(ioFieldStorage, ioFieldStorageAzure, ioFieldAzureContainer)
		if err != nil {
			return nil, err
		}
	}

	// Get endpoint
	if conf.Contains(ioFieldStorage, ioFieldStorageAzure, ioFieldAzureEndpoint) {
		endpoint, err := conf.FieldString(ioFieldStorage, ioFieldStorageAzure, ioFieldAzureEndpoint)
		if err != nil {
			return nil, err
		}
		props[io.AdlsEndpoint] = endpoint
	}

	// Get SAS token - uses container-specific prefix
	if conf.Contains(ioFieldStorage, ioFieldStorageAzure, ioFieldAzureSASToken) {
		sasToken, err := conf.FieldString(ioFieldStorage, ioFieldStorageAzure, ioFieldAzureSASToken)
		if err != nil {
			return nil, err
		}
		// SAS tokens are prefixed with "adls.sas-token.<container>." for container-specific tokens
		if container != "" {
			props[io.AdlsSasTokenPrefix+container] = sasToken
		} else if storageAccount != "" {
			props[io.AdlsSasTokenPrefix+storageAccount] = sasToken
		}
	}

	// Get connection string
	if conf.Contains(ioFieldStorage, ioFieldStorageAzure, ioFieldAzureConnectionString) {
		connStr, err := conf.FieldString(ioFieldStorage, ioFieldStorageAzure, ioFieldAzureConnectionString)
		if err != nil {
			return nil, err
		}
		// Connection strings are prefixed with "adls.connection-string.<account>."
		if storageAccount != "" {
			props[io.AdlsConnectionStringPrefix+storageAccount] = connStr
		}
	}

	// Get shared key credentials
	if conf.Contains(ioFieldStorage, ioFieldStorageAzure, ioFieldAzureAccessKey) {
		key, err := conf.FieldString(ioFieldStorage, ioFieldStorageAzure, ioFieldAzureAccessKey)
		if err != nil {
			return nil, err
		}
		props[io.AdlsSharedKeyAccountName] = storageAccount
		props[io.AdlsSharedKeyAccountKey] = key
	}

	return props, nil
}

// parseSchemaEvolutionConfig parses the schema evolution configuration.
func parseSchemaEvolutionConfig(conf *service.ParsedConfig) (SchemaEvolutionConfig, error) {
	cfg := SchemaEvolutionConfig{}

	// Check if schema evolution config exists
	if !conf.Contains(ioFieldSchemaEvolution) {
		return cfg, nil
	}

	// Parse enabled flag
	var err error
	cfg.Enabled, err = conf.FieldBool(ioFieldSchemaEvolution, ioFieldSchemaEvolutionEnabled)
	if err != nil {
		return cfg, err
	}

	// Parse partition spec if present
	if conf.Contains(ioFieldSchemaEvolution, ioFieldSchemaEvolutionPartitionSpec) {
		cfg.PartitionSpec, err = conf.FieldInterpolatedString(ioFieldSchemaEvolution, ioFieldSchemaEvolutionPartitionSpec)
		if err != nil {
			return cfg, err
		}
	}

	// Parse table location prefix if present
	if conf.Contains(ioFieldSchemaEvolution, ioFieldSchemaEvolutionTableLoc) {
		cfg.TableLocation, err = conf.FieldString(ioFieldSchemaEvolution, ioFieldSchemaEvolutionTableLoc)
		if err != nil {
			return cfg, err
		}
	}

	return cfg, nil
}

// parseCommitConfig parses the commit configuration.
func parseCommitConfig(conf *service.ParsedConfig) (CommitConfig, error) {
	cfg := CommitConfig{
		ManifestMergeEnabled: true,
		MaxSnapshotAge:       24 * time.Hour,
		MaxRetries:           3,
	}
	if !conf.Contains(ioFieldCommit) {
		return cfg, nil
	}
	var err error
	cfg.ManifestMergeEnabled, err = conf.FieldBool(ioFieldCommit, ioFieldManifestMergeEnabled)
	if err != nil {
		return cfg, err
	}
	cfg.MaxSnapshotAge, err = conf.FieldDuration(ioFieldCommit, ioFieldMaxSnapshotAge)
	if err != nil {
		return cfg, err
	}
	cfg.MaxRetries, err = conf.FieldInt(ioFieldCommit, ioFieldMaxCommitRetries)
	if err != nil {
		return cfg, err
	}
	return cfg, nil
}

// Connect establishes connections to the catalog and storage.
func (o *icebergOutput) Connect(_ context.Context) error {
	o.logger.Info("Iceberg output ready")
	return nil
}

// WriteBatch writes a batch of messages to the Iceberg table.
func (o *icebergOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	return o.router.Route(ctx, batch)
}

// Close closes the output and releases resources.
func (o *icebergOutput) Close(_ context.Context) error {
	o.router.Close()
	return nil
}
