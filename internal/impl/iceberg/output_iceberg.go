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

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/storage"
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
	writer *icebergWriter
	logger *service.Logger
}

// newIcebergOutputFromConfig creates a new Iceberg output from parsed configuration.
func newIcebergOutputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*icebergOutput, error) {
	// Parse catalog configuration
	catalogConf, err := parseCatalogConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse catalog config: %w", err)
	}

	// Parse table identification
	namespace, err := conf.FieldString(ioFieldNamespace)
	if err != nil {
		return nil, fmt.Errorf("failed to parse namespace: %w", err)
	}

	tableNameStr, err := conf.FieldInterpolatedString(ioFieldTable)
	if err != nil {
		return nil, fmt.Errorf("failed to parse table name: %w", err)
	}

	// Parse storage configuration
	stor, err := parseStorageConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse storage config: %w", err)
	}

	// Parse schema evolution settings
	ignoreNulls := true
	if conf.Contains(ioFieldSchemaEvolution) {
		if conf.Contains(ioFieldSchemaEvolution, ioFieldSchemaEvolutionIgnoreNulls) {
			ignoreNulls, err = conf.FieldBool(ioFieldSchemaEvolution, ioFieldSchemaEvolutionIgnoreNulls)
			if err != nil {
				return nil, fmt.Errorf("failed to parse ignore_nulls: %w", err)
			}
		}
	}

	// Parse commit timeout
	commitTimeout, err := conf.FieldDuration(ioFieldCommitTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to parse commit_timeout: %w", err)
	}

	// Create catalog client
	catalog, err := newCatalogClient(catalogConf, []string{namespace})
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog client: %w", err)
	}

	// Create writer
	writer := newIcebergWriter(
		catalog,
		stor,
		namespace,
		tableNameStr,
		ignoreNulls,
		commitTimeout,
		mgr.Logger(),
	)

	return &icebergOutput{
		writer: writer,
		logger: mgr.Logger(),
	}, nil
}

// parseCatalogConfig parses the catalog configuration.
func parseCatalogConfig(conf *service.ParsedConfig) (catalogConfig, error) {
	cfg := catalogConfig{
		AuthType: "none", // Default to no auth
	}

	// Parse catalog URL
	var err error
	cfg.URL, err = conf.FieldString(ioFieldCatalog, ioFieldCatalogURL)
	if err != nil {
		return cfg, fmt.Errorf("catalog.url is required: %w", err)
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
		cfg.OAuth2ServerURI, _ = conf.FieldString(ioFieldCatalog, ioFieldCatalogAuth, ioFieldCatalogAuthOAuth2, ioFieldOAuth2ServerURI)
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
		useSigV4, err := conf.FieldBool(ioFieldCatalog, ioFieldCatalogAuth, ioFieldCatalogAuthSigV4)
		if err != nil {
			return cfg, err
		}
		if useSigV4 {
			cfg.AuthType = "sigv4"
		}
		return cfg, nil
	}

	return cfg, nil
}

// parseStorageConfig parses the storage configuration and creates a storage backend.
func parseStorageConfig(conf *service.ParsedConfig) (storage.Storage, error) {
	storageType, err := conf.FieldString(ioFieldStorage, ioFieldStorageType)
	if err != nil {
		return nil, fmt.Errorf("storage.type is required: %w", err)
	}

	switch storageType {
	case "s3":
		return parseS3Storage(conf)
	case "gcs":
		return nil, fmt.Errorf("GCS storage not yet implemented (coming in Phase 3)")
	case "azure":
		return nil, fmt.Errorf("Azure storage not yet implemented (coming in Phase 3)")
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", storageType)
	}
}

// parseS3Storage creates an S3 storage backend from configuration.
func parseS3Storage(conf *service.ParsedConfig) (storage.Storage, error) {
	bucket, err := conf.FieldString(ioFieldStorage, ioFieldStorageBucket)
	if err != nil {
		return nil, err
	}

	region, err := conf.FieldString(ioFieldStorage, ioFieldStorageRegion)
	if err != nil {
		return nil, err
	}

	endpoint := ""
	if conf.Contains(ioFieldStorage, ioFieldStorageEndpoint) {
		endpoint, err = conf.FieldString(ioFieldStorage, ioFieldStorageEndpoint)
		if err != nil {
			return nil, err
		}
	}

	return storage.NewS3Storage(conf, bucket, region, endpoint)
}

// Connect establishes connections to the catalog and storage.
func (o *icebergOutput) Connect(ctx context.Context) error {
	// Connect to storage
	if s3, ok := o.writer.storage.(*storage.S3Storage); ok {
		if err := s3.Connect(ctx); err != nil {
			return fmt.Errorf("failed to connect to S3: %w", err)
		}
	}

	o.logger.Info("Connected to Iceberg catalog and storage")
	return nil
}

// WriteBatch writes a batch of messages to the Iceberg table.
func (o *icebergOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	return o.writer.WriteBatch(ctx, batch)
}

// Close closes the output and releases resources.
func (o *icebergOutput) Close(ctx context.Context) error {
	return o.writer.Close(ctx)
}
