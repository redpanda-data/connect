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

	"github.com/apache/iceberg-go"

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
	router *router
	logger *service.Logger
}

// newIcebergOutputFromConfig creates a new Iceberg output from parsed configuration.
func newIcebergOutputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*icebergOutput, error) {
	// Parse catalog configuration
	catalogCfg, err := parseCatalogConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse catalog config: %w", err)
	}

	// Parse table identification
	namespaceStr, err := conf.FieldInterpolatedString(ioFieldNamespace)
	if err != nil {
		return nil, fmt.Errorf("failed to parse namespace: %w", err)
	}

	tableStr, err := conf.FieldInterpolatedString(ioFieldTable)
	if err != nil {
		return nil, fmt.Errorf("failed to parse table name: %w", err)
	}

	// Create router
	rtr := NewRouter(catalogCfg, namespaceStr, tableStr, mgr.Logger())

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

	// Parse S3 storage configuration for AdditionalProps
	cfg.AdditionalProps, err = parseS3Props(conf)
	if err != nil {
		return cfg, err
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
				return cfg, fmt.Errorf("failed to parse oauth2 server URI: %w", err)
			}
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

// parseS3Props extracts S3 storage properties from config and returns them as iceberg.Properties.
func parseS3Props(conf *service.ParsedConfig) (iceberg.Properties, error) {
	props := make(iceberg.Properties)

	// Check if storage config exists
	if !conf.Contains(ioFieldStorage) {
		return props, nil
	}

	storageType, err := conf.FieldString(ioFieldStorage, ioFieldStorageType)
	if err != nil || storageType != "s3" {
		return props, nil // Only S3 supported for now
	}

	// Get region
	if conf.Contains(ioFieldStorage, ioFieldStorageRegion) {
		region, err := conf.FieldString(ioFieldStorage, ioFieldStorageRegion)
		if err != nil {
			return nil, err
		}
		props["s3.region"] = region
	}

	// Get endpoint
	if conf.Contains(ioFieldStorage, ioFieldStorageEndpoint) {
		endpoint, err := conf.FieldString(ioFieldStorage, ioFieldStorageEndpoint)
		if err != nil {
			return nil, err
		}
		props["s3.endpoint"] = endpoint
		// For custom endpoints (like MinIO), enable path-style access
		props["s3.path-style-access"] = "true"
	}

	// Get credentials - check for static credentials first
	if conf.Contains(ioFieldStorage, "credentials", "id") {
		accessKeyID, err := conf.FieldString(ioFieldStorage, "credentials", "id")
		if err != nil {
			return nil, err
		}
		props["s3.access-key-id"] = accessKeyID
	}
	if conf.Contains(ioFieldStorage, "credentials", "secret") {
		secretAccessKey, err := conf.FieldString(ioFieldStorage, "credentials", "secret")
		if err != nil {
			return nil, err
		}
		props["s3.secret-access-key"] = secretAccessKey
	}

	return props, nil
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
