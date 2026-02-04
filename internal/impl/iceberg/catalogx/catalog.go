// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package catalogx

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
)

// catalogClient wraps the iceberg-go REST catalog client.
type Client struct {
	catalog   catalog.Catalog
	namespace []string
}

// Config holds the catalog configuration.
type Config struct {
	URL             string
	Warehouse       string
	Prefix          string
	AdditionalProps iceberg.Properties

	// Authentication
	AuthType string // "none", "oauth2", "bearer", "sigv4"

	// OAuth2 fields
	OAuth2ServerURI    *url.URL
	OAuth2ClientID     string
	OAuth2ClientSecret string
	OAuth2Scope        string

	// Bearer token
	BearerToken string

	// AWS SigV4 fields
	SigV4Region  string // AWS region for SigV4 signing (e.g., "us-east-1")
	SigV4Service string // AWS service name for SigV4 signing (default: "execute-api")

	// Custom HTTP headers
	Headers map[string]string

	// TLS configuration
	TLSSkipVerify bool
}

// NewCatalogClient creates a new REST catalog client.
func NewCatalogClient(cfg Config, namespace []string) (*Client, error) {
	// Build options for REST catalog
	var opts []rest.Option

	// Configure authentication
	switch cfg.AuthType {
	case "oauth2":
		credential := fmt.Sprintf("%s:%s", cfg.OAuth2ClientID, cfg.OAuth2ClientSecret)
		opts = append(opts, rest.WithCredential(credential))
		if cfg.OAuth2ServerURI != nil {
			opts = append(opts, rest.WithAuthURI(cfg.OAuth2ServerURI))
		}
		if cfg.OAuth2Scope != "" {
			opts = append(opts, rest.WithScope(cfg.OAuth2Scope))
		}
	case "bearer":
		opts = append(opts, rest.WithOAuthToken(cfg.BearerToken))
	case "sigv4":
		// Use region/service-specific SigV4 if provided, otherwise use default
		if cfg.SigV4Region != "" || cfg.SigV4Service != "" {
			opts = append(opts, rest.WithSigV4RegionSvc(cfg.SigV4Region, cfg.SigV4Service))
		} else {
			opts = append(opts, rest.WithSigV4())
		}
	case "none":
		// No authentication
	default:
		return nil, fmt.Errorf("unsupported auth type: %s", cfg.AuthType)
	}

	if cfg.Warehouse != "" {
		opts = append(opts, rest.WithWarehouseLocation(cfg.Warehouse))
	}
	if cfg.Prefix != "" {
		opts = append(opts, rest.WithPrefix(cfg.Prefix))
	}
	if cfg.AdditionalProps != nil {
		opts = append(opts, rest.WithAdditionalProps(cfg.AdditionalProps))
	}

	// Configure custom headers
	if len(cfg.Headers) > 0 {
		opts = append(opts, rest.WithHeaders(cfg.Headers))
	}

	// Configure TLS
	if cfg.TLSSkipVerify {
		opts = append(opts, rest.WithTLSConfig(&tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // User explicitly requested to skip TLS verification
		}))
	}

	// Create REST catalog
	ctx := context.Background()
	restCatalog, err := rest.NewCatalog(
		ctx,
		"rest",
		cfg.URL,
		opts...,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create REST catalog: %w", err)
	}

	return &Client{
		catalog:   restCatalog,
		namespace: namespace,
	}, nil
}

// LoadTable loads an existing table from the catalog.
func (c *Client) LoadTable(ctx context.Context, tableName string) (*table.Table, error) {
	identifier := toTableIdentifier(c.namespace, tableName)
	tbl, err := c.catalog.LoadTable(ctx, identifier)
	if err != nil {
		return nil, fmt.Errorf("failed to load table %s: %w", strings.Join(identifier, "."), err)
	}
	return tbl, nil
}

// CreateTable creates a new table with the given schema.
func (c *Client) CreateTable(ctx context.Context, tableName string, schema *iceberg.Schema) (*table.Table, error) {
	return c.CreateTableWithSpec(ctx, tableName, schema, nil)
}

// CreateTableWithSpec creates a new table with the given schema and partition spec.
func (c *Client) CreateTableWithSpec(ctx context.Context, tableName string, schema *iceberg.Schema, spec *iceberg.PartitionSpec) (*table.Table, error) {
	identifier := toTableIdentifier(c.namespace, tableName)
	var opts []catalog.CreateTableOpt
	if spec != nil {
		opts = append(opts, catalog.WithPartitionSpec(spec))
	}
	tbl, err := c.catalog.CreateTable(ctx, identifier, schema, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create table %s: %w", strings.Join(identifier, "."), err)
	}
	return tbl, nil
}

// UpdateSchema applies schema changes to the table using a transaction.
// The callback function receives an UpdateSchema instance that can be used to add, delete,
// rename, or update columns. The transaction is automatically committed after the callback.
//
// Example usage:
//
//	err := client.UpdateSchema(ctx, tbl, func(us *table.UpdateSchema) {
//	    us.AddColumn([]string{"email"}, iceberg.StringType{}, "Email address", false, nil)
//	    us.AddColumn([]string{"age"}, iceberg.Int32Type{}, "", false, nil)
//	})
func (c *Client) UpdateSchema(ctx context.Context, tbl *table.Table, fn func(*table.UpdateSchema), opts ...table.UpdateSchemaOption) (*table.Table, error) {
	txn := tbl.NewTransaction()
	updateSchema := txn.UpdateSchema(
		true,  // caseSensitive
		false, // allowIncompatibleChanges
		opts...,
	)

	// Let the caller configure the schema changes
	fn(updateSchema)

	// Commit the schema update to the transaction
	if err := updateSchema.Commit(); err != nil {
		return nil, fmt.Errorf("failed to apply schema update: %w", err)
	}

	// Commit the transaction to persist changes
	return txn.Commit(ctx)
}

// AppendDataFiles commits a batch of data files to the table.
func (c *Client) AppendDataFiles(ctx context.Context, tbl *table.Table, dataFiles []string) (*table.Table, error) {
	txn := tbl.NewTransaction()
	if err := txn.AddFiles(ctx, dataFiles, nil, true); err != nil {
		return nil, err
	}
	return txn.Commit(ctx)
}

// Close closes the catalog connection.
func (c *Client) Close() error {
	return nil
}

func toTableIdentifier(ns []string, table string) table.Identifier {
	id := make([]string, len(ns)+1)
	copy(id, ns)
	id[len(ns)] = table
	return id
}
