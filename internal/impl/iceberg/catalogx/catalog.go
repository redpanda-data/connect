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

// catalogConfig holds the catalog configuration.
type Config struct {
	URL             string
	Warehouse       string
	Prefix          string
	AdditionalProps iceberg.Properties

	AuthType string // "none", "oauth2", "bearer", "sigv4"
	// OAuth2 fields
	OAuth2ServerURI    *url.URL
	OAuth2ClientID     string
	OAuth2ClientSecret string
	OAuth2Scope        string
	// Bearer token
	BearerToken string
	// AWS SigV4 uses credentials from storage config
}

// newCatalogClient creates a new REST catalog client.
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
		opts = append(opts, rest.WithSigV4())
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

// AddColumns adds new columns to an existing table.
// This is used for schema evolution.
func (c *Client) UpdateSchema(ctx context.Context, tbl *table.Table, newSchema *iceberg.Schema) error {
	current := tbl.Schema()
	_, _, err := c.catalog.CommitTable(
		ctx,
		tbl.Identifier(),
		table.Requirements{
			table.AssertCurrentSchemaID(current.ID),
		},
		table.Updates{
			table.NewAddSchemaUpdate(newSchema),
			// NOTE: the -1 means we should set the schema to be the one we're adding here
			table.NewSetCurrentSchemaUpdate(-1),
		},
	)
	return err
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
