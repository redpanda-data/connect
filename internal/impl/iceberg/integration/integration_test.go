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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/catalogx"
)

func TestIntegrationIcebergRESTWithMinIO(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	ctx := context.Background()

	// Start test infrastructure (MinIO + Iceberg REST catalog)
	infra := startTestInfrastructure(t, ctx)
	t.Cleanup(func() {
		require.NoError(t, infra.Terminate(context.Background()))
	})

	// Create warehouse bucket in MinIO (must match CATALOG_WAREHOUSE in iceberg-rest-fixture)
	infra.CreateBucket(t, "warehouse")

	// Create namespace via REST API
	namespaceName := "test_ns"
	infra.CreateNamespace(t, namespaceName)

	// Test DuckDB connection to the Iceberg REST catalog
	t.Log("Testing DuckDB connection to Iceberg REST catalog...")

	tables, err := infra.ListIcebergTables(ctx, "demo", namespaceName)
	require.NoError(t, err, "DuckDB should be able to list tables")
	t.Logf("DuckDB listed tables in %s: %v", namespaceName, tables)

	// Namespace is empty, so we expect no tables
	assert.Empty(t, tables, "Namespace should have no tables initially")

	// Create a catalog client using our catalogx package
	// Note: No S3 properties needed - the REST catalog handles file I/O server-side
	c, err := catalogx.NewCatalogClient(catalogx.Config{
		URL:      infra.RestURL,
		AuthType: "none",
	}, []string{namespaceName})
	require.NoError(t, err, "create catalog client")

	// Create a table
	tbl, err := c.CreateTable(
		t.Context(),
		"foo",
		iceberg.NewSchema(-1, iceberg.NestedField{Type: iceberg.Int32Type{}, Name: "col"}),
	)
	require.NoError(t, err, "create table")
	t.Logf("Created table: %s", tbl.Identifier())

	// Verify table is visible via DuckDB
	tables, err = infra.ListIcebergTables(ctx, "demo", namespaceName)
	require.NoError(t, err, "DuckDB should be able to list tables")
	t.Logf("DuckDB listed tables after create: %v", tables)

	assert.Contains(t, tables, "foo", "Table 'foo' should be in the list")

	t.Log("Iceberg REST catalog integration with MinIO verified successfully")
}
