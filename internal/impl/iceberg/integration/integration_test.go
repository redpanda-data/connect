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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationIcebergRESTWithMinIO(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	ctx := context.Background()
	infra := setupTestInfra(t, ctx)

	namespaceName := "test_ns"
	infra.CreateNamespace(t, namespaceName)

	// Verify empty namespace via DuckDB
	type tableNameResult struct {
		TableName string `json:"table_name"`
	}
	tables := querySQL[tableNameResult](t, ctx, infra,
		fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' AND table_catalog = 'iceberg_cat';`, namespaceName))
	assert.Empty(t, tables)

	// Create table via catalogx
	c := infra.NewCatalogClient(t, namespaceName)
	_, err := c.CreateTable(
		t.Context(),
		"foo",
		iceberg.NewSchema(-1, iceberg.NestedField{Type: iceberg.Int32Type{}, Name: "col"}),
	)
	require.NoError(t, err)

	// Verify table visible via DuckDB
	tables = querySQL[tableNameResult](t, ctx, infra,
		fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' AND table_catalog = 'iceberg_cat';`, namespaceName))
	var names []string
	for _, row := range tables {
		names = append(names, row.TableName)
	}
	assert.Contains(t, names, "foo")
}
