// Copyright 2026 Redpanda Data, Inc.
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
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
)

// TestSnapshotRetentionIntegration proves the commit.* table-lifecycle knobs
// against a real REST catalog, not the in-memory one: the catalog must accept
// a commit that carries a remove-snapshots update alongside the new snapshot,
// the manifest-merge property must land on the table through set-properties,
// and a reader must still see every row once older snapshots are gone.
func TestSnapshotRetentionIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	ctx := context.Background()
	infra := setupTestInfra(t)

	const (
		namespace = "retention_ns"
		tableName = "retention_table"
		age       = 1500 * time.Millisecond
	)
	infra.EnsureNamespaceAbsent(t, namespace)
	router := infra.NewRouter(t, namespace, tableName,
		WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{Enabled: true}),
		WithCommitConfig(icebergimpl.CommitConfig{
			ManifestMergeEnabled: true,
			MaxSnapshotAge:       age,
			MaxRetries:           3,
		}),
	)

	// Three commits, each one spaced past the retention horizon, so by the
	// third commit the first two snapshots are expirable.
	const commits = 3
	for i := range commits {
		produce(t, ctx, router, fmt.Sprintf(`{"id": %d, "name": "row-%d"}`, i+1, i+1))
		if i < commits-1 {
			time.Sleep(age + 500*time.Millisecond)
		}
	}

	client := infra.NewCatalogClient(t, namespace)
	tbl, err := client.LoadTable(ctx, tableName)
	require.NoError(t, err)

	snaps := tbl.Metadata().Snapshots()
	assert.LessOrEqual(t, len(snaps), 2,
		"snapshots older than max_snapshot_age must have been expired through the catalog; got %d", len(snaps))
	require.NotNil(t, tbl.CurrentSnapshot())
	assert.Equal(t, "true", tbl.Properties()[table.ManifestMergeEnabledKey],
		"commit.manifest-merge.enabled must be stamped onto the table as a table property")

	// Every row is still readable: expiry drops snapshots from metadata, never
	// data the current snapshot depends on.
	rows := querySQL[countResult](t, ctx, infra,
		fmt.Sprintf(`SELECT COUNT(*) AS count FROM iceberg_cat."%s"."%s";`, namespace, tableName))
	require.Len(t, rows, 1)
	assert.Equal(t, commits, rows[0].Count)
}
