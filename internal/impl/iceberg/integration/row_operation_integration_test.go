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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
)

// opMsg builds a message whose body is the row data and whose `op` metadata
// drives the row_operation interpolation — mirroring how a CDC source would map
// its operation field onto the iceberg output.
func opMsg(t *testing.T, op, body string) *service.Message {
	t.Helper()
	m := service.NewMessage([]byte(body))
	m.MetaSetMut("op", op)
	return m
}

// TestRowOperationsIntegration drives inserts, an upsert, and a delete through
// the iceberg output and asserts the table reflects the correct final state:
// the upserted row wins (no duplicate) and the deleted row is gone.
func TestRowOperationsIntegration(t *testing.T) {
	integration.CheckSkip(t)

	ctx := context.Background()
	infra := setupTestInfra(t, ctx)

	const ns, tbl = "row_ops_ns", "row_ops_test"

	operation, err := service.NewInterpolatedString(`${! meta("op") }`)
	require.NoError(t, err)

	router := infra.NewRouter(t, ns, tbl,
		WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{Enabled: true}),
		WithRowOperation(icebergimpl.RowOpConfig{
			Operation:        operation,
			IdentifierFields: []string{"id"},
		}),
	)

	// Seed three rows. id is a string so the auto-created column is a valid
	// (non-floating-point) identifier key.
	produceMessages(t, ctx, router, service.MessageBatch{
		opMsg(t, "insert", `{"id": "1", "value": "one"}`),
		opMsg(t, "insert", `{"id": "2", "value": "two"}`),
		opMsg(t, "insert", `{"id": "3", "value": "three"}`),
	})

	// Upsert id=2 (replace its value) and delete id=3.
	produceMessages(t, ctx, router, service.MessageBatch{
		opMsg(t, "upsert", `{"id": "2", "value": "two-updated"}`),
	})
	produceMessages(t, ctx, router, service.MessageBatch{
		opMsg(t, "delete", `{"id": "3"}`),
	})

	type row struct {
		ID    string `json:"id"`
		Value string `json:"value"`
	}
	rows := querySQL[row](t, ctx, infra,
		`SELECT id, value FROM iceberg_cat."row_ops_ns"."row_ops_test" ORDER BY id;`)

	require.Len(t, rows, 2, "id=3 should be deleted and id=2 should not be duplicated")
	assert.Equal(t, "1", rows[0].ID)
	assert.Equal(t, "one", rows[0].Value)
	assert.Equal(t, "2", rows[1].ID)
	assert.Equal(t, "two-updated", rows[1].Value, "upsert should replace the prior value for id=2")
}
