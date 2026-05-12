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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
)

// TestIntegrationSchemaMetadataDrivesTimestampColumn feeds the iceberg Router
// with the exact shape of input that schema_registry_decode produces after
// the parser fix in confluent/ecs_avro.go: a structured body plus a "schema"
// metadata entry holding the ToAny() form of a schema.Common with
// Type=Timestamp on the time-typed field.
//
// The test is deliberately isolated from confluent/avro so it exercises only
// the iceberg side of the boundary: given a correctly-shaped @schema, the
// iceberg output must auto-create a timestamp-typed column and scale the
// numeric or time.Time value into a correct parquet timestamp.
//
// Two value-side variants are covered, mirroring what twmb/avro emits for
// different idiomatic Avro shapes once schema_registry_decode finishes:
//
//   - time.Time values  — produced when the Avro schema nests logicalType
//     inside the union object; twmb honours it natively.
//   - int64 values      — produced when the Avro schema places logicalType
//     as a sibling of `type` (Java/JDBC idiom); twmb does not honour the
//     annotation and emits a raw long. The iceberg shredder must consult
//     the @schema metadata's declared Unit to scale it correctly.
//
// In both cases the resulting iceberg table must hold a timestamp column
// (not BIGINT) and the value must round-trip to the correct calendar date.
func TestIntegrationSchemaMetadataDrivesTimestampColumn(t *testing.T) {
	integration.CheckSkip(t)

	ctx := context.Background()
	infra := setupTestInfra(t, ctx)

	const namespace = "schema_meta_timestamp"
	infra.CreateNamespace(t, namespace)

	// Construct the schema.Common that the schema_registry_decode parser
	// must produce after our fix: record { id: string, ts: Optional<Timestamp(Millis,UTC)> }.
	commonSchema := schema.Common{
		Type: schema.Object, Name: "Event",
		Children: []schema.Common{
			{Name: "id", Type: schema.String},
			{
				Name: "ts", Optional: true, Type: schema.Timestamp,
				Logical: &schema.LogicalParams{
					Timestamp: &schema.TimestampParams{
						Unit: schema.TimeUnitMillis, AdjustToUTC: true,
					},
				},
			},
		},
	}
	schemaMeta := commonSchema.ToAny()

	const tsMillis = int64(1700000000000) // 2023-11-14T22:13:20Z
	expectedTimestampDate := "2023-11-14"

	cases := []struct {
		name      string
		tableName string
		tsValue   any
	}{
		{
			name:      "value-is-time.Time",
			tableName: "events_time_value",
			tsValue:   time.UnixMilli(tsMillis).UTC(),
		},
		{
			name:      "value-is-int64",
			tableName: "events_int_value",
			tsValue:   tsMillis,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Build the message exactly as schema_registry_decode would: a
			// structured map for the body plus the schema.Common (in ToAny()
			// form) under the configured metadata key.
			msg := service.NewMessage(nil)
			msg.SetStructured(map[string]any{
				"id": "evt-1",
				"ts": tc.tsValue,
			})
			msg.MetaSetMut("schema", schemaMeta)

			router := infra.NewRouter(t, namespace, tc.tableName,
				WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{
					Enabled:        true,
					SchemaMetadata: "schema",
				}))
			produceMessages(t, ctx, router, service.MessageBatch{msg})

			cols := querySQL[ColumnInfo](t, ctx, infra,
				fmt.Sprintf(`DESCRIBE iceberg_cat."%s"."%s";`, namespace, tc.tableName))
			require.Len(t, cols, 2, "expected exactly two columns")
			typeOf := map[string]string{}
			for _, c := range cols {
				typeOf[c.ColumnName] = c.ColumnType
			}
			assert.Equal(t, "VARCHAR", typeOf["id"])
			assert.Contains(t, typeOf["ts"], "TIMESTAMP",
				"ts column must be a timestamp type; BIGINT means @schema metadata was ignored or didn't carry Type=Timestamp")

			type row struct {
				ID string `json:"id"`
				TS string `json:"ts"`
			}
			rows := querySQL[row](t, ctx, infra,
				fmt.Sprintf(`SELECT id, CAST(ts AS VARCHAR) AS ts FROM iceberg_cat."%s"."%s";`, namespace, tc.tableName))
			require.Len(t, rows, 1)
			assert.Equal(t, "evt-1", rows[0].ID)
			assert.Contains(t, rows[0].TS, expectedTimestampDate,
				"timestamp must round-trip as %s; a year ~55,000 result means the int64 millis was treated as seconds via the fallback path", expectedTimestampDate)
		})
	}
}
