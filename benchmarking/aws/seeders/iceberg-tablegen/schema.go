// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "github.com/apache/iceberg-go"

// ordersSchema is the fixed Iceberg schema for the json-orders bench dataset.
// It MUST track the record shape produced by seeders/json-orders/produce.go
// (id, ts, region, amount, status, payload). Fields are Optional to match what
// Connect's iceberg output infers for the same JSON, so a pre-created table is
// schema-compatible with both engines.
func ordersSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 4, Name: "amount", Type: iceberg.PrimitiveTypes.Float64, Required: false},
		iceberg.NestedField{ID: 5, Name: "status", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 6, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
}
