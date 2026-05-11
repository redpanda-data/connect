// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package confluent

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

// TestCommonToAvroLogicalTypes is the encode-side counterpart to
// TestEcsAvroFromBytesLogicalTypes. It pins that every common-schema
// logical type round-trips into the matching Avro logical-type
// annotation, including the precision/timezone parameters carried by
// the LogicalParams payload — so a Timestamp with Unit=Micros is not
// silently flattened to timestamp-millis, and a local-time field is
// not silently promoted to UTC.
//
// `BigDecimal` is intentionally omitted: it has no fixed precision and
// is rejected at encode time by design (see TestCommonToAvroBigDecimalRejected).
// `duration` (Avro fixed-12) is omitted on both encode and decode
// because benthos has no corresponding common-schema type.
func TestCommonToAvroLogicalTypes(t *testing.T) {
	cases := []struct {
		name            string
		in              schema.Common
		wantType        string
		wantLogicalType string
	}{
		{
			name: "timestamp-millis",
			in: schema.Common{
				Type: schema.Timestamp,
				Logical: &schema.LogicalParams{
					Timestamp: &schema.TimestampParams{Unit: schema.TimeUnitMillis, AdjustToUTC: true},
				},
			},
			wantType:        "long",
			wantLogicalType: "timestamp-millis",
		},
		{
			name: "timestamp-micros",
			in: schema.Common{
				Type: schema.Timestamp,
				Logical: &schema.LogicalParams{
					Timestamp: &schema.TimestampParams{Unit: schema.TimeUnitMicros, AdjustToUTC: true},
				},
			},
			wantType:        "long",
			wantLogicalType: "timestamp-micros",
		},
		{
			name: "local-timestamp-millis",
			in: schema.Common{
				Type: schema.Timestamp,
				Logical: &schema.LogicalParams{
					Timestamp: &schema.TimestampParams{Unit: schema.TimeUnitMillis, AdjustToUTC: false},
				},
			},
			wantType:        "long",
			wantLogicalType: "local-timestamp-millis",
		},
		{
			name: "local-timestamp-micros",
			in: schema.Common{
				Type: schema.Timestamp,
				Logical: &schema.LogicalParams{
					Timestamp: &schema.TimestampParams{Unit: schema.TimeUnitMicros, AdjustToUTC: false},
				},
			},
			wantType:        "long",
			wantLogicalType: "local-timestamp-micros",
		},
		{
			name:            "date",
			in:              schema.Common{Type: schema.Date},
			wantType:        "int",
			wantLogicalType: "date",
		},
		{
			name: "time-millis",
			in: schema.Common{
				Type: schema.TimeOfDay,
				Logical: &schema.LogicalParams{
					TimeOfDay: &schema.TimeOfDayParams{Unit: schema.TimeUnitMillis, AdjustToUTC: false},
				},
			},
			wantType:        "int",
			wantLogicalType: "time-millis",
		},
		{
			name: "time-micros",
			in: schema.Common{
				Type: schema.TimeOfDay,
				Logical: &schema.LogicalParams{
					TimeOfDay: &schema.TimeOfDayParams{Unit: schema.TimeUnitMicros, AdjustToUTC: false},
				},
			},
			wantType:        "long",
			wantLogicalType: "time-micros",
		},
		{
			name:            "uuid",
			in:              schema.Common{Type: schema.UUID},
			wantType:        "string",
			wantLogicalType: "uuid",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := avroUnmarshal(t, tc.in, "", "")
			m, ok := got.(map[string]any)
			if !ok {
				t.Fatalf("expected map representing a logical-type annotation, got %T: %v", got, got)
			}
			assert.Equal(t, tc.wantType, m["type"], "underlying Avro type")
			assert.Equal(t, tc.wantLogicalType, m["logicalType"], "Avro logicalType annotation")
		})
	}
}
