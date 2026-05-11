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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

// TestEcsAvroFromBytesLogicalTypes covers every Avro logical type the
// Avro 1.11 spec defines that has a corresponding type in benthos's
// common schema. Each case parses a single-field Avro record and
// asserts both the resulting CommonType and the LogicalParams payload
// (where applicable) — so a fix that maps every logical type to the
// same Common.Type without preserving precision/UTC semantics still
// fails.
//
// `duration` is intentionally omitted because benthos has no
// corresponding common-schema type for Avro's 12-byte month/day/ms
// triple.
func TestEcsAvroFromBytesLogicalTypes(t *testing.T) {
	cases := []struct {
		name        string
		avroType    string
		wantType    schema.CommonType
		wantLogical *schema.LogicalParams
	}{
		{
			name:     "timestamp-millis",
			avroType: `{"type": "long", "logicalType": "timestamp-millis"}`,
			wantType: schema.Timestamp,
			wantLogical: &schema.LogicalParams{
				Timestamp: &schema.TimestampParams{Unit: schema.TimeUnitMillis, AdjustToUTC: true},
			},
		},
		{
			name:     "timestamp-micros",
			avroType: `{"type": "long", "logicalType": "timestamp-micros"}`,
			wantType: schema.Timestamp,
			wantLogical: &schema.LogicalParams{
				Timestamp: &schema.TimestampParams{Unit: schema.TimeUnitMicros, AdjustToUTC: true},
			},
		},
		{
			name:     "local-timestamp-millis",
			avroType: `{"type": "long", "logicalType": "local-timestamp-millis"}`,
			wantType: schema.Timestamp,
			wantLogical: &schema.LogicalParams{
				Timestamp: &schema.TimestampParams{Unit: schema.TimeUnitMillis, AdjustToUTC: false},
			},
		},
		{
			name:     "local-timestamp-micros",
			avroType: `{"type": "long", "logicalType": "local-timestamp-micros"}`,
			wantType: schema.Timestamp,
			wantLogical: &schema.LogicalParams{
				Timestamp: &schema.TimestampParams{Unit: schema.TimeUnitMicros, AdjustToUTC: false},
			},
		},
		{
			name:     "date",
			avroType: `{"type": "int", "logicalType": "date"}`,
			wantType: schema.Date,
		},
		{
			name:     "time-millis",
			avroType: `{"type": "int", "logicalType": "time-millis"}`,
			wantType: schema.TimeOfDay,
			wantLogical: &schema.LogicalParams{
				TimeOfDay: &schema.TimeOfDayParams{Unit: schema.TimeUnitMillis, AdjustToUTC: false},
			},
		},
		{
			name:     "time-micros",
			avroType: `{"type": "long", "logicalType": "time-micros"}`,
			wantType: schema.TimeOfDay,
			wantLogical: &schema.LogicalParams{
				TimeOfDay: &schema.TimeOfDayParams{Unit: schema.TimeUnitMicros, AdjustToUTC: false},
			},
		},
		{
			name:     "uuid",
			avroType: `{"type": "string", "logicalType": "uuid"}`,
			wantType: schema.UUID,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := fmt.Appendf(nil, `{
				"type": "record",
				"name": "Event",
				"fields": [
					{"name": "f", "type": %s}
				]
			}`, tc.avroType)

			c, err := ecsAvroParseFromBytes(ecsAvroConfig{}, spec)
			require.NoError(t, err)
			require.Equal(t, schema.Object, c.Type)
			require.Len(t, c.Children, 1)

			field := c.Children[0]
			assert.Equal(t, "f", field.Name)
			assert.Equal(t, tc.wantType, field.Type)
			if tc.wantLogical == nil {
				assert.Nil(t, field.Logical)
			} else {
				assert.Equal(t, tc.wantLogical, field.Logical)
			}
		})
	}
}
