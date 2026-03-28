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
	"time"

	"github.com/twmb/avro"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

// preserveLogicalTypeOpts returns CustomType registrations that convert
// Avro logical types to richer Go types for the preserve_logical_types
// decoder path:
//   - time-millis/time-micros: time.Duration → time.Time (time-of-day)
//   - duration: avro.Duration → ISO 8601 duration string
func preserveLogicalTypeOpts() []avro.SchemaOpt {
	return []avro.SchemaOpt{
		avro.NewCustomType[time.Time, int32](
			"time-millis",
			nil,
			func(v int32, _ *avro.SchemaNode) (time.Time, error) {
				return time.Time{}.Add(time.Duration(v) * time.Millisecond), nil
			},
		),
		avro.NewCustomType[time.Time, int64](
			"time-micros",
			nil,
			func(v int64, _ *avro.SchemaNode) (time.Time, error) {
				return time.Time{}.Add(time.Duration(v) * time.Microsecond), nil
			},
		),
		avro.CustomType{
			LogicalType: "duration",
			AvroType:    "fixed",
			Decode: func(v any, _ *avro.SchemaNode) (any, error) {
				b, ok := v.([]byte)
				if !ok || len(b) != 12 {
					return nil, avro.ErrSkipCustomType
				}
				return avro.DurationFromBytes(b).String(), nil
			},
		},
	}
}

// kafkaConnectTypeOpt returns a CustomType that translates Kafka Connect
// (Debezium) temporal types based on the connect.name schema property.
func kafkaConnectTypeOpt() avro.SchemaOpt {
	return avro.CustomType{
		Decode: func(value any, node *avro.SchemaNode) (any, error) {
			name, _ := node.Props["connect.name"].(string)
			switch name {
			case "io.debezium.time.Date":
				n, err := bloblang.ValueAsInt64(value)
				if err != nil {
					return nil, err
				}
				return time.UnixMilli(0).UTC().AddDate(0, 0, int(n)), nil
			case "io.debezium.time.Year":
				n, err := bloblang.ValueAsInt64(value)
				if err != nil {
					return nil, err
				}
				return time.UnixMilli(0).UTC().AddDate(int(n), 0, 0), nil
			case "io.debezium.time.Timestamp", "io.debezium.time.Time":
				n, err := bloblang.ValueAsInt64(value)
				if err != nil {
					return nil, err
				}
				return time.UnixMilli(n).UTC(), nil
			case "io.debezium.time.MicroTimestamp", "io.debezium.time.MicroTime":
				n, err := bloblang.ValueAsInt64(value)
				if err != nil {
					return nil, err
				}
				return time.UnixMilli(0).UTC().Add(time.Duration(n) * time.Microsecond), nil
			case "io.debezium.time.NanoTimestamp", "io.debezium.time.NanoTime":
				n, err := bloblang.ValueAsInt64(value)
				if err != nil {
					return nil, err
				}
				return time.UnixMilli(0).UTC().Add(time.Duration(n) * time.Nanosecond), nil
			case "io.debezium.time.ZonedTimestamp":
				s := bloblang.ValueToString(value)
				t, err := time.ParseInLocation(time.RFC3339Nano, s, time.UTC)
				if err != nil {
					return nil, err
				}
				return t, nil
			}
			return nil, avro.ErrSkipCustomType
		},
	}
}
