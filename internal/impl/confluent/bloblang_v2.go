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
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
)

func init() {
	registerWithSchemaRegistryHeaderV2()
}

func registerWithSchemaRegistryHeaderV2() {
	spec := bloblangv2.NewPluginSpec().
		Category("Encoding").
		Description("Prepends a Confluent Schema Registry wire format header to message bytes. The header is 5 bytes: a magic byte (0x00) followed by a 4-byte big-endian schema ID. This format is required when producing messages to Kafka topics that use Confluent Schema Registry for schema validation and evolution.").
		Param(bloblangv2.NewAnyParam("schema_id").Description("The schema ID from your Schema Registry (0 to 4294967295). This ID references the schema version used to encode the message.")).
		Param(bloblangv2.NewAnyParam("message").Description("The serialized message bytes (e.g., Avro, Protobuf, or JSON Schema encoded data) to prepend the header to."))

	bloblangv2.MustRegisterFunction("with_schema_registry_header", spec, func(args *bloblangv2.ParsedParams) (bloblangv2.Function, error) {
		return func() (any, error) {
			schemaIDRaw, err := args.Get("schema_id")
			if err != nil {
				return nil, err
			}

			messageRaw, err := args.Get("message")
			if err != nil {
				return nil, err
			}

			messageBytes, err := schemaRegistryMessageBytes(messageRaw)
			if err != nil {
				return nil, err
			}

			schemaID, err := schemaRegistryID(schemaIDRaw)
			if err != nil {
				return nil, err
			}

			n := len(messageBytes)
			messageBytes = slices.Grow(messageBytes, 5)
			messageBytes = append(messageBytes, 0, 0, 0, 0, 0)
			copy(messageBytes[5:n+5], messageBytes[0:n])
			messageBytes[0] = 0
			binary.BigEndian.PutUint32(messageBytes[1:5], schemaID)

			return messageBytes, nil
		}, nil
	})
}

// schemaRegistryMessageBytes mirrors V1's bloblang.ValueAsBytes behaviour
// (string or []byte) since V2's strict typed wrappers do not coerce strings
// to bytes.
func schemaRegistryMessageBytes(v any) ([]byte, error) {
	switch t := v.(type) {
	case []byte:
		return t, nil
	case string:
		return []byte(t), nil
	}
	return nil, fmt.Errorf("message must be bytes or string: got %T", v)
}

// schemaRegistryID validates the schema_id argument and converts it to the
// uint32 wire form. The accepted shapes mirror the V1 plugin's type switch.
func schemaRegistryID(v any) (uint32, error) {
	const maxSchemaID = math.MaxUint32
	switch n := v.(type) {
	case int:
		if n < 0 || n > maxSchemaID {
			return 0, fmt.Errorf("schema ID must be between 0 and %d, got %d", maxSchemaID, n)
		}
		return uint32(n), nil
	case int64:
		if n < 0 || n > maxSchemaID {
			return 0, fmt.Errorf("schema ID must be between 0 and %d, got %d", maxSchemaID, n)
		}
		return uint32(n), nil
	case float64:
		if n < 0 || n > maxSchemaID || n != float64(int64(n)) {
			return 0, fmt.Errorf("schema ID must be a valid integer between 0 and %d, got %f", maxSchemaID, n)
		}
		return uint32(n), nil
	}
	return 0, fmt.Errorf("schema ID must be a number, got %T", v)
}
