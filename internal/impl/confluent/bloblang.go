// Copyright 2025 Redpanda Data, Inc.
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

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

func init() {
	registerWithSchemaRegistryHeader()
}

func registerWithSchemaRegistryHeader() {
	spec := bloblang.NewPluginSpec().
		Beta().
		Category("Encoding").
		Description("Prepends a Confluent Schema Registry wire format header to message bytes. The header is 5 bytes: a magic byte (0x00) followed by a 4-byte big-endian schema ID. This format is required when producing messages to Kafka topics that use Confluent Schema Registry for schema validation and evolution.").
		Param(bloblang.NewAnyParam("schema_id").Description("The schema ID from your Schema Registry (0 to 4294967295). This ID references the schema version used to encode the message.")).
		Param(bloblang.NewAnyParam("message").Description("The serialized message bytes (e.g., Avro, Protobuf, or JSON Schema encoded data) to prepend the header to.")).
		Example(
			"Add Schema Registry header to Avro-encoded message",
			`root = with_schema_registry_header(123, content())`,
		).
		Example(
			"Use schema ID from metadata to add header dynamically",
			`root = with_schema_registry_header(meta("schema_id").number(), content())`,
		)

	bloblang.MustRegisterFunctionV2("with_schema_registry_header", spec, func(args *bloblang.ParsedParams) (bloblang.Function, error) {
		return func() (any, error) {
			schemaIDRaw, err := args.Get("schema_id")
			if err != nil {
				return nil, err
			}

			messageRaw, err := args.Get("message")
			if err != nil {
				return nil, err
			}

			// Convert message to bytes
			messageBytes, err := bloblang.ValueAsBytes(messageRaw)
			if err != nil {
				return nil, fmt.Errorf("message must be bytes or string: %w", err)
			}

			const maxSchemaID = math.MaxUint32

			// Convert schema ID to uint32
			var schemaID uint32
			switch v := schemaIDRaw.(type) {
			case int:
				if v < 0 || v > maxSchemaID {
					return nil, fmt.Errorf("schema ID must be between 0 and %d, got %d", maxSchemaID, v)
				}
				schemaID = uint32(v)
			case int64:
				if v < 0 || v > maxSchemaID {
					return nil, fmt.Errorf("schema ID must be between 0 and %d, got %d", maxSchemaID, v)
				}
				schemaID = uint32(v)
			case float64:
				if v < 0 || v > maxSchemaID || v != float64(int64(v)) {
					return nil, fmt.Errorf("schema ID must be a valid integer between 0 and %d, got %f", maxSchemaID, v)
				}
				schemaID = uint32(v)
			default:
				return nil, fmt.Errorf("schema ID must be a number, got %T", v)
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
