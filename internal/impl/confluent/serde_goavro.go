// Copyright 2024 Redpanda Data, Inc.
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
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/linkedin/goavro/v2"
	franz_sr "github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

func resolveGoAvroReferences(ctx context.Context, client *sr.Client, mapping *bloblang.Executor, schema franz_sr.Schema) ([]json.RawMessage, error) {
	mapSchema := func(s franz_sr.Schema) (string, error) {
		if mapping == nil {
			return s.Schema, nil
		}
		msg := service.NewMessage([]byte(s.Schema))
		msg, err := msg.BloblangQuery(mapping)
		if err != nil {
			return "", fmt.Errorf("unable to apply avro schema mapping: %w", err)
		}
		avroSchema, err := msg.AsBytes()
		if err != nil {
			return "", fmt.Errorf("unable to extract avro schema mapping result: %w", err)
		}
		return string(avroSchema), nil
	}

	schemas := []json.RawMessage{}
	if err := client.WalkReferences(ctx, schema.References, func(ctx context.Context, name string, schema franz_sr.Schema) error {
		s, err := mapSchema(schema)
		schemas = append(schemas, []byte(s))
		return err
	}); err != nil {
		return nil, nil
	}

	root, err := mapSchema(schema)
	if err != nil {
		return nil, err
	}
	schemas = append(schemas, []byte(root))
	return schemas, nil
}

func (s *schemaRegistryEncoder) getAvroEncoder(ctx context.Context, schema franz_sr.Schema) (schemaEncoder, error) {
	schemas, err := resolveGoAvroReferences(ctx, s.client, nil, schema)
	if err != nil {
		return nil, err
	}
	var schemaJson json.RawMessage
	numSchemas := len(schemas)
	var numEncodedBytesToStrip int

	var native interface{}
	if err := json.Unmarshal(schemas[len(schemas)-1], &native); err != nil {
		return nil, fmt.Errorf("failed to parse root schema as enum: %w", err)
	}

	if numSchemas == 1 {
		schemaJson = schemas[0]
	} else if numSchemas >= 2 {
		switch v := native.(type) {
		case []interface{}:
			numEncodedBytesToStrip = 0
		case map[string]interface{}:
			numEncodedBytesToStrip = len(intToZigZagBytes(int64(numSchemas - 1)))
		default:
			return nil, fmt.Errorf("unexpected root schema type: %T", v)
		}
	} else {
		return nil, fmt.Errorf("expect at least one schema to decode, got: %d", numSchemas)
	}

	var schemaSpec string
	if schemaJson != nil {
		schemaJson, err := json.Marshal(schemaJson)
		schemaSpec = string(schemaJson)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal hydrated schema: %w", err)
		}
	} else {
		schemasJson, err := json.Marshal(schemas)
		schemaSpec = string(schemasJson)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal hydrated schema: %w", err)
		}
	}

	var codec *goavro.Codec
	if s.avroRawJSON {
		if codec, err = goavro.NewCodecForStandardJSONFull(schemaSpec); err != nil {
			return nil, err
		}
	} else {
		if codec, err = goavro.NewCodec(schemaSpec); err != nil {
			return nil, err
		}
	}

	return func(m *service.Message) error {
		b, err := m.AsBytes()
		if err != nil {
			return err
		}

		datum, _, err := codec.NativeFromTextual(b)
		if err != nil {
			return err
		}

		binary, err := codec.BinaryFromNative(nil, datum)
		if err != nil {
			return err
		}
		binary = binary[numEncodedBytesToStrip:]

		m.SetBytes(binary)
		return nil
	}, nil
}

func (s *schemaRegistryDecoder) getGoAvroDecoder(ctx context.Context, schema franz_sr.Schema) (schemaDecoder, error) {
	schemas, err := resolveGoAvroReferences(ctx, s.client, nil, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve avro references: %w", err)
	}

	var schemaJson json.RawMessage
	numSchemas := len(schemas)
	var unionTypeBytesToAdd []byte

	var native interface{}
	if err := json.Unmarshal(schemas[len(schemas)-1], &native); err != nil {
		return nil, fmt.Errorf("failed to parse root schema as enum: %w", err)
	}

	if numSchemas == 1 {
		schemaJson = schemas[0]
	} else if numSchemas >= 2 {
		switch v := native.(type) {
		case []interface{}:
			unionTypeBytesToAdd = nil
		case map[string]interface{}:
			unionTypeBytesToAdd = intToZigZagBytes(int64(numSchemas - 1))
		default:
			return nil, fmt.Errorf("unexpected root schema type: %T", v)
		}
	} else {
		return nil, fmt.Errorf("expect at least one schema to decode, got: %d", numSchemas)
	}

	var schemaSpec string
	if schemaJson != nil {
		schemaJson, err := json.Marshal(schemaJson)
		schemaSpec = string(schemaJson)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal hydrated schema: %w", err)
		}
	} else {
		schemasJson, err := json.Marshal(schemas)
		schemaSpec = string(schemasJson)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal hydrated schema: %w", err)
		}
	}

	var codec *goavro.Codec
	if s.cfg.avro.rawUnions {
		codec, err = goavro.NewCodecForStandardJSONFull(schemaSpec)
	} else {
		codec, err = goavro.NewCodec(schemaSpec)
	}
	if err != nil {
		return nil, err
	}

	decoder := func(m *service.Message) error {
		b, err := m.AsBytes()
		if unionTypeBytesToAdd != nil {
			b = append(unionTypeBytesToAdd, b...)
		}
		if err != nil {
			return err
		}

		native, _, err := codec.NativeFromBinary(b)
		if err != nil {
			return err
		}

		jb, err := codec.TextualFromNative(nil, native)
		if err != nil {
			return err
		}

		if unionTypeBytesToAdd != nil {
			var unionWrapper map[string]interface{}
			err := json.Unmarshal(jb, &unionWrapper)
			if err != nil {
				return fmt.Errorf("unexpected error when unmarshalling: %w", err)
			}

			if len(unionWrapper) != 1 {
				return fmt.Errorf("expected native to be a map[string]interface{} with length one, got %d", len(unionWrapper))
			}
			for _, value := range unionWrapper {
				unwrapped, err := json.Marshal(value)
				if err != nil {
					return fmt.Errorf("unexpected error when marshalling the unwrapped native: %w", err)
				}
				jb = unwrapped
			}
		}

		m.SetBytes(jb)

		return nil
	}

	return decoder, nil
}

func zigZagEncode(n int64) uint64 {
	return uint64((n << 1) ^ (n >> 63))
}

func intToZigZagBytes(n int64) []byte {
	encoded := zigZagEncode(n)
	buf := make([]byte, binary.MaxVarintLen64)
	numBytes := binary.PutUvarint(buf, encoded)
	return buf[:numBytes]
}
