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
	"encoding/json"
	"fmt"

	"github.com/linkedin/goavro/v2"
	franz_sr "github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

func resolveGoAvroReferences(ctx context.Context, client *sr.Client, mapping *bloblang.Executor, schema franz_sr.Schema) (string, error) {
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
	if len(schema.References) == 0 {
		return mapSchema(schema)
	}

	refsMap := map[string]string{}
	if err := client.WalkReferences(ctx, schema.References, func(ctx context.Context, name string, schema franz_sr.Schema) error {
		s, err := mapSchema(schema)
		refsMap[name] = s
		return err
	}); err != nil {
		return "", nil
	}

	root, err := mapSchema(schema)
	if err != nil {
		return "", err
	}
	schemaDry := []string{}
	if err := json.Unmarshal([]byte(root), &schemaDry); err != nil {
		return "", fmt.Errorf("failed to parse root schema as enum: %w", err)
	}

	schemaHydrated := make([]json.RawMessage, len(schemaDry))
	for i, name := range schemaDry {
		def, exists := refsMap[name]
		if !exists {
			return "", fmt.Errorf("referenced type '%v' was not found in references", name)
		}
		schemaHydrated[i] = []byte(def)
	}

	schemaHydratedBytes, err := json.Marshal(schemaHydrated)
	if err != nil {
		return "", fmt.Errorf("failed to marshal hydrated schema: %w", err)
	}

	return string(schemaHydratedBytes), nil
}

func (s *schemaRegistryEncoder) getAvroEncoder(ctx context.Context, schema franz_sr.Schema) (schemaEncoder, error) {
	schemaSpec, err := resolveGoAvroReferences(ctx, s.client, nil, schema)
	if err != nil {
		return nil, err
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

		m.SetBytes(binary)
		return nil
	}, nil
}

func (s *schemaRegistryDecoder) getGoAvroDecoder(ctx context.Context, schema franz_sr.Schema) (schemaDecoder, error) {
	schemaSpec, err := resolveGoAvroReferences(ctx, s.client, s.cfg.avro.mapping, schema)
	if err != nil {
		return nil, err
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
		m.SetBytes(jb)

		return nil
	}

	return decoder, nil
}
