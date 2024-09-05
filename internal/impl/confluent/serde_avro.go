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

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

func resolveAvroReferences(ctx context.Context, client *sr.Client, info sr.SchemaInfo) (string, error) {
	if len(info.References) == 0 {
		return info.Schema, nil
	}

	refsMap := map[string]string{}
	if err := client.WalkReferences(ctx, info.References, func(ctx context.Context, name string, info sr.SchemaInfo) error {
		refsMap[name] = info.Schema
		return nil
	}); err != nil {
		return "", nil
	}

	schemaDry := []string{}
	if err := json.Unmarshal([]byte(info.Schema), &schemaDry); err != nil {
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

func (s *schemaRegistryEncoder) getAvroEncoder(ctx context.Context, info sr.SchemaInfo) (schemaEncoder, error) {
	schema, err := resolveAvroReferences(ctx, s.client, info)
	if err != nil {
		return nil, err
	}

	var codec *goavro.Codec
	if s.avroRawJSON {
		if codec, err = goavro.NewCodecForStandardJSONFull(schema); err != nil {
			return nil, err
		}
	} else {
		if codec, err = goavro.NewCodec(schema); err != nil {
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

func (s *schemaRegistryDecoder) getAvroDecoder(ctx context.Context, info sr.SchemaInfo) (schemaDecoder, error) {
	schema, err := resolveAvroReferences(ctx, s.client, info)
	if err != nil {
		return nil, err
	}

	var codec *goavro.Codec
	if s.avroRawJSON {
		codec, err = goavro.NewCodecForStandardJSONFull(schema)
	} else {
		codec, err = goavro.NewCodec(schema)
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
