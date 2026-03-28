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

	"github.com/twmb/avro"
	franz_sr "github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

func resolveAvroReferences(ctx context.Context, client *sr.Client, mapping *bloblang.Executor, schema franz_sr.Schema) (string, error) {
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
	if err := client.WalkReferences(ctx, schema.References, func(_ context.Context, name string, schema franz_sr.Schema) error {
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
		return "", fmt.Errorf("parsing root schema as enum: %w", err)
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
		return "", fmt.Errorf("marshalling hydrated schema: %w", err)
	}

	return string(schemaHydratedBytes), nil
}

func (s *schemaRegistryEncoder) getAvroEncoder(ctx context.Context, schemaRef franz_sr.Schema) (schemaEncoder, error) {
	schemaSpec, err := resolveAvroReferences(ctx, s.client, nil, schemaRef)
	if err != nil {
		return nil, err
	}
	return s.newAvroEncoder(schemaSpec)
}

func (*schemaRegistryEncoder) newAvroEncoder(avroJSON string) (schemaEncoder, error) {
	schema, err := avro.Parse(avroJSON)
	if err != nil {
		return nil, fmt.Errorf("parsing Avro schema: %w", err)
	}

	// Encode accepts both bare values (standard JSON) and tagged union
	// maps (Avro JSON), so both avroRawJSON modes use the same path.
	return func(m *service.Message) error {
		data, err := m.AsStructuredMut()
		if err != nil {
			return fmt.Errorf("extracting structured data: %w", err)
		}
		binary, err := schema.Encode(data)
		if err != nil {
			return err
		}
		m.SetBytes(binary)
		return nil
	}, nil
}

func (s *schemaRegistryDecoder) getAvroDecoder(ctx context.Context, aschema franz_sr.Schema) (schemaDecoder, error) {
	schemaSpec, err := resolveAvroReferences(ctx, s.client, s.cfg.avro.mapping, aschema)
	if err != nil {
		return nil, err
	}

	// Build parse options for preserve_logical_types: register custom
	// types that convert time.Duration→time.Time for time-of-day fields,
	// avro.Duration→string, and optionally Kafka Connect types.
	var parseOpts []avro.SchemaOpt
	if s.cfg.avro.preserveLogicalTypes {
		parseOpts = append(parseOpts, preserveLogicalTypeOpts()...)
		if s.cfg.avro.translateKafkaConnectTypes {
			parseOpts = append(parseOpts, kafkaConnectTypeOpt())
		}
	}

	schema, err := avro.Parse(schemaSpec, parseOpts...)
	if err != nil {
		return nil, err
	}

	var commonSchema any
	if s.cfg.avro.storeSchemaMeta != "" {
		if commonSchema, err = ecsAvroFromBytes(ecsAvroConfig{
			rawUnion: s.cfg.avro.rawUnions,
		}, []byte(schemaSpec)); err != nil {
			s.logger.With("error", err).Error("Failed to extract common schema for meta storage")
		}
	}

	// Build decode options for union wrapping. Only needed for
	// preserve_logical_types with non-raw unions (SetStructuredMut path).
	// The EncodeJSON path handles its own union wrapping.
	var decodeOpts []avro.Opt
	if s.cfg.avro.preserveLogicalTypes && !s.cfg.avro.rawUnions {
		decodeOpts = append(decodeOpts, avro.TaggedUnions(), avro.TagLogicalTypes())
	}

	decoder := func(m *service.Message) error {
		b, err := m.AsBytes()
		if err != nil {
			return err
		}

		var native any
		if _, err := schema.Decode(b, &native, decodeOpts...); err != nil {
			return err
		}

		if s.cfg.avro.preserveLogicalTypes {
			m.SetStructuredMut(native)
		} else {
			var jb []byte
			if s.cfg.avro.rawUnions {
				jb, err = schema.EncodeJSON(native, avro.LinkedinFloats())
			} else {
				jb, err = schema.EncodeJSON(native, avro.TaggedUnions(), avro.TagLogicalTypes(), avro.LinkedinFloats())
			}
			if err != nil {
				return err
			}
			m.SetBytes(jb)
		}

		if commonSchema != nil {
			m.MetaSetImmut(s.cfg.avro.storeSchemaMeta, service.ImmutableAny{V: commonSchema})
		}
		return nil
	}

	return decoder, nil
}
