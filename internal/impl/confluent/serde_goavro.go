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
	"errors"
	"fmt"

	"github.com/linkedin/goavro/v2"
	franz_sr "github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/schema"
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

func tryExtractCommonSchema(specBytes []byte) (any, error) {
	var as any
	if err := json.Unmarshal(specBytes, &as); err != nil {
		return nil, err
	}

	commonSchema, err := avroAnyToCommonSchema(as)
	if err != nil {
		return nil, err
	}

	return commonSchema.ToAny(), nil
}

func (s *schemaRegistryDecoder) getGoAvroDecoder(ctx context.Context, aschema franz_sr.Schema) (schemaDecoder, error) {
	schemaSpec, err := resolveGoAvroReferences(ctx, s.client, s.cfg.avro.mapping, aschema)
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

	var commonSchemaAny any
	if s.cfg.avro.storeSchemaMeta != "" {
		if commonSchemaAny, err = tryExtractCommonSchema([]byte(schemaSpec)); err != nil {
			s.logger.With("error", err).Error("Failed to extract common schema for meta storage")
		}
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

		if commonSchemaAny != nil {
			m.MetaSetMut(s.cfg.avro.storeSchemaMeta, commonSchemaAny)
		}
		return nil
	}

	return decoder, nil
}

// If the union is actually just a verbose way of defining an optional field
// then we return the real type and true. E.g. if we see:
//
// `"type": [ "null", "string" ]`
//
// Then we return string and true.
func isUnionJustOptional(types []any) (schema.CommonType, bool) {
	if len(types) != 2 {
		return schema.CommonType(-1), false
	}

	firstTypeStr, ok := types[0].(string)
	if !ok || firstTypeStr != "null" {
		return schema.CommonType(-1), false
	}

	secondTypeStr, ok := types[1].(string)
	if !ok {
		return schema.CommonType(-1), false
	}

	return avroTypeToCommonType(secondTypeStr), true
}

func avroTypeToCommonType(t string) schema.CommonType {
	switch t {
	case "record":
		return schema.Object
	case "null":
		return schema.Null
	case "int":
		return schema.Int32
	case "long":
		return schema.Int64
	case "float":
		return schema.Float32
	case "double":
		return schema.Float64
	case "boolean":
		return schema.Boolean
	case "bytes":
		return schema.ByteArray
	case "string":
		return schema.String
	case "enum":
		return schema.String
	case "map":
		return schema.Map
	case "array":
		return schema.Array
	}
	return schema.CommonType(-1)
}

func avroAnyToCommonSchema(aRoot any) (schema.Common, error) {
	switch t := aRoot.(type) {
	case map[string]any:
		return avroAnyMapToCommonSchema(t)
	case []any:
		root := schema.Common{Type: schema.Union}
		for i, e := range t {
			eObj, ok := e.(map[string]any)
			if !ok {
				return schema.Common{}, fmt.Errorf("expected element %v of root array to be an object, got %T", i, e)
			}

			cObj, err := avroAnyMapToCommonSchema(eObj)
			if err != nil {
				return schema.Common{}, fmt.Errorf("expected element %v: %w", i, err)
			}

			root.Children = append(root.Children, cObj)
		}
		return root, nil
	}
	return schema.Common{}, fmt.Errorf("expected either an array or object at root of schema, got %T", aRoot)
}

func avroAnyMapToCommonSchema(as map[string]any) (schema.Common, error) {
	var c schema.Common
	c.Name, _ = as["name"].(string)

	switch t := as["type"].(type) {
	case []any:
		if c.Type, c.Optional = isUnionJustOptional(t); !c.Optional {
			c.Type = schema.Union
			for i, uObj := range t {
				switch ut := uObj.(type) {
				case string:
					c.Children = append(c.Children, schema.Common{
						Type: avroTypeToCommonType(ut),
					})
				case map[string]any:
					tmpC, err := avroAnyMapToCommonSchema(ut)
					if err != nil {
						return c, fmt.Errorf("union `%v` child '%v': %w", c.Name, i, err)
					}
					c.Children = append(c.Children, tmpC)
				}
			}
		}
	case string:
		c.Type = avroTypeToCommonType(t)
	case map[string]any:
		// This is so ridiculous, I can't believe they've allowed the type field
		// to be a union of three different types SMDH.
		if typeStr, ok := t["type"].(string); ok {
			c.Type = avroTypeToCommonType(typeStr)
		} else {
			return schema.Common{}, errors.New("detected an unrecognized `type` field of type object, missing a `type` field")
		}
	default:
		return schema.Common{}, fmt.Errorf("expected `type` field of type string or array, got %T", t)
	}

	switch c.Type {
	case schema.Map:
		valuesType, exists := as["values"].(string)
		if !exists {
			return schema.Common{}, fmt.Errorf("expected `values` field of type string, got %T", as["values"])
		}

		c.Children = []schema.Common{
			{
				Type: avroTypeToCommonType(valuesType),
			},
		}

	case schema.Array:
		itemsType, exists := as["items"].(string)
		if !exists {
			return schema.Common{}, fmt.Errorf("expected `items` field of type string, got %T", as["items"])
		}

		c.Children = []schema.Common{
			{
				Type: avroTypeToCommonType(itemsType),
			},
		}

	case schema.Object:
		fields, exists := as["fields"].([]any)
		if !exists {
			return schema.Common{}, fmt.Errorf("expected `fields` field of type array, got %T", as["fields"])
		}

		for i, f := range fields {
			fobj, ok := f.(map[string]any)
			if !ok {
				return schema.Common{}, fmt.Errorf("record `%v` field '%v': expected object, got %T", c.Name, i, f)
			}

			cField, err := avroAnyMapToCommonSchema(fobj)
			if err != nil {
				return schema.Common{}, fmt.Errorf("record `%v` field '%v': %w", c.Name, i, err)
			}

			c.Children = append(c.Children, cField)
		}
	}

	return c, nil
}
