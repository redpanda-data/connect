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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

type ecsAvroConfig struct {
	rawUnion bool // Whether unions are going to be serialized as raw JSON
}

// Extract common schema from avro bytes
func ecsAvroFromBytes(cfg ecsAvroConfig, specBytes []byte) (any, error) {
	var as any
	if err := json.Unmarshal(specBytes, &as); err != nil {
		return nil, err
	}

	switch t := as.(type) {
	case map[string]any:
		s, err := ecsAvroFromAnyMap(cfg, t)
		if err != nil {
			return nil, err
		}
		return s.ToAny(), nil
	case []any:
		root := schema.Common{Type: schema.Union}
		for i, e := range t {
			eObj, ok := e.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("expected element %v of root array to be an object, got %T", i, e)
			}

			cObj, err := ecsAvroFromAnyMap(cfg, eObj)
			if err != nil {
				return nil, fmt.Errorf("expected element %v: %w", i, err)
			}

			root.Children = append(root.Children, cObj)
		}
		return root.ToAny(), nil
	}
	return nil, fmt.Errorf("expected either an array or object at root of schema, got %T", as)
}

// If the union is actually just a verbose way of defining an optional field
// then we return the real type and true. E.g. if we see:
//
// `"type": [ "null", "string" ]`
//
// Then we return string and true.
func ecsAvroIsUnionJustOptional(types []any) (schema.CommonType, bool) {
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

	return ecsAvroTypeToCommon(secondTypeStr), true
}

func ecsAvroTypeToCommon(t string) schema.CommonType {
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

func ecsAvroHydrateRawUnion(cfg ecsAvroConfig, c *schema.Common, types []any) error {
	if c.Type, c.Optional = ecsAvroIsUnionJustOptional(types); c.Optional {
		return nil
	}

	c.Type = schema.Union
	for i, uObj := range types {
		switch ut := uObj.(type) {
		case string:
			c.Children = append(c.Children, schema.Common{
				Type: ecsAvroTypeToCommon(ut),
			})
		case map[string]any:
			tmpC, err := ecsAvroFromAnyMap(cfg, ut)
			if err != nil {
				return fmt.Errorf("union `%v` child '%v': %w", c.Name, i, err)
			}
			c.Children = append(c.Children, tmpC)
		}
	}
	return nil
}

func ecsAvroHydrateLameUnion(cfg ecsAvroConfig, c *schema.Common, types []any) error {
	c.Type = schema.Union
	for i, uObj := range types {
		var childT schema.Common

		switch ut := uObj.(type) {
		case string:
			childT = schema.Common{
				Name: ut,
				Type: ecsAvroTypeToCommon(ut),
			}
		case map[string]any:
			var err error
			if childT, err = ecsAvroFromAnyMap(cfg, ut); err != nil {
				return fmt.Errorf("union `%v` child '%v': %w", c.Name, i, err)
			}
		}

		if childT.Type == schema.Null {
			// Null is the only type that encodes in its raw form:
			// https://avro.apache.org/docs/1.10.2/spec.html#json_encoding
			// It's all very silly.
			childT.Name = ""
			c.Children = append(c.Children, childT)
			continue
		}

		c.Children = append(c.Children, schema.Common{
			Type:     schema.Object,
			Children: []schema.Common{childT},
		})
	}

	return nil
}

func ecsAvroFromAnyMap(cfg ecsAvroConfig, as map[string]any) (schema.Common, error) {
	var c schema.Common
	c.Name, _ = as["name"].(string)

	switch t := as["type"].(type) {
	case []any:
		if cfg.rawUnion {
			if err := ecsAvroHydrateRawUnion(cfg, &c, t); err != nil {
				return c, err
			}
		} else {
			if err := ecsAvroHydrateLameUnion(cfg, &c, t); err != nil {
				return c, err
			}
		}
	case string:
		c.Type = ecsAvroTypeToCommon(t)
	case map[string]any:
		// This is so ridiculous, I can't believe they've allowed the type field
		// to be a union of three different types SMDH.
		if typeStr, ok := t["type"].(string); ok {
			c.Type = ecsAvroTypeToCommon(typeStr)
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
				Type: ecsAvroTypeToCommon(valuesType),
			},
		}

	case schema.Array:
		itemsType, exists := as["items"].(string)
		if !exists {
			return schema.Common{}, fmt.Errorf("expected `items` field of type string, got %T", as["items"])
		}

		c.Children = []schema.Common{
			{
				Type: ecsAvroTypeToCommon(itemsType),
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

			cField, err := ecsAvroFromAnyMap(cfg, fobj)
			if err != nil {
				return schema.Common{}, fmt.Errorf("record `%v` field '%v': %w", c.Name, i, err)
			}

			c.Children = append(c.Children, cField)
		}
	}

	return c, nil
}
