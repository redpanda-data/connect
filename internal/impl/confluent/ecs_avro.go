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
	"math/big"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

// avroSpecInt32 reads a JSON-decoded numeric value (float64 or json.Number)
// as an int32. Used for parsing Avro decimal precision and scale annotations.
func avroSpecInt32(v any) (int32, error) {
	switch x := v.(type) {
	case float64:
		return int32(x), nil
	case json.Number:
		n, err := x.Int64()
		if err != nil {
			return 0, fmt.Errorf("not an integer: %v", x)
		}
		return int32(n), nil
	case nil:
		return 0, errors.New("missing")
	default:
		return 0, fmt.Errorf("unexpected type %T", v)
	}
}

type ecsAvroConfig struct {
	rawUnion bool // Whether unions are going to be serialized as raw JSON
}

// ecsAvroParseFromBytes parses an Avro JSON spec into a schema.Common. The
// schema-registry decoder uses the parsed form directly so it can walk
// decimal field paths during value normalisation; callers that just want
// the metadata copy can call ToAny() on the result.
func ecsAvroParseFromBytes(cfg ecsAvroConfig, specBytes []byte) (schema.Common, error) {
	var as any
	if err := json.Unmarshal(specBytes, &as); err != nil {
		return schema.Common{}, err
	}

	switch t := as.(type) {
	case map[string]any:
		return ecsAvroFromAnyMap(cfg, t)
	case []any:
		root := schema.Common{Type: schema.Union}
		for i, e := range t {
			eObj, ok := e.(map[string]any)
			if !ok {
				return schema.Common{}, fmt.Errorf("expected element %v of root array to be an object, got %T", i, e)
			}

			cObj, err := ecsAvroFromAnyMap(cfg, eObj)
			if err != nil {
				return schema.Common{}, fmt.Errorf("expected element %v: %w", i, err)
			}

			root.Children = append(root.Children, cObj)
		}
		return root, nil
	}
	return schema.Common{}, fmt.Errorf("expected either an array or object at root of schema, got %T", as)
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

// ecsAvroIsUnionJustOptionalObject mirrors ecsAvroIsUnionJustOptional but
// for the [null, {object}] shape — Avro's idiom for a nullable named or
// logically-typed field. Returns the resolved Common (with any
// LogicalParams populated) and true on match.
func ecsAvroIsUnionJustOptionalObject(cfg ecsAvroConfig, types []any) (schema.Common, bool) {
	if len(types) != 2 {
		return schema.Common{}, false
	}
	firstStr, ok := types[0].(string)
	if !ok || firstStr != "null" {
		return schema.Common{}, false
	}
	secondMap, ok := types[1].(map[string]any)
	if !ok {
		return schema.Common{}, false
	}
	c, err := ecsAvroFromAnyMap(cfg, secondMap)
	if err != nil {
		return schema.Common{}, false
	}
	return c, true
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
	return schema.Any
}

func ecsAvroHydrateRawUnion(cfg ecsAvroConfig, c *schema.Common, types []any) error {
	// [null, primitive-name] → Optional <primitive>.
	if t, optional := ecsAvroIsUnionJustOptional(types); optional {
		c.Type, c.Optional = t, true
		return nil
	}
	// [null, {object}] → Optional <object>, propagating logical params and
	// nested children. This catches the common Avro idiom for nullable
	// decimal/timestamp/etc. logical types.
	if inner, ok := ecsAvroIsUnionJustOptionalObject(cfg, types); ok {
		name := c.Name
		*c = inner
		if name != "" {
			c.Name = name
		}
		c.Optional = true
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
		// The type field is an object (e.g. {"type":"map","values":"long"}).
		// The old code only read t["type"] and lost the rest (values, items,
		// fields, symbols). Recursing into the full object picks up all
		// complex type metadata. We return early because the recursion
		// handles the switch on c.Type below (which reads values/items/etc
		// from as — but as is the outer field object, not the inner type).
		var err error
		c, err = ecsAvroFromAnyMap(cfg, t)
		if err != nil {
			return schema.Common{}, err
		}
		if name, ok := as["name"].(string); ok {
			c.Name = name
		}
		return c, nil
	default:
		return schema.Common{}, fmt.Errorf("expected `type` field of type string or array, got %T", t)
	}

	if logical, ok := as["logicalType"].(string); ok && logical == "decimal" {
		p, err := avroSpecInt32(as["precision"])
		if err != nil {
			return schema.Common{}, fmt.Errorf("decimal precision: %w", err)
		}
		s, err := avroSpecInt32(as["scale"])
		if err != nil {
			return schema.Common{}, fmt.Errorf("decimal scale: %w", err)
		}
		c.Type = schema.Decimal
		c.Logical = &schema.LogicalParams{
			Decimal: &schema.DecimalParams{Precision: p, Scale: s},
		}
		if err := c.Validate(); err != nil {
			return schema.Common{}, fmt.Errorf("decimal field: %w", err)
		}
		return c, nil
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

// normaliseAvroDecimals walks a value decoded by twmb/avro alongside its
// common schema and replaces *big.Rat values at Decimal field paths with
// canonical decimal strings (the value contract for schema.Decimal). Values
// that don't match the schema (e.g. a non-Decimal field that happens to hold
// a *big.Rat) are left alone. The traversal mutates maps in place; slices
// are mutated in place and returned.
func normaliseAvroDecimals(value any, c schema.Common) any {
	if value == nil {
		return nil
	}
	switch c.Type {
	case schema.Decimal:
		if c.Logical == nil || c.Logical.Decimal == nil {
			return value
		}
		return ratToCanonicalDecimal(value, c.Logical.Decimal.Scale)
	case schema.Object:
		return normaliseAvroDecimalsObject(value, c.Children)
	case schema.Array:
		return normaliseAvroDecimalsArray(value, c.Children)
	case schema.Map:
		return normaliseAvroDecimalsMap(value, c.Children)
	case schema.Union:
		// Tagged-union shape: {"<tag>": innerValue}. Unwrap and try each
		// variant child against the inner value; commit the first one
		// that actually converts.
		if m, ok := value.(map[string]any); ok && len(m) == 1 {
			for k, inner := range m {
				for _, child := range c.Children {
					if next := normaliseAvroDecimals(inner, child); didConvertDecimal(inner, next) {
						m[k] = next
						return m
					}
				}
			}
			return value
		}
		// Untagged: try each variant on the value directly.
		for _, child := range c.Children {
			if next := normaliseAvroDecimals(value, child); didConvertDecimal(value, next) {
				return next
			}
		}
		return value
	default:
		return value
	}
}

func normaliseAvroDecimalsObject(value any, children []schema.Common) any {
	m, ok := value.(map[string]any)
	if !ok {
		return value
	}
	for _, child := range children {
		v, exists := m[child.Name]
		if !exists {
			continue
		}
		m[child.Name] = normaliseAvroDecimals(v, child)
	}
	return m
}

func normaliseAvroDecimalsArray(value any, children []schema.Common) any {
	arr, ok := value.([]any)
	if !ok || len(children) == 0 {
		return value
	}
	for i, v := range arr {
		arr[i] = normaliseAvroDecimals(v, children[0])
	}
	return arr
}

func normaliseAvroDecimalsMap(value any, children []schema.Common) any {
	m, ok := value.(map[string]any)
	if !ok || len(children) == 0 {
		return value
	}
	for k, v := range m {
		m[k] = normaliseAvroDecimals(v, children[0])
	}
	return m
}

// didConvertDecimal reports whether normaliseAvroDecimals actually replaced
// a *big.Rat with a canonical decimal string. Used by the Union dispatch to
// pick the matching variant — anything else, including in-place struct
// mutation, returns false.
func didConvertDecimal(before, after any) bool {
	_, beforeRat := before.(*big.Rat)
	_, afterStr := after.(string)
	return beforeRat && afterStr
}

// ratToCanonicalDecimal converts a *big.Rat (the form twmb/avro produces for
// decimal logical types) to the canonical decimal string at the schema's
// declared scale. Inputs that are already strings or non-Rat values pass
// through unchanged.
func ratToCanonicalDecimal(value any, scale int32) any {
	r, ok := value.(*big.Rat)
	if !ok {
		return value
	}
	denomFactor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	num := new(big.Int).Mul(r.Num(), denomFactor)
	unscaled, rem := new(big.Int).QuoRem(num, r.Denom(), new(big.Int))
	if rem.Sign() != 0 {
		// Value can't be represented exactly at the declared scale;
		// leave it alone so the caller can decide what to do.
		return value
	}
	s, err := schema.FormatDecimal(unscaled, scale)
	if err != nil {
		return value
	}
	return s
}
