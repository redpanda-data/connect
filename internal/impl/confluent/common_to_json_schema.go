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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

// commonToJSONSchema converts a benthos common schema to a JSON Schema string.
func commonToJSONSchema(c schema.Common) (string, error) {
	m, err := commonToJSONSchemaNode(c)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("marshalling JSON Schema: %w", err)
	}
	return string(b), nil
}

func commonToJSONSchemaNode(c schema.Common) (map[string]any, error) {
	switch c.Type {
	case schema.Object:
		return commonToJSONSchemaObject(c)
	case schema.Int32, schema.Int64:
		return map[string]any{"type": "integer"}, nil
	case schema.Float32, schema.Float64:
		return map[string]any{"type": "number"}, nil
	case schema.Boolean:
		return map[string]any{"type": "boolean"}, nil
	case schema.String:
		return map[string]any{"type": "string"}, nil
	case schema.ByteArray:
		return map[string]any{"type": "string", "contentEncoding": "base64"}, nil
	case schema.Null:
		return map[string]any{"type": "null"}, nil
	case schema.Array:
		return commonToJSONSchemaArray(c)
	case schema.Map:
		return commonToJSONSchemaMap(c)
	case schema.Union:
		return commonToJSONSchemaUnion(c)
	case schema.Timestamp:
		return map[string]any{"type": "string", "format": "date-time"}, nil
	case schema.Decimal:
		if c.Logical == nil || c.Logical.Decimal == nil {
			return nil, fmt.Errorf("decimal field %q missing precision/scale", c.Name)
		}
		return map[string]any{
			"type":    "string",
			"pattern": decimalPattern(c.Logical.Decimal.Precision, c.Logical.Decimal.Scale),
		}, nil
	case schema.BigDecimal:
		return map[string]any{
			"type":    "string",
			"pattern": `^-?(0|[1-9][0-9]*)(\.[0-9]+)?$`,
		}, nil
	case schema.Any:
		return map[string]any{}, nil
	default:
		return nil, fmt.Errorf("unsupported schema type: %v", c.Type)
	}
}

func commonToJSONSchemaObject(c schema.Common) (map[string]any, error) {
	properties := make(map[string]any, len(c.Children))
	var required []string
	for _, child := range c.Children {
		childMap, err := commonToJSONSchemaNode(child)
		if err != nil {
			return nil, fmt.Errorf("property %q: %w", child.Name, err)
		}
		properties[child.Name] = childMap
		if !child.Optional {
			required = append(required, child.Name)
		}
	}
	m := map[string]any{
		"type":       "object",
		"properties": properties,
	}
	if len(required) > 0 {
		m["required"] = required
	}
	return m, nil
}

func commonToJSONSchemaArray(c schema.Common) (map[string]any, error) {
	if len(c.Children) == 0 {
		return nil, errors.New("array schema requires at least one child for items type")
	}
	items, err := commonToJSONSchemaNode(c.Children[0])
	if err != nil {
		return nil, fmt.Errorf("array items: %w", err)
	}
	return map[string]any{
		"type":  "array",
		"items": items,
	}, nil
}

func commonToJSONSchemaMap(c schema.Common) (map[string]any, error) {
	if len(c.Children) == 0 {
		return nil, errors.New("map schema requires at least one child for value type")
	}
	values, err := commonToJSONSchemaNode(c.Children[0])
	if err != nil {
		return nil, fmt.Errorf("map values: %w", err)
	}
	return map[string]any{
		"type":                 "object",
		"additionalProperties": values,
	}, nil
}

func commonToJSONSchemaUnion(c schema.Common) (map[string]any, error) {
	oneOf := make([]any, 0, len(c.Children))
	for i, child := range c.Children {
		childMap, err := commonToJSONSchemaNode(child)
		if err != nil {
			return nil, fmt.Errorf("union branch %d: %w", i, err)
		}
		oneOf = append(oneOf, childMap)
	}
	return map[string]any{"oneOf": oneOf}, nil
}

// decimalPattern returns a JSON Schema regex matching the canonical decimal
// string form for Decimal(p, s). The pattern enforces no leading zeros (except
// a single 0 before the decimal point), no scientific notation, optional
// leading minus, and exactly s fractional digits when s > 0.
func decimalPattern(precision, scale int32) string {
	m := precision - scale
	switch {
	case scale == 0:
		// Integer-only: up to precision digits.
		return fmt.Sprintf(`^-?(0|[1-9][0-9]{0,%d})$`, m-1)
	case m == 0:
		// Fractional-only (e.g. Decimal(4,4)): integer part can only be 0.
		return fmt.Sprintf(`^-?0\.[0-9]{%d}$`, scale)
	default:
		return fmt.Sprintf(`^-?(0|[1-9][0-9]{0,%d})\.[0-9]{%d}$`, m-1, scale)
	}
}
