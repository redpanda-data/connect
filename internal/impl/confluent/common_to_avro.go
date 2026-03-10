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
	"strings"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

// commonToAvroSchema converts a benthos common schema to an Avro JSON schema
// string. recordName is used as the name for the root record when the Common
// node itself carries no name. namespace is embedded only on the root record.
func commonToAvroSchema(c schema.Common, recordName, namespace string) (string, error) {
	node, err := commonToAvroNode(c, recordName, namespace, true)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(node)
	if err != nil {
		return "", fmt.Errorf("marshalling Avro schema: %w", err)
	}
	return string(b), nil
}

// commonToAvroNode recursively converts a schema.Common to an Avro schema node.
// isRoot controls whether namespace is injected.
func commonToAvroNode(c schema.Common, recordName, namespace string, isRoot bool) (any, error) {
	inner, err := commonToAvroInner(c, recordName, namespace, isRoot)
	if err != nil {
		return nil, err
	}
	if c.Optional {
		return []any{"null", inner}, nil
	}
	return inner, nil
}

func commonToAvroInner(c schema.Common, recordName, namespace string, isRoot bool) (any, error) {
	switch c.Type {
	case schema.Null:
		return "null", nil
	case schema.Boolean:
		return "boolean", nil
	case schema.Int32:
		return "int", nil
	case schema.Int64:
		return "long", nil
	case schema.Float32:
		return "float", nil
	case schema.Float64:
		return "double", nil
	case schema.String:
		return "string", nil
	case schema.ByteArray:
		return "bytes", nil
	case schema.Any:
		return "bytes", nil
	case schema.Timestamp:
		return map[string]any{
			"type":        "long",
			"logicalType": "timestamp-millis",
		}, nil
	case schema.Array:
		return commonToAvroArray(c)
	case schema.Map:
		return commonToAvroMap(c)
	case schema.Union:
		return commonToAvroUnion(c)
	case schema.Object:
		return commonToAvroRecord(c, recordName, namespace, isRoot)
	default:
		return nil, fmt.Errorf("unsupported schema type: %v", c.Type)
	}
}

func commonToAvroRecord(c schema.Common, recordName, namespace string, isRoot bool) (any, error) {
	name := c.Name
	if name == "" {
		name = recordName
	}
	fields := make([]any, 0, len(c.Children))
	for _, child := range c.Children {
		childNode, err := commonToAvroNode(child, child.Name, "", false)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", child.Name, err)
		}
		field := map[string]any{
			"name": child.Name,
			"type": childNode,
		}
		if child.Optional {
			field["default"] = nil
		}
		fields = append(fields, field)
	}
	m := map[string]any{
		"type":   "record",
		"name":   name,
		"fields": fields,
	}
	if isRoot && namespace != "" {
		m["namespace"] = namespace
	}
	return m, nil
}

func commonToAvroArray(c schema.Common) (any, error) {
	if len(c.Children) == 0 {
		return nil, errors.New("array schema has no items child")
	}
	items, err := commonToAvroNode(c.Children[0], "", "", false)
	if err != nil {
		return nil, fmt.Errorf("array items: %w", err)
	}
	return map[string]any{
		"type":  "array",
		"items": items,
	}, nil
}

func commonToAvroMap(c schema.Common) (any, error) {
	if len(c.Children) == 0 {
		return nil, errors.New("map schema has no values child")
	}
	values, err := commonToAvroNode(c.Children[0], "", "", false)
	if err != nil {
		return nil, fmt.Errorf("map values: %w", err)
	}
	return map[string]any{
		"type":   "map",
		"values": values,
	}, nil
}

func commonToAvroUnion(c schema.Common) (any, error) {
	variants := make([]any, 0, len(c.Children))
	for i, child := range c.Children {
		v, err := commonToAvroNode(child, "", "", false)
		if err != nil {
			return nil, fmt.Errorf("union variant %d: %w", i, err)
		}
		variants = append(variants, v)
	}
	return variants, nil
}

// sanitizeAvroName derives a valid Avro name from an arbitrary subject string.
// Avro names must match [A-Za-z_][A-Za-z0-9_]*. Invalid characters are replaced
// with underscores and a leading digit is prefixed with an underscore.
func sanitizeAvroName(subject string) string {
	if subject == "" {
		return "_"
	}
	var b strings.Builder
	for i, r := range subject {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r == '_':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			if i == 0 {
				b.WriteRune('_')
			}
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}
