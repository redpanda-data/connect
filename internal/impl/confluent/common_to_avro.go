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
	"strconv"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

// The Avro shapes available for the common schema types that have no direct
// Avro equivalent.
const (
	// avroBigDecimalAsError rejects a BigDecimal field, since Avro's decimal
	// needs a precision and scale that a BigDecimal does not carry.
	avroBigDecimalAsError = "error"
	// avroBigDecimalAsDecimal encodes a BigDecimal field as a decimal with a
	// configured precision and scale.
	avroBigDecimalAsDecimal = "decimal"
	// avroBigDecimalAsString encodes a BigDecimal field as its decimal text.
	avroBigDecimalAsString = "string"

	// avroAnyAsBytes encodes an Any field as Avro bytes.
	avroAnyAsBytes = "bytes"
	// avroAnyAsJSONString encodes an Any field as its JSON text.
	avroAnyAsJSONString = "json_string"
)

// avroCommonOpts selects how common schema types with no direct Avro
// equivalent are encoded. The zero value keeps the original behaviour: a
// BigDecimal field is rejected and an Any field is encoded as bytes.
type avroCommonOpts struct {
	bigDecimalAs        string
	bigDecimalPrecision int32
	bigDecimalScale     int32
	anyAs               string
}

// commonToAvroSchema converts a benthos common schema to an Avro JSON schema
// string. recordName is used as the name for the root record when the Common
// node itself carries no name. namespace is embedded only on the root record.
func commonToAvroSchema(c schema.Common, recordName, namespace string, opts avroCommonOpts) (string, error) {
	node, err := commonToAvroNode(c, recordName, namespace, true, opts)
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
func commonToAvroNode(c schema.Common, recordName, namespace string, isRoot bool, opts avroCommonOpts) (any, error) {
	inner, err := commonToAvroInner(c, recordName, namespace, isRoot, opts)
	if err != nil {
		return nil, err
	}
	if c.Optional {
		return []any{"null", inner}, nil
	}
	return inner, nil
}

func commonToAvroInner(c schema.Common, recordName, namespace string, isRoot bool, opts avroCommonOpts) (any, error) {
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
		if opts.anyAs == avroAnyAsJSONString {
			return "string", nil
		}
		return "bytes", nil
	case schema.Timestamp:
		// Honour Logical.Timestamp params if present; legacy nil-Logical
		// schemas fall through EffectiveTimestamp() to {Millis, UTC},
		// preserving the pre-PR encoder output exactly.
		p := c.EffectiveTimestamp()
		base := "long"
		logicalName, err := avroTimestampLogicalName(p)
		if err != nil {
			return nil, fmt.Errorf("timestamp field %q: %w", c.Name, err)
		}
		return map[string]any{
			"type":        base,
			"logicalType": logicalName,
		}, nil
	case schema.Date:
		return map[string]any{
			"type":        "int",
			"logicalType": "date",
		}, nil
	case schema.TimeOfDay:
		if c.Logical == nil || c.Logical.TimeOfDay == nil {
			return nil, fmt.Errorf("time-of-day field %q missing Logical.TimeOfDay parameters", c.Name)
		}
		// Avro time-{millis,micros} carry no zone semantics, so a
		// TimeOfDay{AdjustToUTC=true} cannot be expressed faithfully.
		// Reject loudly rather than silently dropping that bit.
		if c.Logical.TimeOfDay.AdjustToUTC {
			return nil, fmt.Errorf("time-of-day field %q has AdjustToUTC=true; Avro time-millis/time-micros have no UTC-adjust variant — coerce upstream before encoding", c.Name)
		}
		// Avro defines only time-millis (int) and time-micros (long); reject
		// anything else loudly rather than silently downcasting.
		switch c.Logical.TimeOfDay.Unit {
		case schema.TimeUnitMillis:
			return map[string]any{"type": "int", "logicalType": "time-millis"}, nil
		case schema.TimeUnitMicros:
			return map[string]any{"type": "long", "logicalType": "time-micros"}, nil
		default:
			return nil, fmt.Errorf("time-of-day field %q has unit %v which Avro cannot express; only MILLIS and MICROS are supported", c.Name, c.Logical.TimeOfDay.Unit)
		}
	case schema.UUID:
		return map[string]any{
			"type":        "string",
			"logicalType": "uuid",
		}, nil
	case schema.Decimal:
		if c.Logical == nil || c.Logical.Decimal == nil {
			return nil, fmt.Errorf("decimal field %q missing precision/scale", c.Name)
		}
		return map[string]any{
			"type":        "bytes",
			"logicalType": "decimal",
			"precision":   c.Logical.Decimal.Precision,
			"scale":       c.Logical.Decimal.Scale,
		}, nil
	case schema.BigDecimal:
		switch opts.bigDecimalAs {
		case avroBigDecimalAsDecimal:
			return map[string]any{
				"type":        "bytes",
				"logicalType": "decimal",
				"precision":   opts.bigDecimalPrecision,
				"scale":       opts.bigDecimalScale,
			}, nil
		case avroBigDecimalAsString:
			return "string", nil
		}
		return nil, fmt.Errorf("field %q is BigDecimal which has no fixed precision/scale; set %v.%v, or cast or coerce upstream before schema_registry_encode (avro)", c.Name, sreFieldAvro, sreFieldAvroBigDecimalAs)
	case schema.Array:
		return commonToAvroArray(c, opts)
	case schema.Map:
		return commonToAvroMap(c, opts)
	case schema.Union:
		return commonToAvroUnion(c, opts)
	case schema.Object:
		return commonToAvroRecord(c, recordName, namespace, isRoot, opts)
	default:
		return nil, fmt.Errorf("unsupported schema type: %v", c.Type)
	}
}

func commonToAvroRecord(c schema.Common, recordName, namespace string, isRoot bool, opts avroCommonOpts) (any, error) {
	name := c.Name
	if name == "" {
		name = recordName
	}
	if !validAvroFullName(name) {
		name = sanitizeAvroName(name)
	}
	fields := make([]any, 0, len(c.Children))
	for _, child := range c.Children {
		childNode, err := commonToAvroNode(child, child.Name, "", false, opts)
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

func commonToAvroArray(c schema.Common, opts avroCommonOpts) (any, error) {
	if len(c.Children) == 0 {
		return nil, errors.New("array schema has no items child")
	}
	items, err := commonToAvroNode(c.Children[0], "", "", false, opts)
	if err != nil {
		return nil, fmt.Errorf("array items: %w", err)
	}
	return map[string]any{
		"type":  "array",
		"items": items,
	}, nil
}

func commonToAvroMap(c schema.Common, opts avroCommonOpts) (any, error) {
	if len(c.Children) == 0 {
		return nil, errors.New("map schema has no values child")
	}
	values, err := commonToAvroNode(c.Children[0], "", "", false, opts)
	if err != nil {
		return nil, fmt.Errorf("map values: %w", err)
	}
	return map[string]any{
		"type":   "map",
		"values": values,
	}, nil
}

func commonToAvroUnion(c schema.Common, opts avroCommonOpts) (any, error) {
	variants := make([]any, 0, len(c.Children))
	for i, child := range c.Children {
		v, err := commonToAvroNode(child, "", "", false, opts)
		if err != nil {
			return nil, fmt.Errorf("union variant %d: %w", i, err)
		}
		variants = append(variants, v)
	}
	return variants, nil
}

// avroValueCoercer converts a message value to the shape its Avro field
// expects, for the fields whose Avro type differs from what the message holds.
type avroValueCoercer func(v any) (any, error)

// commonToAvroValueCoercer returns the coercer for the values of c under opts,
// or nil when no value needs converting, so that encoding costs nothing extra
// for the schemas, typically most, that need none.
//
// Only the shapes that change the value's type need one: a BigDecimal encoded
// as a string, whose value may arrive as a number, and an Any encoded as JSON
// text, whose value is usually an object or an array. A BigDecimal encoded as
// a decimal needs none, as the encoder already reads decimal values from
// numbers and decimal text.
func commonToAvroValueCoercer(c schema.Common, opts avroCommonOpts) avroValueCoercer {
	switch c.Type {
	case schema.BigDecimal:
		if opts.bigDecimalAs == avroBigDecimalAsString {
			return coerceBigDecimalToString
		}
	case schema.Any:
		if opts.anyAs == avroAnyAsJSONString {
			return coerceAnyToJSONString
		}
	case schema.Object:
		fields := map[string]avroValueCoercer{}
		for _, child := range c.Children {
			if f := commonToAvroValueCoercer(child, opts); f != nil {
				fields[child.Name] = f
			}
		}
		if len(fields) == 0 {
			return nil
		}
		return func(v any) (any, error) {
			obj, ok := v.(map[string]any)
			if !ok {
				// Not an object: leave it for the encoder to report.
				return v, nil
			}
			for name, f := range fields {
				fv, exists := obj[name]
				if !exists {
					continue
				}
				out, err := f(fv)
				if err != nil {
					return nil, fmt.Errorf("field %q: %w", name, err)
				}
				obj[name] = out
			}
			return obj, nil
		}
	case schema.Array:
		if len(c.Children) == 0 {
			return nil
		}
		items := commonToAvroValueCoercer(c.Children[0], opts)
		if items == nil {
			return nil
		}
		return func(v any) (any, error) {
			arr, ok := v.([]any)
			if !ok {
				return v, nil
			}
			for i, e := range arr {
				out, err := items(e)
				if err != nil {
					return nil, fmt.Errorf("array item %d: %w", i, err)
				}
				arr[i] = out
			}
			return arr, nil
		}
	case schema.Map:
		if len(c.Children) == 0 {
			return nil
		}
		values := commonToAvroValueCoercer(c.Children[0], opts)
		if values == nil {
			return nil
		}
		return func(v any) (any, error) {
			m, ok := v.(map[string]any)
			if !ok {
				return v, nil
			}
			for k, e := range m {
				out, err := values(e)
				if err != nil {
					return nil, fmt.Errorf("map value %q: %w", k, err)
				}
				m[k] = out
			}
			return m, nil
		}
	}
	// Unions are left alone: which variant a value belongs to is the
	// encoder's call, not this one's.
	return nil
}

// coerceBigDecimalToString returns a BigDecimal value as its decimal text.
// Floats use the shortest representation that reads back as the same float,
// as a float carries no more precision than that.
func coerceBigDecimalToString(v any) (any, error) {
	switch x := v.(type) {
	case nil, string:
		return v, nil
	case json.Number:
		return x.String(), nil
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32), nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprint(x), nil
	}
	return nil, fmt.Errorf("cannot encode a %T as big decimal text", v)
}

// coerceAnyToJSONString returns an Any value as JSON text. A string is kept
// as it is rather than quoted, since sources that type a column Any, such as
// postgres_cdc for types it does not map, already deliver those values as
// text.
func coerceAnyToJSONString(v any) (any, error) {
	switch v.(type) {
	case nil, string:
		return v, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("serialising value as JSON: %w", err)
	}
	return string(b), nil
}

// avroTimestampLogicalName picks the Avro logical-type name for a
// TimestampParams value. The mapping mirrors the reverse path in
// applyAvroLogicalType: AdjustToUTC=true picks `timestamp-*`, false picks
// `local-timestamp-*`. Avro 1.10+ supports nanos; older brokers may reject it.
func avroTimestampLogicalName(p schema.TimestampParams) (string, error) {
	var unit string
	switch p.Unit {
	case schema.TimeUnitMillis:
		unit = "millis"
	case schema.TimeUnitMicros:
		unit = "micros"
	case schema.TimeUnitNanos:
		unit = "nanos"
	default:
		return "", fmt.Errorf("unsupported timestamp unit %v (Avro supports only MILLIS, MICROS, NANOS)", p.Unit)
	}
	if p.AdjustToUTC {
		return "timestamp-" + unit, nil
	}
	return "local-timestamp-" + unit, nil
}

// validAvroFullName reports whether name is a valid Avro full name: one or
// more dot-separated names, each matching [A-Za-z_][A-Za-z0-9_]*.
func validAvroFullName(name string) bool {
	for part := range strings.SplitSeq(name, ".") {
		if part == "" || sanitizeAvroName(part) != part {
			return false
		}
	}
	return true
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
