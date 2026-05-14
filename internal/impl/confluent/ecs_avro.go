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
	"math/big"
	"strings"

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

	// preserveLogicalTypes controls whether Avro logical-type annotations
	// (timestamp-{millis,micros,nanos}, local-timestamp-*, date,
	// time-{millis,micros}, uuid) are promoted to their semantic
	// schema.Common type in the parsed metadata. When false the metadata
	// keeps the base primitive (Int64/Int32/String) — matching the value
	// side under the same flag, where twmb/avro returns raw long/int/
	// string for these types unless preserve_logical_types asks for the
	// richer Go time/uuid values.
	//
	// The decimal logical type is intentionally excluded from this gate:
	// it pre-dates the preserve_logical_types contract and is load-bearing
	// for the value-side normaliseAvroDecimals path, which keys off
	// schema.Common.Type == Decimal. Treating decimal as always-on keeps
	// existing pipelines that rely on schema metadata for decimal values
	// (without enabling preserve_logical_types) working unchanged.
	preserveLogicalTypes bool

	// translateKafkaConnectTypes mirrors the schema_registry_decode
	// processor's flag of the same name. When true, the metadata parser
	// also recognises Kafka Connect / Debezium `connect.name`
	// annotations and maps them to their corresponding schema.Common
	// types (Date, Timestamp(Unit), etc.), keeping the metadata side in
	// lock-step with the value side (kafkaConnectTypeOpt in
	// avro_walker.go). Without this, a Debezium-sourced pipeline using
	// `translate_kafka_connect_types: true` produces time.Time values
	// but the metadata still claims Int64 — the exact mismatch fixed
	// for sibling-form Avro by [applyAvroLogicalType].
	translateKafkaConnectTypes bool
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

// applyAvroLogicalType reads the optional "logicalType" annotation from an
// Avro schema node and refines c (whose Type was already set from the base
// primitive) with the matching schema.Common type and Logical parameters.
//
// Returns (true, nil) when a logical type was recognised and fully populated
// c; the caller should treat c as final and skip the container-handling
// switch in [ecsAvroFromAnyMap]. Returns (false, nil) when no logicalType is
// present or it isn't recognised — per the Avro 1.10 spec, readers must
// silently fall back to the base type for unknown logical types. Returns an
// error only for malformed annotations on a known logical type (e.g. a
// decimal missing precision, a primitive mismatch we want to flag).
func applyAvroLogicalType(cfg ecsAvroConfig, c *schema.Common, as map[string]any) (bool, error) {
	logical, ok := as["logicalType"].(string)
	if !ok {
		return false, nil
	}

	// Gate non-decimal logical types on preserve_logical_types so that the
	// schema metadata stays coherent with the value side. With
	// preserve_logical_types=false the value side emits raw long/int/string
	// for timestamps/dates/times/uuids; surfacing TIMESTAMP/DATE/TIME_OF_DAY/
	// UUID in metadata under that flag would re-introduce the metadata-vs-
	// value divergence #4399 was meant to close. Decimal is exempt — see
	// the field comment on ecsAvroConfig.preserveLogicalTypes.
	if !cfg.preserveLogicalTypes && logical != "decimal" {
		return false, nil
	}

	switch logical {
	case "decimal":
		// Per the Avro spec, decimal sits on top of bytes or fixed only.
		// Other base primitives are malformed — fall back to the base
		// type and ignore the annotation, matching how we treat every
		// other primitive/logical-type mismatch below. Note that
		// ecsAvroTypeToCommon maps `fixed` to schema.Any (it's not in
		// the named primitive set), so we allow Any through too.
		if c.Type != schema.ByteArray && c.Type != schema.Any {
			return false, nil
		}
		p, err := avroSpecInt32(as["precision"])
		if err != nil {
			return false, fmt.Errorf("decimal precision: %w", err)
		}
		// Per the Avro spec scale is optional and defaults to 0 when absent;
		// only an unparseable scale value (wrong type, non-integer) is an error.
		var s int32
		if _, present := as["scale"]; present {
			s, err = avroSpecInt32(as["scale"])
			if err != nil {
				return false, fmt.Errorf("decimal scale: %w", err)
			}
		}
		c.Type = schema.Decimal
		c.Logical = &schema.LogicalParams{
			Decimal: &schema.DecimalParams{Precision: p, Scale: s},
		}
		if err := c.Validate(); err != nil {
			return false, fmt.Errorf("decimal field: %w", err)
		}
		return true, nil

	case "uuid":
		// Avro UUID logical type sits on top of `string`. If the base
		// primitive doesn't match, treat as unknown and fall back per spec.
		if c.Type != schema.String {
			return false, nil
		}
		c.Type = schema.UUID
		return true, nil

	case "date":
		if c.Type != schema.Int32 {
			return false, nil
		}
		c.Type = schema.Date
		return true, nil

	case "time-millis":
		if c.Type != schema.Int32 {
			return false, nil
		}
		c.Type = schema.TimeOfDay
		c.Logical = &schema.LogicalParams{
			TimeOfDay: &schema.TimeOfDayParams{Unit: schema.TimeUnitMillis},
		}
		return true, nil

	case "time-micros":
		if c.Type != schema.Int64 {
			return false, nil
		}
		c.Type = schema.TimeOfDay
		c.Logical = &schema.LogicalParams{
			TimeOfDay: &schema.TimeOfDayParams{Unit: schema.TimeUnitMicros},
		}
		return true, nil

	case "duration":
		// Avro `duration` logical type sits on top of `fixed` (12 bytes).
		// Our value side (preserveLogicalTypeOpts in avro_walker.go)
		// decodes it to an ISO 8601 duration string. schema.Common has
		// no dedicated Duration type, so we map metadata to schema.String
		// — matching what the value side produces and giving iceberg a
		// VARCHAR column the operator can query as text.
		if c.Type != schema.ByteArray && c.Type != schema.Any {
			return false, nil
		}
		c.Type = schema.String
		return true, nil

	case "timestamp-millis", "timestamp-micros", "timestamp-nanos",
		"local-timestamp-millis", "local-timestamp-micros", "local-timestamp-nanos":
		// All six timestamp logical types use long as their base primitive.
		if c.Type != schema.Int64 {
			return false, nil
		}
		unit := avroLogicalTimestampUnit(logical)
		adjust := !strings.HasPrefix(logical, "local-")
		c.Type = schema.Timestamp
		c.Logical = &schema.LogicalParams{
			Timestamp: &schema.TimestampParams{Unit: unit, AdjustToUTC: adjust},
		}
		return true, nil

	default:
		// Unknown logicalType. Per Avro 1.10 spec readers must ignore.
		return false, nil
	}
}

// avroLogicalTimestampUnit maps the suffix of an Avro timestamp logical-type
// name to the corresponding [schema.TimeUnit]. Both `timestamp-*` and
// `local-timestamp-*` variants share the suffix → unit mapping.
//
// The caller in [applyAvroLogicalType] has already filtered to one of the
// six supported timestamp logical-type names, so the default branch is
// unreachable. We panic rather than return an invalid TimeUnit(0)
// sentinel that would propagate a malformed Logical past Validate().
func avroLogicalTimestampUnit(logical string) schema.TimeUnit {
	switch {
	case strings.HasSuffix(logical, "-millis"):
		return schema.TimeUnitMillis
	case strings.HasSuffix(logical, "-micros"):
		return schema.TimeUnitMicros
	case strings.HasSuffix(logical, "-nanos"):
		return schema.TimeUnitNanos
	default:
		panic(fmt.Sprintf("unreachable: avroLogicalTimestampUnit called with non-timestamp logical type %q", logical))
	}
}

// applyKafkaConnectType reads the optional "connect.name" annotation
// from an Avro schema node (the Kafka Connect / Debezium convention for
// expressing extended semantic types alongside the Avro base primitive)
// and refines c with the matching schema.Common type and Logical params.
//
// This mirrors [kafkaConnectTypeOpt] in avro_walker.go which handles the
// same annotations on the value side: with translate_kafka_connect_types
// enabled the value becomes a time.Time for the temporal entries. Without
// the matching metadata-side mapping the @schema would still claim the
// raw base primitive (Int64/Int32/String) and downstream sinks (notably
// iceberg) would create columns of the wrong type — the same value-vs-
// metadata mismatch [applyAvroLogicalType] fixes for sibling-form
// logicalType. So we gate this on translateKafkaConnectTypes and apply
// it from the same call sites.
//
// Year is mapped to Date rather than a dedicated type: the value side
// returns time.Time at January 1 of the year, which round-trips through
// an iceberg DATE column as days-since-epoch without losing the year
// component (and DATE columns are widely supported by query engines).
//
// Time and Timestamp both map to schema.Timestamp despite Time being
// semantically a time-of-day. The value side returns time.Time for
// both (kafkaConnectTypeOpt's case "io.debezium.time.Timestamp",
// "io.debezium.time.Time"), so the metadata side matches that shape to
// keep the value/metadata contract symmetrical. Operators who need a
// distinct TIME column for time-of-day fields can override via
// new_column_type_mapping.
//
// Returns (false, nil) when the annotation is absent, when it isn't
// recognised (per the Avro convention of silently ignoring unknown
// properties), or when translateKafkaConnectTypes is disabled.
func applyKafkaConnectType(cfg ecsAvroConfig, c *schema.Common, as map[string]any) (bool, error) {
	if !cfg.translateKafkaConnectTypes {
		return false, nil
	}
	name, ok := as["connect.name"].(string)
	if !ok || name == "" {
		return false, nil
	}

	switch name {
	case "io.debezium.time.Date":
		// Wire is int32 days-since-epoch; map straight to Date.
		if c.Type != schema.Int32 {
			return false, nil
		}
		c.Type = schema.Date
		return true, nil

	case "io.debezium.time.Year":
		// Wire is int32 year. The value side returns time.Time at
		// Jan 1 of the year; mapping to Date keeps that representation
		// faithful end-to-end (days-since-epoch is well-defined for
		// any Jan 1 value).
		if c.Type != schema.Int32 {
			return false, nil
		}
		c.Type = schema.Date
		return true, nil

	case "io.debezium.time.Timestamp", "io.debezium.time.Time":
		// Both wire as int64 milliseconds. See the doc comment for
		// the rationale on conflating Time with Timestamp.
		if c.Type != schema.Int64 {
			return false, nil
		}
		c.Type = schema.Timestamp
		c.Logical = &schema.LogicalParams{
			Timestamp: &schema.TimestampParams{
				Unit: schema.TimeUnitMillis, AdjustToUTC: true,
			},
		}
		return true, nil

	case "io.debezium.time.MicroTimestamp", "io.debezium.time.MicroTime":
		if c.Type != schema.Int64 {
			return false, nil
		}
		c.Type = schema.Timestamp
		c.Logical = &schema.LogicalParams{
			Timestamp: &schema.TimestampParams{
				Unit: schema.TimeUnitMicros, AdjustToUTC: true,
			},
		}
		return true, nil

	case "io.debezium.time.NanoTimestamp", "io.debezium.time.NanoTime":
		if c.Type != schema.Int64 {
			return false, nil
		}
		c.Type = schema.Timestamp
		c.Logical = &schema.LogicalParams{
			Timestamp: &schema.TimestampParams{
				Unit: schema.TimeUnitNanos, AdjustToUTC: true,
			},
		}
		return true, nil

	case "io.debezium.time.ZonedTimestamp":
		// Wire is string (RFC3339). The value side parses it to a
		// time.Time and the AdjustToUTC=true semantics match the
		// time-zone-aware nature of the type.
		if c.Type != schema.String {
			return false, nil
		}
		c.Type = schema.Timestamp
		c.Logical = &schema.LogicalParams{
			Timestamp: &schema.TimestampParams{
				Unit: schema.TimeUnitMillis, AdjustToUTC: true,
			},
		}
		return true, nil

	default:
		// Unknown connect.name. Per Avro convention readers must
		// silently ignore unrecognised annotations.
		return false, nil
	}
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
		// The union hydrators fully populate c (including any nested
		// children and logical params), so the bottom switch must not run -
		// it would try to read fields/values/items from `as`, which is the
		// outer field object, not the resolved inner type.
		//
		// Before returning, apply any `logicalType` annotation that sits as
		// a sibling of `type` on the field-level object. This is the
		// Java/JDBC Avro idiom, where a nullable timestamp field is written
		// as `{"type": ["null", "long"], "logicalType": "timestamp-millis"}`
		// rather than nesting the annotation inside the union element. The
		// value-side decoder honours both shapes, so we must too - otherwise
		// the resulting Common reports the base primitive and consumers
		// (e.g. the iceberg output) end up with a column type that mismatches
		// the decoded values.
		if _, err := applyAvroLogicalType(cfg, &c, as); err != nil {
			return c, err
		}
		if _, err := applyKafkaConnectType(cfg, &c, as); err != nil {
			return c, err
		}
		return c, nil
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

	if applied, err := applyAvroLogicalType(cfg, &c, as); err != nil {
		return schema.Common{}, err
	} else if applied {
		return c, nil
	}

	if applied, err := applyKafkaConnectType(cfg, &c, as); err != nil {
		return schema.Common{}, err
	} else if applied {
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
		raw, present := as["fields"]
		if !present || raw == nil {
			// Some generators emit records without a fields key (or with
			// fields:null). Return an opaque record rather than failing
			// the entire schema extraction.
			return c, nil
		}
		fields, ok := raw.([]any)
		if !ok {
			return schema.Common{}, fmt.Errorf("expected `fields` field of type array, got %T", raw)
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
