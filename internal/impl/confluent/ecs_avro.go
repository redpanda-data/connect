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

	// names is the lexical-scope registry of resolved record/enum/fixed
	// definitions, keyed by Avro fullname plus an unambiguous short-name
	// shortcut for each declaration. Stored values must be treated as
	// immutable — every retrieval clones via cloneCommon so callers can
	// mutate freely without corrupting later look-ups.
	names map[string]schema.Common

	// nameOwners tracks the fullname that owns each key in [names]. A
	// fullname-key is its own owner ("Fee" → "Fee" for a root-scope Fee,
	// "com.a.Fee" → "com.a.Fee" for a namespaced Fee). A short-name key
	// inherits its owning declaration's fullname ("Fee" → "com.a.Fee").
	// When a second declaration tries to claim a short-name key that a
	// different fullname already owns the entry is marked ambiguous —
	// owner becomes "" and the [names] entry is deleted — so unqualified
	// references fall through to schema.Any instead of silently binding
	// to whichever fullname registered last. A canonical fullname binding
	// (key equals owner) always wins over a colliding short-name claim.
	nameOwners map[string]string

	// namespace is the enclosing Avro namespace, threaded through the
	// recursion by value. It is updated when entering a named-type
	// declaration that introduces a new namespace, per the Avro spec's
	// inheritance rule (a name with no dots and no `namespace` field
	// inherits the most tightly enclosing namespace).
	namespace string
}

// ecsAvroParseFromBytes parses an Avro JSON spec into a schema.Common. The
// schema-registry decoder uses the parsed form directly so it can walk
// decimal field paths during value normalisation; callers that just want
// the metadata copy can call ToAny() on the result.
func ecsAvroParseFromBytes(cfg ecsAvroConfig, specBytes []byte) (schema.Common, error) {
	if cfg.names == nil {
		cfg.names = map[string]schema.Common{}
	}
	if cfg.nameOwners == nil {
		cfg.nameOwners = map[string]string{}
	}
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

// ecsAvroResolveOptionalUnion checks whether a 2-element union is just a
// nullable wrapper (one branch is "null") and returns the resolved non-null
// branch as a Common with Optional=true.
//
// Handles both orderings ([null, X] and [X, null]) and resolves the non-null
// branch in three forms:
//
//   - a primitive type name string (e.g. "string"),
//   - a previously-defined named-type reference string (e.g. "Fee"),
//     resolved via cfg.names per the Avro lexical-scope rule,
//   - an inline type definition object (e.g. {"type":"record",...}).
//
// The matched bool reports whether the union has the [null, X] / [X, null]
// shape; the error is non-nil only when the shape matched but resolving the
// non-null branch failed (e.g. a malformed inline decimal). Callers must
// surface the error rather than falling through to the general-union path —
// the fall-through would also fail, with a less informative message.
func ecsAvroResolveOptionalUnion(cfg ecsAvroConfig, types []any) (resolved schema.Common, matched bool, err error) {
	if len(types) != 2 {
		return schema.Common{}, false, nil
	}
	var other any
	for _, t := range types {
		if s, ok := t.(string); ok && s == "null" {
			continue
		}
		if other != nil {
			// Two non-null branches — not a nullable wrapper.
			return schema.Common{}, false, nil
		}
		other = t
	}
	if other == nil {
		return schema.Common{}, false, nil
	}
	inner, err := ecsAvroResolveTypeRef(cfg, other)
	if err != nil {
		return schema.Common{}, true, err
	}
	inner.Optional = true
	return inner, true, nil
}

// ecsAvroResolveTypeRef resolves a single Avro type reference — the value
// of a "type" field, or one branch of a union — to a Common. The reference
// may be a primitive type name string, a previously-defined named-type
// reference string (resolved via cfg.names), or an inline type definition
// object.
//
// Unknown string names fall back to schema.Any so downstream sinks see a
// sensible (if structureless) column. An error is returned only when an
// inline object fails to parse (the wrapped cause flows back to the caller)
// or when ref is neither a string nor a map (a malformed Avro JSON shape
// the upstream parser couldn't reject).
func ecsAvroResolveTypeRef(cfg ecsAvroConfig, ref any) (schema.Common, error) {
	switch b := ref.(type) {
	case string:
		// Try the names map first so a name reference takes priority over
		// the schema.Any fallback in ecsAvroTypeToCommon. Primitive names
		// are never registered in the map, so primitives reach the
		// fallback unchanged.
		if resolved, ok := ecsAvroLookupName(cfg, b); ok {
			return resolved, nil
		}
		return schema.Common{Type: ecsAvroTypeToCommon(b)}, nil
	case map[string]any:
		return ecsAvroFromAnyMap(cfg, b)
	}
	return schema.Common{}, fmt.Errorf("expected type reference to be a string or object, got %T", ref)
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
		//
		// Caveat: promotion is one-way. After this branch fires the
		// resulting schema.Common is just `Decimal(P,S)` — the fact that
		// the Avro source was `fixed[N]` rather than `bytes` is gone.
		// Every downstream consumer of schema.Common (iceberg, parquet,
		// JSON Schema, value-side normaliseAvroDecimals) treats decimal
		// purely by precision/scale and picks its own physical encoding,
		// so this is lossless for them. The single leak is
		// common_to_avro.go's re-emit path, which will always produce a
		// `bytes`-backed decimal even when the source schema was
		// fixed-backed — semantically equivalent but a different Avro
		// schema shape, which can matter for Schema Registry
		// compatibility checks that compare by equality.
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

// ecsAvroLookupName resolves a string reference to a previously-registered
// named type, applying Avro's name-resolution rules: a reference that
// contains a dot is treated as a fullname; an unqualified reference is
// looked up first against the enclosing namespace, then against the bare
// name as a fallback for root-scope references.
//
// The returned Common is cloned so callers can mutate it freely without
// corrupting the registered entry.
func ecsAvroLookupName(cfg ecsAvroConfig, ref string) (schema.Common, bool) {
	if !strings.ContainsRune(ref, '.') && cfg.namespace != "" {
		if resolved, ok := cfg.names[cfg.namespace+"."+ref]; ok {
			return cloneCommon(resolved), true
		}
	}
	if resolved, ok := cfg.names[ref]; ok {
		return cloneCommon(resolved), true
	}
	return schema.Common{}, false
}

// cloneCommon deep-copies a schema.Common, allocating fresh slice and
// pointer storage for Children and Logical so the result aliases nothing
// with the source.
func cloneCommon(c schema.Common) schema.Common {
	if c.Children != nil {
		children := make([]schema.Common, len(c.Children))
		for i := range c.Children {
			children[i] = cloneCommon(c.Children[i])
		}
		c.Children = children
	}
	if c.Logical != nil {
		l := *c.Logical
		if l.Decimal != nil {
			d := *l.Decimal
			l.Decimal = &d
		}
		if l.Timestamp != nil {
			ts := *l.Timestamp
			l.Timestamp = &ts
		}
		if l.TimeOfDay != nil {
			tod := *l.TimeOfDay
			l.TimeOfDay = &tod
		}
		c.Logical = &l
	}
	return c
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
	// [null, X] or [X, null] → Optional X. ecsAvroResolveOptionalUnion
	// handles primitive names, named-type references, and inline objects
	// in either ordering.
	if inner, matched, err := ecsAvroResolveOptionalUnion(cfg, types); matched {
		if err != nil {
			return fmt.Errorf("union `%v`: %w", c.Name, err)
		}
		name := c.Name
		*c = inner
		if name != "" {
			c.Name = name
		}
		return nil
	}

	c.Type = schema.Union
	for i, uObj := range types {
		child, err := ecsAvroResolveTypeRef(cfg, uObj)
		if err != nil {
			return fmt.Errorf("union `%v` child '%v': %w", c.Name, i, err)
		}
		c.Children = append(c.Children, child)
	}
	return nil
}

func ecsAvroHydrateLameUnion(cfg ecsAvroConfig, c *schema.Common, types []any) error {
	c.Type = schema.Union
	for i, uObj := range types {
		childT, err := ecsAvroResolveTypeRef(cfg, uObj)
		if err != nil {
			return fmt.Errorf("union `%v` child '%v': %w", c.Name, i, err)
		}
		if s, isStr := uObj.(string); isStr {
			// Lame-union children keep the type-name as the Common.Name so
			// the tagged-JSON envelope key matches the Avro wire form.
			childT.Name = s
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
	// Pre-register a structural placeholder before walking children so a
	// self-reference (e.g. a linked-list record with a `next` field of its
	// own type) resolves to a one-level stub rather than collapsing to
	// schema.Any. The placeholder is overwritten with the fully-resolved
	// Common once the walk completes. Mutual recursion across distinct
	// records is still not supported — the second record's placeholder
	// does not exist while the first is being walked.
	typeName, _ := as["type"].(string)
	fullname, shortName, childNamespace := ecsAvroAssignFullname(cfg.namespace, typeName, as)
	if fullname != "" {
		placeholder := ecsAvroPlaceholder(typeName, shortName)
		putName(cfg, fullname, fullname, placeholder)
		if shortName != fullname {
			putName(cfg, shortName, fullname, placeholder)
		}
		// Inheritable namespace propagates into the child walk; sibling
		// scopes are unaffected because cfg is passed by value.
		cfg.namespace = childNamespace
	}

	c, err := ecsAvroFromAnyMapImpl(cfg, as)
	if err == nil && fullname != "" {
		putName(cfg, fullname, fullname, c)
		if shortName != fullname {
			putName(cfg, shortName, fullname, c)
		}
	}
	return c, err
}

// putName registers a resolved Common under one lookup key in cfg.names,
// arbitrating short-name collisions across namespaces. A fullname-as-key
// call (`key == owner`) is canonical and always wins. Two short-name
// claims from different fullnames mark the key as ambiguous (`nameOwners`
// becomes "" and the [names] entry is deleted), so unqualified references
// fall through to schema.Any instead of silently binding to whichever
// fullname registered last. A short-name claim that collides with an
// existing canonical fullname binding for the same key is dropped — the
// fullname keeps the slot.
func putName(cfg ecsAvroConfig, key, owner string, c schema.Common) {
	if key == "" {
		return
	}
	existing, seen := cfg.nameOwners[key]
	if !seen {
		cfg.nameOwners[key] = owner
		cfg.names[key] = c
		return
	}
	if existing == "" {
		return // already ambiguous
	}
	if existing == owner {
		cfg.names[key] = c // same owner; placeholder→final replacement
		return
	}
	if key == owner {
		// This call is the canonical fullname registration — take the slot
		// over from whoever was using it as a short-name shortcut.
		cfg.nameOwners[key] = owner
		cfg.names[key] = c
		return
	}
	if key == existing {
		// Existing owner's fullname matches the key — canonical, leave it.
		return
	}
	// Both claims are short-name shortcuts from different fullnames.
	cfg.nameOwners[key] = ""
	delete(cfg.names, key)
}

// ecsAvroAssignFullname computes the Avro fullname of a named-type
// declaration ([record, enum, fixed]) from its declaration map and the
// enclosing namespace, alongside the short name and the namespace that
// should be threaded into the child walk. Returns empty fullname when the
// node is not a named-type declaration or lacks a `name` field.
//
// Per the Avro spec (`Names` section):
//  1. If `name` contains a dot, it IS the fullname and any `namespace`
//     field is ignored.
//  2. Else if `namespace` is set, the fullname is `namespace.name`.
//  3. Else the fullname inherits the enclosing namespace.
func ecsAvroAssignFullname(enclosing, typeName string, as map[string]any) (fullname, shortName, childNamespace string) {
	switch typeName {
	case "record", "enum", "fixed":
	default:
		return "", "", enclosing
	}
	name, _ := as["name"].(string)
	if name == "" {
		return "", "", enclosing
	}
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		return name, name[idx+1:], name[:idx]
	}
	if ns, _ := as["namespace"].(string); ns != "" {
		return ns + "." + name, name, ns
	}
	if enclosing != "" {
		return enclosing + "." + name, name, enclosing
	}
	return name, name, ""
}

// ecsAvroPlaceholder returns the structural stub that stands in for a
// self-referencing named type while its definition is being walked. The
// placeholder uses the short name (matching what ecsAvroFromAnyMapImpl
// would set) and the closest leaf type so downstream sinks see a coherent
// shape rather than schema.Any.
func ecsAvroPlaceholder(typeName, shortName string) schema.Common {
	switch typeName {
	case "record":
		return schema.Common{Name: shortName, Type: schema.Object}
	case "enum":
		return schema.Common{Name: shortName, Type: schema.String}
	case "fixed":
		return schema.Common{Name: shortName, Type: schema.ByteArray}
	}
	return schema.Common{Name: shortName}
}

func ecsAvroFromAnyMapImpl(cfg ecsAvroConfig, as map[string]any) (schema.Common, error) {
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
		// String form may be a primitive type name OR a name reference to
		// a previously-defined record/enum/fixed in lexical scope.
		if resolved, ok := ecsAvroLookupName(cfg, t); ok {
			fieldName := c.Name
			c = resolved
			if fieldName != "" {
				c.Name = fieldName
			}
			return c, nil
		}
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
