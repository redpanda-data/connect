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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	franz_sr "github.com/twmb/franz-go/pkg/sr"
	"github.com/xeipuuv/gojsonschema"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

// jsonSchemaCoercer rebuilds decoded JSON data using native Go types that match
// the declared JSON Schema. A plain JSON decode turns every number into
// float64 and leaves date-time values as strings, which causes downstream
// components that infer their schema from Go values (such as the iceberg sink)
// to map JSON-schema integers to double rather than bigint. Coercing the data
// against the schema produces int64 for `integer` fields, time.Time for
// `date-time` strings, and so on, restoring fidelity with the declared schema.
type jsonSchemaCoercer struct {
	// rawSchema is the parsed root JSON Schema document.
	rawSchema map[string]any
	// refs maps schema-registry reference names to their parsed documents, for
	// resolving external `$ref`s.
	refs map[string]map[string]any
	// compiledBranches memoises gojsonschema schemas used for oneOf/anyOf branch
	// validation, keyed by the branch's marshalled JSON. Branch schemas are
	// immutable for the life of the coercer, so compiling them once (rather than
	// per message) avoids repeating the expensive schema compilation on a hot
	// path. The map is safe for the concurrent Process calls that share a
	// coercer.
	compiledBranches sync.Map
}

// maxCoerceDepth bounds the number of `$ref` resolutions along a single
// coercion path. Non-`$ref` nesting is bounded by the (finite) schema, so only
// reference chains can recurse without limit; a cyclic or self-referential
// `$ref` is caught here rather than overflowing the stack.
const maxCoerceDepth = 1000

// buildJSONSchemaCoercer parses the given schema (and any of its registry
// references) into walkable documents for coercion.
func buildJSONSchemaCoercer(ctx context.Context, cl *sr.Client, schema franz_sr.Schema) (*jsonSchemaCoercer, error) {
	refs := map[string]string{}
	if len(schema.References) > 0 {
		if err := cl.WalkReferences(ctx, schema.References, func(_ context.Context, name string, s franz_sr.Schema) error {
			refs[name] = s.Schema
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return newJSONSchemaCoercer(schema.Schema, refs)
}

// newJSONSchemaCoercer parses a root JSON Schema document and any named
// references (as raw schema strings) into walkable documents. It is the
// client-independent core of buildJSONSchemaCoercer.
func newJSONSchemaCoercer(root string, refs map[string]string) (*jsonSchemaCoercer, error) {
	var rootDoc map[string]any
	if err := json.Unmarshal([]byte(root), &rootDoc); err != nil {
		return nil, fmt.Errorf("parsing JSON schema for coercion: %w", err)
	}

	parsed := make(map[string]map[string]any, len(refs))
	for name, s := range refs {
		var doc map[string]any
		if err := json.Unmarshal([]byte(s), &doc); err != nil {
			return nil, fmt.Errorf("parsing referenced JSON schema %q: %w", name, err)
		}
		parsed[name] = doc
	}

	return &jsonSchemaCoercer{rawSchema: rootDoc, refs: parsed}, nil
}

// coerceMessage decodes raw JSON bytes preserving numeric precision (via
// UseNumber, so large integers are not truncated through float64) and returns
// the value coerced against the schema, ready for SetStructuredMut.
func (c *jsonSchemaCoercer) coerceMessage(b []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()

	var data any
	if err := dec.Decode(&data); err != nil {
		return nil, fmt.Errorf("decoding message for coercion: %w", err)
	}
	return c.coerce(data, c.rawSchema, c.rawSchema, 0)
}

// coerce walks data against the schema node, returning the value rebuilt with
// native Go types. doc is the document that node belongs to, used to resolve
// local (`#/...`) references. depth counts `$ref` resolutions on the current
// path to bound cyclic references.
func (c *jsonSchemaCoercer) coerce(data any, node, doc map[string]any, depth int) (any, error) {
	if node == nil {
		return c.passthrough(data), nil
	}

	// Resolve `$ref` before anything else.
	if ref, ok := node["$ref"].(string); ok {
		if depth >= maxCoerceDepth {
			return nil, fmt.Errorf("maximum $ref resolution depth (%d) exceeded resolving %q; the schema may contain a cyclic reference", maxCoerceDepth, ref)
		}
		target, targetDoc, err := c.resolveRef(ref, doc)
		if err != nil {
			return nil, err
		}
		return c.coerce(data, target, targetDoc, depth+1)
	}

	// Schema combiners.
	if raw, ok := node["allOf"]; ok {
		return c.coerceAllOf(data, raw, doc, depth)
	}
	if raw, ok := node["oneOf"]; ok {
		return c.coerceBranch(data, raw, doc, "oneOf", depth)
	}
	if raw, ok := node["anyOf"]; ok {
		return c.coerceBranch(data, raw, doc, "anyOf", depth)
	}

	types, _ := nodeType(node)

	// A null/absent value is preserved regardless of the declared type so that
	// nullable fields (`"type": ["integer", "null"]`) round-trip.
	if data == nil {
		return nil, nil
	}

	var typ string
	switch len(types) {
	case 0:
		typ = ""
	case 1:
		typ = types[0]
	default:
		// Ambiguous union (e.g. ["integer","string"]): pick the type matching
		// the value's kind, else fall through to passthrough.
		typ = matchTypeToValue(types, data)
	}

	switch typ {
	case "integer":
		return toInt64(data)
	case "number":
		return toFloat64(data)
	case "boolean":
		return toBool(data)
	case "string":
		return coerceString(data, node)
	case "null":
		return nil, nil
	case "object":
		return c.coerceObject(data, node, doc, depth)
	case "array":
		return c.coerceArray(data, node, doc, depth)
	case "":
		// No explicit type: infer structural intent from keywords, otherwise
		// pass the value through with numbers normalised to float64.
		if _, ok := node["properties"]; ok {
			return c.coerceObject(data, node, doc, depth)
		}
		if _, ok := node["items"]; ok {
			return c.coerceArray(data, node, doc, depth)
		}
		return c.passthrough(data), nil
	default:
		return c.passthrough(data), nil
	}
}

func (c *jsonSchemaCoercer) coerceObject(data any, node, doc map[string]any, depth int) (any, error) {
	m, ok := data.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("cannot coerce %T to object", data)
	}

	props, _ := node["properties"].(map[string]any)
	addl, _ := node["additionalProperties"].(map[string]any)

	out := make(map[string]any, len(m))
	for k, v := range m {
		switch ps, ok := props[k].(map[string]any); {
		case ok:
			cv, err := c.coerce(v, ps, doc, depth)
			if err != nil {
				return nil, fmt.Errorf("property %q: %w", k, err)
			}
			out[k] = cv
		case addl != nil:
			cv, err := c.coerce(v, addl, doc, depth)
			if err != nil {
				return nil, fmt.Errorf("additional property %q: %w", k, err)
			}
			out[k] = cv
		default:
			out[k] = c.passthrough(v)
		}
	}

	// Apply defaults for properties absent from the data.
	for k, ps := range props {
		if _, present := m[k]; present {
			continue
		}
		psm, ok := ps.(map[string]any)
		if !ok {
			continue
		}
		def, ok := psm["default"]
		if !ok {
			continue
		}
		cv, err := c.coerce(def, psm, doc, depth)
		if err != nil {
			return nil, fmt.Errorf("default for property %q: %w", k, err)
		}
		out[k] = cv
	}

	return out, nil
}

func (c *jsonSchemaCoercer) coerceArray(data any, node, doc map[string]any, depth int) (any, error) {
	arr, ok := data.([]any)
	if !ok {
		return nil, fmt.Errorf("cannot coerce %T to array", data)
	}

	out := make([]any, len(arr))
	switch items := node["items"].(type) {
	case map[string]any:
		for i, e := range arr {
			cv, err := c.coerce(e, items, doc, depth)
			if err != nil {
				return nil, fmt.Errorf("array index %d: %w", i, err)
			}
			out[i] = cv
		}
	case []any:
		// Tuple validation: coerce positionally, pass through any extras.
		for i, e := range arr {
			if i < len(items) {
				if is, ok := items[i].(map[string]any); ok {
					cv, err := c.coerce(e, is, doc, depth)
					if err != nil {
						return nil, fmt.Errorf("array index %d: %w", i, err)
					}
					out[i] = cv
					continue
				}
			}
			out[i] = c.passthrough(e)
		}
	default:
		for i, e := range arr {
			out[i] = c.passthrough(e)
		}
	}

	return out, nil
}

func (c *jsonSchemaCoercer) coerceAllOf(data, raw any, doc map[string]any, depth int) (any, error) {
	subs, ok := raw.([]any)
	if !ok {
		return c.passthrough(data), nil
	}
	out := data
	for i, s := range subs {
		sm, ok := s.(map[string]any)
		if !ok {
			continue
		}
		var err error
		if out, err = c.coerce(out, sm, doc, depth); err != nil {
			return nil, fmt.Errorf("allOf[%d]: %w", i, err)
		}
	}
	return out, nil
}

// coerceBranch handles oneOf/anyOf by selecting the first branch the data
// validates against and coercing against it.
func (c *jsonSchemaCoercer) coerceBranch(data, raw any, doc map[string]any, kw string, depth int) (any, error) {
	subs, ok := raw.([]any)
	if !ok {
		return c.passthrough(data), nil
	}

	normalised := normaliseForValidation(data)
	for i, s := range subs {
		sm, ok := s.(map[string]any)
		if !ok {
			continue
		}

		// Resolve a branch that is itself a bare `$ref` for validation only; the
		// coercion below re-resolves it via coerce so the depth guard applies.
		resolved := sm
		if ref, ok := sm["$ref"].(string); ok {
			target, _, err := c.resolveRef(ref, doc)
			if err != nil {
				return nil, err
			}
			resolved = target
		}

		match, err := c.validatesAgainst(resolved, normalised)
		if err != nil {
			return nil, err
		}
		if match {
			cv, err := c.coerce(data, sm, doc, depth)
			if err != nil {
				return nil, fmt.Errorf("%s[%d]: %w", kw, i, err)
			}
			return cv, nil
		}
	}

	return nil, fmt.Errorf("value does not match any %s branch", kw)
}

// resolveRef resolves a JSON Schema `$ref`, returning the target node and the
// document it lives within (so that further local references resolve correctly).
func (c *jsonSchemaCoercer) resolveRef(ref string, doc map[string]any) (node, refDoc map[string]any, err error) {
	if ref == "" {
		return nil, nil, errors.New("empty $ref")
	}

	// Local reference within the current document.
	if strings.HasPrefix(ref, "#") {
		n, err := resolvePointer(doc, ref)
		return n, doc, err
	}

	// External reference, optionally with a fragment ("Name#/path").
	name, fragment := ref, ""
	if i := strings.IndexByte(ref, '#'); i >= 0 {
		name, fragment = ref[:i], ref[i:]
	}
	extDoc, ok := c.refs[name]
	if !ok {
		return nil, nil, fmt.Errorf("unresolved $ref %q", ref)
	}
	if fragment == "" || fragment == "#" {
		return extDoc, extDoc, nil
	}
	n, err := resolvePointer(extDoc, fragment)
	return n, extDoc, err
}

// resolvePointer walks a JSON Pointer fragment (e.g. "#/definitions/Foo")
// within doc and returns the schema object it points to.
func resolvePointer(doc map[string]any, ref string) (map[string]any, error) {
	ptr := strings.TrimPrefix(strings.TrimPrefix(ref, "#"), "/")
	if ptr == "" {
		return doc, nil
	}

	var cur any = doc
	for token := range strings.SplitSeq(ptr, "/") {
		token = strings.ReplaceAll(token, "~1", "/")
		token = strings.ReplaceAll(token, "~0", "~")
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("cannot resolve $ref %q: %q is not an object", ref, token)
		}
		if cur, ok = m[token]; !ok {
			return nil, fmt.Errorf("cannot resolve $ref %q: token %q not found", ref, token)
		}
	}

	node, ok := cur.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("$ref %q does not point to a schema object", ref)
	}
	return node, nil
}

// passthrough returns the value with numbers normalised to float64 (matching a
// plain JSON decode) and nested containers recursed into, for data the schema
// does not type.
func (c *jsonSchemaCoercer) passthrough(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = c.passthrough(e)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = c.passthrough(e)
		}
		return out
	case json.Number:
		if f, err := t.Float64(); err == nil {
			return f
		}
		return t.String()
	default:
		return v
	}
}

func coerceString(data any, node map[string]any) (any, error) {
	s, ok := data.(string)
	if !ok {
		return nil, fmt.Errorf("cannot coerce %T to string", data)
	}
	// A date-time string maps to a timestamp downstream; other formats (date,
	// time, uuid, ...) stay strings because the iceberg inferrer would
	// misidentify a date-only time.Time as a timestamp with timezone.
	if format, _ := node["format"].(string); format == "date-time" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return nil, fmt.Errorf("parsing date-time %q: %w", s, err)
		}
		return t, nil
	}
	return s, nil
}

func toInt64(v any) (any, error) {
	switch n := v.(type) {
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i, nil
		}
		f, err := n.Float64()
		if err != nil {
			return nil, fmt.Errorf("value %q is not a valid integer", n.String())
		}
		return floatToInt64(f)
	case float64:
		return floatToInt64(n)
	case int:
		return int64(n), nil
	case int64:
		return n, nil
	default:
		return nil, fmt.Errorf("cannot coerce %T to integer", v)
	}
}

func floatToInt64(f float64) (any, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, fmt.Errorf("value %v is not an integer", f)
	}
	if f != math.Trunc(f) {
		return nil, fmt.Errorf("value %v is not an integer", f)
	}
	// math.MaxInt64 is not exactly representable as a float64 (it rounds up to
	// 2^63), so comparing against it would let 2^63 itself slip through and then
	// wrap on conversion. 2^63 is exact, so use it as the first overflowing
	// value; -2^63 (== math.MinInt64) is exact and valid.
	if f >= (1<<63) || f < -(1<<63) {
		return nil, fmt.Errorf("value %v overflows int64", f)
	}
	return int64(f), nil
}

func toFloat64(v any) (any, error) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		if err != nil {
			return nil, fmt.Errorf("value %q is not a valid number", n.String())
		}
		return f, nil
	case float64:
		return n, nil
	case float32:
		return float64(n), nil
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	default:
		return nil, fmt.Errorf("cannot coerce %T to number", v)
	}
}

func toBool(v any) (any, error) {
	b, ok := v.(bool)
	if !ok {
		return nil, fmt.Errorf("cannot coerce %T to boolean", v)
	}
	return b, nil
}

// nodeType returns the declared non-null JSON Schema types for a node (handling
// `type` given as a string or an array) and whether "null" was permitted.
func nodeType(node map[string]any) (types []string, nullable bool) {
	switch t := node["type"].(type) {
	case string:
		return []string{t}, false
	case []any:
		for _, e := range t {
			s, _ := e.(string)
			if s == "null" {
				nullable = true
				continue
			}
			if s != "" {
				types = append(types, s)
			}
		}
		return types, nullable
	}
	return nil, false
}

// matchTypeToValue chooses, from an ambiguous multi-type union (e.g.
// `["integer","string"]`), the declared type that matches the runtime kind of
// data. It returns "" when no candidate matches, so the value is passed through
// uncoerced rather than forced into an incorrect (and possibly failing)
// coercion.
func matchTypeToValue(types []string, data any) string {
	switch data.(type) {
	case string:
		if slices.Contains(types, "string") {
			return "string"
		}
	case bool:
		if slices.Contains(types, "boolean") {
			return "boolean"
		}
	case map[string]any:
		if slices.Contains(types, "object") {
			return "object"
		}
	case []any:
		if slices.Contains(types, "array") {
			return "array"
		}
	case json.Number, float64:
		// Prefer integer for integral values when both are allowed.
		if slices.Contains(types, "integer") && isIntegralNumber(data) {
			return "integer"
		}
		if slices.Contains(types, "number") {
			return "number"
		}
		if slices.Contains(types, "integer") {
			return "integer"
		}
	}
	return ""
}

func isIntegralNumber(data any) bool {
	switch n := data.(type) {
	case json.Number:
		if _, err := n.Int64(); err == nil {
			return true
		}
		f, err := n.Float64()
		return err == nil && f == math.Trunc(f)
	case float64:
		return n == math.Trunc(n)
	}
	return false
}

// normaliseForValidation deep-converts json.Number values to float64 so that
// gojsonschema (which expects standard JSON primitives) can validate data that
// was decoded with UseNumber.
func normaliseForValidation(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = normaliseForValidation(e)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = normaliseForValidation(e)
		}
		return out
	case json.Number:
		if f, err := t.Float64(); err == nil {
			return f
		}
		return t.String()
	default:
		return v
	}
}

// validatesAgainst reports whether data conforms to the given schema node,
// compiling (and caching) the node's schema so repeated branch checks across
// messages do not recompile it.
func (c *jsonSchemaCoercer) validatesAgainst(node map[string]any, data any) (bool, error) {
	b, err := json.Marshal(node)
	if err != nil {
		return false, err
	}

	key := string(b)
	var schema *gojsonschema.Schema
	if cached, ok := c.compiledBranches.Load(key); ok {
		schema = cached.(*gojsonschema.Schema)
	} else {
		if schema, err = gojsonschema.NewSchema(gojsonschema.NewBytesLoader(b)); err != nil {
			return false, err
		}
		c.compiledBranches.Store(key, schema)
	}

	res, err := schema.Validate(gojsonschema.NewGoLoader(data))
	if err != nil {
		return false, err
	}
	return res.Valid(), nil
}
