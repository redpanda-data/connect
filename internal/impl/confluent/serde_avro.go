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

	bschema "github.com/redpanda-data/benthos/v4/public/schema"

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

	refsMap := map[string]json.RawMessage{}
	if err := client.WalkReferences(ctx, schema.References, func(_ context.Context, name string, refSchema franz_sr.Schema) error {
		s, err := mapSchema(refSchema)
		if err != nil {
			return err
		}
		refsMap[name] = json.RawMessage(s)
		return nil
	}); err != nil {
		return "", fmt.Errorf("walking avro schema references: %w", err)
	}

	root, err := mapSchema(schema)
	if err != nil {
		return "", err
	}
	var rootNode any
	if err := json.Unmarshal([]byte(root), &rootNode); err != nil {
		return "", fmt.Errorf("unmarshaling root avro schema: %w", err)
	}
	hydrated, err := hydrateAvroRefs(rootNode, refsMap, make(map[string]bool))
	if err != nil {
		return "", fmt.Errorf("hydrating avro references: %w", err)
	}
	out, err := json.Marshal(hydrated)
	if err != nil {
		return "", fmt.Errorf("marshaling hydrated avro schema: %w", err)
	}
	return string(out), nil
}

// hydrateAvroRefs recursively replaces named type references with their
// inlined definitions throughout an Avro schema JSON tree. It walks only
// type positions — record fields' type values, array items, map values,
// and union branches — so name/namespace/doc/aliases/symbols strings that
// happen to match a reference name are left alone.
//
// Each named type is inlined at most once per walk; subsequent references
// to the same name are left as string name references so Avro's
// one-definition-many-references semantics are preserved. This correctly
// handles self-referential types, mutually recursive subjects, and shared
// subgraphs.
func hydrateAvroRefs(node any, refs map[string]json.RawMessage, inlined map[string]bool) (any, error) {
	switch v := node.(type) {
	case string:
		if inlined[v] {
			return v, nil
		}
		def, ok := refs[v]
		if !ok {
			return v, nil
		}
		inlined[v] = true
		var parsed any
		if err := json.Unmarshal(def, &parsed); err != nil {
			return nil, fmt.Errorf("unmarshaling avro reference %q: %w", v, err)
		}
		return hydrateAvroRefs(parsed, refs, inlined)
	case []any:
		for i, item := range v {
			h, err := hydrateAvroRefs(item, refs, inlined)
			if err != nil {
				return nil, err
			}
			v[i] = h
		}
		return v, nil
	case map[string]any:
		typ, _ := v["type"].(string)
		switch typ {
		case "record", "error":
			fields, ok := v["fields"].([]any)
			if !ok {
				return v, nil
			}
			for i, f := range fields {
				fm, ok := f.(map[string]any)
				if !ok {
					continue
				}
				if ft, ok := fm["type"]; ok {
					h, err := hydrateAvroRefs(ft, refs, inlined)
					if err != nil {
						return nil, err
					}
					fm["type"] = h
				}
				fields[i] = fm
			}
			v["fields"] = fields
		case "array":
			if items, ok := v["items"]; ok {
				h, err := hydrateAvroRefs(items, refs, inlined)
				if err != nil {
					return nil, err
				}
				v["items"] = h
			}
		case "map":
			if values, ok := v["values"]; ok {
				h, err := hydrateAvroRefs(values, refs, inlined)
				if err != nil {
					return nil, err
				}
				v["values"] = h
			}
		}
		return v, nil
	default:
		return v, nil
	}
}

func (s *schemaRegistryEncoder) getAvroEncoder(ctx context.Context, schemaRef franz_sr.Schema) (schemaEncoder, error) {
	schemaSpec, err := resolveAvroReferences(ctx, s.client, nil, schemaRef)
	if err != nil {
		return nil, err
	}
	return s.newAvroEncoder(schemaSpec)
}

func (s *schemaRegistryEncoder) newAvroEncoder(avroJSON string) (schemaEncoder, error) {
	schema, err := avro.Parse(avroJSON)
	if err != nil {
		return nil, fmt.Errorf("parsing Avro schema: %w", err)
	}

	switch s.avroInputEnc {
	case avroInputEncAvroJSON:
		// Every message is Avro JSON, whatever form it arrives in. This is
		// what a pipeline sets when its data came from
		// schema_registry_decode and something in between — a mapping, a
		// branch — has parsed the payload, leaving Avro JSON values in a
		// structured message that auto would read the other way.
		return func(m *service.Message) error {
			b, err := avroJSONBytes(m)
			if err != nil {
				return err
			}
			var native any
			if err := schema.DecodeJSON(b, &native); err != nil {
				return err
			}
			return encodeAvro(m, schema, native)
		}, nil
	case avroInputEncNative:
		// Every message is Go and plain JSON values. Encode is the only
		// reader that takes []byte, time.Time and RFC 3339 text, and the only
		// one that reads a string for a decimal as decimal notation, so
		// hand-written JSON keeps the meaning its author intended.
		return func(m *service.Message) error {
			native, err := m.AsStructuredMut()
			if err != nil {
				return fmt.Errorf("extracting structured data: %w", err)
			}
			return encodeAvro(m, schema, native)
		}, nil
	}

	// Neither reader is a superset of the other, so which one a message
	// gets depends on the form it arrives in.
	//
	// A raw byte payload is Avro JSON: it is what schema_registry_decode
	// emits (EncodeJSON) in both avroRawJSON modes — only union tagging
	// differs, and DecodeJSON accepts tagged and bare unions alike — so one
	// path still serves both. DecodeJSON is also the only reader that
	// implements Avro JSON's bytes and fixed semantics, where a JSON string
	// carries one byte per codepoint. Encode does not: it reads a Go string
	// as UTF-8, mangling any byte above 0x7f, and for a decimal it accepts
	// only numeric text, so every decimal backed by bytes or fixed failed to
	// re-encode from what the decoder emitted.
	//
	// A structured message must never be routed that way. Serialising it to
	// get bytes goes through encoding/json, which writes a nested []byte as
	// base64 text; DecodeJSON would then read those base64 characters as the
	// value and succeed, silently encoding the wrong bytes. Encode is also
	// the only reader that takes the Go values a structured message carries:
	// []byte for bytes and fixed, and time.Time or an RFC 3339 string for
	// timestamps, a shape CDC sources emit and Avro JSON cannot spell.
	//
	// HasStructured reports the form without converting either way, so the
	// choice costs nothing. Raw JSON that DecodeJSON rejects still falls
	// through to Encode, the more permissive reader, and its error is the
	// one worth reporting.
	//
	// A message can hold both forms — a raw payload that something upstream
	// read as structured caches the parse — and those go to Encode. That is
	// the reader they had before either was a choice, so nothing regresses:
	// a bytes-backed decimal reaching Encode fails loudly rather than
	// encoding the wrong bytes.
	return func(m *service.Message) error {
		if !m.HasStructured() {
			if b, err := m.AsBytes(); err == nil {
				var native any
				if err := schema.DecodeJSON(b, &native); err == nil {
					return encodeAvro(m, schema, native)
				}
			}
		}
		native, err := m.AsStructuredMut()
		if err != nil {
			return fmt.Errorf("extracting structured data: %w", err)
		}
		return encodeAvro(m, schema, native)
	}, nil
}

// avroJSONBytes returns the message as the Avro JSON bytes to decode.
//
// A message holding only a raw payload gives it up directly. A structured one
// has to be serialised, and that is faithful only while every value in the tree
// is one encoding/json spells unambiguously — a []byte becomes base64 text that
// reads back as an entirely different byte sequence, and a time.Time becomes
// RFC 3339 text that Avro JSON cannot spell at all. Those are refused rather
// than silently encoded as the wrong value, because a tree carrying them was
// never Avro JSON to begin with.
//
// The bytes come from marshalling the tree that was checked, never from the
// message. AsBytes caches a serialisation of the structured form the moment
// anything asks for the payload — a log processor, an output interpolation, or
// this processor's own subject interpolation — so HasBytes says nothing about
// where those bytes came from, and a message can be holding the base64 text of
// the very []byte this check exists to reject. Marshalling here also keeps the
// message untouched when serialisation fails, which AsBytes does not: it
// reports no error and caches the empty result, emptying the payload that a
// dead-letter output still needs.
func avroJSONBytes(m *service.Message) ([]byte, error) {
	if !m.HasStructured() {
		return m.AsBytes()
	}
	// AsStructured rather than AsStructuredMut: the latter drops the raw
	// payload, and reading the message must not change what it holds.
	native, err := m.AsStructured()
	if err != nil {
		return nil, fmt.Errorf("extracting structured data: %w", err)
	}
	if !jsonNativeTree(native) {
		return nil, fmt.Errorf(
			"message holds values Avro JSON cannot spell, such as []byte, time.Time, or a typed map or slice: "+
				"set %v.%v to %v or %v to encode it",
			sreFieldAvro, sreFieldAvroInputEnc, avroInputEncNative, avroInputEncAuto)
	}
	b, err := json.Marshal(native)
	if err != nil {
		// Non-finite floats land here: jsonNativeTree admits float64 and
		// float32 because almost all of them are spelled faithfully, and JSON
		// has no spelling for the handful that are not. Avro binary spells
		// them without trouble, so the other two modes encode these — worth
		// saying, since only the Avro JSON hop is the obstacle.
		return nil, fmt.Errorf(
			"serialising structured data as Avro JSON: %w: set %v.%v to %v or %v to encode it",
			err, sreFieldAvro, sreFieldAvroInputEnc, avroInputEncNative, avroInputEncAuto)
	}
	return b, nil
}

// jsonNativeTree reports whether v holds only values encoding/json spells
// unambiguously, so that serialising it yields back the same Avro JSON the
// values came from.
//
// It is a check on types, not on values: NaN and the infinities are admitted
// here and rejected by json.Marshal instead, since JSON cannot spell them.
func jsonNativeTree(v any) bool {
	switch v := v.(type) {
	case nil, bool, string, float64, float32,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		json.Number:
		return true
	case map[string]any:
		for _, e := range v {
			if !jsonNativeTree(e) {
				return false
			}
		}
		return true
	case []any:
		for _, e := range v {
			if !jsonNativeTree(e) {
				return false
			}
		}
		return true
	default:
		// []byte, time.Time, big.Rat, avro.Duration and anything else a
		// mapping or a decoder can leave in the tree.
		return false
	}
}

func encodeAvro(m *service.Message, schema *avro.Schema, native any) error {
	binary, err := schema.Encode(native)
	if err != nil {
		return err
	}
	m.SetBytes(binary)
	return nil
}

func (s *schemaRegistryDecoder) getAvroDecoder(ctx context.Context, aschema franz_sr.Schema) (schemaDecoder, error) {
	schemaSpec, err := resolveAvroReferences(ctx, s.client, s.cfg.avro.mapping, aschema)
	if err != nil {
		return nil, err
	}

	// Build parse options for preserve_logical_types: register custom
	// types that convert time.Duration→time.Time for time-of-day fields,
	// avro.Duration→string, decimal bytes→json.Number, and optionally
	// Kafka Connect (Debezium) types.
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

	var (
		commonSchema     any
		commonSchemaRoot bschema.Common
		hasCommonSchema  bool
	)
	if s.cfg.avro.storeSchemaMeta != "" {
		c, parseErr := ecsAvroParseFromBytes(ecsAvroConfig{
			rawUnion:                   s.cfg.avro.rawUnions,
			preserveLogicalTypes:       s.cfg.avro.preserveLogicalTypes,
			translateKafkaConnectTypes: s.cfg.avro.translateKafkaConnectTypes,
		}, []byte(schemaSpec))
		if parseErr != nil {
			s.logger.With("error", parseErr).Error("Failed to extract common schema for meta storage")
		} else {
			commonSchema = c.ToAny()
			commonSchemaRoot = c
			hasCommonSchema = true
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

		if hasCommonSchema {
			native = normaliseAvroDecimals(native, commonSchemaRoot)
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
