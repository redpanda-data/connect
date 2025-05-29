// Copyright 2025 Redpanda Data, Inc.
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
	"math/big"
	"strings"
	"time"

	franz_sr "github.com/twmb/franz-go/pkg/sr"

	"github.com/hamba/avro/v2"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

func resolveHambaAvroReferences(ctx context.Context, client *sr.Client, schema franz_sr.Schema) ([]franz_sr.Schema, error) {
	if len(schema.References) == 0 {
		return []franz_sr.Schema{schema}, nil
	}
	schemas := []franz_sr.Schema{}
	if err := client.WalkReferences(ctx, schema.References, func(_ context.Context, _ string, schema franz_sr.Schema) error {
		schemas = append(schemas, schema)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("unable to walk schema references: %w", err)
	}

	schemas = append(schemas, schema)
	return schemas, nil
}

func (s *schemaRegistryDecoder) getHambaAvroDecoder(ctx context.Context, schema franz_sr.Schema) (schemaDecoder, error) {
	schemaSpecs, err := resolveHambaAvroReferences(ctx, s.client, schema)
	if err != nil {
		return nil, err
	}
	cache := &avro.SchemaCache{}
	var codec avro.Schema
	for _, schema := range schemaSpecs {
		avroSchema := []byte(schema.Schema)
		if s.cfg.avro.mapping != nil {
			msg := service.NewMessage(avroSchema)
			msg, err = msg.BloblangQuery(s.cfg.avro.mapping)
			if err != nil {
				return nil, fmt.Errorf("unable to apply avro schema mapping: %w", err)
			}
			avroSchema, err = msg.AsBytes()
			if err != nil {
				return nil, fmt.Errorf("unable to extract avro schema mapping result: %w", err)
			}
		}
		codec, err = avro.ParseBytesWithCache(avroSchema, "", cache)
		if err != nil {
			return nil, fmt.Errorf("unable to parse schema %w", err)
		}
	}

	decoder := func(m *service.Message) error {
		b, err := m.AsBytes()
		if err != nil {
			return fmt.Errorf("unable to extract bytes from message: %w", err)
		}
		r := avro.NewReader(nil, 0).Reset(b)
		native := r.ReadNext(codec)
		if r.Error != nil {
			return fmt.Errorf("unable to unmarshal avro: %w", r.Error)
		}
		var w avroSchemaWalker
		w.unnestUnions = s.cfg.avro.rawUnions
		w.translateKafkaConnectTypes = s.cfg.avro.translateKafkaConnectTypes
		if native, err = w.walk(native, codec); err != nil {
			return fmt.Errorf("unable to transform avro data into expected format: %w", err)
		}
		m.SetStructuredMut(native)
		return nil
	}

	return decoder, nil
}

type avroSchemaWalker struct {
	unnestUnions               bool
	translateKafkaConnectTypes bool
}

var errUnknownKafkaConnectType = errors.New("unknown kafka connect type")

func (w *avroSchemaWalker) walk(root any, schema avro.Schema) (any, error) {
	if w.translateKafkaConnectTypes {
		if s, ok := schema.(avro.PropertySchema); ok {
			v, err := w.translateKafkaConnectValue(root, s)
			if !errors.Is(err, errUnknownKafkaConnectType) {
				return v, err
			}
		}
	}
	switch s := schema.(type) {
	case *avro.RecordSchema:
		v, ok := root.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected map for RecordSchema got: %T", root)
		}
		return w.walkRecord(v, s)
	case *avro.MapSchema:
		v, ok := root.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected map for MapSchema got: %T", root)
		}
		return w.walkMap(v, s)
	case *avro.ArraySchema:
		v, ok := root.([]any)
		if !ok {
			return nil, fmt.Errorf("expected slice for ArraySchema got: %T", root)
		}
		return w.walkSlice(v, s)
	case *avro.RefSchema:
		return w.walk(root, s.Schema())
	case *avro.UnionSchema:
		if root == nil {
			return nil, nil
		}
		u, ok := root.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected map for UnionSchema got: %T", root)
		}
		if len(u) != 1 {
			return nil, fmt.Errorf("expected map with size 1 for UnionSchema got: %v", len(u))
		}
		for k, v := range u {
			t, _ := s.Types().Get(k)
			if t == nil {
				names := []string{}
				for _, t := range s.Types() {
					names = append(names, string(t.Type()))
				}
				return nil, fmt.Errorf("unknown union variant %q, expected one of [%s]", k, strings.Join(names, ", "))
			}
			if w.unnestUnions {
				return w.walk(v, t)
			}
			var err error
			u[k], err = w.walk(v, t)
			return u, err
		}
		return nil, fmt.Errorf("impossible empty map, got size: %v", len(u))
	case avro.LogicalTypeSchema:
		l := s.Logical()
		if l == nil {
			return root, nil
		}
		switch l.Type() {
		case avro.Decimal:
			v, ok := root.(*big.Rat)
			if !ok {
				return nil, fmt.Errorf("expected *big.Rat for DecimalLogicalType got: %T", root)
			}
			ls, ok := l.(*avro.DecimalLogicalSchema)
			if !ok {
				return nil, fmt.Errorf("expected *avro.LogicalTypeSchema for DecimalLogicalType got: %T", l)
			}
			return json.Number(v.FloatString(ls.Scale())), nil
		case avro.TimeMicros, avro.TimeMillis:
			v, ok := root.(time.Duration)
			if !ok {
				return nil, fmt.Errorf("expected time.Duration for %v got: %T", l.Type(), root)
			}
			// Convert time units to timestamps, as that is the most natural representation in blobl
			return time.Time{}.Add(v), nil
		case avro.Duration:
			v, ok := root.(time.Duration)
			if !ok {
				return nil, fmt.Errorf("expected time.Duration for %v got: %T", l.Type(), root)
			}
			return v.String(), nil
		}
		return root, nil
	default:
		return root, nil
	}
}

func (w *avroSchemaWalker) walkRecord(record map[string]any, schema *avro.RecordSchema) (map[string]any, error) {
	var err error
	for _, f := range schema.Fields() {
		v, ok := record[f.Name()]
		if !ok {
			return nil, fmt.Errorf("unexpected missing field from avro record: %q", f.Name())
		}
		if record[f.Name()], err = w.walk(v, f.Type()); err != nil {
			return nil, err
		}
	}
	return record, nil
}

func (w *avroSchemaWalker) walkMap(dict map[string]any, schema *avro.MapSchema) (map[string]any, error) {
	var err error
	for k, v := range dict {
		if dict[k], err = w.walk(v, schema.Values()); err != nil {
			return nil, err
		}
	}
	return dict, nil
}

func (w *avroSchemaWalker) walkSlice(slice []any, schema *avro.ArraySchema) ([]any, error) {
	var err error
	for i, v := range slice {
		if slice[i], err = w.walk(v, schema.Items()); err != nil {
			return nil, err
		}
	}
	return slice, nil
}

func (*avroSchemaWalker) translateKafkaConnectValue(value any, schema avro.PropertySchema) (any, error) {
	name := schema.Prop("connect.name")
	switch name {
	case "io.debezium.time.Date":
		v, err := bloblang.ValueAsInt64(value)
		if err != nil {
			return nil, fmt.Errorf("expected number for io.debezium.time.Date got: %T", value)
		}
		return time.UnixMilli(0).UTC().AddDate(0, 0, int(v)), nil
	case "io.debezium.time.Year":
		v, err := bloblang.ValueAsInt64(value)
		if err != nil {
			return nil, fmt.Errorf("expected number for io.debezium.time.Date got: %T", value)
		}
		return time.UnixMilli(0).UTC().AddDate(int(v), 0, 0), nil
	case "io.debezium.time.Timestamp", "io.debezium.time.Time":
		v, err := bloblang.ValueAsInt64(value)
		if err != nil {
			return nil, fmt.Errorf("expected number for %s got: %T", name, value)
		}
		return time.UnixMilli(v).UTC(), nil
	case "io.debezium.time.MicroTimestamp", "io.debezium.time.MicroTime":
		v, err := bloblang.ValueAsInt64(value)
		if err != nil {
			return nil, fmt.Errorf("expected number for %s got: %T", name, value)
		}
		return time.UnixMilli(0).UTC().Add(time.Duration(v) * time.Microsecond), nil
	case "io.debezium.time.NanoTimestamp", "io.debezium.time.NanoTime":
		v, err := bloblang.ValueAsInt64(value)
		if err != nil {
			return nil, fmt.Errorf("expected number for %s got: %T", name, value)
		}
		return time.UnixMilli(0).UTC().Add(time.Duration(v) * time.Nanosecond), nil
	case "io.debezium.time.ZonedTimestamp":
		v := bloblang.ValueToString(value)
		t, err := time.ParseInLocation(time.RFC3339Nano, v, time.UTC)
		if err != nil {
			return nil, fmt.Errorf("expected valid ISO formatted timestamp for io.debezium.time.ZonedTimestamp got: %q", v)
		}
		return t, nil
	}
	return nil, errUnknownKafkaConnectType
}
