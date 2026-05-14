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

package parquet

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/redpanda-data/benthos/v4/public/schema"

	"github.com/redpanda-data/connect/v4/internal/impl/parquet/parquetdecimal"
)

type schemaVisitor interface {
	visitLeaf(value any, schemaNode parquet.Node) (any, error)
}

func visitWithSchema(visitor schemaVisitor, value any, schemaNode parquet.Node) (any, error) {
	if schemaNode.Leaf() {
		if schemaNode.Optional() && value == nil {
			return nil, nil
		}
		return visitor.visitLeaf(value, schemaNode)
	}

	switch group := value.(type) {
	case map[string]any:
		for _, childSchemaNode := range schemaNode.Fields() {
			name := childSchemaNode.Name()
			if childValue, ok := group[name]; ok {
				var err error
				group[name], err = visitWithSchema(visitor, childValue, childSchemaNode)
				if err != nil {
					return nil, fmt.Errorf("visiting [%s]: %w", name, err)
				}
			}
		}
		return group, nil

	case []any:
		for i := range group {
			var err error
			group[i], err = visitWithSchema(visitor, group[i], schemaNode)
			if err != nil {
				return nil, fmt.Errorf("visiting [%d]: %w", i, err)
			}
		}
		return group, nil

	case nil:
		return nil, nil

	default:
		panic(fmt.Sprintf("unexpected group value type: %T", value))
	}
}

type encodingCoercionVisitor struct{}

func (encodingCoercionVisitor) visitLeaf(value any, schemaNode parquet.Node) (any, error) {
	logicalType := schemaNode.Type().LogicalType()
	if logicalType == nil {
		return value, nil
	}
	if logicalType.Timestamp != nil {
		return coerceTimestampForEncode(value, logicalType.Timestamp)
	} else if logicalType.Date != nil {
		return coerceDateForEncode(value)
	} else if logicalType.Time != nil {
		return coerceTimeForEncode(value, logicalType.Time)
	} else if logicalType.Json != nil {
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encoding value as JSON: %w", err)
		}
		return jsonBytes, nil
	} else if logicalType.UUID != nil {
		switch v := value.(type) {
		case string:
			id, err := uuid.FromString(v)
			if err != nil {
				return nil, fmt.Errorf("parsing string as UUID: %w", err)
			}
			return id.Bytes(), nil
		default:
			return value, nil
		}
	} else if logicalType.Decimal != nil {
		return coerceDecimalForEncode(value, schemaNode, logicalType.Decimal)
	}

	return value, nil
}

// coerceTimestampForEncode converts a value into the parquet physical
// representation for a TIMESTAMP-typed column (int64 in the column's
// declared unit). Accepts:
//
//   - time.Time — produced by the value-side decoder when
//     preserve_logical_types is enabled. Scaled to the column's unit
//     via UnixMilli/UnixMicro/UnixNano.
//   - numeric (int64 / int32 / int / float64) — assumed to be already
//     in the column's declared unit. This matches the column-honouring
//     path the iceberg shredder uses for numeric inputs into time-typed
//     columns; the schema's Unit is the authoritative source of truth.
//   - RFC3339 string — historical path for callers that pre-format
//     timestamps as ISO 8601 strings. Parsed via time.Parse then scaled.
func coerceTimestampForEncode(value any, ts *format.TimestampType) (any, error) {
	scale := func(t time.Time) (int64, error) {
		switch unit := ts.Unit; {
		case unit.Millis != nil:
			return t.UnixMilli(), nil
		case unit.Micros != nil:
			return t.UnixMicro(), nil
		case unit.Nanos != nil:
			return t.UnixNano(), nil
		default:
			return 0, errors.New("unreachable branch while processing parquet timestamp")
		}
	}
	switch v := value.(type) {
	case time.Time:
		return scale(v)
	case string:
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return nil, fmt.Errorf("parsing string RFC3339 timestamp: %w", err)
		}
		return scale(t)
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case int:
		return int64(v), nil
	case float64:
		// int64(NaN) / int64(±Inf) is implementation-defined garbage —
		// reject explicitly so silent corruption (year 1970 or worse)
		// cannot reach the column. Mirrors the iceberg shredder's guard.
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("cannot convert %v to TIMESTAMP", v)
		}
		return int64(v), nil
	default:
		return nil, fmt.Errorf("TIMESTAMP values must be time.Time, RFC3339 string, or numeric; got %T", value)
	}
}

// coerceDateForEncode converts a value into the parquet physical
// representation for a DATE column (int32 days-since-epoch). Accepts
// time.Time (UTC-floored to midnight), numeric (assumed already
// days-since-epoch), and ISO 8601 date strings.
func coerceDateForEncode(value any) (any, error) {
	switch v := value.(type) {
	case time.Time:
		secs := v.UTC().Unix()
		days := secs / 86400
		if secs < 0 && secs%86400 != 0 {
			days--
		}
		return int32(days), nil
	case string:
		t, errRFC := time.Parse(time.RFC3339, v)
		if errRFC != nil {
			t2, errDate := time.Parse("2006-01-02", v)
			if errDate != nil {
				// Surface both attempts — a malformed bare date like
				// "2024-13-99" would otherwise yield only the RFC3339
				// error, which misleadingly suggests the user must add
				// a time component.
				return nil, fmt.Errorf("parsing DATE string %q: tried RFC3339 (%v) and YYYY-MM-DD (%v)", v, errRFC, errDate)
			}
			t = t2
		}
		return int32(t.UTC().Unix() / 86400), nil
	case int32:
		return v, nil
	case int64:
		return int32(v), nil
	case int:
		return int32(v), nil
	case float64:
		// int32(NaN) / int32(±Inf) is implementation-defined garbage —
		// reject explicitly so silent corruption cannot reach the column.
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("cannot convert %v to DATE", v)
		}
		return int32(v), nil
	default:
		return nil, fmt.Errorf("DATE values must be time.Time, date string, or numeric days-since-epoch; got %T", value)
	}
}

// coerceTimeForEncode converts a value into the parquet physical
// representation for a TIME column (int32 millis for millis unit, int64
// otherwise). Accepts time.Duration, time.Time (wall-clock portion),
// and numeric inputs assumed to already be in the column's declared
// unit.
func coerceTimeForEncode(value any, tt *format.TimeType) (any, error) {
	isMillis := tt.Unit.Millis != nil
	durationToUnit := func(d time.Duration) int64 {
		switch unit := tt.Unit; {
		case unit.Millis != nil:
			return d.Milliseconds()
		case unit.Micros != nil:
			return d.Microseconds()
		case unit.Nanos != nil:
			return d.Nanoseconds()
		default:
			return int64(d)
		}
	}
	wrap := func(n int64) any {
		if isMillis {
			return int32(n)
		}
		return n
	}
	switch v := value.(type) {
	case time.Duration:
		return wrap(durationToUnit(v)), nil
	case time.Time:
		d := time.Duration(v.Hour())*time.Hour +
			time.Duration(v.Minute())*time.Minute +
			time.Duration(v.Second())*time.Second +
			time.Duration(v.Nanosecond())*time.Nanosecond
		return wrap(durationToUnit(d)), nil
	case int64:
		return wrap(v), nil
	case int32:
		return wrap(int64(v)), nil
	case int:
		return wrap(int64(v)), nil
	case float64:
		// int64(NaN) / int64(±Inf) is implementation-defined garbage —
		// reject explicitly so silent corruption cannot reach the column.
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("cannot convert %v to TIME", v)
		}
		return wrap(int64(v)), nil
	default:
		return nil, fmt.Errorf("TIME values must be time.Duration, time.Time, or numeric; got %T", value)
	}
}

// coerceDecimalForEncode converts a canonical decimal string (the value
// contract for schema.Decimal fields) into the parquet physical form
// matching the column's declared precision: int32 for p<=9, int64 for p<=18,
// fixed-length bytes for p<=38.
func coerceDecimalForEncode(value any, schemaNode parquet.Node, dt *format.DecimalType) (any, error) {
	s, ok := value.(string)
	if !ok {
		// Already in a physical form; trust the caller.
		return value, nil
	}
	unscaled, err := schema.ParseDecimal(s, dt.Scale)
	if err != nil {
		return nil, fmt.Errorf("parsing decimal %q at scale %d: %w", s, dt.Scale, err)
	}
	switch schemaNode.Type().Kind() {
	case parquet.Int32:
		return int32(unscaled.Int64()), nil
	case parquet.Int64:
		return unscaled.Int64(), nil
	case parquet.FixedLenByteArray:
		return parquetdecimal.EncodeBytes(unscaled, int(dt.Precision)), nil
	default:
		return nil, fmt.Errorf("unsupported parquet physical type for decimal: %v", schemaNode.Type().Kind())
	}
}

type decodingCoercionVisitor struct {
	version int
}

func (d *decodingCoercionVisitor) visitLeaf(value any, schemaNode parquet.Node) (any, error) {
	logicalType := schemaNode.Type().LogicalType()
	if logicalType == nil {
		return value, nil
	}

	if d.version >= 1 {
		if logicalType.Timestamp != nil {
			tsNum, ok := value.(int64)
			if !ok {
				return nil, fmt.Errorf("decoding timestamp but physical type is not an integer: %T", value)
			}

			schemaSpec := logicalType.Timestamp
			var ts time.Time
			switch {
			case schemaSpec.Unit.Millis != nil:
				ts = time.UnixMilli(tsNum)
			case schemaSpec.Unit.Micros != nil:
				ts = time.UnixMicro(tsNum)
			case schemaSpec.Unit.Nanos != nil:
				ts = time.Unix(tsNum/1e9, tsNum%1e9)
			default:
				return nil, errors.New("unreachable branch while processing parquet timestamp")
			}
			if schemaSpec.IsAdjustedToUTC {
				return ts.UTC(), nil
			} else {
				return ts.Local(), nil
			}
		} else if logicalType.UUID != nil {
			uuidBytes, ok := value.([]byte)
			if !ok {
				return nil, fmt.Errorf("decoding UUID, physical type is not []byte: %T", value)
			}
			id, err := uuid.FromBytes(uuidBytes)
			if err != nil {
				return nil, fmt.Errorf("parsing value as UUID: %w", err)
			}
			return id.String(), nil
		}
	}

	return value, nil
}
