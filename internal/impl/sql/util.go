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

package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/pgvector/pgvector-go"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func sqlRowsToArray(rows *sql.Rows) ([]any, error) {
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	jArray := []any{}
	for rows.Next() {
		values := make([]any, len(columnNames))
		valuesWrapped := make([]any, 0, len(columnNames))
		for i := range values {
			valuesWrapped = append(valuesWrapped, &values[i])
		}
		if err := rows.Scan(valuesWrapped...); err != nil {
			return nil, err
		}
		jObj := map[string]any{}
		for i, v := range values {
			col := columnNames[i]
			switch t := v.(type) {
			case string:
				jObj[col] = t
			case []byte:
				jObj[col] = string(t)
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				jObj[col] = t
			case float32, float64:
				jObj[col] = t
			case bool:
				jObj[col] = t
			default:
				jObj[col] = t
			}
		}
		jArray = append(jArray, jObj)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return jArray, nil
}

func sqlRowToMap(rows *sql.Rows) (map[string]any, error) {
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	values := make([]any, len(columnNames))
	valuesWrapped := make([]any, 0, len(columnNames))
	for i := range values {
		valuesWrapped = append(valuesWrapped, &values[i])
	}
	if err := rows.Scan(valuesWrapped...); err != nil {
		return nil, err
	}
	jObj := map[string]any{}
	for i, v := range values {
		col := columnNames[i]
		switch t := v.(type) {
		case string:
			jObj[col] = t
		case []byte:
			jObj[col] = string(t)
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			jObj[col] = t
		case float32, float64:
			jObj[col] = t
		case bool:
			jObj[col] = t
		default:
			jObj[col] = t
		}
	}
	return jObj, nil
}

type argsConverter func([]any) []any

func bloblValuesToPgSQLValues(v []any) []any {
	hasVector := slices.ContainsFunc(v, func(e any) bool {
		_, ok := e.(vector)
		return ok
	})
	// Don't allocate the output array if there are no vectors
	if !hasVector {
		return v
	}
	o := make([]any, len(v))
	for i, e := range v {
		vec, ok := e.(vector)
		if ok {
			o[i] = pgvector.NewVector(vec.value)
		} else {
			o[i] = e
		}
	}
	return o
}

// newClickhouseArgsConverter builds an argsConverter that coerces Bloblang
// output values into the concrete Go types the ClickHouse driver expects.
//
// Bloblang produces generic types like map[string]any, but the ClickHouse
// driver requires maps and slices to have concrete key/value types matching
// the column schema (e.g. map[string]int64 for Map(String, Int64)). Without
// this conversion, inserts with Map columns fail with a type mismatch.
//
// At connect time we introspect the column scan types once, then use them on
// every batch to normalize only the arguments that need it.
func newClickhouseArgsConverter(ctx context.Context, db *sql.DB, table string, columns []string, logger *service.Logger) (argsConverter, error) {
	scanTypes, err := clickhouseColumnScanTypes(ctx, db, table, columns)
	if err != nil {
		return nil, err
	}
	return func(v []any) []any {
		o := make([]any, len(v))
		for i, e := range v {
			o[i] = e
			if i >= len(scanTypes) {
				continue
			}
			if normalized, ok := normalizeClickhouseValueToType(e, scanTypes[i]); ok {
				o[i] = normalized
			} else if scanTypes[i] != nil && clickhouseNeedsContainerNormalization(scanTypes[i]) {
				logger.Warnf("ClickHouse: failed to normalize column %q (value type %T, target %v); passing original value to driver", columns[i], e, scanTypes[i])
			}
		}
		return o
	}, nil
}

// clickhouseColumnScanTypes issues a zero-row SELECT to discover the Go types
// the ClickHouse driver uses to scan each column. These reflect.Types tell us
// what concrete types (e.g. map[string]int64) the driver expects for inserts.
func clickhouseColumnScanTypes(ctx context.Context, db *sql.DB, table string, columns []string) ([]reflect.Type, error) {
	query := fmt.Sprintf("SELECT %s FROM %s LIMIT 0", strings.Join(columns, ", "), table)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	scanTypes := make([]reflect.Type, len(columnTypes))
	for i, ct := range columnTypes {
		scanTypes[i] = ct.ScanType()
	}
	return scanTypes, nil
}

// normalizeClickhouseValueToType converts a generic value (typically
// map[string]any from Bloblang) into the concrete Go type the ClickHouse
// driver expects. It works by JSON-round-tripping: marshal the value to JSON,
// then unmarshal into a new instance of the target type. This lets encoding/json
// handle all the recursive key/value type coercion for maps and slices.
//
// Returns (normalized, true) on success, or (nil, false) if the value doesn't
// need normalization (e.g. scalar types) or can't be converted.
func normalizeClickhouseValueToType(v any, target reflect.Type) (any, bool) {
	if target == nil {
		return nil, false
	}
	// Unwrap pointer types, recursing into the element type. Nullable ClickHouse
	// columns (e.g. Nullable(Map(...))) surface as pointer scan types.
	for target.Kind() == reflect.Pointer {
		if v == nil {
			return nil, true
		}
		normalized, ok := normalizeClickhouseValueToType(v, target.Elem())
		if !ok {
			return nil, false
		}
		ptr := reflect.New(target.Elem())
		ptr.Elem().Set(reflect.ValueOf(normalized))
		return ptr.Interface(), true
	}
	// Only normalize container types (maps and slices). Scalar types like
	// string, int64, etc. are already handled correctly by the driver.
	if !clickhouseNeedsContainerNormalization(target) {
		return nil, false
	}

	// Fast path: if the value already has the exact type the driver wants,
	// skip the JSON round-trip entirely.
	value := reflect.ValueOf(v)
	if value.IsValid() && value.Type().AssignableTo(target) {
		return v, true
	}

	// JSON round-trip: marshal the generic value, then decode into the target
	// type so encoding/json coerces all nested keys and values appropriately.
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, false
	}

	normalized := reflect.New(target)
	if err := json.Unmarshal(raw, normalized.Interface()); err != nil {
		return nil, false
	}
	return normalized.Elem().Interface(), true
}

// clickhouseNeedsContainerNormalization returns true for types that require
// JSON round-trip normalization (maps and slices). Scalar types pass through
// to the driver as-is.
func clickhouseNeedsContainerNormalization(target reflect.Type) bool {
	switch target.Kind() {
	case reflect.Map, reflect.Slice:
		return true
	default:
		return false
	}
}

