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
	"database/sql"
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
