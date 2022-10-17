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
