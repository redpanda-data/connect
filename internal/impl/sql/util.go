package sql

import (
	"database/sql"
)

func sqlRowsToArray(rows *sql.Rows) ([]interface{}, error) {
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	jArray := []interface{}{}
	for rows.Next() {
		values := make([]interface{}, len(columnNames))
		valuesWrapped := make([]interface{}, len(columnNames))
		for i := range values {
			valuesWrapped[i] = &values[i]
		}
		if err := rows.Scan(valuesWrapped...); err != nil {
			return nil, err
		}
		jObj := map[string]interface{}{}
		for i, v := range values {
			switch t := v.(type) {
			case string:
				jObj[columnNames[i]] = t
			case []byte:
				jObj[columnNames[i]] = string(t)
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				jObj[columnNames[i]] = t
			case float32, float64:
				jObj[columnNames[i]] = t
			case bool:
				jObj[columnNames[i]] = t
			default:
				jObj[columnNames[i]] = t
			}
		}
		jArray = append(jArray, jObj)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return jArray, nil
}

func sqlRowToMap(rows *sql.Rows) (map[string]interface{}, error) {
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	values := make([]interface{}, len(columnNames))
	valuesWrapped := make([]interface{}, len(columnNames))
	for i := range values {
		valuesWrapped[i] = &values[i]
	}
	if err := rows.Scan(valuesWrapped...); err != nil {
		return nil, err
	}
	jObj := map[string]interface{}{}
	for i, v := range values {
		switch t := v.(type) {
		case string:
			jObj[columnNames[i]] = t
		case []byte:
			jObj[columnNames[i]] = string(t)
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			jObj[columnNames[i]] = t
		case float32, float64:
			jObj[columnNames[i]] = t
		case bool:
			jObj[columnNames[i]] = t
		default:
			jObj[columnNames[i]] = t
		}
	}
	return jObj, nil
}
