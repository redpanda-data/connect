package parquet

import "encoding/json"

func scrubJSONNumbers(v any) any {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		if f, err := t.Float64(); err == nil {
			return f
		}
		return 0
	case map[string]any:
		scrubJSONNumbersObj(t)
		return t
	case []any:
		scrubJSONNumbersArr(t)
		return t
	}
	return v
}

func scrubJSONNumbersObj(obj map[string]any) {
	for k, v := range obj {
		obj[k] = scrubJSONNumbers(v)
	}
}

func scrubJSONNumbersArr(arr []any) {
	for i, v := range arr {
		arr[i] = scrubJSONNumbers(v)
	}
}
