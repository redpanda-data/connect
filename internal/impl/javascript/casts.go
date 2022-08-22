package javascript

import (
	"errors"

	"github.com/dop251/goja"
)

func getMapFromValue(val goja.Value) (map[string]interface{}, error) {
	outVal := val.Export()
	v, ok := outVal.(map[string]interface{})
	if !ok {
		return nil, errors.New("value is not of type map")
	}
	return v, nil
}

func getSliceFromValue(val goja.Value) ([]interface{}, error) {
	outVal := val.Export()
	v, ok := outVal.([]interface{})
	if !ok {
		return nil, errors.New("value is not of type slice")
	}
	return v, nil
}

func getMapSliceFromValue(val goja.Value) ([]map[string]interface{}, error) {
	outVal := val.Export()
	if v, ok := outVal.([]map[string]interface{}); ok {
		return v, nil
	}
	vSlice, ok := outVal.([]interface{})
	if !ok {
		return nil, errors.New("value is not of type map slice")
	}
	v := make([]map[string]interface{}, len(vSlice))
	for i, e := range vSlice {
		v[i], ok = e.(map[string]interface{})
		if !ok {
			return nil, errors.New("value is not of type map slice")
		}
	}
	return v, nil
}
