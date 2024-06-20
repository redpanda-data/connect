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
