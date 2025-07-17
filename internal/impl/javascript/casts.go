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

func getMapFromValue(val goja.Value) (map[string]any, error) {
	outVal := val.Export()
	v, ok := outVal.(map[string]any)
	if !ok {
		return nil, errors.New("value is not of type map")
	}
	return v, nil
}

func getSliceFromValue(val goja.Value) ([]any, error) {
	outVal := val.Export()
	v, ok := outVal.([]any)
	if !ok {
		return nil, errors.New("value is not of type slice")
	}
	return v, nil
}

func getMapSliceFromValue(val goja.Value) ([]map[string]any, error) {
	outVal := val.Export()
	if v, ok := outVal.([]map[string]any); ok {
		return v, nil
	}
	vSlice, ok := outVal.([]any)
	if !ok {
		return nil, errors.New("value is not of type map slice")
	}
	v := make([]map[string]any, len(vSlice))
	for i, e := range vSlice {
		v[i], ok = e.(map[string]any)
		if !ok {
			return nil, errors.New("value is not of type map slice")
		}
	}
	return v, nil
}
