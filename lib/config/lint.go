// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package config

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

// Rules regarding object key/value combinations for paths.
var keyValueRules = []func(path, key string, value interface{}) []string{
	// Check for batch processor outside of input section.
	func(path, key string, value interface{}) []string {
		valueStr, ok := value.(string)
		if !ok {
			return nil
		}
		if key == "type" && valueStr == "batch" {
			if !strings.HasPrefix(path, "input.") {
				return []string{fmt.Sprintf("%v: Type 'batch' is unsafe outside of the 'input' section, for more information read https://docs.benthos.dev/processors/#batch", path)}
			}
		}
		return nil
	},
}

func lintWalkObj(path string, raw, processed map[interface{}]interface{}) []string {
	lints := []string{}

	keys := []string{}
	for k := range raw {
		keys = append(keys, fmt.Sprintf("%v", k))
	}
	sort.Strings(keys)
	for _, k := range keys {
		y := raw[k]
		x, exists := processed[k]
		if !exists {
			lints = append(lints, fmt.Sprintf("%v: Key '%v' found but is ignored", path, k))
			continue
		}
		var newPath string
		if len(path) > 0 {
			newPath = fmt.Sprintf("%v.%v", path, k)
		} else {
			newPath = fmt.Sprintf("%v", k)
		}
		for _, rule := range keyValueRules {
			lints = append(lints, rule(newPath, k, y)...)
		}
		if l := lintWalk(newPath, y, x); len(l) > 0 {
			lints = append(lints, l...)
		}
	}
	return lints
}

func mapToObjMap(m map[string]interface{}) map[interface{}]interface{} {
	om := make(map[interface{}]interface{}, len(m))
	for k, v := range m {
		om[k] = v
	}
	return om
}

func getObjMap(v interface{}) (map[interface{}]interface{}, bool) {
	switch t := v.(type) {
	case map[interface{}]interface{}:
		return t, true
	case map[string]interface{}:
		return mapToObjMap(t), true
	}
	return nil, false
}

func lintWalk(path string, raw, processed interface{}) []string {
	switch x := processed.(type) {
	case map[interface{}]interface{}:
		y, ok := getObjMap(raw)
		if !ok {
			return []string{fmt.Sprintf("%v: wrong type detected. Expected object but found %T", path, raw)}
		}
		return lintWalkObj(path, y, x)
	case map[string]interface{}:
		y, ok := getObjMap(raw)
		if !ok {
			return []string{fmt.Sprintf("%v: wrong type detected. Expected object but found %T", path, raw)}
		}
		return lintWalkObj(path, y, mapToObjMap(x))
	case []interface{}:
		y, ok := raw.([]interface{})
		if !ok {
			return []string{fmt.Sprintf("%v: wrong type detected. Expected array but found %T", path, raw)}
		}
		lints := []string{}
		for i, v := range y {
			if i >= len(x) {
				break
			}
			if l := lintWalk(fmt.Sprintf("%v[%v]", path, i), v, x[i]); len(l) > 0 {
				lints = append(lints, l...)
			}
		}
		return lints
	default:
		// We assume that any other type will match since the parses should
		// enforce that themselves.
	}
	return nil
}

//------------------------------------------------------------------------------

// Lint attempts to report errors within a user config. Returns a slice of lint
// results.
func Lint(rawBytes []byte, config Type) ([]string, error) {
	var raw, processed interface{}
	if err := yaml.Unmarshal(rawBytes, &raw); err != nil {
		return nil, err
	}
	sanit, err := config.Sanitised()
	if err != nil {
		return nil, err
	}
	if processedBytes, err := yaml.Marshal(sanit); err != nil {
		return nil, err
	} else if err = yaml.Unmarshal(processedBytes, &processed); err != nil {
		return nil, err
	}
	return lintWalk("", raw, processed), nil
}

//------------------------------------------------------------------------------
