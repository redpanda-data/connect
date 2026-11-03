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

package parquet

import (
	"encoding/json"
	"maps"
	"slices"
)

// scrubJSONNumbers replaces json.Number values with int64 or float64. When
// mutable is false v is left unchanged, and any container holding a replaced
// value is copied rather than modified.
func scrubJSONNumbers(v any, mutable bool) any {
	v, _ = scrubJSONNumbersChanged(v, mutable)
	return v
}

// scrubJSONNumbersChanged is scrubJSONNumbers that also reports whether
// anything was replaced.
func scrubJSONNumbersChanged(v any, mutable bool) (any, bool) {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i, true
		}
		if f, err := t.Float64(); err == nil {
			return f, true
		}
		return 0, true
	case map[string]any:
		out, owned, changed := t, mutable, false
		for k, child := range t {
			if newChild, ok := scrubJSONNumbersChanged(child, mutable); ok {
				if !owned {
					out, owned = maps.Clone(t), true
				}
				out[k], changed = newChild, true
			}
		}
		return out, changed
	case []any:
		out, owned, changed := t, mutable, false
		for i, child := range t {
			if newChild, ok := scrubJSONNumbersChanged(child, mutable); ok {
				if !owned {
					out, owned = slices.Clone(t), true
				}
				out[i], changed = newChild, true
			}
		}
		return out, changed
	}
	return v, false
}
