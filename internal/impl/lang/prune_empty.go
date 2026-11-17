// Copyright 2026 Redpanda Data, Inc.
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

package lang

// pruneEmpty returns v without null, [] and {} children, at all depths. If
// strs is true, it also removes "" children. The root value is never removed.
//
// The check runs after the children are pruned, so a container with only
// empty children is removed too. Each object and array in the result is a new
// value, so the caller can change the result and v stays the same. Only
// map[string]any and []any are containers. Other values (json.Number, bytes,
// timestamps, other map and slice types) are returned as they are.
func pruneEmpty(v any, strs bool) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, c := range t {
			if c = pruneEmpty(c, strs); !isPrunable(c, strs) {
				out[k] = c
			}
		}
		return out
	case []any:
		out := make([]any, 0, len(t))
		for _, c := range t {
			if c = pruneEmpty(c, strs); !isPrunable(c, strs) {
				out = append(out, c)
			}
		}
		return out
	}
	return v
}

// isPrunable reports whether pruneEmpty removes the already pruned child v
// from its parent.
func isPrunable(v any, strs bool) bool {
	switch t := v.(type) {
	case nil:
		return true
	case map[string]any:
		return len(t) == 0
	case []any:
		return len(t) == 0
	case string:
		return strs && t == ""
	}
	return false
}
