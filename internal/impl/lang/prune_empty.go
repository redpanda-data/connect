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

import (
	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

func init() {
	pruneEmptySpec := bloblang.NewPluginSpec().
		Experimental().
		Category("Object & Array Manipulation").
		Description("Removes `null` values, empty arrays and empty objects from an object or array, at all depths. "+
			"A container that becomes empty after the removal of its children is removed too. "+
			"Array elements are removed, so the positions of the next elements shift. "+
			"Do not use this method on arrays where the position has a meaning, for example coordinates or rows that match a header. "+
			"The values `0`, `false` and `\"\"` are kept, unless `empty_strings` is `true`, which also removes `\"\"` and empty byte values. "+
			"This method only removes values that hold no data. "+
			"Zeros, `false` and strings that contain only whitespace are data, and this method never removes them. "+
			"The target value itself is never removed: `null` gives `null` and a scalar gives itself. "+
			"The target is not changed. This method is much faster than a recursive mapping that does the same.").
		Version("4.119.0").
		Param(bloblang.NewBoolParam("empty_strings").Description("Also removes empty strings and empty byte values.").Default(false)).
		Example("Remove null values and empty containers at all depths",
			`root = this.prune_empty()`,
			[2]string{
				`{"id":1,"name":null,"tags":[],"meta":{"owner":{"email":null},"score":0},"lines":[null,{},{"sku":"a"}]}`,
				`{"id":1,"lines":[{"sku":"a"}],"meta":{"score":0}}`,
			}).
		Example("Array elements are removed, so the positions of the next elements shift",
			`root = this.prune_empty()`,
			[2]string{
				`{"items":["a",null,"b",[],{"x":null},"c"]}`,
				`{"items":["a","b","c"]}`,
			}).
		Example("Also remove empty strings",
			`root = this.prune_empty(empty_strings: true)`,
			[2]string{
				`{"a":"","b":{"c":""},"d":"x","e":false}`,
				`{"d":"x","e":false}`,
			}).
		Example("Prune only a part of the document",
			`root = this
root.payload = this.payload.prune_empty()`,
			[2]string{
				`{"key":null,"payload":{"a":1,"b":null,"c":[]}}`,
				`{"key":null,"payload":{"a":1}}`,
			}).
		Example("Keep an empty value at one path",
			`root = this.prune_empty()
root.meta.flags = this.meta.flags`,
			[2]string{
				`{"id":1,"note":null,"meta":{"flags":{},"owner":null}}`,
				`{"id":1,"meta":{"flags":{}}}`,
			})

	if err := bloblang.RegisterMethodV2(
		"prune_empty", pruneEmptySpec,
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			strs, err := args.GetBool("empty_strings")
			if err != nil {
				return nil, err
			}
			return func(v any) (any, error) {
				return pruneEmpty(v, strs), nil
			}, nil
		},
	); err != nil {
		panic(err)
	}
}

// pruneEmpty returns v without null, [] and {} children, at all depths. If
// strs is true, it also removes "" and empty []byte children. The root value
// is never removed.
//
// The check runs after the children are pruned, so a container with only
// empty children is removed too. Only map[string]any and []any are
// containers. Each of them in the result is a new value, so the caller can
// add, remove and replace their entries and v stays the same. Other values
// (json.Number, []byte, timestamps, other map and slice types) are returned
// as they are, so the result and v share them. A change in place to one of
// these also changes v.
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
	case []byte:
		// Bloblang compares bytes and strings as equal, and an empty bytes
		// value serializes to "". Thus the empty_strings mode removes it too.
		return strs && len(t) == 0
	}
	return false
}
