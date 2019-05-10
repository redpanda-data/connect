// Copyright (c) 2019 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, sub to the following conditions:
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

import "sort"

//------------------------------------------------------------------------------

// GetInferenceCandidates checks a generic config structure (YAML or JSON) and,
// if an explicit type value is not found, returns a list of candidate types by
// walking the root objects field names.
func GetInferenceCandidates(raw interface{}) []string {
	switch t := raw.(type) {
	case map[string]interface{}:
		if _, exists := t["type"]; exists {
			return nil
		}
		candidates := make([]string, 0, len(t))
		for k := range t {
			candidates = append(candidates, k)
		}
		sort.Strings(candidates)
		return candidates
	case map[interface{}]interface{}:
		if _, exists := t["type"]; exists {
			return nil
		}
		candidates := make([]string, 0, len(t))
		for k := range t {
			if kStr, isStr := k.(string); isStr {
				candidates = append(candidates, kStr)
			}
		}
		sort.Strings(candidates)
		return candidates
	}
	return nil
}

//------------------------------------------------------------------------------
