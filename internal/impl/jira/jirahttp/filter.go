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

// filter.go provides utilities for filtering and normalizing Jira data based on requested fields.
// It defines the selectorTree type for building hierarchical field selectors and implements logic to:
//
//   - Construct selector trees from input field lists
//   - Filter JSON payloads by traversing these selectors
//   - Handle custom fields by mapping between Jira's internal keys
//     (e.g. "custom_field_10100") and user-friendly names (e.g. "Story Points")
//   - Normalize input queries so field references are resolved consistently
//
// These helpers are used by the Jira processor to return only the fields
// requested in user queries while preserving correct custom field mappings.

package jirahttp

import (
	"errors"
	"fmt"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// selectorTree is used to build a tree from the elements present in Fields input message
// The tree is then used for filtering output messages and including only what is present in the Fields
type selectorTree map[string]selectorTree

// selectorTreeFrom builds a selectorTree from the Fields []string object
// in the input message used for the attribute filtering
//
// Example: '"fields": ["summary", "assignee.displayName", "status.name", "parent.key", "parent.fields.status.name"]'
// Will result in returning a tree of the form:
//
//	{
//		"assignee": {
//			"displayName": {}
//		},
//		"parent": {
//			"fields": {
//				"status": {
//					"name": {}
//				}
//			},
//			"key": {}
//		},
//		"status": {
//			"name": {}
//		},
//		"summary": {}
//	}
//
// If custom fields are present, they will also be included in the selectorTree
// Example: '"fields": ["summary", "Sprint.name", "assignee.displayName", "Story Points"]'
// Will result in returning a tree of the form:
//
//	{
//	"assignee": {
//		"displayName": {}
//	},
//	"custom_field_10022": {
//		"name": {}
//	},
//	"custom_field_10100": {},
//	"summary": {}
//	}
func selectorTreeFrom(log *service.Logger, fields []string, custom map[string]string) (selectorTree, error) {
	log.Debugf("building selector tree based on filters: %v", fields)
	tree := make(selectorTree)
	for _, field := range fields {
		if strings.TrimSpace(field) == "" {
			return nil, errors.New("invalid field: empty string")
		}
		parts := strings.Split(field, ".")
		cur := tree
		for _, part := range parts {
			if strings.TrimSpace(part) == "" {
				return nil, fmt.Errorf("invalid field path: %q", field)
			}
			if _, ok := cur[part]; !ok {
				cur[part] = make(selectorTree)
			}
			cur = cur[part]
		}
	}
	for _, value := range custom {
		if strings.TrimSpace(value) == "" {
			return nil, errors.New("invalid field: empty string")
		}
		if _, ok := tree[value]; !ok {
			tree[value] = make(selectorTree)
		}
	}
	return tree, nil
}

// The filter function takes the data JSON and selectorTree and returns only what is
// found in the selectorTree by comparing keys from data and keys from selectorTree.
// If customFields are present in the data, they will also be replaced with their real name;
// example: custom_field_10100 will be replaced with "Story Points"
func (j *JiraProc) filter(data any, selectors selectorTree, custom map[string]string) (any, error) {
	switch val := data.(type) {
	case map[string]any:
		res := make(map[string]any)
		for key, sub := range selectors {
			if subData, ok := val[key]; ok {
				if len(sub) > 0 {
					filtered, err := j.filter(subData, sub, custom)
					if err != nil {
						return nil, err
					}
					if value, exists := custom[key]; exists {
						res[value] = filtered
					} else {
						res[key] = filtered
					}
				} else {
					if value, exists := custom[key]; exists {
						res[value] = subData
					} else {
						res[key] = subData
					}
				}
			}
		}
		return res, nil
	case []any:
		out := make([]any, 0, len(val))
		for _, it := range val {
			filtered, err := j.filter(it, selectors, custom)
			if err != nil {
				return nil, err
			}
			out = append(out, filtered)
		}
		return out, nil
	case nil:
		return nil, nil
	default:
		if len(selectors) > 0 {
			return nil, errors.New("type mismatch: expected object/array but got primitive")
		}
		return val, nil
	}
}

// reverseCustomFields creates a new map by swapping keys and values from the input map.
// Parameters:
// - m: map[string]string → input map to reverse
// Returns:
// - map[string]string → new map with values as keys and keys as values
func reverseCustomFields(m map[string]string) map[string]string {
	r := make(map[string]string, len(m))
	for k, v := range m {
		r[v] = k
	}
	return r
}

// normalizeInputFields replaces field names in the query with their corresponding  custom field keys when available.
// Parameters:
// - q: *jsonInputQuery → query object containing the list of fields
// - custom: map[string]string → mapping of display names to custom field keys
// Returns:
// - none (modifies q.Fields in place)
func normalizeInputFields(q *jsonInputQuery, custom map[string]string) {
	for i, v := range q.Fields {
		if dot := strings.Index(v, "."); dot != -1 {
			if cf, ok := custom[v[:dot]]; ok {
				q.Fields[i] = cf + v[dot:]
			}
		} else if cf, ok := custom[v]; ok {
			q.Fields[i] = cf
		}
	}
}
