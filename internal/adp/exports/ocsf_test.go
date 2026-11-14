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

package exports_test

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// ocsfSchema is the compiled OCSF schema (schema.ocsf.io export), trimmed to
// the classes the pipeline emits. The validator below applies the checks the
// official OCSF validator applies: every attribute must exist on the event's
// class (attributes contributed by a profile count only when the event
// declares that profile in metadata.profiles), required attributes must be
// present, values must have the attribute's type, enum values must be known,
// and object constraints (at_least_one, just_one) must hold.
type ocsfSchema struct {
	Version string                `json:"version"`
	Classes map[string]ocsfClass  `json:"classes"`
	Objects map[string]ocsfObject `json:"objects"`
	Types   map[string]string     `json:"types"`
	byUID   map[int]ocsfClass
}

type ocsfClass struct {
	Name        string                   `json:"name"`
	UID         int                      `json:"uid"`
	Profiles    []string                 `json:"profiles"`
	Constraints map[string][]string      `json:"constraints"`
	Attributes  map[string]ocsfAttribute `json:"attributes"`
}

type ocsfObject struct {
	Name        string                   `json:"name"`
	Constraints map[string][]string      `json:"constraints"`
	Attributes  map[string]ocsfAttribute `json:"attributes"`
}

type ocsfAttribute struct {
	Type        string   `json:"type"`
	ObjectType  string   `json:"object_type"`
	IsArray     bool     `json:"is_array"`
	Requirement string   `json:"requirement"`
	Profiles    []string `json:"profiles"`
	Enum        []string `json:"enum"`
}

var (
	ocsfOnce   sync.Once
	ocsfLoaded *ocsfSchema
	ocsfErr    error
)

func loadOCSFSchema(t *testing.T) *ocsfSchema {
	t.Helper()
	ocsfOnce.Do(func() {
		b, err := os.ReadFile("testdata/ocsf-1.8.0.json")
		if err != nil {
			ocsfErr = err
			return
		}
		var s ocsfSchema
		if err := json.Unmarshal(b, &s); err != nil {
			ocsfErr = err
			return
		}
		s.byUID = map[int]ocsfClass{}
		for _, c := range s.Classes {
			s.byUID[c.UID] = c
		}
		ocsfLoaded = &s
	})
	require.NoError(t, ocsfErr)
	return ocsfLoaded
}

// requireValidOCSF fails the test if event is not a valid OCSF event of its
// class, listing every problem found.
func requireValidOCSF(t *testing.T, name string, event map[string]any) {
	t.Helper()
	problems := loadOCSFSchema(t).validate(event)
	require.Empty(t, problems, "%s is not valid OCSF %s", name, loadOCSFSchema(t).Version)
}

func (s *ocsfSchema) validate(event map[string]any) []string {
	uid, ok := asInt(event["class_uid"])
	if !ok {
		return []string{"class_uid missing or not an integer"}
	}
	class, ok := s.byUID[uid]
	if !ok {
		return []string{fmt.Sprintf("class_uid %d is not a class this schema knows", uid)}
	}

	var profiles []string
	if meta, ok := event["metadata"].(map[string]any); ok {
		if list, ok := meta["profiles"].([]any); ok {
			for _, p := range list {
				if ps, ok := p.(string); ok {
					profiles = append(profiles, ps)
				}
			}
		}
	}
	var problems []string
	for _, p := range profiles {
		if !slices.Contains(class.Profiles, p) {
			problems = append(problems, fmt.Sprintf("metadata.profiles declares %q, which class %s does not support", p, class.Name))
		}
	}
	return append(problems, s.validateAttributes("", class.Attributes, class.Constraints, event, profiles)...)
}

func (s *ocsfSchema) validateAttributes(path string, attrs map[string]ocsfAttribute, constraints map[string][]string, obj map[string]any, profiles []string) []string {
	var problems []string
	at := func(key string) string {
		if path == "" {
			return key
		}
		return path + "." + key
	}
	enabled := func(a ocsfAttribute) bool {
		if len(a.Profiles) == 0 {
			return true
		}
		return slices.ContainsFunc(a.Profiles, func(p string) bool { return slices.Contains(profiles, p) })
	}

	for key, value := range obj {
		attr, known := attrs[key]
		switch {
		case !known:
			problems = append(problems, fmt.Sprintf("%s: unknown attribute", at(key)))
			continue
		case !enabled(attr):
			problems = append(problems, fmt.Sprintf("%s: attribute belongs to profile(s) %v, which the event does not declare in metadata.profiles", at(key), attr.Profiles))
			continue
		}
		if attr.IsArray {
			list, ok := value.([]any)
			if !ok {
				problems = append(problems, fmt.Sprintf("%s: expected an array", at(key)))
				continue
			}
			for i, item := range list {
				problems = append(problems, s.validateValue(fmt.Sprintf("%s[%d]", at(key), i), attr, item, profiles)...)
			}
			continue
		}
		problems = append(problems, s.validateValue(at(key), attr, value, profiles)...)
	}

	for key, attr := range attrs {
		if attr.Requirement == "required" && enabled(attr) {
			if _, present := obj[key]; !present {
				problems = append(problems, fmt.Sprintf("%s: required attribute missing", at(key)))
			}
		}
	}

	for kind, keys := range constraints {
		n := 0
		for _, k := range keys {
			if _, present := obj[k]; present {
				n++
			}
		}
		switch kind {
		case "at_least_one":
			if n == 0 {
				problems = append(problems, fmt.Sprintf("%s: at least one of %v is required", orRoot(path), keys))
			}
		case "just_one":
			if n != 1 {
				problems = append(problems, fmt.Sprintf("%s: exactly one of %v is required, found %d", orRoot(path), keys, n))
			}
		}
	}
	return problems
}

func (s *ocsfSchema) validateValue(path string, attr ocsfAttribute, value any, profiles []string) []string {
	if attr.Type == "object_t" {
		obj, ok := value.(map[string]any)
		if !ok {
			return []string{fmt.Sprintf("%s: expected an object", path)}
		}
		if attr.ObjectType == "object" {
			return nil // free-form (e.g. unmapped)
		}
		def, ok := s.Objects[attr.ObjectType]
		if !ok {
			return []string{fmt.Sprintf("%s: object type %q is not in the schema", path, attr.ObjectType)}
		}
		return s.validateAttributes(path, def.Attributes, def.Constraints, obj, profiles)
	}

	base := attr.Type
	for range 4 {
		if next, ok := s.Types[base]; ok && next != base {
			base = next
			continue
		}
		break
	}
	switch base {
	case "string_t":
		if _, ok := value.(string); !ok {
			return []string{fmt.Sprintf("%s: expected a string (%s)", path, attr.Type)}
		}
	case "integer_t", "long_t":
		n, ok := asInt(value)
		if !ok {
			return []string{fmt.Sprintf("%s: expected an integer (%s), got %v", path, attr.Type, value)}
		}
		if len(attr.Enum) > 0 && !slices.Contains(attr.Enum, strconv.Itoa(n)) {
			return []string{fmt.Sprintf("%s: %d is not one of the enum values %v", path, n, attr.Enum)}
		}
	case "float_t":
		if _, ok := value.(float64); !ok {
			return []string{fmt.Sprintf("%s: expected a number (%s)", path, attr.Type)}
		}
	case "boolean_t":
		if _, ok := value.(bool); !ok {
			return []string{fmt.Sprintf("%s: expected a boolean", path)}
		}
	case "json_t":
	default:
		return []string{fmt.Sprintf("%s: attribute type %q is not handled by this validator", path, attr.Type)}
	}
	return nil
}

func asInt(v any) (int, bool) {
	f, ok := v.(float64)
	if !ok || f != math.Trunc(f) {
		return 0, false
	}
	return int(f), true
}

func orRoot(path string) string {
	if path == "" {
		return "event"
	}
	return path
}

// TestExpectedOutputIsValidOCSF checks the golden pipeline output against the
// OCSF schema, so the fixtures themselves can never drift into something the
// official validator would reject.
func TestExpectedOutputIsValidOCSF(t *testing.T) {
	expected := loadNDJSON(t, "testdata/expected.ndjson")
	fixtures := loadFixtures(t, "testdata/events.jsonl")
	require.Len(t, expected, len(fixtures))
	for i, rec := range expected {
		requireValidOCSF(t, fixtures[i].Name, rec)
	}
}

// TestOCSFValidatorCatchesKnownProblems pins the validator's own behaviour
// on the shapes the producer emits that OCSF rejects, so a regression in
// the pipeline's conformance mapping cannot slip past a validator that has
// quietly stopped checking.
func TestOCSFValidatorCatchesKnownProblems(t *testing.T) {
	expected := loadNDJSON(t, "testdata/expected.ndjson")
	var apiActivity, entityManagement map[string]any
	for _, rec := range expected {
		switch rec["class_uid"] {
		case float64(6003):
			if apiActivity == nil {
				apiActivity = rec
			}
		case float64(3004):
			if entityManagement == nil {
				entityManagement = rec
			}
		}
	}
	require.NotNil(t, apiActivity)
	require.NotNil(t, entityManagement)
	schema := loadOCSFSchema(t)

	for name, tc := range map[string]struct {
		base   map[string]any
		mutate func(map[string]any)
		want   string
	}{
		"entity on api_activity": {apiActivity, func(e map[string]any) { e["entity"] = map[string]any{"uid": "x"} }, "entity: unknown attribute"},
		"trace without the trace profile": {apiActivity, func(e map[string]any) {
			e["trace"] = map[string]any{"uid": "x"}
			e["metadata"].(map[string]any)["profiles"] = []any{"security_control"}
		}, "trace: attribute belongs to profile(s) [trace]"},
		"api on entity_management":       {entityManagement, func(e map[string]any) { e["api"] = map[string]any{"operation": "x"} }, "api: attribute belongs to profile(s) [cloud]"},
		"resources on entity_management": {entityManagement, func(e map[string]any) { e["resources"] = []any{} }, "resources: unknown attribute"},
		"time as a string":               {apiActivity, func(e map[string]any) { e["time"] = "1793610000000" }, "time: expected an integer"},
		"enum as a name":                 {apiActivity, func(e map[string]any) { e["severity_id"] = "SEVERITY_ID_INFORMATIONAL" }, "severity_id: expected an integer"},
		"unknown enum value":             {apiActivity, func(e map[string]any) { e["severity_id"] = float64(42) }, "severity_id: 42 is not one of the enum values"},
		"missing required metadata":      {apiActivity, func(e map[string]any) { delete(e, "metadata") }, "metadata: required attribute missing"},
		"camelCase field name":           {apiActivity, func(e map[string]any) { e["classUid"] = float64(6003) }, "classUid: unknown attribute"},
	} {
		t.Run(name, func(t *testing.T) {
			var event map[string]any
			b, err := json.Marshal(tc.base)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(b, &event))
			tc.mutate(event)
			problems := schema.validate(event)
			require.NotEmpty(t, problems, "validator accepted a known-bad event")
			require.Contains(t, fmt.Sprint(problems), tc.want)
		})
	}
}
