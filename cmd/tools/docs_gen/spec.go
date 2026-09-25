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

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

// The types below mirror the JSON produced by ConfigSchema.MarshalJSONV0 (the
// same document `redpanda-connect list --format json-full` prints). Working
// from the JSON rather than the benthos template structs keeps field presence
// (for example a default of null versus no default) intact.

type fieldSpec struct {
	Name             string            `json:"name"`
	Type             string            `json:"type"`
	Kind             string            `json:"kind"`
	Description      string            `json:"description"`
	Interpolated     bool              `json:"interpolated"`
	IsSecret         bool              `json:"is_secret"`
	IsAdvanced       bool              `json:"is_advanced"`
	IsDeprecated     bool              `json:"is_deprecated"`
	IsOptional       bool              `json:"is_optional"`
	Default          json.RawMessage   `json:"default"`
	Examples         []json.RawMessage `json:"examples"`
	AnnotatedOptions [][2]string       `json:"annotated_options"`
	Options          []string          `json:"options"`
	Version          string            `json:"version"`
	Children         []fieldSpec       `json:"children"`
}

func (f fieldSpec) hasDefault() bool { return f.Default != nil }

func (f fieldSpec) defaultValue() any { return decodeValue(f.Default) }

type componentExample struct {
	Title   string `json:"title"`
	Summary string `json:"summary"`
	Config  string `json:"config"`
}

type componentSpec struct {
	Name        string             `json:"name"`
	Type        string             `json:"type"`
	Status      string             `json:"status"`
	Summary     string             `json:"summary"`
	Description string             `json:"description"`
	Version     string             `json:"version"`
	Config      fieldSpec          `json:"config"`
	Examples    []componentExample `json:"examples"`
}

type bloblangParam struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
	IsOptional  bool   `json:"is_optional"`
}

type bloblangExample struct {
	Mapping string      `json:"mapping"`
	Summary string      `json:"summary"`
	Results [][2]string `json:"results"`
}

type bloblangCategory struct {
	Category string `json:"Category"`
}

type bloblangSpec struct {
	Name        string `json:"name"`
	Status      string `json:"status"`
	Description string `json:"description"`
	Params      *struct {
		Named []bloblangParam `json:"named"`
	} `json:"params"`
	Examples   []bloblangExample  `json:"examples"`
	Categories []bloblangCategory `json:"categories"`
}

// componentGroup is one component family, keyed by its name in the schema
// JSON (for example `rate-limits`). The key doubles as the directory for the
// fields and examples partials and the config snippets, which is what
// published pages already include.
type componentGroup struct {
	Key        string
	Components []componentSpec
}

type fullSchema struct {
	Groups            []componentGroup
	BloblangFunctions []bloblangSpec
	BloblangMethods   []bloblangSpec
}

var componentKeys = []string{
	"buffers", "caches", "inputs", "outputs", "processors",
	"rate-limits", "metrics", "tracers", "scanners",
}

func parseFullSchema(raw []byte) (*fullSchema, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	s := &fullSchema{}
	for _, k := range componentKeys {
		var comps []componentSpec
		if v, ok := doc[k]; ok {
			if err := json.Unmarshal(v, &comps); err != nil {
				return nil, fmt.Errorf("%v: %w", k, err)
			}
		}
		s.Groups = append(s.Groups, componentGroup{Key: k, Components: comps})
	}
	if err := json.Unmarshal(doc["bloblang-functions"], &s.BloblangFunctions); err != nil {
		return nil, fmt.Errorf("bloblang-functions: %w", err)
	}
	if err := json.Unmarshal(doc["bloblang-methods"], &s.BloblangMethods); err != nil {
		return nil, fmt.Errorf("bloblang-methods: %w", err)
	}
	return s, nil
}

// decodeValue decodes an arbitrary JSON value, keeping numbers as json.Number
// so they print exactly as the schema declares them.
func decodeValue(raw json.RawMessage) any {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		panic(err)
	}
	return v
}

// pageTypeDir maps a schema key to the directory component pages live in,
// which is also where the description and metadata partials are written.
func pageTypeDir(key string) string {
	if key == "rate-limits" {
		return "rate_limits"
	}
	return key
}

// Sorting and string conversion follow the published docs, which were
// generated with JavaScript: fields sort with a case- and accent-insensitive
// collation, and numbers print the way JavaScript prints them.

var looseCollator = collate.New(language.Und, collate.Loose)

func sortFieldsByName(fields []fieldSpec) []fieldSpec {
	sorted := append([]fieldSpec(nil), fields...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return looseCollator.CompareString(sorted[i].Name, sorted[j].Name) < 0
	})
	return sorted
}

func jsNumber(n json.Number) string {
	f, err := strconv.ParseFloat(string(n), 64)
	if err != nil {
		return string(n)
	}
	if f == 0 {
		return "0"
	}
	if abs := math.Abs(f); abs >= 1e21 || abs < 1e-6 {
		s := strconv.FormatFloat(f, 'e', -1, 64)
		// Go writes 1e+21 and 1e-07; JavaScript writes 1e+21 and 1e-7.
		mant, exp, _ := strings.Cut(s, "e")
		sign := exp[:1]
		exp = strings.TrimLeft(exp[1:], "0")
		return mant + "e" + sign + exp
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// jsString converts a scalar the way JavaScript's String() does.
func jsString(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case json.Number:
		return jsNumber(t)
	case []any:
		parts := make([]string, len(t))
		for i, e := range t {
			if e != nil {
				parts[i] = jsString(e)
			}
		}
		return strings.Join(parts, ",")
	case map[string]any:
		return "[object Object]"
	}
	return fmt.Sprint(v)
}

var arrayIndexKey = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)

// objectKeys returns keys in the order JavaScript iterates a parsed object:
// integer-like keys ascending, then the rest in document order. Go marshals
// maps with sorted keys, so document order is sorted order.
func objectKeys(m map[string]any) []string {
	var ints, rest []string
	for k := range m {
		if arrayIndexKey.MatchString(k) && len(k) < 10 {
			ints = append(ints, k)
		} else {
			rest = append(rest, k)
		}
	}
	sort.Slice(ints, func(i, j int) bool {
		a, _ := strconv.Atoi(ints[i])
		b, _ := strconv.Atoi(ints[j])
		return a < b
	})
	sort.Strings(rest)
	return append(ints, rest...)
}
