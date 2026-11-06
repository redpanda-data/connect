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
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/public/schema"
)

const statusDeprecated = "deprecated"

// deprecatedBloblang returns the names of the deprecated Bloblang functions
// and methods registered in env.
func deprecatedBloblang(env *bloblang.Environment) (funcs, methods []string) {
	env.WalkFunctions(func(name string, v *bloblang.FunctionView) {
		if v.TemplateData().Status == statusDeprecated {
			funcs = append(funcs, name)
		}
	})
	env.WalkMethods(func(name string, v *bloblang.MethodView) {
		if v.TemplateData().Status == statusDeprecated {
			methods = append(methods, name)
		}
	})
	return funcs, methods
}

// withoutDeprecatedBloblang returns env with every deprecated Bloblang
// function and method removed, so a mapping that uses one fails to parse.
func withoutDeprecatedBloblang(env *bloblang.Environment) *bloblang.Environment {
	funcs, methods := deprecatedBloblang(env)
	return env.WithoutFunctions(funcs...).WithoutMethods(methods...)
}

// lintCounts counts each lint message so two lint runs can be compared.
func lintCounts(lints []service.Lint) map[string]int {
	m := map[string]int{}
	for _, l := range lints {
		m[l.Error()]++
	}
	return m
}

// deprecatedUse is one deprecated component, field, Bloblang function or
// Bloblang method that a published example uses.
type deprecatedUse struct {
	example string // such as `processors/metric example "Counter"`
	what    string // the lint or parse error that names the deprecated item
}

// firstLine drops the source excerpt that Bloblang errors end with.
func firstLine(s string) string {
	return strings.TrimSpace(strings.SplitN(s, "\n", 2)[0])
}

// deprecationCheckers holds the schema and the plain and strict linters and
// Bloblang environments that the deprecation tests compare.
type deprecationCheckers struct {
	full      *service.ConfigSchema
	raw       []byte
	blobl     *bloblang.Environment
	stripped  *bloblang.Environment
	plainEnv  *service.Environment
	strictEnv *service.Environment
	plain     *service.StreamConfigLinter
	strict    *service.StreamConfigLinter
}

func newDeprecationCheckers(t *testing.T) *deprecationCheckers {
	t.Helper()

	full := schema.Standard("", "")
	raw, err := full.MarshalJSONV0()
	require.NoError(t, err)

	blobl := bloblang.GlobalEnvironment()
	funcs, methods := deprecatedBloblang(blobl)
	require.NotEmpty(t, funcs, "found no deprecated Bloblang functions, so the walk is broken")
	require.NotEmpty(t, methods, "found no deprecated Bloblang methods, so the walk is broken")
	stripped := withoutDeprecatedBloblang(blobl)

	// The plain linter shows the lints an example has anyway. The strict one
	// also rejects deprecated components and fields, and lints Bloblang
	// against an environment with the deprecated functions and methods
	// removed. Any lint only the strict one raises is a deprecated use.
	strictEnv := full.Environment().Clone()
	strictEnv.UseBloblangEnvironment(stripped)
	strictSchema := schema.Standard("", "")
	strictSchema.SetEnvironment(strictEnv)
	return &deprecationCheckers{
		full:      full,
		raw:       raw,
		blobl:     blobl,
		stripped:  stripped,
		plainEnv:  full.Environment(),
		strictEnv: strictEnv,
		plain:     full.NewStreamConfigLinter().SetSkipEnvVarCheck(true),
		strict:    strictSchema.NewStreamConfigLinter().SetSkipEnvVarCheck(true).SetRejectDeprecated(true),
	}
}

// lintConfig returns the lints that only the strict linter raises on config.
func (d *deprecationCheckers) lintConfig(t *testing.T, where string, config []byte) []string {
	t.Helper()
	before, err := d.plain.LintYAML(config)
	require.NoError(t, err, where)
	after, err := d.strict.LintYAML(config)
	require.NoError(t, err, where)
	seen := lintCounts(before)
	var extra []string
	for _, l := range after {
		if seen[l.Error()] > 0 {
			seen[l.Error()]--
			continue
		}
		extra = append(extra, firstLine(l.Error()))
	}
	return extra
}

// mappingError returns the parse error of a mapping that parses in the full
// Bloblang environment but not without the deprecated functions and methods.
// Mappings that don't parse at all aren't a deprecation problem; the example
// tests cover them.
func (d *deprecationCheckers) mappingError(mapping string) string {
	if _, err := d.blobl.Parse(mapping); err != nil {
		return ""
	}
	if _, err := d.stripped.Parse(mapping); err != nil {
		return firstLine(err.Error())
	}
	return ""
}

var interpolationSpec = service.NewConfigSpec().Field(service.NewInterpolatedStringField("s"))

func parseInterpolation(env *service.Environment, s string) error {
	b, err := yaml.Marshal(map[string]string{"s": s})
	if err != nil {
		return err
	}
	pc, err := interpolationSpec.ParseYAML(string(b), env)
	if err != nil {
		return err
	}
	_, err = pc.FieldInterpolatedString("s")
	return err
}

// interpolationError is mappingError for an interpolated string.
func (d *deprecationCheckers) interpolationError(s string) string {
	if err := parseInterpolation(d.plainEnv, s); err != nil {
		return ""
	}
	if err := parseInterpolation(d.strictEnv, s); err != nil {
		return firstLine(err.Error())
	}
	return ""
}

// deprecatedExampleUses returns every deprecated use in the component spec
// examples and the Bloblang function and method examples. Examples on a
// deprecated component or Bloblang plugin are left out, since that page is
// about the deprecated thing.
func deprecatedExampleUses(t *testing.T) []deprecatedUse {
	t.Helper()

	d := newDeprecationCheckers(t)
	spec, err := parseFullSchema(d.raw)
	require.NoError(t, err)

	var uses []deprecatedUse
	for _, g := range spec.Groups {
		for _, c := range g.Components {
			if c.Status == statusDeprecated {
				continue
			}
			for _, e := range c.Examples {
				where := fmt.Sprintf("%s/%s example %q", g.Key, c.Name, e.Title)
				for _, what := range d.lintConfig(t, where, []byte(e.Config)) {
					uses = append(uses, deprecatedUse{where, what})
				}
			}
		}
	}

	checkMapping := func(where, mapping string) {
		if what := d.mappingError(mapping); what != "" {
			uses = append(uses, deprecatedUse{where, what})
		}
	}
	for _, kind := range []struct {
		name  string
		specs []bloblangSpec
	}{{"bloblang-functions", spec.BloblangFunctions}, {"bloblang-methods", spec.BloblangMethods}} {
		for _, b := range kind.specs {
			if b.Status == statusDeprecated {
				continue
			}
			for i, e := range b.Examples {
				checkMapping(fmt.Sprintf("%s/%s example %d", kind.name, b.Name, i+1), e.Mapping)
			}
			for _, cat := range b.Categories {
				for i, e := range cat.Examples {
					checkMapping(fmt.Sprintf("%s/%s %s example %d", kind.name, b.Name, cat.Category, i+1), e.Mapping)
				}
			}
		}
	}
	sort.Slice(uses, func(i, j int) bool {
		if uses[i].example != uses[j].example {
			return uses[i].example < uses[j].example
		}
		return uses[i].what < uses[j].what
	})
	return uses
}

// knownDeprecatedExamples are the examples that still use deprecated items.
// They are registered in benthos and are fixed by
// https://github.com/redpanda-data/benthos/pull/553. Remove these
// entries once connect bumps to a benthos release that includes it. The test
// fails on a deprecated use in any other example, and on an entry here that
// no longer uses anything deprecated.
var knownDeprecatedExamples = map[string]bool{
	`buffers/system_window example "Counting Passengers at Traffic"`:     true, // meta()
	`inputs/read_until example "Consume N Messages"`:                     true, // kafka input
	`inputs/read_until example "Read from a kafka and close when empty"`: true, // kafka input
	`outputs/drop_on example "Dropping failed HTTP requests"`:            true, // kafka output
	`processors/cached example "Cached Enrichment"`:                      true, // meta()
	`processors/dedupe example "Deduplicate based on Kafka key"`:         true, // meta()
	`processors/group_by example "Grouped Processing"`:                   true, // meta()
	`processors/metric example "Counter"`:                                true, // meta()
	`processors/metric example "Gauge"`:                                  true, // meta()
}

// Published examples must not teach deprecated components, fields, Bloblang
// functions or Bloblang methods. Deprecation status comes from the
// registered specs, so a newly deprecated item is picked up with no change
// here.
func TestExamplesAvoidDeprecated(t *testing.T) {
	offending := map[string]bool{}
	for _, u := range deprecatedExampleUses(t) {
		offending[u.example] = true
		if !knownDeprecatedExamples[u.example] {
			t.Errorf("%s uses a deprecated item: %s", u.example, u.what)
		}
	}
	for ex := range knownDeprecatedExamples {
		if !offending[ex] {
			t.Errorf("%s no longer uses a deprecated item; remove it from knownDeprecatedExamples", ex)
		}
	}
}

// exampleField is the part of a schema field that the field example check
// needs. fieldSpec leaves out the bloblang flag.
type exampleField struct {
	Name         string            `json:"name"`
	Type         string            `json:"type"`
	Kind         string            `json:"kind"`
	Interpolated bool              `json:"interpolated"`
	Bloblang     bool              `json:"bloblang"`
	IsDeprecated bool              `json:"is_deprecated"`
	Examples     []json.RawMessage `json:"examples"`
	Children     []exampleField    `json:"children"`
}

// componentConfigWrappers place one component config of a field type in a
// stream config, so the stream linter can check it.
var componentConfigWrappers = map[string]func(v any) any{
	"input":      func(v any) any { return map[string]any{"input": v} },
	"output":     func(v any) any { return map[string]any{"output": v} },
	"buffer":     func(v any) any { return map[string]any{"buffer": v} },
	"metrics":    func(v any) any { return map[string]any{"metrics": v} },
	"tracer":     func(v any) any { return map[string]any{"tracer": v} },
	"processor":  func(v any) any { return map[string]any{"pipeline": map[string]any{"processors": []any{v}}} },
	"cache":      func(v any) any { return map[string]any{"cache_resources": []any{v}} },
	"rate_limit": func(v any) any { return map[string]any{"rate_limit_resources": []any{v}} },
	"scanner":    func(v any) any { return map[string]any{"input": map[string]any{"stdin": map[string]any{"scanner": v}}} },
}

// fieldExampleUses returns every deprecated use in the field examples of
// every non-deprecated component and of the top-level config. Bloblang field
// examples are parsed without the deprecated functions and methods, and so
// are interpolated string examples. Examples of fields that hold component
// configs are linted like a component example. Object examples are matched
// against the field's children, and any string in them with no child spec is
// checked as both a mapping and an interpolation. Plain string and number
// examples aren't checked.
func fieldExampleUses(t *testing.T) []deprecatedUse {
	t.Helper()

	d := newDeprecationCheckers(t)
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(d.raw, &doc))

	var uses []deprecatedUse
	var where string
	add := func(at, what string) {
		if what != "" {
			uses = append(uses, deprecatedUse{where, at + ": " + what})
		}
	}

	var checkLeaves func(at string, v any)
	checkLeaves = func(at string, v any) {
		switch x := v.(type) {
		case string:
			add(at, d.mappingError(x))
			if strings.Contains(x, "${!") {
				add(at, d.interpolationError(x))
			}
		case []any:
			for i, e := range x {
				checkLeaves(fmt.Sprintf("%s[%d]", at, i), e)
			}
		case map[string]any:
			for _, k := range objectKeys(x) {
				checkLeaves(at+"."+k, x[k])
			}
		}
	}

	var checkValue func(at string, f exampleField, kind string, v any)
	checkValue = func(at string, f exampleField, kind string, v any) {
		switch kind {
		case "array", "2darray":
			inner := "scalar"
			if kind == "2darray" {
				inner = "array"
			}
			if arr, ok := v.([]any); ok {
				for i, e := range arr {
					checkValue(fmt.Sprintf("%s[%d]", at, i), f, inner, e)
				}
				return
			}
		case "map":
			if m, ok := v.(map[string]any); ok {
				for _, k := range objectKeys(m) {
					checkValue(at+"."+k, f, "scalar", m[k])
				}
				return
			}
		}
		if wrap, ok := componentConfigWrappers[f.Type]; ok {
			config, err := yaml.Marshal(wrap(v))
			require.NoError(t, err, where)
			for _, what := range d.lintConfig(t, where, config) {
				add(at, what)
			}
			return
		}
		s, isString := v.(string)
		switch {
		case isString && f.Bloblang:
			add(at, d.mappingError(s))
		case isString && f.Interpolated:
			add(at, d.interpolationError(s))
		case isString:
			// A plain string has nothing deprecated to check.
		default:
			m, ok := v.(map[string]any)
			if !ok || len(f.Children) == 0 {
				checkLeaves(at, v)
				return
			}
			for _, k := range objectKeys(m) {
				child, found := exampleField{}, false
				for _, c := range f.Children {
					if c.Name == k {
						child, found = c, true
						break
					}
				}
				if found {
					checkValue(at+"."+k, child, child.Kind, m[k])
				} else {
					checkLeaves(at+"."+k, m[k])
				}
			}
		}
	}

	var walk func(prefix, path string, f exampleField)
	walk = func(prefix, path string, f exampleField) {
		if f.IsDeprecated {
			return
		}
		if f.Name != "" {
			path = strings.TrimPrefix(path+"."+f.Name, ".")
		}
		for i, raw := range f.Examples {
			var v any
			require.NoError(t, json.Unmarshal(raw, &v), prefix)
			where = fmt.Sprintf("%s field %s example %d", prefix, path, i+1)
			checkValue(path, f, f.Kind, v)
		}
		for _, c := range f.Children {
			walk(prefix, path, c)
		}
	}

	for _, k := range componentKeys {
		var comps []struct {
			Name   string       `json:"name"`
			Status string       `json:"status"`
			Config exampleField `json:"config"`
		}
		if raw, ok := doc[k]; ok {
			require.NoError(t, json.Unmarshal(raw, &comps), k)
		}
		for _, c := range comps {
			if c.Status == statusDeprecated {
				continue
			}
			walk(k+"/"+c.Name, "", c.Config)
		}
	}
	var top []exampleField
	require.NoError(t, json.Unmarshal(doc["config"], &top))
	for _, f := range top {
		walk("config", "", f)
	}

	sort.SliceStable(uses, func(i, j int) bool { return uses[i].example < uses[j].example })
	return uses
}

// knownDeprecatedFieldExamples are field examples that still use deprecated
// items. They are registered in benthos and are fixed by
// https://github.com/redpanda-data/benthos/pull/553. Remove these
// entries once connect bumps to a benthos release that includes it. The test
// fails on a deprecated use in any other field example, and on an entry here
// that no longer uses anything deprecated.
var knownDeprecatedFieldExamples = map[string]bool{
	`buffers/system_window field timestamp_mapping example 2`: true, // meta()
	`inputs/http_server field sync_response.status example 2`: true, // meta()
	`outputs/cache field key example 3`:                       true, // meta()
	`processors/archive field path example 2`:                 true, // meta()
	`processors/cached field key example 3`:                   true, // meta()
	`processors/cached field key example 4`:                   true, // meta()
	`processors/dedupe field key example 1`:                   true, // meta()
	`processors/group_by_value field value example 1`:         true, // meta()
	`processors/group_by_value field value example 2`:         true, // meta()
	`processors/log field fields_mapping example 1`:           true, // meta()
	`processors/metric field labels example 1`:                true, // meta()
}

// Field examples must not teach deprecated components, fields, Bloblang
// functions or Bloblang methods either.
func TestFieldExamplesAvoidDeprecated(t *testing.T) {
	offending := map[string]bool{}
	for _, u := range fieldExampleUses(t) {
		offending[u.example] = true
		if !knownDeprecatedFieldExamples[u.example] {
			t.Errorf("%s uses a deprecated item: %s", u.example, u.what)
		}
	}
	for ex := range knownDeprecatedFieldExamples {
		if !offending[ex] {
			t.Errorf("%s no longer uses a deprecated item; remove it from knownDeprecatedFieldExamples", ex)
		}
	}
}
