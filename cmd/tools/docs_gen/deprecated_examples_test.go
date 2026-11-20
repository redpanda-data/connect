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
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

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

// deprecatedExampleUses returns every deprecated use in the component spec
// examples and the Bloblang function and method examples. Examples on a
// deprecated component or Bloblang plugin are left out, since that page is
// about the deprecated thing.
func deprecatedExampleUses(t *testing.T) []deprecatedUse {
	t.Helper()

	full := schema.Standard("", "")
	raw, err := full.MarshalJSONV0()
	require.NoError(t, err)
	spec, err := parseFullSchema(raw)
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
	plain := full.NewStreamConfigLinter().SetSkipEnvVarCheck(true)
	strictEnv := full.Environment().Clone()
	strictEnv.UseBloblangEnvironment(stripped)
	strictSchema := schema.Standard("", "")
	strictSchema.SetEnvironment(strictEnv)
	strict := strictSchema.NewStreamConfigLinter().SetSkipEnvVarCheck(true).SetRejectDeprecated(true)

	var uses []deprecatedUse
	for _, g := range spec.Groups {
		for _, c := range g.Components {
			if c.Status == statusDeprecated {
				continue
			}
			for _, e := range c.Examples {
				where := fmt.Sprintf("%s/%s example %q", g.Key, c.Name, e.Title)
				before, err := plain.LintYAML([]byte(e.Config))
				require.NoError(t, err, where)
				after, err := strict.LintYAML([]byte(e.Config))
				require.NoError(t, err, where)
				seen := lintCounts(before)
				for _, l := range after {
					if seen[l.Error()] > 0 {
						seen[l.Error()]--
						continue
					}
					uses = append(uses, deprecatedUse{where, firstLine(l.Error())})
				}
			}
		}
	}

	checkMapping := func(where, mapping string) {
		if _, err := blobl.Parse(mapping); err != nil {
			return // Not a deprecation problem; the example tests cover it.
		}
		if _, err := stripped.Parse(mapping); err != nil {
			uses = append(uses, deprecatedUse{where, firstLine(err.Error())})
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
