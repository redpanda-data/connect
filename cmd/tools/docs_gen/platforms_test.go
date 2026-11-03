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
	"flag"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/plugins"
	"github.com/redpanda-data/connect/v4/public/schema"
)

func schemaWith(groups map[string][]componentSpec) *fullSchema {
	s := &fullSchema{}
	for _, k := range componentKeys {
		s.Groups = append(s.Groups, componentGroup{Key: k, Components: groups[k]})
	}
	return s
}

func TestStandardBuildEnv(t *testing.T) {
	env := standardBuildEnv([]string{
		"PATH=/usr/bin",
		"CGO_ENABLED=1",
		"GOFLAGS=-buildvcs=false -tags=x_benthos_extra -mod=mod",
		"HOME=/root",
	})
	assert.Equal(t, []string{"PATH=/usr/bin", "GOFLAGS=-buildvcs=false -mod=mod", "HOME=/root", "CGO_ENABLED=0"}, env)

	assert.Equal(t, []string{"CGO_ENABLED=0"}, standardBuildEnv([]string{"GOFLAGS=-tags=x_benthos_extra"}))
}

func TestNewPlatformSet(t *testing.T) {
	full := schemaWith(map[string][]componentSpec{
		"inputs":     {{Name: "kafka_franz"}, {Name: "zmq4"}, {Name: "jira"}},
		"processors": {{Name: "ffi"}, {Name: "ollama_chat"}},
	})
	standard := schemaWith(map[string][]componentSpec{
		"inputs":     {{Name: "kafka_franz"}, {Name: "jira"}},
		"processors": {{Name: "ollama_chat"}},
	})
	cloud := schemaWith(map[string][]componentSpec{"inputs": {{Name: "kafka_franz"}, {Name: "jira"}}})
	cloudAI := schemaWith(map[string][]componentSpec{
		"inputs":     {{Name: "kafka_franz"}},
		"processors": {{Name: "ollama_chat"}},
	})

	p, err := newPlatformSet(full, standard, cloud, cloudAI)
	require.NoError(t, err)
	assert.Equal(t, []string{"inputs/zmq4", "processors/ffi"}, p.cgoOnlyKeys())
	assert.Equal(t, componentPlatform{Cloud: true, CloudAI: true}, p.get("inputs", "kafka_franz"))
	assert.Equal(t, componentPlatform{Cloud: true}, p.get("inputs", "jira"))
	assert.Equal(t, componentPlatform{CloudAI: true}, p.get("processors", "ollama_chat"))
	assert.Equal(t, componentPlatform{CgoOnly: true}, p.get("inputs", "zmq4"))

	assert.True(t, p.cloudExcluded("inputs", "zmq4"))
	assert.False(t, p.cloudExcluded("processors", "ollama_chat"), "Cloud GPU pipelines count as Cloud")
	assert.False(t, p.cloudExcluded("inputs", "not_a_component"), "only documented components are excluded")

	_, err = newPlatformSet(full, schemaWith(nil), cloud, cloudAI)
	require.Error(t, err, "an empty standard build means the list couldn't be computed")

	extra := schemaWith(map[string][]componentSpec{"inputs": {{Name: "kafka_franz"}, {Name: "only_in_standard"}}})
	_, err = newPlatformSet(full, extra, cloud, cloudAI)
	require.ErrorContains(t, err, "inputs/only_in_standard")
}

func TestRenderAvailabilityPartial(t *testing.T) {
	empty := renderAvailabilityPartial(componentPlatform{Cloud: true, CloudAI: true})
	assert.Equal(t, emptyAvailabilityBanner+"\n", empty)
	assert.Equal(t, emptyAvailabilityBanner+"\n", renderAvailabilityPartial(componentPlatform{}))

	cgo := renderAvailabilityPartial(componentPlatform{CgoOnly: true})
	assert.Contains(t, cgo, "ifndef::env-cloud[]\n[NOTE]")
	assert.Contains(t, cgo, "redpanda-connect-cgo_<version>_linux_amd64")
	assert.Contains(t, cgo, "CGO_ENABLED=1 go build -tags x_benthos_extra,timetzdata ./cmd/redpanda-connect")
	assert.Contains(t, cgo, "libzmq")
	assert.NotContains(t, cgo, "ifdef::env-cloud[]")

	noGPU := renderAvailabilityPartial(componentPlatform{Cloud: true})
	assert.Contains(t, noGPU, "ifdef::env-cloud[]\nNOTE: This component isn't available in ")
	assert.NotContains(t, noGPU, "[NOTE]")

	gpuOnly := renderAvailabilityPartial(componentPlatform{CloudAI: true})
	assert.Contains(t, gpuOnly, "ifdef::env-cloud[]\nNOTE: This component is available only in ")
}

func TestRenderCatalog(t *testing.T) {
	full := schemaWith(map[string][]componentSpec{
		"outputs": {{Name: "kafka", Type: "output", Status: "stable", Summary: "Writes to `Kafka`."}},
		"inputs": {
			{Name: "zmq4", Type: "input", Status: "stable", Version: "4.0.0", Categories: []string{"Network"}, Summary: "Consumes from ZeroMQ."},
			{Name: "kafka", Type: "input", Status: "deprecated", Summary: "Reads from xref:components:inputs/kafka.adoc[Kafka]."},
		},
		"rate-limits": {{Name: "local", Type: "rate_limit", Status: "stable"}},
	})
	plat, err := newPlatformSet(full,
		schemaWith(map[string][]componentSpec{"outputs": {{Name: "kafka"}}, "inputs": {{Name: "kafka"}}, "rate-limits": {{Name: "local"}}}),
		schemaWith(map[string][]componentSpec{"inputs": {{Name: "kafka"}}}),
		schemaWith(nil))
	require.NoError(t, err)
	info := plugins.InfoCollection{
		"kafka-input":  {Name: "kafka", Type: plugins.TypeInput, CommercialName: "Apache Kafka", Support: "certified"},
		"kafka-output": {Name: "kafka", Type: plugins.TypeOutput, CommercialName: "kafka", Support: "community"},
	}
	out, err := renderCatalog(full, plat, info)
	require.NoError(t, err)

	var entries []catalogEntry
	require.NoError(t, json.Unmarshal([]byte(out), &entries))
	assert.Equal(t, []catalogEntry{
		{Type: "input", Name: "kafka", Status: "deprecated", Categories: []string{}, Summary: "Reads from Kafka.", Cloud: true, Support: "certified", CommercialNames: []string{"Apache Kafka"}},
		{Type: "input", Name: "zmq4", Status: "stable", Version: "4.0.0", Categories: []string{"Network"}, Summary: "Consumes from ZeroMQ.", CgoOnly: true, CommercialNames: []string{}},
		{Type: "output", Name: "kafka", Status: "stable", Categories: []string{}, Summary: "Writes to Kafka.", Support: "community", CommercialNames: []string{"kafka"}},
		{Type: "rate_limit", Name: "local", Status: "stable", Categories: []string{}, CommercialNames: []string{}},
	}, entries)
	assert.Contains(t, out, `"categories": []`, "empty lists stay arrays, not null")
}

// TestPlatformsFromBuilds lists the real release builds with go list and
// pins the exact cloud-only and cgo-only sets, so a component that moves
// between distributions fails it until the expected sets are updated. It also
// fails when a release build registers a component that docs_gen doesn't
// document. It needs a Go toolchain, so -short skips it.
func TestPlatformsFromBuilds(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go list on the real builds")
	}
	raw, err := marshalSchema(schema.Standard("", "").MarshalJSONV0())
	require.NoError(t, err)
	plat, err := loadPlatforms(raw)
	require.NoError(t, err)

	// These come from go list on the builds in .goreleaser, whatever tags
	// this test runs with.
	cgoOnly, err := cgoOnlyComponents()
	require.NoError(t, err)
	cloudOnly, err := cloudOnlyComponents()
	require.NoError(t, err)
	got := platformSets{CloudOnly: sortedKeys(cloudOnly), CgoOnly: sortedKeys(cgoOnly)}
	if *updatePlatformSets {
		writePlatformSets(t, got)
	}
	want := readPlatformSets(t)
	assertSameSet(t, "cloud_only", want.CloudOnly, got.CloudOnly)
	assertSameSet(t, "cgo_only", want.CgoOnly, got.CgoOnly)
	if builtWithAllComponents {
		assertSameSet(t, "cgo_only (cgoOnlyKeys)", want.CgoOnly, plat.cgoOnlyKeys())
	}

	builds := map[string]map[string]bool{}
	for _, name := range []string{standardRelease, cgoRelease, cloudRelease} {
		b, err := readReleaseBuild(name)
		require.NoError(t, err)
		builds[name] = buildRegistrations(t, b)
	}
	for k := range cloudOnly {
		assert.True(t, builds[cloudRelease][k], "cloud-only %v isn't registered by the Cloud build", k)
		assert.False(t, builds[standardRelease][k], "cloud-only %v is registered by the standard build", k)
	}
	for k := range cgoOnly {
		assert.True(t, builds[cgoRelease][k], "cgo-only %v isn't registered by the cgo build", k)
		assert.False(t, builds[standardRelease][k], "cgo-only %v is registered by the standard build", k)
	}
	for _, k := range plat.cgoOnlyKeys() {
		assert.True(t, cgoOnly[k], "%v is cgo-only in the platform set but not in the cgo build comparison", k)
	}

	// Sentinels, to show that real builds were compared. If one of these
	// components moves to another distribution, pick another sentinel.
	assert.True(t, cloudOnly["processors/a2a_message"], "a2a_message is in the Cloud build only")
	assert.True(t, cgoOnly["processors/ffi"], "ffi is in the cgo build only")
	assert.False(t, plat.get("processors", "a2a_message").CgoOnly, "Cloud-only isn't cgo-only")

	// docs_gen documents only allcomponents, so a component that a release
	// build registers and allcomponents doesn't would ship without docs.
	report, err := undocumentedReport()
	require.NoError(t, err)
	for _, u := range report {
		t.Errorf("%v/%v is registered by the %v build(s) but not documented: add its package to cmd/tools/docs_gen/allcomponents", u.Type, u.Name, strings.Join(u.Builds, ", "))
	}
}

var updatePlatformSets = flag.Bool("update", false, "rewrite "+platformSetsFile+" from the real release builds")

// platformSetsFile holds the expected cloud-only and cgo-only sets, which
// decide the cloudOnly and requiresCgo flags in the published connector data.
// Rewrite it with go test ./cmd/tools/docs_gen -run TestPlatformsFromBuilds -update.
const platformSetsFile = "testdata/platform_sets.json"

type platformSets struct {
	CloudOnly []string `json:"cloud_only"`
	CgoOnly   []string `json:"cgo_only"`
}

func readPlatformSets(t *testing.T) platformSets {
	t.Helper()
	b, err := os.ReadFile(platformSetsFile)
	require.NoError(t, err)
	var s platformSets
	require.NoError(t, json.Unmarshal(b, &s))
	return s
}

func writePlatformSets(t *testing.T, s platformSets) {
	t.Helper()
	b, err := json.MarshalIndent(s, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(platformSetsFile), 0o755))
	require.NoError(t, os.WriteFile(platformSetsFile, append(b, '\n'), 0o644))
}

func sortedKeys(m map[string]bool) []string {
	out := []string{}
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// assertSameSet fails with the components added to and removed from the
// expected set, so a change to the published flags shows in review.
func assertSameSet(t *testing.T, name string, want, got []string) {
	t.Helper()
	wantSet, gotSet := map[string]bool{}, map[string]bool{}
	for _, k := range want {
		wantSet[k] = true
	}
	for _, k := range got {
		gotSet[k] = true
	}
	var added, removed []string
	for _, k := range got {
		if !wantSet[k] {
			added = append(added, k)
		}
	}
	for _, k := range want {
		if !gotSet[k] {
			removed = append(removed, k)
		}
	}
	if len(added) > 0 || len(removed) > 0 {
		t.Errorf("%v changed: added %v, removed %v. If the change is intended, rerun go test ./cmd/tools/docs_gen -run TestPlatformsFromBuilds -update and commit %v", name, added, removed, filepath.Join("cmd/tools/docs_gen", platformSetsFile))
	}
}

// buildRegistrations returns every component that a register call in the
// connect or benthos files of build b names. It reads all of the build's
// files, not only the ones another build lacks, and skips calls whose name it
// can't read, so it checks the comparison independently of
// registrationsOnlyIn.
func buildRegistrations(t *testing.T, b releaseBuild) map[string]bool {
	t.Helper()
	files, err := b.files(b.main)
	require.NoError(t, err)
	modDirs, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", connectModule, "github.com/redpanda-data/benthos/v4").Output()
	require.NoError(t, err)
	mods := strings.Fields(string(modDirs))
	keys := map[string]bool{}
	fset := token.NewFileSet()
	for path := range files {
		inModule := false
		for _, m := range mods {
			inModule = inModule || strings.HasPrefix(path, m+string(filepath.Separator))
		}
		if !inModule || strings.HasSuffix(path, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, err)
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if x, ok := sel.X.(*ast.Ident); !ok || x.Name != "service" {
				return true
			}
			group, ok := registerGroups[strings.TrimPrefix(strings.TrimPrefix(sel.Sel.Name, "Must"), "Register")]
			if !ok {
				return true
			}
			if name, err := registeredName(call.Args[0], filepath.Dir(path), files, fset); err == nil {
				keys[componentKey(group, name)] = true
			}
			return true
		})
	}
	return keys
}

func TestFileRegistrations(t *testing.T) {
	dir := t.TempDir()
	write := func(name, src string) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte("package x\n\n"+src), 0o644))
		return p
	}
	literal := write("a.go", `func init() { service.MustRegisterBatchInput("zmq4", nil, nil) }`)
	constant := write("b.go", `const outName = "zmq4"

func init() { service.MustRegisterBatchOutput(outName, nil, nil) }`)
	template := write("c.go", `func init() { service.MustRegisterTemplateYAML(string(tmpl)) }`)
	helper := write("d.go", `func init() { registerAll() }`)
	importsOnly := write("e.go", `import _ "example.com/x"`)
	files := map[string]bool{literal: true, constant: true, template: true, helper: true, importsOnly: true}
	fset := token.NewFileSet()

	got, err := fileRegistrations(literal, files, fset)
	require.NoError(t, err)
	assert.Equal(t, []string{"inputs/zmq4"}, got)

	got, err = fileRegistrations(constant, files, fset)
	require.NoError(t, err)
	assert.Equal(t, []string{"outputs/zmq4"}, got)

	_, err = fileRegistrations(template, files, fset)
	require.ErrorContains(t, err, "MustRegisterTemplateYAML", "a kind docs_gen can't read is an error, not a gap")

	_, err = fileRegistrations(helper, files, fset)
	require.ErrorContains(t, err, "init function", "an init that registers nothing visible is an error")

	got, err = fileRegistrations(importsOnly, files, fset)
	require.NoError(t, err)
	assert.Empty(t, got, "a file that only imports components registers nothing itself")
}
