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
	"go/token"
	"os"
	"path/filepath"
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

// TestPlatformsFromBuilds lists the real standard and docs_gen builds with go
// list and checks the components that build constraints are known to gate.
// It needs a Go toolchain, so -short skips it. The docs task runs it with
// x_benthos_extra, where it checks the exact cgo-only list.
func TestPlatformsFromBuilds(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go list on the real builds")
	}
	raw, err := marshalSchema(schema.Standard("", "").MarshalJSONV0())
	require.NoError(t, err)
	plat, err := loadPlatforms(raw)
	require.NoError(t, err)

	assert.False(t, plat.get("inputs", "kafka_franz").CgoOnly)
	assert.False(t, plat.get("processors", "a2a_message").CgoOnly, "Cloud-only isn't cgo-only")
	gated := map[string]bool{"inputs/zmq4": true, "outputs/zmq4": true, "processors/ffi": true, "inputs/tigerbeetle_cdc": true}
	for _, k := range plat.cgoOnlyKeys() {
		assert.True(t, gated[k], "unexpected cgo-only component %v", k)
	}
	if builtWithAllComponents {
		assert.Equal(t, []string{"inputs/tigerbeetle_cdc", "inputs/zmq4", "outputs/zmq4", "processors/ffi"}, plat.cgoOnlyKeys())
	}
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
