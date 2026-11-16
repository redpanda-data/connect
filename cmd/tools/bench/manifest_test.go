// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	validManifest = `name: widget_cdc
component: widget_cdc
kind: input
summary: Read throughput of the widget input
requires:
  tools: [docker]
  env: [WIDGET_DSN]
scenarios:
  - name: snapshot
  - name: streaming
    task: bench:run:cdc
    vars: {CORES: "4"}
    max_duration: 2m
`
	validTaskfile = `version: '3'
tasks:
  bench:up: {cmd: "true"}
  bench:load: {cmd: "true"}
  bench:run: {cmd: "true"}
  bench:run:cdc: {cmd: "true"}
  bench:reset: {cmd: "true"}
  bench:down: {cmd: "true"}
`
)

// writeSuite creates a suite directory under a fresh repository root. An
// empty content leaves the file out.
func writeSuite(t *testing.T, root, dir, manifest, taskfile string) {
	t.Helper()
	abs := filepath.Join(root, filepath.FromSlash(dir))
	require.NoError(t, os.MkdirAll(abs, 0o755))
	files := map[string]string{manifestFile: manifest, "Taskfile.yaml": taskfile, "README.md": "# Bench\n"}
	for name, content := range files {
		if content != "" {
			require.NoError(t, os.WriteFile(filepath.Join(abs, name), []byte(content), 0o644))
		}
	}
}

func TestDiscoverConformantSuite(t *testing.T) {
	root := t.TempDir()
	writeSuite(t, root, "internal/impl/widget/bench", validManifest, validTaskfile)

	suites, err := discover(root)
	require.NoError(t, err)
	require.Len(t, suites, 1)

	s := suites[0]
	assert.Empty(t, s.Problems)
	assert.True(t, s.Conformant)
	assert.Equal(t, "internal/impl/widget/bench", s.Dir)
	assert.Equal(t, "widget_cdc", s.Name)
	assert.Equal(t, []string{"WIDGET_DSN"}, s.Requires.Env)
	assert.Contains(t, s.Tasks, "bench:run:cdc")

	first, err := s.scenario("")
	require.NoError(t, err)
	assert.Equal(t, "snapshot", first.Name)
	assert.Equal(t, taskRun, first.runTask())
	assert.Equal(t, defaultMaxDuration, first.maxDuration())

	streaming, err := s.scenario("streaming")
	require.NoError(t, err)
	assert.Equal(t, "bench:run:cdc", streaming.runTask())
	assert.Equal(t, "2m0s", streaming.maxDuration().String())

	_, err = s.scenario("nope")
	require.Error(t, err)

	found, err := findSuite(suites, "widget_cdc")
	require.NoError(t, err)
	assert.Same(t, s, found)
	found, err = findSuite(suites, "internal/impl/widget/bench/")
	require.NoError(t, err)
	assert.Same(t, s, found)
	_, err = findSuite(suites, "gadget")
	require.ErrorContains(t, err, "widget_cdc")
}

func TestDiscoverReportsProblems(t *testing.T) {
	tests := []struct {
		name     string
		manifest string
		taskfile string
		problem  string
	}{
		{
			name:     "no manifest lists the tasks to add",
			taskfile: "version: '3'\ntasks:\n  bench:run: {cmd: \"true\"}\n  up: {cmd: \"true\"}\n",
			problem:  "missing tasks: bench:up, bench:load, bench:reset, bench:down",
		},
		{
			name:     "missing task",
			manifest: validManifest,
			taskfile: "version: '3'\ntasks:\n  bench:up: {cmd: \"true\"}\n  bench:run: {cmd: \"true\"}\n  bench:run:cdc: {cmd: \"true\"}\n  bench:reset: {cmd: \"true\"}\n  bench:down: {cmd: \"true\"}\n",
			problem:  "missing tasks: bench:load",
		},
		{
			name:     "scenario task not in the Taskfile",
			manifest: validManifest + "  - name: other\n    task: bench:run:other\n",
			taskfile: validTaskfile,
			problem:  `scenario "other": task bench:run:other is not in the Taskfile`,
		},
		{
			name:     "unknown manifest key",
			manifest: validManifest + "prerequisites: [docker]\n",
			taskfile: validTaskfile,
			problem:  "field prerequisites not found",
		},
		{
			name:     "no scenario",
			manifest: "name: widget\ncomponent: widget\nkind: input\nsummary: s\nrequires: {tools: [go]}\n",
			taskfile: validTaskfile,
			problem:  "at least one scenario is required",
		},
		{
			name:     "bad kind, name, vars and duration",
			manifest: "name: Widget CDC\ncomponent: widget\nkind: source\nsummary: s\nrequires: {tools: [go], env: [dsn]}\nscenarios:\n  - name: a\n    vars: {cores: \"1\"}\n    max_duration: soon\n  - name: a\n",
			taskfile: validTaskfile,
			problem:  `kind "source" must be one of`,
		},
		{
			name:     "missing results file",
			manifest: validManifest + "results: docs/benchmark-results/widget.md\n",
			taskfile: validTaskfile,
			problem:  "results file docs/benchmark-results/widget.md does not exist",
		},
		{
			name:     "no tools declared",
			manifest: "name: widget\ncomponent: widget\nkind: input\nsummary: s\nscenarios:\n  - name: a\n",
			taskfile: validTaskfile,
			problem:  "requires.tools is required",
		},
		{
			name:     "no Taskfile",
			manifest: validManifest,
			problem:  "no Taskfile.yaml",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			writeSuite(t, root, "internal/impl/widget/bench", test.manifest, test.taskfile)

			suites, err := discover(root)
			require.NoError(t, err)
			require.Len(t, suites, 1)
			assert.False(t, suites[0].Conformant)
			assert.Equal(t, test.manifest != "", suites[0].declared)

			var joined string
			for _, p := range suites[0].Problems {
				joined += p + "\n"
			}
			assert.Contains(t, joined, test.problem)
		})
	}
}

func TestDiscoverAllProblemsOfAManifest(t *testing.T) {
	root := t.TempDir()
	writeSuite(t, root, "internal/impl/widget/bench",
		"name: Widget CDC\ncomponent: widget\nkind: source\nsummary: s\nrequires: {tools: [go], env: [dsn]}\nscenarios:\n  - name: a\n    vars: {cores: \"1\"}\n    max_duration: soon\n  - name: a\n",
		validTaskfile)

	suites, err := discover(root)
	require.NoError(t, err)
	require.Len(t, suites, 1)
	// Every problem is reported in one pass, not only the first one.
	assert.Len(t, suites[0].Problems, 6, "%v", suites[0].Problems)
}

func TestDiscoverScope(t *testing.T) {
	root := t.TempDir()
	// A second suite reusing a name, in a directory not called bench.
	writeSuite(t, root, "internal/impl/widget/bench", validManifest, validTaskfile)
	writeSuite(t, root, "internal/impl/widget/perf", validManifest, validTaskfile)
	// A Taskfile outside a bench directory is not a suite.
	writeSuite(t, root, "internal/impl/widget/tools", "", validTaskfile)
	// Nested legacy suites are found one by one.
	writeSuite(t, root, "internal/impl/gadget/bench/read/rpcn", "", validTaskfile)

	suites, err := discover(root)
	require.NoError(t, err)
	var dirs []string
	for _, s := range suites {
		dirs = append(dirs, s.Dir)
	}
	assert.Equal(t, []string{
		"internal/impl/gadget/bench/read/rpcn",
		"internal/impl/widget/bench",
		"internal/impl/widget/perf",
	}, dirs)

	assert.Equal(t, []string{"no bench.yaml"}, suites[0].Problems)
	assert.True(t, suites[1].Conformant)
	assert.False(t, suites[2].Conformant)
	assert.Contains(t, suites[2].Problems[0], "already used by internal/impl/widget/bench")
}

func TestListJSON(t *testing.T) {
	root := t.TempDir()
	writeSuite(t, root, "internal/impl/widget/bench", validManifest, validTaskfile)
	suites, err := discover(root)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, writeJSON(&buf, suites))
	var got []map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
	require.Len(t, got, 1)
	assert.Equal(t, "widget_cdc", got[0]["name"])
	assert.Equal(t, true, got[0]["conformant"])
	assert.Equal(t, map[string]any{"tools": []any{"docker"}, "env": []any{"WIDGET_DSN"}}, got[0]["requires"])
	assert.Len(t, got[0]["scenarios"], 2)
}

// TestRepositorySuites keeps every suite that declares a manifest honest: a
// manifest that drifts from its Taskfile fails here, without a CI job of its
// own.
func TestRepositorySuites(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	require.NoError(t, err)

	suites, err := discover(root)
	require.NoError(t, err)

	declared := 0
	for _, s := range suites {
		if !s.declared {
			continue
		}
		declared++
		assert.Empty(t, s.Problems, "suite %s does not honour the contract in cmd/tools/bench/README.md", s.Dir)
	}
	assert.Positive(t, declared, "no suite with a %s was found", manifestFile)
}
