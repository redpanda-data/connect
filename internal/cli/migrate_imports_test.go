// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cli

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helpersBlobl is the V1 import body shared across the import-handling tests.
// `map double` is a trivial named map the bloblang migrator translates
// mechanically; the goal isn't to exercise complex translations but to
// confirm the import closure flows through the connect CLI.
const helpersBlobl = `map double { root = this * 2 }
`

// importYAMLFixture is a stream config whose bloblang processor body imports
// the helpers file. After migration the body is wrapped in a `bloblang_v2:`
// processor and the import path is suffix-rewritten.
const importYAMLFixture = `
input:
  generate:
    mapping: 'root = {"value": 21}'

pipeline:
  processors:
    - bloblang: |
        import "./helpers.blobl"
        root = this.value.apply("double")

output:
  drop: {}
`

func TestMigrateV5ImportSiblingOutput(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "simple.yaml")
	helpersPath := filepath.Join(dir, "helpers.blobl")
	require.NoError(t, os.WriteFile(yamlPath, []byte(importYAMLFixture), 0o644))
	require.NoError(t, os.WriteFile(helpersPath, []byte(helpersBlobl), 0o644))

	ctx := newCommandContext(t, map[string]string{
		"report": "text",
	}, yamlPath)

	var stdout, stderr bytes.Buffer
	failed, err := runMigrateV5With(ctx, &stdout, &stderr)
	require.NoError(t, err)
	require.False(t, failed, "stderr=%s\nstdout=%s", stderr.String(), stdout.String())

	// YAML rewritten with the import path suffixed.
	yamlBody, err := os.ReadFile(filepath.Join(dir, "simple.v5.yaml"))
	require.NoError(t, err)
	yaml := string(yamlBody)
	assert.Contains(t, yaml, "bloblang_v2:")
	assert.Contains(t, yaml, "./helpers.v5.blobl",
		"V1 import path should be rewritten in the migrated YAML body:\n%s", yaml)
	assert.NotContains(t, yaml, `"./helpers.blobl"`,
		"V1 import path should not survive in the migrated YAML body:\n%s", yaml)

	// Migrated import file written alongside.
	helperBody, err := os.ReadFile(filepath.Join(dir, "helpers.v5.blobl"))
	require.NoError(t, err)
	assert.Contains(t, string(helperBody), "double",
		"migrated helpers should keep the named-map identifier:\n%s", helperBody)

	// V1 source files should be left untouched.
	originalYAML, err := os.ReadFile(yamlPath)
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(importYAMLFixture), strings.TrimSpace(string(originalYAML)))

	originalHelpers, err := os.ReadFile(helpersPath)
	require.NoError(t, err)
	assert.Equal(t, helpersBlobl, string(originalHelpers))
}

func TestMigrateV5ImportTransitive(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "simple.yaml")
	require.NoError(t, os.WriteFile(yamlPath, []byte(`
input:
  generate:
    mapping: 'root = {"value": 5}'

pipeline:
  processors:
    - bloblang: |
        import "./a.blobl"
        root = this.value.apply("double")

output:
  drop: {}
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.blobl"),
		[]byte("import \"./b.blobl\"\nmap double { root = this * 2 }\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.blobl"),
		[]byte("map noop { root = this }\n"), 0o644))

	ctx := newCommandContext(t, map[string]string{
		"report": "text",
	}, yamlPath)

	var stdout, stderr bytes.Buffer
	failed, err := runMigrateV5With(ctx, &stdout, &stderr)
	require.NoError(t, err)
	require.False(t, failed, "stderr=%s", stderr.String())

	for _, name := range []string{"a.v5.blobl", "b.v5.blobl"} {
		_, err := os.ReadFile(filepath.Join(dir, name))
		require.NoErrorf(t, err, "missing migrated import %s", name)
	}

	// The transitive `b.blobl` import inside `a.blobl` should also be
	// suffix-rewritten in the migrated `a.v5.blobl`.
	aBody, err := os.ReadFile(filepath.Join(dir, "a.v5.blobl"))
	require.NoError(t, err)
	assert.Contains(t, string(aBody), "./b.v5.blobl",
		"transitive import path should be rewritten:\n%s", aBody)
}

func TestMigrateV5ImportCheckMode(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "simple.yaml")
	helpersPath := filepath.Join(dir, "helpers.blobl")
	require.NoError(t, os.WriteFile(yamlPath, []byte(importYAMLFixture), 0o644))
	require.NoError(t, os.WriteFile(helpersPath, []byte(helpersBlobl), 0o644))

	ctx := newCommandContext(t, map[string]string{
		"check":  "true",
		"report": "text",
	}, yamlPath)

	var stdout, stderr bytes.Buffer
	failed, err := runMigrateV5With(ctx, &stdout, &stderr)
	require.NoError(t, err)
	require.False(t, failed, "stderr=%s", stderr.String())

	// Nothing written to disk.
	if _, err := os.Stat(filepath.Join(dir, "simple.v5.yaml")); err == nil {
		t.Fatalf("--check unexpectedly produced a sibling YAML")
	}
	if _, err := os.Stat(filepath.Join(dir, "helpers.v5.blobl")); err == nil {
		t.Fatalf("--check unexpectedly produced a migrated import file")
	}

	// Report should enumerate the would-be import migration.
	assert.Contains(t, stdout.String(), "would migrate import:")
	assert.Contains(t, stdout.String(), helpersPath)
	assert.Contains(t, stdout.String(), "helpers.v5.blobl")
}

func TestMigrateV5ImportInPlaceWithBackup(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "simple.yaml")
	helpersPath := filepath.Join(dir, "helpers.blobl")
	require.NoError(t, os.WriteFile(yamlPath, []byte(importYAMLFixture), 0o644))
	require.NoError(t, os.WriteFile(helpersPath, []byte(helpersBlobl), 0o644))

	ctx := newCommandContext(t, map[string]string{
		"in-place": "true",
		"report":   "text",
	}, yamlPath)

	var stdout, stderr bytes.Buffer
	failed, err := runMigrateV5With(ctx, &stdout, &stderr)
	require.NoError(t, err)
	require.False(t, failed, "stderr=%s", stderr.String())

	// Source YAML overwritten.
	yamlBody, err := os.ReadFile(yamlPath)
	require.NoError(t, err)
	assert.Contains(t, string(yamlBody), "bloblang_v2:")
	// In-place mode keeps the import path identical to the original.
	assert.Contains(t, string(yamlBody), `"./helpers.blobl"`,
		"in-place mode should keep the V1 import path verbatim:\n%s", yamlBody)

	// Source helpers overwritten with V2 content.
	helpersBody, err := os.ReadFile(helpersPath)
	require.NoError(t, err)
	assert.NotEqual(t, helpersBlobl, string(helpersBody),
		"helpers should have been overwritten with V2 content")

	// Backups for both files.
	yamlBak, err := os.ReadFile(yamlPath + ".bak")
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(importYAMLFixture), strings.TrimSpace(string(yamlBak)))

	helpersBak, err := os.ReadFile(helpersPath + ".bak")
	require.NoError(t, err)
	assert.Equal(t, helpersBlobl, string(helpersBak))
}

func TestMigrateV5ImportInPlaceNoBackup(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "simple.yaml")
	helpersPath := filepath.Join(dir, "helpers.blobl")
	require.NoError(t, os.WriteFile(yamlPath, []byte(importYAMLFixture), 0o644))
	require.NoError(t, os.WriteFile(helpersPath, []byte(helpersBlobl), 0o644))

	ctx := newCommandContext(t, map[string]string{
		"in-place":  "true",
		"no-backup": "true",
		"report":    "text",
	}, yamlPath)

	var stdout, stderr bytes.Buffer
	failed, err := runMigrateV5With(ctx, &stdout, &stderr)
	require.NoError(t, err)
	require.False(t, failed, "stderr=%s", stderr.String())

	for _, p := range []string{yamlPath + ".bak", helpersPath + ".bak"} {
		if _, err := os.Stat(p); err == nil {
			t.Fatalf("--no-backup should have suppressed %s", p)
		} else if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("unexpected stat error on %s: %v", p, err)
		}
	}
}

func TestMigrateV5ImportMissing(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "simple.yaml")
	require.NoError(t, os.WriteFile(yamlPath, []byte(`
input:
  generate:
    mapping: 'root = {"value": 21}'

pipeline:
  processors:
    - bloblang: |
        import "./nonexistent.blobl"
        root = this

output:
  drop: {}
`), 0o644))

	ctx := newCommandContext(t, map[string]string{
		"check":  "true",
		"report": "text",
	}, yamlPath)

	var stdout, stderr bytes.Buffer
	failed, err := runMigrateV5With(ctx, &stdout, &stderr)
	require.NoError(t, err)
	// A missing import becomes an Unsupported import-site change inside the
	// processor's bloblang body. The wrapping bloblang_v2 rule still fires,
	// so component-level Coverage.Unsupported stays at 0; the failure
	// surfaces in the embedded BloblangReport rather than the outer counts.
	// This test pins that the run completes without crashing — the missing
	// import is documented in the report instead of being a fatal error.
	_ = failed
	report := stdout.String()
	require.Contains(t, report, yamlPath)
	require.Contains(t, report, "rewritten",
		"the wrapping processor rule should still fire even with a missing import:\n%s", report)
}

func TestMigrateV5ImportSharedAcrossYAMLs(t *testing.T) {
	dir := t.TempDir()
	helpersPath := filepath.Join(dir, "helpers.blobl")
	require.NoError(t, os.WriteFile(helpersPath, []byte(helpersBlobl), 0o644))

	for _, name := range []string{"a.yaml", "b.yaml"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(importYAMLFixture), 0o644))
	}

	ctx := newCommandContext(t, map[string]string{
		"report": "text",
	}, filepath.Join(dir, "*.yaml"))

	var stdout, stderr bytes.Buffer
	failed, err := runMigrateV5With(ctx, &stdout, &stderr)
	require.NoError(t, err)
	require.False(t, failed, "stderr=%s", stderr.String())

	// Both YAMLs migrated.
	for _, name := range []string{"a.v5.yaml", "b.v5.yaml"} {
		_, err := os.ReadFile(filepath.Join(dir, name))
		require.NoErrorf(t, err, "missing migrated YAML %s", name)
	}

	// The shared import was emitted.
	body, err := os.ReadFile(filepath.Join(dir, "helpers.v5.blobl"))
	require.NoError(t, err)
	assert.Contains(t, string(body), "double")
}
