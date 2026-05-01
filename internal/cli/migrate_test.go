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
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"

	// Side-effect import so the V2 msgpack plugins are registered against the
	// global environment when the linter parses the migrated bloblang body.
	_ "github.com/redpanda-data/connect/v4/internal/impl/msgpack"
)

const msgpackFixture = `
input:
  generate:
    mapping: 'root = {"foo":"bar"}'

pipeline:
  processors:
    - bloblang: |
        root = this.format_msgpack().parse_msgpack()

output:
  drop: {}
`

// newCommandContext builds a fully-populated *cli.Context against the
// migrate v5 command's flag set. The args slice is forwarded as positional
// arguments. It mirrors what urfave/cli does for the user when the action
// fires.
func newCommandContext(t *testing.T, flags map[string]string, args ...string) *cli.Context {
	t.Helper()

	app := cli.NewApp()
	cmd := migrateV5Cli()

	fs := flag.NewFlagSet("migrate-v5", flag.ContinueOnError)
	for _, f := range cmd.Flags {
		require.NoError(t, f.Apply(fs))
	}
	cliArgs := []string{}
	for k, v := range flags {
		cliArgs = append(cliArgs, "--"+k+"="+v)
	}
	cliArgs = append(cliArgs, args...)
	require.NoError(t, fs.Parse(cliArgs))

	ctx := cli.NewContext(app, fs, nil)
	ctx.Command = cmd
	ctx.Context = context.Background()
	return ctx
}

func TestMigrateV5SiblingOutput(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "stream.yaml")
	require.NoError(t, os.WriteFile(src, []byte(msgpackFixture), 0o644))

	ctx := newCommandContext(t, map[string]string{
		"report": "text",
	}, src)

	var stdout, stderr bytes.Buffer
	failed, err := runMigrateV5With(ctx, &stdout, &stderr)
	require.NoError(t, err)
	require.False(t, failed, "stderr=%s", stderr.String())

	out := filepath.Join(dir, "stream.v5.yaml")
	bodyBytes, err := os.ReadFile(out)
	require.NoError(t, err)
	body := string(bodyBytes)
	assert.Contains(t, body, "bloblang_v2:")
	assert.NotContains(t, body, "bloblang: |")
	assert.Contains(t, body, "format_msgpack")
	assert.Contains(t, body, "parse_msgpack")

	// Source must be unchanged in default (sibling) mode.
	src2, err := os.ReadFile(src)
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(msgpackFixture), strings.TrimSpace(string(src2)))
}

func TestMigrateV5CheckMode(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "stream.yaml")
	require.NoError(t, os.WriteFile(src, []byte(msgpackFixture), 0o644))

	ctx := newCommandContext(t, map[string]string{
		"check":  "true",
		"report": "json",
	}, src)

	var stdout, stderr bytes.Buffer
	failed, err := runMigrateV5With(ctx, &stdout, &stderr)
	require.NoError(t, err)
	assert.False(t, failed, "stderr=%s", stderr.String())

	// --check must not write a sibling.
	if _, err := os.Stat(filepath.Join(dir, "stream.v5.yaml")); err == nil {
		t.Fatalf("--check unexpectedly produced a sibling file")
	}

	// JSON report should contain coverage and outcome metadata.
	report := stdout.String()
	assert.Contains(t, report, `"file":`)
	assert.Contains(t, report, `"Coverage":`)
	assert.Contains(t, report, `"Rewritten":1`)
	assert.Contains(t, report, `"Unsupported":0`)
}

func TestMigrateV5InPlaceWithBackup(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "stream.yaml")
	require.NoError(t, os.WriteFile(src, []byte(msgpackFixture), 0o644))

	ctx := newCommandContext(t, map[string]string{
		"in-place": "true",
		"report":   "text",
	}, src)

	var stdout, stderr bytes.Buffer
	failed, err := runMigrateV5With(ctx, &stdout, &stderr)
	require.NoError(t, err)
	require.False(t, failed, "stderr=%s", stderr.String())

	rewritten, err := os.ReadFile(src)
	require.NoError(t, err)
	assert.Contains(t, string(rewritten), "bloblang_v2:")

	backup, err := os.ReadFile(src + ".bak")
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(msgpackFixture), strings.TrimSpace(string(backup)))
}

func TestMigrateV5RejectsCheckPlusInPlace(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "stream.yaml")
	require.NoError(t, os.WriteFile(src, []byte(msgpackFixture), 0o644))

	ctx := newCommandContext(t, map[string]string{
		"check":    "true",
		"in-place": "true",
	}, src)

	var stdout, stderr bytes.Buffer
	_, err := runMigrateV5With(ctx, &stdout, &stderr)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}

func TestSiblingPath(t *testing.T) {
	cases := []struct {
		input, suffix, want string
	}{
		{"foo.yaml", ".v5", "foo.v5.yaml"},
		{"a/b/foo.yml", ".v5", "a/b/foo.v5.yml"},
		{"plain", ".v5", "plain.v5"},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, siblingPath(tc.input, tc.suffix), tc.input)
	}
}
