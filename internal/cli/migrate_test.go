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

	// Side-effect imports so V2 plugin counterparts are registered when the
	// migrate fixture exercises a callsite for each.
	_ "github.com/redpanda-data/connect/v4/internal/impl/changelog"
	_ "github.com/redpanda-data/connect/v4/internal/impl/confluent"
	_ "github.com/redpanda-data/connect/v4/internal/impl/crypto"
	_ "github.com/redpanda-data/connect/v4/internal/impl/html"
	_ "github.com/redpanda-data/connect/v4/internal/impl/jsonpath"
	_ "github.com/redpanda-data/connect/v4/internal/impl/lang"
	_ "github.com/redpanda-data/connect/v4/internal/impl/maxmind"
	_ "github.com/redpanda-data/connect/v4/internal/impl/msgpack"
	_ "github.com/redpanda-data/connect/v4/internal/impl/parquet"
	_ "github.com/redpanda-data/connect/v4/internal/impl/sql"
	_ "github.com/redpanda-data/connect/v4/internal/impl/xml"
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

// allPluginsFixture is a stream config whose bloblang processor body invokes
// one callsite for every connect-registered Bloblang V1 plugin (43 in total).
// The migrator must rewrite each callsite to its V2 equivalent in place; the
// surrounding YAML should remain otherwise unchanged.
const allPluginsFixture = `
input:
  generate:
    mapping: 'root = {}'

pipeline:
  processors:
    - bloblang: |
        # methods
        root.a01 = this.payload.parse_msgpack()
        root.a02 = this.format_msgpack()
        root.a03 = this.parse_parquet()
        root.a04 = this.embeddings.vector()
        root.a05 = this.json_path("$..name")
        root.a06 = this.html.strip_html()
        root.a07 = this.html.strip_html(["strong"])
        root.a08 = this.doc.parse_xml()
        root.a09 = this.format_xml()
        root.a10 = this.before.diff(this.after)
        root.a11 = this.input.patch(this.changelog)
        root.a12 = this.title.slug()
        root.a13 = this.text.unicode_segments("word")
        root.a14 = this.user.compare_argon2("$argon2id$v=19$m=4096,t=3,p=1$c2FsdHktbWNzYWx0ZmFjZQ$RMUMwgtS32/mbszd+ke4o4Ej1jFpYiUqY6MHWa69X7Y")
        root.a15 = this.user.compare_bcrypt("$2y$10$Dtnt5NNzVtMCOZONT705tOcS8It6krJX8bEjnDJnwxiFKsz1C.3Ay")
        # jwt parse (9)
        root.a16 = this.token.parse_jwt_hs256("dont-tell-anyone")
        root.a17 = this.token.parse_jwt_hs384("dont-tell-anyone")
        root.a18 = this.token.parse_jwt_hs512("dont-tell-anyone")
        root.a19 = this.token.parse_jwt_rs256("dummy-rsa")
        root.a20 = this.token.parse_jwt_rs384("dummy-rsa")
        root.a21 = this.token.parse_jwt_rs512("dummy-rsa")
        root.a22 = this.token.parse_jwt_es256("dummy-ecdsa")
        root.a23 = this.token.parse_jwt_es384("dummy-ecdsa")
        root.a24 = this.token.parse_jwt_es512("dummy-ecdsa")
        # jwt sign (9)
        root.a25 = this.claims.sign_jwt_hs256("dont-tell-anyone")
        root.a26 = this.claims.sign_jwt_hs384("dont-tell-anyone")
        root.a27 = this.claims.sign_jwt_hs512("dont-tell-anyone")
        root.a28 = this.claims.sign_jwt_rs256("dummy-rsa")
        root.a29 = this.claims.sign_jwt_rs384("dummy-rsa")
        root.a30 = this.claims.sign_jwt_rs512("dummy-rsa")
        root.a31 = this.claims.sign_jwt_es256("dummy-ecdsa")
        root.a32 = this.claims.sign_jwt_es384("dummy-ecdsa")
        root.a33 = this.claims.sign_jwt_es512("dummy-ecdsa")
        # geoip (8)
        root.a34 = this.ip.geoip_city("/path/to/city.mmdb")
        root.a35 = this.ip.geoip_country("/path/to/country.mmdb")
        root.a36 = this.ip.geoip_asn("/path/to/asn.mmdb")
        root.a37 = this.ip.geoip_enterprise("/path/to/enterprise.mmdb")
        root.a38 = this.ip.geoip_anonymous_ip("/path/to/anon.mmdb")
        root.a39 = this.ip.geoip_connection_type("/path/to/conn.mmdb")
        root.a40 = this.ip.geoip_domain("/path/to/domain.mmdb")
        root.a41 = this.ip.geoip_isp("/path/to/isp.mmdb")
        # functions
        root.a42 = with_schema_registry_header(123, "x")
        root.a43 = fake("email")
        root.a44 = snowflake_id()
        root.a45 = ulid()

output:
  drop: {}
`

// TestMigrateV5AllPluginsFixture exercises one callsite for every Bloblang
// plugin shipped by connect. The migrator should rewrite the embedded body
// successfully and leave coverage at 1.0 with zero unsupported.
func TestMigrateV5AllPluginsFixture(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "stream.yaml")
	require.NoError(t, os.WriteFile(src, []byte(allPluginsFixture), 0o644))

	ctx := newCommandContext(t, map[string]string{
		"check":  "true",
		"report": "json",
	}, src)

	var stdout, stderr bytes.Buffer
	failed, err := runMigrateV5With(ctx, &stdout, &stderr)
	require.NoError(t, err)
	require.False(t, failed, "stderr=%s\nstdout=%s", stderr.String(), stdout.String())

	report := stdout.String()
	assert.Contains(t, report, `"Rewritten":1`)
	assert.Contains(t, report, `"Unsupported":0`)

	// Now run again without --check so the rewritten file is produced; assert
	// every connect plugin shows up under its V2 name in the output.
	ctx = newCommandContext(t, map[string]string{
		"report": "text",
	}, src)
	stdout.Reset()
	stderr.Reset()
	failed, err = runMigrateV5With(ctx, &stdout, &stderr)
	require.NoError(t, err)
	require.False(t, failed, "stderr=%s", stderr.String())

	rewritten, err := os.ReadFile(filepath.Join(dir, "stream.v5.yaml"))
	require.NoError(t, err)
	body := string(rewritten)

	for _, name := range []string{
		"parse_msgpack", "format_msgpack",
		"parse_parquet", "vector", "json_path",
		"strip_html", "parse_xml", "format_xml",
		"diff", "patch",
		"compare_argon2", "compare_bcrypt",
		"slug", "unicode_segments",
		"parse_jwt_hs256", "parse_jwt_hs384", "parse_jwt_hs512",
		"parse_jwt_rs256", "parse_jwt_rs384", "parse_jwt_rs512",
		"parse_jwt_es256", "parse_jwt_es384", "parse_jwt_es512",
		"sign_jwt_hs256", "sign_jwt_hs384", "sign_jwt_hs512",
		"sign_jwt_rs256", "sign_jwt_rs384", "sign_jwt_rs512",
		"sign_jwt_es256", "sign_jwt_es384", "sign_jwt_es512",
		"geoip_city", "geoip_country", "geoip_asn", "geoip_enterprise",
		"geoip_anonymous_ip", "geoip_connection_type", "geoip_domain", "geoip_isp",
		"with_schema_registry_header",
		"fake", "snowflake_id", "ulid",
	} {
		assert.Containsf(t, body, name, "rewritten config missing plugin %q", name)
	}

	// And the wrapping processor type must have switched.
	assert.Contains(t, body, "bloblang_v2:")
	assert.NotContains(t, body, "bloblang: |")
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
