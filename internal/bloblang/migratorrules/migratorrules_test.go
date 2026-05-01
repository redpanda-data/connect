// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package migratorrules_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
	bloblmig "github.com/redpanda-data/benthos/v4/public/bloblangv2/migrator"

	// Side-effect imports: register V2 plugin counterparts so the migrated
	// mappings below parse against the global V2 environment.
	"github.com/redpanda-data/connect/v4/internal/bloblang/migratorrules"
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

func TestPluginRulesPassThrough(t *testing.T) {
	cases := []struct {
		name string
		v1   string
		want string
		// skipV2Parse skips the post-migration bloblangv2.Parse round-trip for
		// plugins whose constructor opens an external resource at parse time
		// (e.g. geoip_* loads an mmdb file via positional literal-fold). The
		// migrator rewrite is still asserted via the `want` substring.
		skipV2Parse bool
	}{
		// methods
		{
			name: "parse_msgpack zero-arg method",
			v1:   `root = this.payload.parse_msgpack()`,
			want: ".parse_msgpack()",
		},
		{
			name: "format_msgpack zero-arg method",
			v1:   `root = this.format_msgpack()`,
			want: ".format_msgpack()",
		},
		{
			name: "parse_parquet with named arg",
			v1:   `root = this.parse_parquet(byte_array_as_string: true)`,
			want: ".parse_parquet(",
		},
		{
			name: "vector zero-arg method",
			v1:   `root = this.embeddings.vector()`,
			want: ".vector()",
		},
		{
			name: "json_path with string arg",
			v1:   `root = this.json_path("$..name")`,
			want: `.json_path("$..name")`,
		},
		{
			name: "strip_html zero-arg method",
			v1:   `root = this.html.strip_html()`,
			want: ".strip_html()",
		},
		{
			name: "strip_html with array arg",
			v1:   `root = this.html.strip_html(["strong"])`,
			want: ".strip_html(",
		},
		{
			name: "parse_xml with named arg",
			v1:   `root = this.doc.parse_xml(cast: true)`,
			want: ".parse_xml(",
		},
		{
			name: "format_xml with named arg",
			v1:   `root = this.format_xml(no_indent: true)`,
			want: ".format_xml(",
		},
		{
			name: "diff with arg",
			v1:   `root = this.before.diff(this.after)`,
			want: ".diff(",
		},
		{
			name: "patch with arg",
			v1:   `root = this.input.patch(this.changelog)`,
			want: ".patch(",
		},
		{
			name: "compare_argon2 with hashed secret",
			v1:   `root = this.user.compare_argon2("$argon2id$v=19$m=4096,t=3,p=1$c2FsdHktbWNzYWx0ZmFjZQ$XTu19IC4rYL/ERsDZr2HOZe9bcMx88ARJ/VVfT2Lb3U")`,
			want: ".compare_argon2(",
		},
		{
			name: "compare_bcrypt with hashed secret",
			v1:   `root = this.user.compare_bcrypt("$2y$10$Dtnt5NNzVtMCOZONT705tOcS8It6krJX8bEjnDJnwxiFKsz1C.3Ay")`,
			want: ".compare_bcrypt(",
		},
		{
			name: "slug with default lang",
			v1:   `root = this.title.slug()`,
			want: ".slug()",
		},
		{
			name: "unicode_segments with arg",
			v1:   `root = this.text.unicode_segments("word")`,
			want: ".unicode_segments(",
		},
		// functions
		{
			name: "with_schema_registry_header function call",
			v1:   `root = with_schema_registry_header(123, "hello")`,
			want: "with_schema_registry_header(",
		},
		{
			name: "fake function with name",
			v1:   `root = fake("email")`,
			want: `fake("email")`,
		},
		{
			name: "snowflake_id with default node",
			v1:   `root = snowflake_id()`,
			want: "snowflake_id(",
		},
		{
			name: "ulid with default args",
			v1:   `root = ulid()`,
			want: "ulid(",
		},
		{
			name: "parse_jwt_hs256 with secret",
			v1:   `root = this.token.parse_jwt_hs256("dont-tell-anyone")`,
			want: ".parse_jwt_hs256(",
		},
		{
			name: "sign_jwt_es512 with named arg",
			v1:   `root = this.claims.sign_jwt_es512(signing_secret: "x")`,
			want: ".sign_jwt_es512(",
		},
		{
			name:        "geoip_city with path",
			v1:          `root = this.ip.geoip_city("/path/to/city.mmdb")`,
			want:        ".geoip_city(",
			skipV2Parse: true,
		},
		{
			name:        "geoip_isp with path",
			v1:          `root = this.ip.geoip_isp("/path/to/isp.mmdb")`,
			want:        ".geoip_isp(",
			skipV2Parse: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mig := bloblmig.New()
			migratorrules.Register(mig)

			rep, err := mig.Migrate(tc.v1, bloblmig.Options{})
			require.NoError(t, err)

			assert.Contains(t, rep.V2Mapping, tc.want)

			if tc.skipV2Parse {
				return
			}
			// The migrated mapping must parse against the V2 environment that
			// has the V2 plugin registrations applied.
			_, err = bloblangv2.Parse(rep.V2Mapping)
			require.NoErrorf(t, err, "V2 mapping failed to parse:\n%s", rep.V2Mapping)
		})
	}
}

func TestMsgpackEndToEndEvaluation(t *testing.T) {
	// Confirm the migrated mapping is functionally equivalent to V1 by
	// round-tripping a value: format_msgpack -> parse_msgpack should
	// recover the input.
	mig := bloblmig.New()
	migratorrules.Register(mig)

	rep, err := mig.Migrate(`root = this.format_msgpack().parse_msgpack()`, bloblmig.Options{})
	require.NoError(t, err)
	require.True(t, strings.Contains(rep.V2Mapping, "format_msgpack") && strings.Contains(rep.V2Mapping, "parse_msgpack"))

	exec, err := bloblangv2.Parse(rep.V2Mapping)
	require.NoError(t, err)

	got, err := exec.Query(map[string]any{"foo": "bar"})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"foo": "bar"}, got)
}
