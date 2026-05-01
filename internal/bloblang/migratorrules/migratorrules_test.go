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

	// Side-effect import: registers the V2 plugin counterparts so the
	// migrated mapping below parses against the global V2 environment.
	"github.com/redpanda-data/connect/v4/internal/bloblang/migratorrules"
	_ "github.com/redpanda-data/connect/v4/internal/impl/msgpack"
)

func TestMethodRulesPassThrough(t *testing.T) {
	cases := []struct {
		name string
		v1   string
		want string
	}{
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
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mig := bloblmig.New()
			migratorrules.Register(mig)

			rep, err := mig.Migrate(tc.v1, bloblmig.Options{})
			require.NoError(t, err)

			assert.Contains(t, rep.V2Mapping, tc.want)

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
