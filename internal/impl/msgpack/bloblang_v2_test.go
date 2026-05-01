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

package msgpack

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"

	"github.com/redpanda-data/connect/v4/internal/bloblang/migratortest"
)

func TestMsgpackParseV2FromBytes(t *testing.T) {
	encoded, err := hex.DecodeString("81a3666f6fa3626172")
	require.NoError(t, err)

	exec, err := bloblangv2.Parse(`output = input.parse_msgpack()`)
	require.NoError(t, err)

	got, err := exec.Query(encoded)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"foo": "bar"}, got)
}

func TestMsgpackParseV2FromString(t *testing.T) {
	encoded, err := hex.DecodeString("81a3666f6fa3626172")
	require.NoError(t, err)

	exec, err := bloblangv2.Parse(`output = input.parse_msgpack()`)
	require.NoError(t, err)

	got, err := exec.Query(string(encoded))
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"foo": "bar"}, got)
}

func TestMsgpackFormatV2RoundTrip(t *testing.T) {
	formatExec, err := bloblangv2.Parse(`output = input.format_msgpack()`)
	require.NoError(t, err)

	parseExec, err := bloblangv2.Parse(`output = input.parse_msgpack()`)
	require.NoError(t, err)

	formatted, err := formatExec.Query(map[string]any{"foo": "bar"})
	require.NoError(t, err)

	parsed, err := parseExec.Query(formatted)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"foo": "bar"}, parsed)
}

func TestMsgpackParseV2RejectsNonBytesReceiver(t *testing.T) {
	exec, err := bloblangv2.Parse(`output = input.parse_msgpack()`)
	require.NoError(t, err)

	_, err = exec.Query(42)
	require.Error(t, err)
}

func TestMsgpackEquivalenceV1V2(t *testing.T) {
	encoded, err := hex.DecodeString("81a3666f6fa3626172")
	require.NoError(t, err)

	t.Run("parse_msgpack from bytes", func(t *testing.T) {
		migratortest.AssertEquivalent(t,
			`root = this.parse_msgpack()`,
			encoded,
			map[string]any{"foo": "bar"},
		)
	})
	t.Run("parse_msgpack from string", func(t *testing.T) {
		migratortest.AssertEquivalent(t,
			`root = this.parse_msgpack()`,
			string(encoded),
			map[string]any{"foo": "bar"},
		)
	})
	t.Run("format_msgpack then parse_msgpack round trip", func(t *testing.T) {
		migratortest.AssertEquivalent(t,
			`root = this.format_msgpack().parse_msgpack()`,
			map[string]any{"foo": "bar"},
			map[string]any{"foo": "bar"},
		)
	})
}
