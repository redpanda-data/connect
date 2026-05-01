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

package lang

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
)

func TestFakeFunctionV2Invalid(t *testing.T) {
	e, err := bloblangv2.Parse(`output = fake("foo")`)
	require.NoError(t, err)

	res, err := e.Query(nil)
	require.Error(t, err)
	assert.Empty(t, res)
}

func TestFakeFunctionV2(t *testing.T) {
	tests := []struct{ name, function string }{
		{"default", ""},
		{"email function", "email"},
		{"phone number function", "phone_number"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e, err := bloblangv2.Parse(fmt.Sprintf(`output = fake("%v")`, test.function))
			require.NoError(t, err)
			res, err := e.Query(nil)
			require.NoError(t, err)
			assert.NotEmpty(t, res)
		})
	}
}

func TestULIDV2Crockford(t *testing.T) {
	ex, err := bloblangv2.Parse(`output = ulid()`)
	require.NoError(t, err)

	res, err := ex.Query(nil)
	require.NoError(t, err)
	require.Len(t, res.(string), 26)
}

func TestULIDV2FastRandom(t *testing.T) {
	ex, err := bloblangv2.Parse(`output = ulid("crockford", "fast_random")`)
	require.NoError(t, err)

	res, err := ex.Query(nil)
	require.NoError(t, err)
	require.Len(t, res.(string), 26)
}

func TestULIDV2HexEncoding(t *testing.T) {
	ex, err := bloblangv2.Parse(`output = ulid("hex")`)
	require.NoError(t, err)

	res, err := ex.Query(nil)
	require.NoError(t, err)
	require.Len(t, res.(string), 32)
}

func TestULIDV2BadEncoding(t *testing.T) {
	_, err := bloblangv2.Parse(`output = ulid("what-the-heck")`)
	require.ErrorContains(t, err, "invalid ulid encoding: what-the-heck")
}

func TestULIDV2BadRandom(t *testing.T) {
	_, err := bloblangv2.Parse(`output = ulid("hex", "not-very-random")`)
	require.ErrorContains(t, err, "invalid randomness source: not-very-random")
}

func TestUnicodeSegmentationV2Grapheme(t *testing.T) {
	e, err := bloblangv2.Parse(`output = "foo❤️‍🔥".unicode_segments("grapheme")`)
	require.NoError(t, err)
	res, err := e.Query(nil)
	require.NoError(t, err)
	assert.Equal(t, []any{"f", "o", "o", "❤️‍🔥"}, res)
}

func TestUnicodeSegmentationV2Word(t *testing.T) {
	e, err := bloblangv2.Parse(`output = "what's up?".unicode_segments("word")`)
	require.NoError(t, err)
	res, err := e.Query(nil)
	require.NoError(t, err)
	assert.Equal(t, []any{"what's", " ", "up", "?"}, res)
}

func TestUnicodeSegmentationV2Sentence(t *testing.T) {
	e, err := bloblangv2.Parse(`output = "This is sentence 1.0. This is 2.0!".unicode_segments("sentence")`)
	require.NoError(t, err)
	res, err := e.Query(nil)
	require.NoError(t, err)
	assert.Equal(t, []any{"This is sentence 1.0. ", "This is 2.0!"}, res)
}

func TestSlugV2(t *testing.T) {
	e, err := bloblangv2.Parse(`output = input.slug()`)
	require.NoError(t, err)

	res, err := e.Query("Hello World!")
	require.NoError(t, err)
	assert.Equal(t, "hello-world", res)
}

func TestSnowflakeIDV2(t *testing.T) {
	e, err := bloblangv2.Parse(`output = snowflake_id()`)
	require.NoError(t, err)

	res, err := e.Query(nil)
	require.NoError(t, err)
	assert.NotEmpty(t, res)
}
