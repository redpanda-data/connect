// Copyright 2024 Redpanda Data, Inc.
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

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

func TestFakeFunction_Invalid(t *testing.T) {
	e, err := bloblang.Parse(`root = fake("foo")`)
	require.NoError(t, err)

	res, err := e.Query(nil)
	require.Error(t, err, "invalid faker function: foo")
	assert.Empty(t, res)
}

func TestFieldsFromNode(t *testing.T) {
	tests := []struct {
		name     string
		function string
	}{
		{
			name:     "default",
			function: "",
		},
		{
			name:     "email function",
			function: "email",
		},
		{
			name:     "phone number function",
			function: "phone_number",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e, err := bloblang.Parse(fmt.Sprintf(`root = fake("%v")`, test.function))
			require.NoError(t, err)

			res, err := e.Query(nil)
			require.NoError(t, err)

			assert.NotEmpty(t, res)
		})
	}
}

func TestULID(t *testing.T) {
	mapping := `root = ulid()`
	ex, err := bloblang.Parse(mapping)
	require.NoError(t, err, "failed to parse bloblang mapping")

	res, err := ex.Query(nil)
	require.NoError(t, err)

	require.Len(t, res.(string), 26, "ULIDs with crockford base32 encoding must be 26 characters long")
}

func TestULID_FastRandom(t *testing.T) {
	mapping := `root = ulid("crockford", "fast_random")`
	ex, err := bloblang.Parse(mapping)
	require.NoError(t, err, "failed to parse bloblang mapping")

	res, err := ex.Query(nil)
	require.NoError(t, err)

	require.Len(t, res.(string), 26, "ULIDs with crockford base32 encoding must be 26 characters long")
}

func TestULID_HexEncoding(t *testing.T) {
	mapping := `root = ulid("hex")`
	ex, err := bloblang.Parse(mapping)
	require.NoError(t, err, "failed to parse bloblang mapping")

	res, err := ex.Query(nil)
	require.NoError(t, err)

	require.Len(t, res.(string), 32, "ULIDs with hex encoding must be 32 characters long")
}

func TestULID_BadEncoding(t *testing.T) {
	mapping := `root = ulid("what-the-heck")`
	ex, err := bloblang.Parse(mapping)
	require.ErrorContains(t, err, "invalid ulid encoding: what-the-heck")
	require.Nil(t, ex, "did not expect an executable mapping")
}

func TestULID_BadRandom(t *testing.T) {
	mapping := `root = ulid("hex", "not-very-random")`
	ex, err := bloblang.Parse(mapping)
	require.ErrorContains(t, err, "invalid randomness source: not-very-random")
	require.Nil(t, ex, "did not expect an executable mapping")
}

func TestUnicodeSegmentation_Grapheme(t *testing.T) {
	e, err := bloblang.Parse(`root = "foo❤️‍🔥".unicode_segments("grapheme")`)
	require.NoError(t, err)
	res, err := e.Query(nil)
	require.NoError(t, err)
	assert.Equal(t, []any{"f", "o", "o", "❤️‍🔥"}, res)
}

func TestUnicodeSegmentation_Word(t *testing.T) {
	e, err := bloblang.Parse(`root = "what's up?".unicode_segments("word")`)
	require.NoError(t, err)
	res, err := e.Query(nil)
	require.NoError(t, err)
	assert.Equal(t, []any{"what's", " ", "up", "?"}, res)
}

func TestUnicodeSegmentation_Sentence(t *testing.T) {
	e, err := bloblang.Parse(`root = "This is sentence 1.0. This is 2.0!".unicode_segments("sentence")`)
	require.NoError(t, err)
	res, err := e.Query(nil)
	require.NoError(t, err)
	assert.Equal(t, []any{"This is sentence 1.0. ", "This is 2.0!"}, res)
}
