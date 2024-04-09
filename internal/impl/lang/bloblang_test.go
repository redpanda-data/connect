package lang

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func TestFakeFunction_Invalid(t *testing.T) {
	e, err := query.InitFunctionHelper("fake", "foo")
	require.NoError(t, err)

	res, err := e.Exec(query.FunctionContext{})
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
			e, err := query.InitFunctionHelper("fake", test.function)
			require.NoError(t, err)

			res, err := e.Exec(query.FunctionContext{})
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
