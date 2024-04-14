package bloblang

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsedParamsNameless(t *testing.T) {
	params := NewPluginSpec().
		Param(NewStringParam("first")).
		Param(NewInt64Param("second").Default(5)).
		Param(NewBoolParam("third"))

	parsedInternal, err := params.params.PopulateNameless("foo", 9, true)
	require.NoError(t, err)

	assert.Equal(t, []any{
		"foo", int64(9), true,
	}, parsedInternal.Raw())

	parsed := &ParsedParams{par: parsedInternal}

	v, err := parsed.Get("first")
	require.NoError(t, err)
	assert.Equal(t, "foo", v)

	v, err = parsed.Get("second")
	require.NoError(t, err)
	assert.Equal(t, int64(9), v)

	v, err = parsed.Get("third")
	require.NoError(t, err)
	assert.Equal(t, true, v)

	_, err = parsed.Get("fourth")
	require.Error(t, err)
}

func TestParsedParamsNamed(t *testing.T) {
	params := NewPluginSpec().
		Param(NewStringParam("first")).
		Param(NewInt64Param("second").Default(5)).
		Param(NewBoolParam("third"))

	parsedInternal, err := params.params.PopulateNamed(map[string]any{
		"first":  "foo",
		"second": 9,
		"third":  true,
	})
	require.NoError(t, err)

	assert.Equal(t, []any{
		"foo", int64(9), true,
	}, parsedInternal.Raw())

	parsed := &ParsedParams{par: parsedInternal}

	v, err := parsed.Get("first")
	require.NoError(t, err)
	assert.Equal(t, "foo", v)

	v, err = parsed.Get("second")
	require.NoError(t, err)
	assert.Equal(t, int64(9), v)

	v, err = parsed.Get("third")
	require.NoError(t, err)
	assert.Equal(t, true, v)

	_, err = parsed.Get("fourth")
	require.Error(t, err)
}

func TestParsedParams(t *testing.T) {
	params := NewPluginSpec().
		Param(NewStringParam("first").Optional()).
		Param(NewInt64Param("second").Optional()).
		Param(NewFloat64Param("third").Optional()).
		Param(NewBoolParam("fourth").Optional()).
		Param(NewTimestampParam("fifth").Optional())

	parsedInternal, err := params.params.PopulateNameless("one", 2, 3.0, true, "2023-10-13T08:39:28+00:00")
	require.NoError(t, err)

	parsed := &ParsedParams{par: parsedInternal}

	s, err := parsed.GetString("first")
	require.NoError(t, err)
	assert.Equal(t, "one", s)

	i, err := parsed.GetInt64("second")
	require.NoError(t, err)
	assert.Equal(t, int64(2), i)

	f, err := parsed.GetFloat64("third")
	require.NoError(t, err)
	assert.Equal(t, 3.0, f)

	b, err := parsed.GetBool("fourth")
	require.NoError(t, err)
	assert.True(t, b)

	ts, err := parsed.GetTimestamp("fifth")
	require.NoError(t, err)
	assert.Equal(t, ts.UTC(), time.Unix(1697186368, 0).UTC())
}

func TestParsedParamsOptional(t *testing.T) {
	params := NewPluginSpec().
		Param(NewStringParam("first").Optional()).
		Param(NewInt64Param("second").Optional()).
		Param(NewFloat64Param("third").Optional()).
		Param(NewBoolParam("fourth").Optional()).
		Param(NewTimestampParam("fifth").Optional())

	parsedInternal, err := params.params.PopulateNameless("one", 2, 3.0, true, "2023-10-13T08:39:28+00:00")
	require.NoError(t, err)

	parsed := &ParsedParams{par: parsedInternal}

	s, err := parsed.GetOptionalString("first")
	require.NoError(t, err)
	require.NotNil(t, s)
	assert.Equal(t, "one", *s)

	i, err := parsed.GetOptionalInt64("second")
	require.NoError(t, err)
	require.NotNil(t, i)
	assert.Equal(t, int64(2), *i)

	f, err := parsed.GetOptionalFloat64("third")
	require.NoError(t, err)
	require.NotNil(t, f)
	assert.Equal(t, 3.0, *f)

	b, err := parsed.GetOptionalBool("fourth")
	require.NoError(t, err)
	require.NotNil(t, b)
	assert.True(t, *b)

	ts, err := parsed.GetOptionalTimestamp("fifth")
	require.NoError(t, err)
	require.NotNil(t, ts)
	assert.Equal(t, ts.UTC(), time.Unix(1697186368, 0).UTC())

	// Without any args
	parsedInternal, err = params.params.PopulateNameless()
	require.NoError(t, err)

	parsed = &ParsedParams{par: parsedInternal}

	s, err = parsed.GetOptionalString("first")
	require.NoError(t, err)
	assert.Nil(t, s)

	i, err = parsed.GetOptionalInt64("second")
	require.NoError(t, err)
	assert.Nil(t, i)

	f, err = parsed.GetOptionalFloat64("third")
	require.NoError(t, err)
	assert.Nil(t, f)

	b, err = parsed.GetOptionalBool("fourth")
	require.NoError(t, err)
	assert.Nil(t, b)

	ts, err = parsed.GetOptionalTimestamp("fifth")
	require.NoError(t, err)
	assert.Nil(t, ts)
}
