package bloblang

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsedParamsNameless(t *testing.T) {
	params := NewParamsSpec("", "").
		Add(ParamString("first", "")).
		Add(ParamInt64("second", "").Default(5)).
		Add(ParamBool("third", ""))

	parsedInternal, err := params.params.PopulateNameless("foo", 9, true)
	require.NoError(t, err)

	assert.Equal(t, []interface{}{
		"foo", int64(9), true,
	}, parsedInternal.Raw())

	parsed := &ParsedParams{par: parsedInternal}

	v, err := parsed.Field("first")
	require.NoError(t, err)
	assert.Equal(t, "foo", v)

	v, err = parsed.Field("second")
	require.NoError(t, err)
	assert.Equal(t, int64(9), v)

	v, err = parsed.Field("third")
	require.NoError(t, err)
	assert.Equal(t, true, v)

	_, err = parsed.Field("fourth")
	require.Error(t, err)
}

func TestParsedParamsNamed(t *testing.T) {
	params := NewParamsSpec("", "").
		Add(ParamString("first", "")).
		Add(ParamInt64("second", "").Default(5)).
		Add(ParamBool("third", ""))

	parsedInternal, err := params.params.PopulateNamed(map[string]interface{}{
		"first":  "foo",
		"second": 9,
		"third":  true,
	})
	require.NoError(t, err)

	assert.Equal(t, []interface{}{
		"foo", int64(9), true,
	}, parsedInternal.Raw())

	parsed := &ParsedParams{par: parsedInternal}

	v, err := parsed.Field("first")
	require.NoError(t, err)
	assert.Equal(t, "foo", v)

	v, err = parsed.Field("second")
	require.NoError(t, err)
	assert.Equal(t, int64(9), v)

	v, err = parsed.Field("third")
	require.NoError(t, err)
	assert.Equal(t, true, v)

	_, err = parsed.Field("fourth")
	require.Error(t, err)
}

func TestParsedParams(t *testing.T) {
	params := NewParamsSpec("", "").
		Add(ParamString("first", "").Optional()).
		Add(ParamInt64("second", "").Optional()).
		Add(ParamFloat64("third", "").Optional()).
		Add(ParamBool("fourth", "").Optional())

	parsedInternal, err := params.params.PopulateNameless("one", 2, 3.0, true)
	require.NoError(t, err)

	parsed := &ParsedParams{par: parsedInternal}

	s, err := parsed.FieldString("first")
	require.NoError(t, err)
	assert.Equal(t, "one", s)

	i, err := parsed.FieldInt64("second")
	require.NoError(t, err)
	assert.Equal(t, int64(2), i)

	f, err := parsed.FieldFloat64("third")
	require.NoError(t, err)
	assert.Equal(t, 3.0, f)

	b, err := parsed.FieldBool("fourth")
	require.NoError(t, err)
	assert.Equal(t, true, b)
}

func TestParsedParamsOptional(t *testing.T) {
	params := NewParamsSpec("", "").
		Add(ParamString("first", "").Optional()).
		Add(ParamInt64("second", "").Optional()).
		Add(ParamFloat64("third", "").Optional()).
		Add(ParamBool("fourth", "").Optional())

	parsedInternal, err := params.params.PopulateNameless("one", 2, 3.0, true)
	require.NoError(t, err)

	parsed := &ParsedParams{par: parsedInternal}

	s, err := parsed.FieldOptionalString("first")
	require.NoError(t, err)
	require.NotNil(t, s)
	assert.Equal(t, "one", *s)

	i, err := parsed.FieldOptionalInt64("second")
	require.NoError(t, err)
	require.NotNil(t, i)
	assert.Equal(t, int64(2), *i)

	f, err := parsed.FieldOptionalFloat64("third")
	require.NoError(t, err)
	require.NotNil(t, f)
	assert.Equal(t, 3.0, *f)

	b, err := parsed.FieldOptionalBool("fourth")
	require.NoError(t, err)
	require.NotNil(t, b)
	assert.Equal(t, true, *b)

	// Without any args
	parsedInternal, err = params.params.PopulateNameless()
	require.NoError(t, err)

	parsed = &ParsedParams{par: parsedInternal}

	s, err = parsed.FieldOptionalString("first")
	require.NoError(t, err)
	assert.Nil(t, s)

	i, err = parsed.FieldOptionalInt64("second")
	require.NoError(t, err)
	assert.Nil(t, i)

	f, err = parsed.FieldOptionalFloat64("third")
	require.NoError(t, err)
	assert.Nil(t, f)

	b, err = parsed.FieldOptionalBool("fourth")
	require.NoError(t, err)
	assert.Nil(t, b)
}
