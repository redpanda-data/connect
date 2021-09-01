package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParamsValidation(t *testing.T) {
	tests := []struct {
		name        string
		params      Params
		errContains string
	}{
		{
			name: "basic fields all normal",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Default(5)).
				Add(ParamBool("third", "")).
				Add(ParamFloat("fourth", "")).
				Add(ParamQuery("fifth", "")).
				Add(ParamArray("sixth", "")).
				Add(ParamObject("seventh", "")),
		},
		{
			name: "old style with fields",
			params: OldStyleParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Default(5)).
				Add(ParamBool("third", "").Default(true)),
			errContains: "cannot add named parameters to an old style",
		},
		{
			name: "variadic with fields",
			params: VariadicParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Default(5)).
				Add(ParamBool("third", "").Default(true)),
			errContains: "cannot add named parameters to a variadic parameter definition",
		},
		{
			name: "empty field name",
			params: NewParams().
				Add(ParamString("", "")),
			errContains: "parameter name '' does not match",
		},
		{
			name: "bad field name",
			params: NewParams().
				Add(ParamString("contains naughty chars!", "")),
			errContains: "parameter name 'contains naughty chars!' does not match",
		},
		{
			name: "duplicate field names",
			params: NewParams().
				Add(ParamString("foo", "")).
				Add(ParamString("bar", "")).
				Add(ParamString("foo", "")),
			errContains: "duplicate parameter name: foo",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.params.validate()
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestParamsNameless(t *testing.T) {
	tests := []struct {
		name        string
		params      Params
		input       []interface{}
		output      []interface{}
		errContains string
	}{
		{
			name: "basic fields all populated",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Default(5)).
				Add(ParamBool("third", "")).
				Add(ParamFloat("fourth", "")).
				Add(ParamQuery("fifth", "")).
				Add(ParamArray("sixth", "")).
				Add(ParamObject("seventh", "")),
			input: []interface{}{
				"foo", 5, false, 6.4, NewFieldFunction("nah"), []interface{}{"one", "two"}, map[string]interface{}{"a": "aaa", "b": "bbb"},
			},
			output: []interface{}{
				"foo", int64(5), false, 6.4, NewFieldFunction("nah"), []interface{}{"one", "two"}, map[string]interface{}{"a": "aaa", "b": "bbb"},
			},
		},
		{
			name: "basic fields defaults",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Default(5)).
				Add(ParamBool("third", "").Default(true)),
			input: []interface{}{"bar"},
			output: []interface{}{
				"bar", int64(5), true,
			},
		},
		{
			name: "basic fields optional no defaults",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Optional()).
				Add(ParamBool("third", "").Optional()),
			input: []interface{}{"bar"},
			output: []interface{}{
				"bar", nil, nil,
			},
		},
		{
			name: "missing field",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Default(5)).
				Add(ParamBool("third", "").Default(true)),
			input:       []interface{}{},
			errContains: "missing parameter: first",
		},
		{
			name: "multiple missing fields",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "")).
				Add(ParamBool("third", "")),
			input:       []interface{}{},
			errContains: "missing parameters: first, second, third",
		},
		{
			name: "too many args",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Default(5)).
				Add(ParamBool("third", "").Default(true)),
			input:       []interface{}{"foo", 10, false, "bar"},
			errContains: "wrong number of arguments, expected 3, got 4",
		},
		{
			name: "bad type args",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Default(5)).
				Add(ParamBool("third", "").Default(true)),
			input:       []interface{}{"foo", true, 10},
			errContains: "field second: expected number",
		},
		{
			name: "function args unchanged",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Default(5)).
				Add(ParamBool("third", "")),
			input: []interface{}{
				"foo", NewFieldFunction("doc.value"), false,
			},
			output: []interface{}{
				"foo", NewFieldFunction("doc.value"), false,
			},
		},
		{
			name: "literal args expanded",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Default(5)).
				Add(ParamBool("third", "")),
			input: []interface{}{
				"foo", NewLiteralFunction("testing", int64(7)), false,
			},
			output: []interface{}{
				"foo", int64(7), false,
			},
		},
		{
			name:   "old style args expanded",
			params: OldStyleParams(),
			input: []interface{}{
				"foo", NewLiteralFunction("testing", int64(7)), false,
			},
			output: []interface{}{
				"foo", int64(7), false,
			},
		},
		{
			name:   "variadic args expanded",
			params: VariadicParams(),
			input: []interface{}{
				"foo", NewLiteralFunction("testing", int64(7)), false,
			},
			output: []interface{}{
				"foo", int64(7), false,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.params.processNameless(test.input)
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.output, res)
			}
		})
	}
}

func TestParamsNamed(t *testing.T) {
	tests := []struct {
		name        string
		params      Params
		input       map[string]interface{}
		output      []interface{}
		errContains string
	}{
		{
			name: "basic fields all populated",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Default(5)).
				Add(ParamBool("third", "")).
				Add(ParamFloat("fourth", "")).
				Add(ParamQuery("fifth", "")).
				Add(ParamArray("sixth", "")).
				Add(ParamObject("seventh", "")),
			input: map[string]interface{}{
				"first": "foo", "second": 5, "third": false, "fourth": 6.4,
				"fifth": NewFieldFunction("nah"), "sixth": []interface{}{"one", "two"},
				"seventh": map[string]interface{}{"a": "aaa", "b": "bbb"},
			},
			output: []interface{}{
				"foo", int64(5), false, 6.4, NewFieldFunction("nah"), []interface{}{"one", "two"}, map[string]interface{}{"a": "aaa", "b": "bbb"},
			},
		},
		{
			name: "basic fields defaults",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Default(5)).
				Add(ParamBool("third", "").Default(true)),
			input: map[string]interface{}{"first": "bar"},
			output: []interface{}{
				"bar", int64(5), true,
			},
		},
		{
			name: "basic fields optional no defaults",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Optional()).
				Add(ParamBool("third", "").Optional()),
			input: map[string]interface{}{"first": "bar"},
			output: []interface{}{
				"bar", nil, nil,
			},
		},
		{
			name: "missing field",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Default(5)).
				Add(ParamBool("third", "").Default(true)),
			input:       map[string]interface{}{},
			errContains: "missing parameter: first",
		},
		{
			name: "multiple missing fields",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "")).
				Add(ParamBool("third", "")),
			input:       map[string]interface{}{},
			errContains: "missing parameters: first, second, third",
		},
		{
			name: "too many args",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Default(5)).
				Add(ParamBool("third", "").Default(true)),
			input: map[string]interface{}{
				"first": "foo", "second": 10, "third": false, "fourth": "bar"},
			errContains: "unknown parameter fourth",
		},
		{
			name: "bad type args",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Default(5)).
				Add(ParamBool("third", "").Default(true)),
			input:       map[string]interface{}{"first": "foo", "second": true, "third": 10},
			errContains: "field second: expected number",
		},
		{
			name: "function args unchanged",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Default(5)).
				Add(ParamBool("third", "")),
			input: map[string]interface{}{
				"first": "foo", "second": NewFieldFunction("doc.value"), "third": false,
			},
			output: []interface{}{
				"foo", NewFieldFunction("doc.value"), false,
			},
		},
		{
			name: "literal args expanded",
			params: NewParams().
				Add(ParamString("first", "")).
				Add(ParamInt64("second", "").Default(5)).
				Add(ParamBool("third", "")),
			input: map[string]interface{}{
				"first": "foo", "second": NewLiteralFunction("testing", 7), "third": false,
			},
			output: []interface{}{
				"foo", int64(7), false,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.params.processNamed(test.input)
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.output, res)
			}
		})
	}
}

func TestOldStyleParams(t *testing.T) {
	p := Params{oldStyle: true}

	exp := []interface{}{
		"foo", 20, 34.5, true,
	}
	res, err := p.processNameless(exp)
	require.NoError(t, err)
	assert.Equal(t, exp, res)

	_, err = p.processNamed(nil)
	require.Error(t, err)
}

func TestDynamicArgs(t *testing.T) {
	p := NewParams().
		Add(ParamString("foo", "")).
		Add(ParamQuery("bar", "")).
		Add(ParamString("baz", ""))

	exp := []dynamicArgIndex(nil)
	res := p.gatherDynamicArgs([]interface{}{"first", "second", "third"})
	assert.Equal(t, exp, res)

	exp = []dynamicArgIndex{
		{index: 0, fn: NewFieldFunction("first")},
		{index: 2, fn: NewFieldFunction("third")},
	}
	res = p.gatherDynamicArgs([]interface{}{
		NewFieldFunction("first"),
		NewFieldFunction("second"),
		NewFieldFunction("third"),
	})
	assert.Equal(t, exp, res)
}

func TestDynamicVariadicArgs(t *testing.T) {
	p := VariadicParams()

	exp := []dynamicArgIndex(nil)
	res := p.gatherDynamicArgs([]interface{}{"first", "second", "third"})
	assert.Equal(t, exp, res)

	dynArgs := []interface{}{
		NewFieldFunction("first"),
		NewFieldFunction("second"),
		NewFieldFunction("third"),
	}

	exp = []dynamicArgIndex{
		{index: 0, fn: NewFieldFunction("first")},
		{index: 1, fn: NewFieldFunction("second")},
		{index: 2, fn: NewFieldFunction("third")},
	}
	res = p.gatherDynamicArgs(dynArgs)
	assert.Equal(t, exp, res)

	parsed, err := p.PopulateNameless(dynArgs...)
	require.NoError(t, err)

	newParsed, err := parsed.ResolveDynamic(FunctionContext{}.WithValue(map[string]interface{}{
		"first":  "first value",
		"second": "second value",
		"third":  "third value",
	}))
	require.NoError(t, err)

	assert.Equal(t, []interface{}{"first value", "second value", "third value"}, newParsed.Raw())
}

func TestParsedParamsNameless(t *testing.T) {
	params := NewParams().
		Add(ParamString("first", "")).
		Add(ParamInt64("second", "").Default(5)).
		Add(ParamBool("third", ""))

	parsed, err := params.PopulateNameless("foo", 9, true)
	require.NoError(t, err)

	assert.Empty(t, parsed.dynamic())
	assert.Equal(t, []interface{}{
		"foo", int64(9), true,
	}, parsed.Raw())

	v, err := parsed.Index(0)
	require.NoError(t, err)
	assert.Equal(t, "foo", v)

	v, err = parsed.Field("first")
	require.NoError(t, err)
	assert.Equal(t, "foo", v)

	v, err = parsed.Index(1)
	require.NoError(t, err)
	assert.Equal(t, int64(9), v)

	v, err = parsed.Field("second")
	require.NoError(t, err)
	assert.Equal(t, int64(9), v)

	v, err = parsed.Index(2)
	require.NoError(t, err)
	assert.Equal(t, true, v)

	v, err = parsed.Field("third")
	require.NoError(t, err)
	assert.Equal(t, true, v)

	_, err = parsed.Index(-1)
	require.Error(t, err)

	_, err = parsed.Index(3)
	require.Error(t, err)

	_, err = parsed.Field("fourth")
	require.Error(t, err)
}

func TestParsedParamsNamed(t *testing.T) {
	params := NewParams().
		Add(ParamString("first", "")).
		Add(ParamInt64("second", "").Default(5)).
		Add(ParamBool("third", ""))

	parsed, err := params.PopulateNamed(map[string]interface{}{
		"first":  "foo",
		"second": 9,
		"third":  true,
	})
	require.NoError(t, err)

	assert.Empty(t, parsed.dynamic())
	assert.Equal(t, []interface{}{
		"foo", int64(9), true,
	}, parsed.Raw())

	v, err := parsed.Index(0)
	require.NoError(t, err)
	assert.Equal(t, "foo", v)

	v, err = parsed.Field("first")
	require.NoError(t, err)
	assert.Equal(t, "foo", v)

	v, err = parsed.Index(1)
	require.NoError(t, err)
	assert.Equal(t, int64(9), v)

	v, err = parsed.Field("second")
	require.NoError(t, err)
	assert.Equal(t, int64(9), v)

	v, err = parsed.Index(2)
	require.NoError(t, err)
	assert.Equal(t, true, v)

	v, err = parsed.Field("third")
	require.NoError(t, err)
	assert.Equal(t, true, v)

	_, err = parsed.Index(-1)
	require.Error(t, err)

	_, err = parsed.Index(3)
	require.Error(t, err)

	_, err = parsed.Field("fourth")
	require.Error(t, err)
}

func TestParsedParamsDynamic(t *testing.T) {
	params := NewParams().
		Add(ParamString("first", "")).
		Add(ParamInt64("second", "").Default(5)).
		Add(ParamBool("third", ""))

	parsed, err := params.PopulateNameless(NewFieldFunction("doc.foo"), 9, NewFieldFunction("doc.bar"))
	require.NoError(t, err)

	assert.Equal(t, []Function{
		NewFieldFunction("doc.foo"),
		NewFieldFunction("doc.bar"),
	}, parsed.dynamic())

	parsedTwo, err := parsed.ResolveDynamic(FunctionContext{}.WithValue(map[string]interface{}{
		"doc": map[string]interface{}{
			"foo": "foo first value",
			"bar": true,
		},
	}))
	require.NoError(t, err)

	parsedThree, err := parsed.ResolveDynamic(FunctionContext{}.WithValue(map[string]interface{}{
		"doc": map[string]interface{}{
			"foo": "foo second value",
			"bar": false,
		},
	}))
	require.NoError(t, err)

	assert.Empty(t, parsedTwo.dynamic())
	assert.Equal(t, []interface{}{
		"foo first value", int64(9), true,
	}, parsedTwo.Raw())

	require.NoError(t, err)
	assert.Empty(t, parsedThree.dynamic())
	assert.Equal(t, []interface{}{
		"foo second value", int64(9), false,
	}, parsedThree.Raw())
}

func TestParsedParamsDynamicErrors(t *testing.T) {
	tests := []struct {
		name        string
		input       interface{}
		errContains string
	}{
		{
			name:        "function fails",
			errContains: "context was undefined",
		},
		{
			name: "function gives wrong type",
			input: map[string]interface{}{
				"doc": map[string]interface{}{
					"foo": 60,
					"bar": "not a bool",
				},
			},
			errContains: "first: wrong argument type",
		},
	}

	params := NewParams().
		Add(ParamString("first", "")).
		Add(ParamInt64("second", "").Default(5)).
		Add(ParamBool("third", ""))

	parsed, err := params.PopulateNameless(NewFieldFunction("doc.foo"), 9, NewFieldFunction("doc.bar"))
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := FunctionContext{}
			if test.input != nil {
				ctx = ctx.WithValue(test.input)
			}
			_, err := parsed.ResolveDynamic(ctx)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.errContains)
		})
	}
}

func TestParsedParams(t *testing.T) {
	params := NewParams().
		Add(ParamString("first", "").Optional()).
		Add(ParamInt64("second", "").Optional()).
		Add(ParamFloat("third", "").Optional()).
		Add(ParamBool("fourth", "").Optional()).
		Add(ParamQuery("fifth", "").Optional())

	parsed, err := params.PopulateNameless("one", 2, 3.0, true, NewFieldFunction("doc.foo"))
	require.NoError(t, err)

	s, err := parsed.FieldString("first")
	require.NoError(t, err)
	assert.Equal(t, "one", s)

	i, err := parsed.FieldInt64("second")
	require.NoError(t, err)
	assert.Equal(t, int64(2), i)

	f, err := parsed.FieldFloat("third")
	require.NoError(t, err)
	assert.Equal(t, 3.0, f)

	b, err := parsed.FieldBool("fourth")
	require.NoError(t, err)
	assert.Equal(t, true, b)

	q, err := parsed.FieldQuery("fifth")
	require.NoError(t, err)
	assert.Equal(t, NewFieldFunction("doc.foo"), q)
}

func TestParsedParamsOptional(t *testing.T) {
	params := NewParams().
		Add(ParamString("first", "").Optional()).
		Add(ParamInt64("second", "").Optional()).
		Add(ParamFloat("third", "").Optional()).
		Add(ParamBool("fourth", "").Optional()).
		Add(ParamQuery("fifth", "").Optional())

	parsed, err := params.PopulateNameless("one", 2, 3.0, true, NewFieldFunction("doc.foo"))
	require.NoError(t, err)

	s, err := parsed.FieldOptionalString("first")
	require.NoError(t, err)
	require.NotNil(t, s)
	assert.Equal(t, "one", *s)

	i, err := parsed.FieldOptionalInt64("second")
	require.NoError(t, err)
	require.NotNil(t, i)
	assert.Equal(t, int64(2), *i)

	f, err := parsed.FieldOptionalFloat("third")
	require.NoError(t, err)
	require.NotNil(t, f)
	assert.Equal(t, 3.0, *f)

	b, err := parsed.FieldOptionalBool("fourth")
	require.NoError(t, err)
	require.NotNil(t, b)
	assert.Equal(t, true, *b)

	q, err := parsed.FieldOptionalQuery("fifth")
	require.NoError(t, err)
	require.NotNil(t, q)
	assert.Equal(t, NewFieldFunction("doc.foo"), q)

	// Without any args
	parsed, err = params.PopulateNameless()
	require.NoError(t, err)

	s, err = parsed.FieldOptionalString("first")
	require.NoError(t, err)
	assert.Nil(t, s)

	i, err = parsed.FieldOptionalInt64("second")
	require.NoError(t, err)
	assert.Nil(t, i)

	f, err = parsed.FieldOptionalFloat("third")
	require.NoError(t, err)
	assert.Nil(t, f)

	b, err = parsed.FieldOptionalBool("fourth")
	require.NoError(t, err)
	assert.Nil(t, b)

	q, err = parsed.FieldOptionalQuery("fifth")
	require.NoError(t, err)
	assert.Nil(t, q)
}
