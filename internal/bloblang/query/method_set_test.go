package query

import (
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func listMethods(m *MethodSet) []string {
	methodNames := make([]string, 0, len(m.methods))
	for k := range m.methods {
		methodNames = append(methodNames, k)
	}
	sort.Strings(methodNames)
	return methodNames
}

func TestMethodSetWithout(t *testing.T) {
	setOne := AllMethods
	setTwo := setOne.Without("explode")

	assert.Contains(t, listMethods(setOne), "explode")
	assert.NotContains(t, listMethods(setTwo), "explode")

	explodeParamSpec, _ := setOne.Params("explode")
	explodeParams, err := explodeParamSpec.PopulateNameless("foo.bar")
	require.NoError(t, err)

	_, err = setOne.Init("explode", NewLiteralFunction("", nil), explodeParams)
	assert.NoError(t, err)

	_, err = setTwo.Init("explode", NewLiteralFunction("", nil), explodeParams)
	assert.EqualError(t, err, "unrecognised method 'explode'")

	mapEachParamSpec, _ := setTwo.Params("map_each")
	mapEachParams, err := mapEachParamSpec.PopulateNameless(NewFieldFunction("foo"))
	require.NoError(t, err)

	_, err = setTwo.Init("map_each", NewLiteralFunction("", nil), mapEachParams)
	assert.NoError(t, err)
}

func TestMethodSetDeactivated(t *testing.T) {
	setOne := AllMethods.Without()
	setTwo := setOne.Deactivated()

	customErr := errors.New("custom error")

	spec := NewMethodSpec("meow", "").Param(ParamString("val1", ""))
	require.NoError(t, setOne.Add(spec, func(target Function, args *ParsedParams) (Function, error) {
		return ClosureFunction("", func(ctx FunctionContext) (any, error) {
			return nil, customErr
		}, func(ctx TargetsContext) (TargetsContext, []TargetPath) { return ctx, nil }), nil
	}))

	assert.Contains(t, listMethods(setOne), "meow")
	assert.Contains(t, listMethods(setTwo), "meow")

	goodArgs, err := spec.Params.PopulateNameless("hello")
	require.NoError(t, err)

	fnOne, err := setOne.Init("meow", NewLiteralFunction("", nil), goodArgs)
	require.NoError(t, err)

	fnTwo, err := setTwo.Init("meow", NewLiteralFunction("", nil), goodArgs)
	require.NoError(t, err)

	_, err = fnOne.Exec(FunctionContext{})
	assert.Equal(t, customErr, err)

	_, err = fnTwo.Exec(FunctionContext{})
	assert.EqualError(t, err, "this method has been disabled")
}

func TestMethodResolveParamError(t *testing.T) {
	setOne := AllMethods.Without()

	spec := NewMethodSpec("meow", "").Param(ParamString("val1", ""))
	require.NoError(t, setOne.Add(spec, func(target Function, args *ParsedParams) (Function, error) {
		return ClosureFunction("", func(ctx FunctionContext) (any, error) {
			return "ok", nil
		}, func(ctx TargetsContext) (TargetsContext, []TargetPath) { return ctx, nil }), nil
	}))

	assert.Contains(t, listMethods(setOne), "meow")

	badArgs, err := spec.Params.PopulateNameless(NewFieldFunction("doc.foo"))
	require.NoError(t, err)

	fnOne, err := setOne.Init("meow", NewLiteralFunction("", nil), badArgs)
	require.NoError(t, err)

	_, err = fnOne.Exec(FunctionContext{})
	assert.EqualError(t, err, "method 'meow': failed to extract input arg 'val1': context was undefined, unable to reference `doc.foo`")
}

func TestMethodBadName(t *testing.T) {
	testCases := map[string]string{
		"!no":         "method name '!no' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"foo__bar":    "method name 'foo__bar' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"-foo-bar":    "method name '-foo-bar' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"foo-bar-":    "method name 'foo-bar-' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"":            "method name '' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"foo-bar":     "method name 'foo-bar' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"foo-bar_baz": "method name 'foo-bar_baz' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"FOO":         "method name 'FOO' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"foobarbaz":   "",
		"foobarbaz89": "",
		"foo_bar_baz": "",
		"fo1_ba2_ba3": "",
	}

	for k, v := range testCases {
		t.Run(k, func(t *testing.T) {
			setOne := AllMethods.Without()
			err := setOne.Add(NewMethodSpec(k, ""), nil)
			if v != "" {
				assert.EqualError(t, err, v)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
