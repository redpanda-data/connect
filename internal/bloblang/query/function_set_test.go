package query

import (
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func listFunctions(f *FunctionSet) []string {
	functionNames := make([]string, 0, len(f.functions))
	for k := range f.functions {
		functionNames = append(functionNames, k)
	}
	sort.Strings(functionNames)
	return functionNames
}

func TestFunctionSetWithout(t *testing.T) {
	setOne := AllFunctions
	setTwo := setOne.Without("uuid_v4")

	assert.Contains(t, listFunctions(setOne), "uuid_v4")
	assert.NotContains(t, listFunctions(setTwo), "uuid_v4")

	_, err := setOne.Init("uuid_v4", nil)
	assert.NoError(t, err)

	_, err = setTwo.Init("uuid_v4", nil)
	assert.EqualError(t, err, "unrecognised function 'uuid_v4'")

	_, err = setTwo.Init("timestamp_unix", nil)
	assert.NoError(t, err)
}

func TestFunctionSetOnlyPure(t *testing.T) {
	setOne := AllFunctions
	require.NoError(t, setOne.Add(NewFunctionSpec("meower", "meow", "Does impure meows.").MarkImpure(), func(args *ParsedParams) (Function, error) {
		return nil, errors.New("not implemented")
	}))
	setTwo := setOne.OnlyPure()

	assert.Contains(t, listFunctions(setOne), "meow")
	assert.NotContains(t, listFunctions(setTwo), "meow")
}

func TestFunctionSetDeactivated(t *testing.T) {
	setOne := AllFunctions.Without()
	setTwo := setOne.Deactivated()

	customErr := errors.New("custom error")

	spec := NewFunctionSpec(FunctionCategoryGeneral, "meow", "").Param(ParamString("val1", ""))
	require.NoError(t, setOne.Add(spec, func(args *ParsedParams) (Function, error) {
		return ClosureFunction("", func(ctx FunctionContext) (any, error) {
			return nil, customErr
		}, func(ctx TargetsContext) (TargetsContext, []TargetPath) { return ctx, nil }), nil
	}))

	assert.Contains(t, listFunctions(setOne), "meow")
	assert.Contains(t, listFunctions(setTwo), "meow")

	goodArgs, err := spec.Params.PopulateNameless("hello")
	require.NoError(t, err)

	fnOne, err := setOne.Init("meow", goodArgs)
	require.NoError(t, err)

	fnTwo, err := setTwo.Init("meow", goodArgs)
	require.NoError(t, err)

	_, err = fnOne.Exec(FunctionContext{})
	assert.Equal(t, customErr, err)

	_, err = fnTwo.Exec(FunctionContext{})
	assert.EqualError(t, err, "this function has been disabled")
}

func TestFunctionResolveParamError(t *testing.T) {
	setOne := AllFunctions.Without()

	spec := NewFunctionSpec(FunctionCategoryGeneral, "meow", "").Param(ParamString("val1", ""))
	require.NoError(t, setOne.Add(spec, func(args *ParsedParams) (Function, error) {
		return ClosureFunction("", func(ctx FunctionContext) (any, error) {
			return "ok", nil
		}, func(ctx TargetsContext) (TargetsContext, []TargetPath) { return ctx, nil }), nil
	}))

	assert.Contains(t, listFunctions(setOne), "meow")

	badArgs, err := spec.Params.PopulateNameless(NewFieldFunction("doc.foo"))
	require.NoError(t, err)

	fnOne, err := setOne.Init("meow", badArgs)
	require.NoError(t, err)

	_, err = fnOne.Exec(FunctionContext{})
	assert.EqualError(t, err, "function 'meow': failed to extract input arg 'val1': context was undefined, unable to reference `doc.foo`")
}

func TestFunctionBadName(t *testing.T) {
	testCases := map[string]string{
		"!no":         "function name '!no' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"foo__bar":    "function name 'foo__bar' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"-foo-bar":    "function name '-foo-bar' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"foo-bar-":    "function name 'foo-bar-' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"":            "function name '' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"foo-bar":     "function name 'foo-bar' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"foo-bar_baz": "function name 'foo-bar_baz' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"FOO":         "function name 'FOO' does not match the required regular expression /^[a-z0-9]+(_[a-z0-9]+)*$/",
		"foobarbaz":   "",
		"foobarbaz89": "",
		"foo_bar_baz": "",
		"fo1_ba2_ba3": "",
	}

	for k, v := range testCases {
		t.Run(k, func(t *testing.T) {
			setOne := AllFunctions.Without()
			err := setOne.Add(NewFunctionSpec(FunctionCategoryGeneral, k, ""), nil)
			if len(v) > 0 {
				assert.EqualError(t, err, v)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
