package bloblang_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func TestFunctionContext(t *testing.T) {
	env := bloblang.NewEnvironment()

	require.NoError(t, env.RegisterAdvancedFunction(
		"foo", bloblang.NewPluginSpec().
			Param(bloblang.NewQueryParam("thing", true)),

		func(args *bloblang.ParsedParams) (bloblang.AdvancedFunction, error) {
			thingFn, err := args.GetQuery("thing")
			if err != nil {
				return nil, err
			}

			return func(ctx *bloblang.ExecContext) (any, error) {
				v, err := ctx.Exec(thingFn)
				if err != nil {
					return "Meow: " + err.Error(), nil
				}
				return v, nil
			}, nil
		}))

	require.NoError(t, env.RegisterAdvancedMethod(
		"bar", bloblang.NewPluginSpec().
			Param(bloblang.NewQueryParam("thing", true).Optional()),

		func(args *bloblang.ParsedParams) (bloblang.AdvancedMethod, error) {
			thingFn, err := args.GetOptionalQuery("thing")
			if err != nil {
				return nil, err
			}

			return func(ctx *bloblang.ExecContext, target *bloblang.ExecFunction) (any, error) {
				v, err := ctx.Exec(target)
				if err != nil && thingFn != nil {
					v, err = ctx.Exec(thingFn)
				}
				if err != nil {
					return "Meow: " + err.Error(), nil
				}
				return v, nil
			}, nil
		}))

	exec, err := env.Parse(`
root.a = foo(this.a + " must be a string")
root.b = (this.a + " must be a string").bar()
root.c = (this.a + " must be a string").bar("%v wasnt a string".format(this.a))
`)
	require.NoError(t, err)

	res, err := exec.Query(map[string]any{
		"a": 1,
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"a": "Meow: cannot add types number (from field `this.a`) and string (from string literal)",
		"b": "Meow: cannot add types number (from field `this.a`) and string (from string literal)",
		"c": "1 wasnt a string",
	}, res)

	res, err = exec.Query(map[string]any{
		"a": "woof",
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"a": "woof must be a string",
		"b": "woof must be a string",
		"c": "woof must be a string",
	}, res)
}
