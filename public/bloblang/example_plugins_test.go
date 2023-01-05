package bloblang_test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"

	"github.com/benthosdev/benthos/v4/public/bloblang"
)

// This example demonstrates how to create a Bloblang function with the simpler
// V1 API and execute them with a Bloblang mapping. Note that functions
// registered this way will NOT support named parameters, for named parameters
// use the V2 register API.
func Example_bloblangFunctionPluginV1() {
	if err := bloblang.RegisterFunction("add_but_always_slightly_wrong", func(args ...any) (bloblang.Function, error) {
		var left, right float64

		if err := bloblang.NewArgSpec().
			Float64Var(&left).
			Float64Var(&right).
			Extract(args); err != nil {
			return nil, err
		}

		return func() (any, error) {
			return left + right + 0.02, nil
		}, nil
	}); err != nil {
		panic(err)
	}

	mapping := `
root.num = add_but_always_slightly_wrong(this.a, this.b)
`

	exe, err := bloblang.Parse(mapping)
	if err != nil {
		panic(err)
	}

	res, err := exe.Query(map[string]any{
		"a": 1.2, "b": 2.6,
	})
	if err != nil {
		panic(err)
	}

	jsonBytes, err := json.Marshal(res)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(jsonBytes))
	// Output: {"num":3.82}
}

// This example demonstrates how to create Bloblang methods with the simpler API
// and execute them with a Bloblang mapping. Note that methods registered this
// way will NOT support named parameters, for named parameters use the V2
// register API.
func Example_bloblangMethodPluginV1() {
	if err := bloblang.RegisterMethod("cuddle", func(args ...any) (bloblang.Method, error) {
		var prefix string
		var suffix string

		if err := bloblang.NewArgSpec().
			StringVar(&prefix).
			StringVar(&suffix).
			Extract(args); err != nil {
			return nil, err
		}

		return bloblang.StringMethod(func(s string) (any, error) {
			return prefix + s + suffix, nil
		}), nil
	}); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethod("shuffle", func(_ ...any) (bloblang.Method, error) {
		rand := rand.New(rand.NewSource(0))
		return bloblang.ArrayMethod(func(in []any) (any, error) {
			out := make([]any, len(in))
			copy(out, in)
			rand.Shuffle(len(out), func(i, j int) {
				out[i], out[j] = out[j], out[i]
			})
			return out, nil
		}), nil
	}); err != nil {
		panic(err)
	}

	mapping := `
root.new_summary = this.summary.cuddle("meow", "woof")
root.shuffled = this.names.shuffle()
`

	exe, err := bloblang.Parse(mapping)
	if err != nil {
		panic(err)
	}

	res, err := exe.Query(map[string]any{
		"summary": "quack",
		"names":   []any{"denny", "pixie", "olaf", "jen", "spuz"},
	})
	if err != nil {
		panic(err)
	}

	jsonBytes, err := json.Marshal(res)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(jsonBytes))
	// Output: {"new_summary":"meowquackwoof","shuffled":["olaf","jen","pixie","denny","spuz"]}
}

// This example demonstrates how to create and use an isolated Bloblang
// environment with some standard functions removed.
func Example_bloblangRestrictedEnvironment() {
	env := bloblang.NewEnvironment().WithoutFunctions("env", "file")

	if err := env.RegisterMethod("custom_thing", func(args ...any) (bloblang.Method, error) {
		return bloblang.StringMethod(func(s string) (any, error) {
			return strings.ToUpper(s), nil
		}), nil
	}); err != nil {
		panic(err)
	}

	mapping := `
root.foo = this.foo.string()
root.bar = this.bar + this.baz
root.buz = this.buz.content.custom_thing()
`

	exe, err := env.Parse(mapping)
	if err != nil {
		panic(err)
	}

	res, err := exe.Query(map[string]any{
		"foo": 50.0,
		"bar": "first bit ",
		"baz": "second bit",
		"buz": map[string]any{
			"id":      "XXXX",
			"content": "some nested content",
		},
	})
	if err != nil {
		panic(err)
	}

	jsonBytes, err := json.Marshal(res)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(jsonBytes))
	// Output: {"bar":"first bit second bit","buz":"SOME NESTED CONTENT","foo":"50"}
}
