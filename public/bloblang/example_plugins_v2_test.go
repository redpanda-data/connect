package bloblang_test

import (
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/benthosdev/benthos/v4/public/bloblang"
)

// This example demonstrates how to create Bloblang methods and functions and
// execute them with a Bloblang mapping using the new V2 methods, which adds
// support to our functions and methods for optional named parameters.
func Example_bloblangFunctionPluginV2() {
	multiplyWrongSpec := bloblang.NewPluginSpec().
		Description("Multiplies two numbers together but gets it slightly wrong. Whoops.").
		Param(bloblang.NewFloat64Param("left").Description("The first of two numbers to multiply.")).
		Param(bloblang.NewFloat64Param("right").Description("The second of two numbers to multiply."))

	if err := bloblang.RegisterFunctionV2(
		"multiply_but_always_slightly_wrong", multiplyWrongSpec,
		func(args *bloblang.ParsedParams) (bloblang.Function, error) {
			left, err := args.GetFloat64("left")
			if err != nil {
				return nil, err
			}

			right, err := args.GetFloat64("right")
			if err != nil {
				return nil, err
			}

			return func() (any, error) {
				return left*right + 0.02, nil
			}, nil
		}); err != nil {
		panic(err)
	}

	// Our function now optionally supports named parameters, when a function is
	// instantiated with unamed parameters they must follow the order in which
	// the parameters are registered.
	mapping := `
root.num_ab = multiply_but_always_slightly_wrong(left: this.a, right: this.b)
root.num_cd = multiply_but_always_slightly_wrong(this.c, this.d)
`

	exe, err := bloblang.Parse(mapping)
	if err != nil {
		panic(err)
	}

	res, err := exe.Query(map[string]any{
		"a": 1.2, "b": 2.6, "c": 5.3, "d": 8.2,
	})
	if err != nil {
		panic(err)
	}

	jsonBytes, err := json.Marshal(res)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(jsonBytes))
	// Output: {"num_ab":3.14,"num_cd":43.48}
}

// This example demonstrates how to create Bloblang methods and functions and
// execute them with a Bloblang mapping using the new V2 methods, which adds
// support to our functions and methods for optional named parameters.
func Example_bloblangMethodPluginV2() {
	hugStringSpec := bloblang.NewPluginSpec().
		Description("Wraps a string with a prefix and suffix.").
		Param(bloblang.NewStringParam("prefix").Description("The prefix to insert.")).
		Param(bloblang.NewStringParam("suffix").Description("The suffix to append."))

	if err := bloblang.RegisterMethodV2("hug_string", hugStringSpec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		prefix, err := args.GetString("prefix")
		if err != nil {
			return nil, err
		}

		suffix, err := args.GetString("suffix")
		if err != nil {
			return nil, err
		}

		return bloblang.StringMethod(func(s string) (any, error) {
			return prefix + s + suffix, nil
		}), nil
	}); err != nil {
		panic(err)
	}

	reverseSpec := bloblang.NewPluginSpec().
		Description("Reverses the order of an array target, but sometimes it randomly doesn't. Whoops.")

	if err := bloblang.RegisterMethodV2("sometimes_reverse", reverseSpec, func(*bloblang.ParsedParams) (bloblang.Method, error) {
		rand := rand.New(rand.NewSource(0))
		return bloblang.ArrayMethod(func(in []any) (any, error) {
			if rand.Int()%3 == 0 {
				// Whoopsie
				return in, nil
			}
			out := make([]any, len(in))
			copy(out, in)
			for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
				out[i], out[j] = out[j], out[i]
			}
			return out, nil
		}), nil
	}); err != nil {
		panic(err)
	}

	// Our methods now optionally support named parameters, when a method is
	// instantiated with unamed parameters they must follow the order in which
	// the parameters are registered.
	mapping := `
root.new_summary = this.summary.hug_string(prefix: "meow", suffix: "woof")
root.reversed = this.names.sometimes_reverse()
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
	// Output: {"new_summary":"meowquackwoof","reversed":["spuz","jen","olaf","pixie","denny"]}
}
