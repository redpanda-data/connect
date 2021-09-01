package bloblang_test

import (
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/Jeffail/benthos/v3/public/bloblang"
)

// This example demonstrates how to create Bloblang methods and functions and
// execute them with a Bloblang mapping using the new V2 methods, which adds
// support to our functions and methods for optional named parameters.
func Example_bloblangPluginsNamedArgs() {
	cuddleSpec := bloblang.NewParamsSpec("hug_string", "Wraps a string with a prefix and suffix.").
		Add(bloblang.ParamString("prefix", "The prefix to insert.")).
		Add(bloblang.ParamString("suffix", "The suffix to append."))

	if err := bloblang.RegisterMethodV2(cuddleSpec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		prefix, err := args.FieldString("prefix")
		if err != nil {
			return nil, err
		}

		suffix, err := args.FieldString("suffix")
		if err != nil {
			return nil, err
		}

		return bloblang.StringMethod(func(s string) (interface{}, error) {
			return prefix + s + suffix, nil
		}), nil
	}); err != nil {
		panic(err)
	}

	reverseSpec := bloblang.NewParamsSpec(
		"sometimes_reverse",
		"Reverses the order of an array target, but sometimes it randomly doesn't. Whoops.",
	)
	if err := bloblang.RegisterMethodV2(reverseSpec, func(*bloblang.ParsedParams) (bloblang.Method, error) {
		rand := rand.New(rand.NewSource(0))
		return bloblang.ArrayMethod(func(in []interface{}) (interface{}, error) {
			if rand.Int()%3 == 0 {
				// Whoopsie
				return in, nil
			}
			out := make([]interface{}, len(in))
			copy(out, in)
			for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
				out[i], out[j] = out[j], out[i]
			}
			return out, nil
		}), nil
	}); err != nil {
		panic(err)
	}

	multiplyWrongSpec := bloblang.NewParamsSpec(
		"multiply_but_always_slightly_wrong",
		"Multiplies two numbers together but gets it slightly wrong. Whoops.",
	).
		Add(bloblang.ParamFloat64("left", "The first of two numbers to multiply.")).
		Add(bloblang.ParamFloat64("right", "The second of two numbers to multiply."))
	if err := bloblang.RegisterFunctionV2(multiplyWrongSpec, func(args *bloblang.ParsedParams) (bloblang.Function, error) {
		left, err := args.FieldFloat64("left")
		if err != nil {
			return nil, err
		}

		right, err := args.FieldFloat64("right")
		if err != nil {
			return nil, err
		}

		return func() (interface{}, error) {
			return left*right + 0.02, nil
		}, nil
	}); err != nil {
		panic(err)
	}

	// Our methods and functions now optionally support named parameters:
	mapping := `
root.new_summary = this.summary.hug_string(prefix: "meow", suffix: "woof")
root.reversed = this.names.sometimes_reverse()
root.num = multiply_but_always_slightly_wrong(1.2, 2.6)
`

	exe, err := bloblang.Parse(mapping)
	if err != nil {
		panic(err)
	}

	res, err := exe.Query(map[string]interface{}{
		"summary": "quack",
		"names":   []interface{}{"denny", "pixie", "olaf", "jen", "spuz"},
	})
	if err != nil {
		panic(err)
	}

	jsonBytes, err := json.Marshal(res)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(jsonBytes))
	// Output: {"new_summary":"meowquackwoof","num":3.14,"reversed":["spuz","jen","olaf","pixie","denny"]}
}
