package bloblang_test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"

	"github.com/Jeffail/benthos/v3/public/bloblang"
)

// ExampleBloblangPlugins demonstrates how to create Bloblang methods and
// functions and execute them with a Bloblang mapping.
func Example_bloblangPlugins() {
	if err := bloblang.RegisterMethod("cuddle", func(args ...interface{}) (bloblang.Method, error) {
		var prefix string
		var suffix string

		if err := bloblang.NewArgSpec().
			StringVar(&prefix).
			StringVar(&suffix).
			Extract(args); err != nil {
			return nil, err
		}

		return bloblang.StringMethod(func(s string) (interface{}, error) {
			return prefix + s + suffix, nil
		}), nil
	}); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethod("shuffle", func(_ ...interface{}) (bloblang.Method, error) {
		rand := rand.New(rand.NewSource(0))
		return bloblang.ArrayMethod(func(in []interface{}) (interface{}, error) {
			out := make([]interface{}, len(in))
			copy(out, in)
			rand.Shuffle(len(out), func(i, j int) {
				out[i], out[j] = out[j], out[i]
			})
			return out, nil
		}), nil
	}); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterFunction("add_but_always_slightly_wrong", func(args ...interface{}) (bloblang.Function, error) {
		var left, right float64

		if err := bloblang.NewArgSpec().
			Float64Var(&left).
			Float64Var(&right).
			Extract(args); err != nil {
			return nil, err
		}

		return func() (interface{}, error) {
			return left + right + 0.02, nil
		}, nil
	}); err != nil {
		panic(err)
	}

	mapping := `
root.new_summary = this.summary.cuddle("meow", "woof")
root.shuffled = this.names.shuffle()
root.num = add_but_always_slightly_wrong(1.2, 2.6)
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
	// Output: {"new_summary":"meowquackwoof","num":3.82,"shuffled":["olaf","jen","pixie","denny","spuz"]}
}

// ExampleBloblangRestrictedEnvironment demonstrates how to create and use an
// isolated Bloblang environment with some standard functions removed.
func Example_bloblangRestrictedEnvironment() {
	env := bloblang.NewEnvironment().WithoutFunctions("env", "file")

	if err := env.RegisterMethod("custom_thing", func(args ...interface{}) (bloblang.Method, error) {
		return bloblang.StringMethod(func(s string) (interface{}, error) {
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

	res, err := exe.Query(map[string]interface{}{
		"foo": 50.0,
		"bar": "first bit ",
		"baz": "second bit",
		"buz": map[string]interface{}{
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
