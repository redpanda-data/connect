package pure

import (
	"errors"
	"fmt"

	"github.com/Jeffail/gabs/v2"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/value"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func init() {
	if err := bloblang.RegisterMethodV2("squash",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryObjectAndArray).
			Description("Squashes an array of objects into a single object, where key collisions result in the values being merged (following similar rules as the `.merge()` method)").
			Example("", `root.locations = this.locations.map_each(loc -> {loc.state: [loc.name]}).squash()`,
				[2]string{
					`{"locations":[{"name":"Seattle","state":"WA"},{"name":"New York","state":"NY"},{"name":"Bellevue","state":"WA"},{"name":"Olympia","state":"WA"}]}`,
					`{"locations":{"NY":["New York"],"WA":["Seattle","Bellevue","Olympia"]}}`,
				},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return bloblang.ArrayMethod(func(i []any) (any, error) {
				root := gabs.New()
				for _, v := range i {
					if err := root.Merge(gabs.Wrap(v)); err != nil {
						return nil, err
					}
				}
				return root.Data(), nil
			}), nil
		}); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("with",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryObjectAndArray).
			Variadic().
			Description(`Returns an object where all but one or more [field path][field_paths] arguments are removed. Each path specifies a specific field to be retained from the input object, allowing for nested fields.

If a key within a nested path does not exist then it is ignored.`).
			Example("", `root = this.with("inner.a","inner.c","d")`,
				[2]string{
					`{"inner":{"a":"first","b":"second","c":"third"},"d":"fourth","e":"fifth"}`,
					`{"d":"fourth","inner":{"a":"first","c":"third"}}`,
				},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			includeList := make([][]string, 0, len(args.AsSlice()))
			for i, argVal := range args.AsSlice() {
				argStr, err := value.IGetString(argVal)
				if err != nil {
					return nil, fmt.Errorf("argument %v: %w", i, err)
				}
				includeList = append(includeList, gabs.DotPathToSlice(argStr))
			}
			return bloblang.ObjectMethod(func(i map[string]any) (any, error) {
				return mapWith(i, includeList), nil
			}), nil
		}); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("concat",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryObjectAndArray).
			Variadic().
			Description("Concatenates an array value with one or more argument arrays.").
			Example("", `root.foo = this.foo.concat(this.bar, this.baz)`,
				[2]string{
					`{"foo":["a","b"],"bar":["c"],"baz":["d","e","f"]}`,
					`{"foo":["a","b","c","d","e","f"]}`,
				},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			argAnys := args.AsSlice()
			argSlices := make([][]any, len(argAnys))
			tally := 0
			for i, a := range argAnys {
				var ok bool
				if argSlices[i], ok = a.([]any); !ok {
					return nil, value.NewTypeError(a, value.TArray)
				}
				tally += len(argSlices[i])
			}

			return bloblang.ArrayMethod(func(i []any) (any, error) {
				resSlice := make([]any, 0, len(i)+tally)
				resSlice = append(resSlice, i...)
				for _, s := range argSlices {
					resSlice = append(resSlice, s...)
				}
				return resSlice, nil
			}), nil
		}); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("zip",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryObjectAndArray).
			Variadic().
			Description("Zip an array value with one or more argument arrays. Each array must match in length.").
			Example("", `root.foo = this.foo.zip(this.bar, this.baz)`,
				[2]string{
					`{"foo":["a","b","c"],"bar":[1,2,3],"baz":[4,5,6]}`,
					`{"foo":[["a",1,4],["b",2,5],["c",3,6]]}`,
				},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			sizeError := errors.New("can't zip different length array values")
			argError := errors.New("zip requires at least one argument")

			argAnys := args.AsSlice()
			argSlices := make([][]any, len(argAnys))
			for i, a := range argAnys {
				var ok bool
				if argSlices[i], ok = a.([]any); !ok {
					return nil, value.NewTypeError(a, value.TArray)
				}
				if len(argSlices[i]) != len(argSlices[0]) {
					return nil, sizeError
				}
			}

			return bloblang.ArrayMethod(func(i []any) (any, error) {
				if len(argSlices) == 0 {
					return nil, argError
				}
				if len(i) != len(argSlices[0]) {
					return nil, sizeError
				}
				resSlice := make([]any, 0, len(i))
				for offset, value := range i {
					zipValue := make([]any, 0, len(argSlices)+1)
					zipValue = append(zipValue, value)
					for _, argSlice := range argSlices {
						zipValue = append(zipValue, argSlice[offset])
					}
					resSlice = append(resSlice, zipValue)
				}
				return resSlice, nil
			}), nil
		}); err != nil {
		panic(err)
	}
}

func mapWith(m map[string]any, paths [][]string) map[string]any {
	newMap := make(map[string]any, len(m))
	for k, v := range m {
		included := false
		var nestedInclude [][]string
		for _, p := range paths {
			if p[0] == k {
				included = true
				if len(p) > 1 {
					nestedInclude = append(nestedInclude, p[1:])
				}
			}
		}
		if included {
			if len(nestedInclude) > 0 {
				vMap, ok := v.(map[string]any)
				if ok {
					newMap[k] = mapWith(vMap, nestedInclude)
				} else {
					newMap[k] = v
				}
			} else {
				newMap[k] = v
			}
		}
	}
	return newMap
}
