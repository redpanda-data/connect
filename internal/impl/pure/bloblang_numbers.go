package pure

import (
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func init() {
	if err := bloblang.RegisterMethodV2("int64",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryNumbers).
			Description(`
Converts a numerical type into a 64-bit signed integer, this is for advanced use cases where a specific data type is needed for a given component (such as the ClickHouse SQL driver).

If the value is a string then an attempt will be made to parse it as a 64-bit integer. If the target value exceeds the capacity of an integer or contains decimal values then this method will throw an error. In order to convert a floating point number containing decimals first use `+"[`.round()`](#round)"+` on the value first.`).
			Example("", `
root.a = this.a.int64()
root.b = this.b.round().int64()
root.c = this.c.int64()
`,
				[2]string{
					`{"a":12,"b":12.34,"c":"12"}`,
					`{"a":12,"b":12,"c":12}`,
				},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return func(input any) (any, error) {
				return query.IToInt(input)
			}, nil
		}); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("int32",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryNumbers).
			Description(`
Converts a numerical type into a 32-bit signed integer, this is for advanced use cases where a specific data type is needed for a given component (such as the ClickHouse SQL driver).

If the value is a string then an attempt will be made to parse it as a 32-bit integer. If the target value exceeds the capacity of an integer or contains decimal values then this method will throw an error. In order to convert a floating point number containing decimals first use `+"[`.round()`](#round)"+` on the value first.`).
			Example("", `
root.a = this.a.int32()
root.b = this.b.round().int32()
root.c = this.c.int32()
`,
				[2]string{
					`{"a":12,"b":12.34,"c":"12"}`,
					`{"a":12,"b":12,"c":12}`,
				},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return func(input any) (any, error) {
				return query.IToInt32(input)
			}, nil
		}); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("uint64",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryNumbers).
			Description(`
Converts a numerical type into a 64-bit unsigned integer, this is for advanced use cases where a specific data type is needed for a given component (such as the ClickHouse SQL driver).

If the value is a string then an attempt will be made to parse it as a 64-bit unsigned integer. If the target value exceeds the capacity of an integer or contains decimal values then this method will throw an error. In order to convert a floating point number containing decimals first use `+"[`.round()`](#round)"+` on the value first.`).
			Example("", `
root.a = this.a.uint64()
root.b = this.b.round().uint64()
root.c = this.c.uint64()
root.d = this.d.uint64().catch(0)
`,
				[2]string{
					`{"a":12,"b":12.34,"c":"12","d":-12}`,
					`{"a":12,"b":12,"c":12,"d":0}`,
				},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return func(input any) (any, error) {
				return query.IToUint(input)
			}, nil
		}); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("uint32",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryNumbers).
			Description(`
Converts a numerical type into a 32-bit unsigned integer, this is for advanced use cases where a specific data type is needed for a given component (such as the ClickHouse SQL driver).

If the value is a string then an attempt will be made to parse it as a 32-bit unsigned integer. If the target value exceeds the capacity of an integer or contains decimal values then this method will throw an error. In order to convert a floating point number containing decimals first use `+"[`.round()`](#round)"+` on the value first.`).
			Example("", `
root.a = this.a.uint32()
root.b = this.b.round().uint32()
root.c = this.c.uint32()
root.d = this.d.uint32().catch(0)
`,
				[2]string{
					`{"a":12,"b":12.34,"c":"12","d":-12}`,
					`{"a":12,"b":12,"c":12,"d":0}`,
				},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return func(input any) (any, error) {
				return query.IToUint32(input)
			}, nil
		}); err != nil {
		panic(err)
	}
}
