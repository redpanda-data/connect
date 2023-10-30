package pure

import (
	"math"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func registerIntMethod(name, longName, exampleIn, exampleOut string, method func(input any) (any, error)) {
	replacer := strings.NewReplacer("$NAME", name, "$LONGNAME", longName)

	exampleOneBody := replacer.Replace(`
root.a = this.a.$NAME()
root.b = this.b.round().$NAME()
root.c = this.c.$NAME()
root.d = this.d.$NAME().catch(0)
`)
	exampleOneIO := [2]string{
		`{"a":12,"b":12.34,"c":"12","d":-12}`,
		`{"a":12,"b":12,"c":12,"d":-12}`,
	}
	if name[0] == 'u' {
		exampleOneIO[1] = `{"a":12,"b":12,"c":12,"d":0}`
	}

	if err := bloblang.RegisterMethodV2(name,
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryNumbers).
			Description(replacer.Replace(`
Converts a numerical type into a $LONGNAME, this is for advanced use cases where a specific data type is needed for a given component (such as the ClickHouse SQL driver).

If the value is a string then an attempt will be made to parse it as a $LONGNAME. If the target value exceeds the capacity of an integer or contains decimal values then this method will throw an error. In order to convert a floating point number containing decimals first use `+"[`.round()`](#round)"+` on the value. Please refer to the [`+"`strconv.ParseInt`"+` documentation](https://pkg.go.dev/strconv#ParseInt) for details regarding the supported formats.`)).
			Example("", exampleOneBody, exampleOneIO).
			Example("", replacer.Replace(`
root = this.$NAME()
`),
				[2]string{exampleIn, exampleOut},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return method, nil
		}); err != nil {
		panic(err)
	}
}

func init() {
	registerIntMethod(
		"int64", "64-bit signed integer",
		`"0xDEADBEEF"`, "3735928559",
		func(input any) (any, error) {
			return query.IToInt(input)
		})

	registerIntMethod(
		"int32", "32-bit signed integer",
		`"0xDEAD"`, "57005",
		func(input any) (any, error) {
			return query.IToInt32(input)
		})

	registerIntMethod(
		"int16", "16-bit signed integer",
		`"0xDE"`, "222",
		func(input any) (any, error) {
			return query.IToInt16(input)
		})

	registerIntMethod(
		"int8", "8-bit signed integer",
		`"0xD"`, "13",
		func(input any) (any, error) {
			return query.IToInt8(input)
		})

	registerIntMethod(
		"uint64", "64-bit unsigned integer",
		`"0xDEADBEEF"`, "3735928559",
		func(input any) (any, error) {
			return query.IToUint(input)
		})

	registerIntMethod(
		"uint32", "32-bit unsigned integer",
		`"0xDEAD"`, "57005",
		func(input any) (any, error) {
			return query.IToUint32(input)
		})

	registerIntMethod(
		"uint16", "16-bit unsigned integer",
		`"0xDE"`, "222",
		func(input any) (any, error) {
			return query.IToUint16(input)
		})

	registerIntMethod(
		"uint8", "8-bit unsigned integer",
		`"0xD"`, "13",
		func(input any) (any, error) {
			return query.IToUint8(input)
		})

	if err := bloblang.RegisterMethodV2("float64",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryNumbers).
			Description(`
Converts a numerical type into a 64-bit floating point number, this is for advanced use cases where a specific data type is needed for a given component (such as the ClickHouse SQL driver).

If the value is a string then an attempt will be made to parse it as a 64-bit floating point number. Please refer to the [`+"`strconv.ParseFloat`"+` documentation](https://pkg.go.dev/strconv#ParseFloat) for details regarding the supported formats.`).
			Example("", `
root.out = this.in.float64()
`,
				[2]string{`{"in":"6.674282313423543523453425345e-11"}`, `{"out":6.674282313423544e-11}`},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return func(input any) (any, error) {
				return query.IToFloat64(input)
			}, nil
		}); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("float32",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryNumbers).
			Description(`
Converts a numerical type into a 32-bit floating point number, this is for advanced use cases where a specific data type is needed for a given component (such as the ClickHouse SQL driver).

If the value is a string then an attempt will be made to parse it as a 32-bit floating point number. Please refer to the [`+"`strconv.ParseFloat`"+` documentation](https://pkg.go.dev/strconv#ParseFloat) for details regarding the supported formats.`).
			Example("", `
root.out = this.in.float32()
`,
				[2]string{`{"in":"6.674282313423543523453425345e-11"}`, `{"out":6.674283e-11}`},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return func(input any) (any, error) {
				return query.IToFloat32(input)
			}, nil
		}); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("abs",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryNumbers).
			Description(`Returns the absolute value of an int64 or float64 number. As a special case, when an integer is provided that is the minimum value it is converted to the maximum value.`).
			Example("", `
root.outs = this.ins.map_each(ele -> ele.abs())
`,
				[2]string{`{"ins":[9,-18,1.23,-4.56]}`, `{"outs":[9,18,1.23,4.56]}`},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return func(input any) (any, error) {
				sanitInput := query.ISanitize(input)
				switch v := sanitInput.(type) {
				case float64:
					return math.Abs(v), nil
				case int64:
					switch {
					case v >= 0:
						return v, nil
					case v == query.MinInt:
						return query.MaxInt, nil
					default:
						return -v, nil
					}
				}
				return nil, query.NewTypeError(input, query.ValueNumber, query.ValueInt)
			}, nil
		}); err != nil {
		panic(err)
	}
}
