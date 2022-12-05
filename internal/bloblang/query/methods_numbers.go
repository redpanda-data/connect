package query

import (
	"errors"
	"fmt"
	"math"
)

var _ = registerSimpleMethod(
	NewMethodSpec("abs", "Returns the absolute value of a number.").InCategory(
		MethodCategoryNumbers, "",
		NewExampleSpec("",
			`root.new_value = this.value.abs()`,
			`{"value":5.3}`,
			`{"new_value":5.3}`,
			`{"value":-5.9}`,
			`{"new_value":5.9}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return numberMethod(func(f *float64, i *int64, ui *uint64) (any, error) {
			var v float64
			if f != nil {
				v = *f
			} else if i != nil {
				v = float64(*i)
			} else {
				v = float64(*ui)
			}
			return math.Abs(v), nil
		}), nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec("ceil", "Returns the least integer value greater than or equal to a number. If the resulting value fits within a 64-bit integer then that is returned, otherwise a new floating point number is returned.").InCategory(
		MethodCategoryNumbers, "",
		NewExampleSpec("",
			`root.new_value = this.value.ceil()`,
			`{"value":5.3}`,
			`{"new_value":6}`,
			`{"value":-5.9}`,
			`{"new_value":-5}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return numberMethod(func(f *float64, i *int64, ui *uint64) (any, error) {
			if f != nil {
				ceiled := math.Ceil(*f)
				if i, err := IToInt(ceiled); err == nil {
					return i, nil
				}
				return ceiled, nil
			}
			if i != nil {
				return *i, nil
			}
			return *ui, nil
		}), nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"floor", "Returns the greatest integer value less than or equal to the target number. If the resulting value fits within a 64-bit integer then that is returned, otherwise a new floating point number is returned.",
	).InCategory(
		MethodCategoryNumbers,
		"",
		NewExampleSpec("",
			`root.new_value = this.value.floor()`,
			`{"value":5.7}`,
			`{"new_value":5}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return numberMethod(func(f *float64, i *int64, ui *uint64) (any, error) {
			if f != nil {
				floored := math.Floor(*f)
				if i, err := IToInt(floored); err == nil {
					return i, nil
				}
				return floored, nil
			}
			if i != nil {
				return *i, nil
			}
			return *ui, nil
		}), nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec("log", "Returns the natural logarithm of a number.").InCategory(
		MethodCategoryNumbers, "",
		NewExampleSpec("",
			`root.new_value = this.value.log().round()`,
			`{"value":1}`,
			`{"new_value":0}`,
			`{"value":2.7183}`,
			`{"new_value":1}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return numberMethod(func(f *float64, i *int64, ui *uint64) (any, error) {
			var v float64
			if f != nil {
				v = *f
			} else if i != nil {
				v = float64(*i)
			} else {
				v = float64(*ui)
			}
			return math.Log(v), nil
		}), nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec("log10", "Returns the decimal logarithm of a number.").InCategory(
		MethodCategoryNumbers, "",
		NewExampleSpec("",
			`root.new_value = this.value.log10()`,
			`{"value":100}`,
			`{"new_value":2}`,
			`{"value":1000}`,
			`{"new_value":3}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return numberMethod(func(f *float64, i *int64, ui *uint64) (any, error) {
			var v float64
			if f != nil {
				v = *f
			} else if i != nil {
				v = float64(*i)
			} else {
				v = float64(*ui)
			}
			return math.Log10(v), nil
		}), nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"max",
		"Returns the largest numerical value found within an array. All values must be numerical and the array must not be empty, otherwise an error is returned.",
	).InCategory(
		MethodCategoryNumbers, "",
		NewExampleSpec("",
			`root.biggest = this.values.max()`,
			`{"values":[0,3,2.5,7,5]}`,
			`{"biggest":7}`,
		),
		NewExampleSpec("",
			`root.new_value = [0,this.value].max()`,
			`{"value":-1}`,
			`{"new_value":0}`,
			`{"value":7}`,
			`{"new_value":7}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v any, ctx FunctionContext) (any, error) {
			arr, ok := v.([]any)
			if !ok {
				return nil, NewTypeError(v, ValueArray)
			}
			if len(arr) == 0 {
				return nil, errors.New("the array was empty")
			}
			var max float64
			for i, n := range arr {
				f, err := IGetNumber(n)
				if err != nil {
					return nil, fmt.Errorf("index %v of array: %w", i, err)
				}
				if i == 0 || f > max {
					max = f
				}
			}
			return max, nil
		}, nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"min",
		"Returns the smallest numerical value found within an array. All values must be numerical and the array must not be empty, otherwise an error is returned.",
	).InCategory(
		MethodCategoryNumbers, "",
		NewExampleSpec("",
			`root.smallest = this.values.min()`,
			`{"values":[0,3,-2.5,7,5]}`,
			`{"smallest":-2.5}`,
		),
		NewExampleSpec("",
			`root.new_value = [10,this.value].min()`,
			`{"value":2}`,
			`{"new_value":2}`,
			`{"value":23}`,
			`{"new_value":10}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v any, ctx FunctionContext) (any, error) {
			arr, ok := v.([]any)
			if !ok {
				return nil, NewTypeError(v, ValueArray)
			}
			if len(arr) == 0 {
				return nil, errors.New("the array was empty")
			}
			var max float64
			for i, n := range arr {
				f, err := IGetNumber(n)
				if err != nil {
					return nil, fmt.Errorf("index %v of array: %w", i, err)
				}
				if i == 0 || f < max {
					max = f
				}
			}
			return max, nil
		}, nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"round", "Rounds numbers to the nearest integer, rounding half away from zero. If the resulting value fits within a 64-bit integer then that is returned, otherwise a new floating point number is returned.",
	).InCategory(
		MethodCategoryNumbers,
		"",
		NewExampleSpec("",
			`root.new_value = this.value.round()`,
			`{"value":5.3}`,
			`{"new_value":5}`,
			`{"value":5.9}`,
			`{"new_value":6}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return numberMethod(func(f *float64, i *int64, ui *uint64) (any, error) {
			if f != nil {
				rounded := math.Round(*f)
				if i, err := IToInt(rounded); err == nil {
					return i, nil
				}
				return rounded, nil
			}
			if i != nil {
				return *i, nil
			}
			return *ui, nil
		}), nil
	},
)
