package query

import (
	"errors"
	"fmt"
	"math"
)

//------------------------------------------------------------------------------

var _ = RegisterMethod(
	NewMethodSpec("abs", "Returns the absolute value of a number.").InCategory(
		MethodCategoryNumbers, "",
		NewExampleSpec("",
			`root.new_value = this.value.abs()`,
			`{"value":5.3}`,
			`{"new_value":5.3}`,
			`{"value":-5.9}`,
			`{"new_value":5.9}`,
		),
	), false,
	func(target Function, args ...interface{}) (Function, error) {
		return numberMethod(target, func(f *float64, i *int64, ui *uint64, ctx FunctionContext) (interface{}, error) {
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
	ExpectNArgs(0),
)

var _ = RegisterMethod(
	NewMethodSpec("ceil", "Returns the least integer value greater than or equal to a number.").InCategory(
		MethodCategoryNumbers, "",
		NewExampleSpec("",
			`root.new_value = this.value.ceil()`,
			`{"value":5.3}`,
			`{"new_value":6}`,
			`{"value":-5.9}`,
			`{"new_value":-5}`,
		),
	), false,
	func(target Function, args ...interface{}) (Function, error) {
		return numberMethod(target, func(f *float64, i *int64, ui *uint64, ctx FunctionContext) (interface{}, error) {
			if f != nil {
				return math.Ceil(*f), nil
			}
			if i != nil {
				return *i, nil
			}
			return *ui, nil
		}), nil
	},
	ExpectNArgs(0),
)

var _ = RegisterMethod(
	NewMethodSpec(
		"floor", "Returns the greatest integer value less than or equal to the target number.",
	).InCategory(
		MethodCategoryNumbers,
		"",
		NewExampleSpec("",
			`root.new_value = this.value.floor()`,
			`{"value":5.7}`,
			`{"new_value":5}`,
		),
	),
	false, floorMethod,
	ExpectNArgs(0),
)

func floorMethod(target Function, args ...interface{}) (Function, error) {
	return numberMethod(target, func(f *float64, i *int64, ui *uint64, ctx FunctionContext) (interface{}, error) {
		if f != nil {
			return math.Floor(*f), nil
		}
		if i != nil {
			return *i, nil
		}
		return *ui, nil
	}), nil
}

var _ = RegisterMethod(
	NewMethodSpec("log", "Returns the natural logarithm of a number.").InCategory(
		MethodCategoryNumbers, "",
		NewExampleSpec("",
			`root.new_value = this.value.log().round()`,
			`{"value":1}`,
			`{"new_value":0}`,
			`{"value":2.7183}`,
			`{"new_value":1}`,
		),
	), false,
	func(target Function, args ...interface{}) (Function, error) {
		return numberMethod(target, func(f *float64, i *int64, ui *uint64, ctx FunctionContext) (interface{}, error) {
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
	ExpectNArgs(0),
)

var _ = RegisterMethod(
	NewMethodSpec("log10", "Returns the decimal logarithm of a number.").InCategory(
		MethodCategoryNumbers, "",
		NewExampleSpec("",
			`root.new_value = this.value.log10()`,
			`{"value":100}`,
			`{"new_value":2}`,
			`{"value":1000}`,
			`{"new_value":3}`,
		),
	), false,
	func(target Function, args ...interface{}) (Function, error) {
		return numberMethod(target, func(f *float64, i *int64, ui *uint64, ctx FunctionContext) (interface{}, error) {
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
	ExpectNArgs(0),
)

var _ = RegisterMethod(
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
	), false,
	func(target Function, args ...interface{}) (Function, error) {
		return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
			arr, ok := v.([]interface{})
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
		}), nil
	},
	ExpectNArgs(0),
)

var _ = RegisterMethod(
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
	), false,
	func(target Function, args ...interface{}) (Function, error) {
		return simpleMethod(target, func(v interface{}, ctx FunctionContext) (interface{}, error) {
			arr, ok := v.([]interface{})
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
		}), nil
	},
	ExpectNArgs(0),
)

var _ = RegisterMethod(
	NewMethodSpec(
		"round", "Rounds numbers to the nearest integer, rounding half away from zero.",
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
	false, roundMethod,
	ExpectNArgs(0),
)

func roundMethod(target Function, args ...interface{}) (Function, error) {
	return numberMethod(target, func(f *float64, i *int64, ui *uint64, ctx FunctionContext) (interface{}, error) {
		if f != nil {
			return math.Round(*f), nil
		}
		if i != nil {
			return *i, nil
		}
		return *ui, nil
	}), nil
}
