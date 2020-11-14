package query

import "math"

//------------------------------------------------------------------------------

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

//------------------------------------------------------------------------------

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

//------------------------------------------------------------------------------
