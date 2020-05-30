package query

import (
	"bytes"
	"fmt"
)

// TypeError represents an error where a value of a type was required for a
// function, method or operator but instead a different type was found.
type TypeError struct {
	Expected []ValueType
	Actual   ValueType
	Value    interface{}
}

// Error implements the standard error interface for TypeError.
func (t *TypeError) Error() string {
	if len(t.Expected) == 0 {
		return fmt.Sprintf("found unexpected value type %v", string(t.Actual))
	}
	var expStr bytes.Buffer
	for i, exp := range t.Expected {
		if i > 0 {
			if len(t.Expected) > 2 && i < (len(t.Expected)-1) {
				expStr.WriteString(", ")
			} else {
				expStr.WriteString(" or ")
			}
		}
		expStr.WriteString(string(exp))
	}
	if t.Value != nil {
		return fmt.Sprintf("expected %v value, found %v: %v", expStr.String(), string(t.Actual), t.Value)
	}
	return fmt.Sprintf("expected %v value, found %v", expStr.String(), string(t.Actual))
}

// NewTypeError creates a new type error.
func NewTypeError(actual interface{}, exp ...ValueType) *TypeError {
	actType := ITypeOf(actual)
	switch actType {
	case ValueString, ValueBool, ValueNumber:
	default:
		actual = nil
	}
	return &TypeError{
		Expected: exp,
		Actual:   actType,
		Value:    actual,
	}
}

//------------------------------------------------------------------------------

// TypeMismatch represents an error where two values should be a comparable type
// but are not.
type TypeMismatch struct {
	Left  ValueType
	Right ValueType
}

// Error implements the standard error interface.
func (t *TypeMismatch) Error() string {
	return fmt.Sprintf("found incomparable types %v and %v", string(t.Left), string(t.Right))
}

// NewTypeMismatch creates a new type mismatch error.
func NewTypeMismatch(left, right interface{}) *TypeMismatch {
	return &TypeMismatch{
		Left:  ITypeOf(left),
		Right: ITypeOf(right),
	}
}
