package query

import (
	"bytes"
	"errors"
	"fmt"
)

// ErrNoContext is a common query error where a query attempts to reference a
// structured field when there is no context.
type ErrNoContext struct {
	FieldName string
}

// Error returns an attempt at a useful error message.
func (e ErrNoContext) Error() string {
	if e.FieldName != "" {
		return fmt.Sprintf("context was undefined, unable to reference `%v`", e.FieldName)
	}
	return "context was undefined"
}

//------------------------------------------------------------------------------

// TypeError represents an error where a value of a type was required for a
// function, method or operator but instead a different type was found.
type TypeError struct {
	From     string
	Expected []ValueType
	Actual   ValueType
	Value    string
}

// Error implements the standard error interface for TypeError.
func (t *TypeError) Error() string {
	var errStr bytes.Buffer
	if len(t.Expected) > 0 {
		errStr.WriteString("expected ")
		for i, exp := range t.Expected {
			if i > 0 {
				if len(t.Expected) > 2 && i < (len(t.Expected)-1) {
					errStr.WriteString(", ")
				} else {
					errStr.WriteString(" or ")
				}
			}
			errStr.WriteString(string(exp))
		}
		errStr.WriteString(" value")
	} else {
		errStr.WriteString("unexpected value")
	}

	fmt.Fprintf(&errStr, ", got %v", t.Actual)

	if t.From != "" {
		fmt.Fprintf(&errStr, " from %v", t.From)
	}

	if t.Value != "" {
		fmt.Fprintf(&errStr, " (%v)", t.Value)
	}

	return errStr.String()
}

// NewTypeError creates a new type error.
func NewTypeError(value any, exp ...ValueType) *TypeError {
	return NewTypeErrorFrom("", value, exp...)
}

// NewTypeErrorFrom creates a new type error with an annotation of the query
// that provided the wrong type.
func NewTypeErrorFrom(from string, value any, exp ...ValueType) *TypeError {
	valueStr := ""
	valueType := ITypeOf(value)
	switch valueType {
	case ValueString:
		valueStr = fmt.Sprintf(`"%v"`, value)
	case ValueBool, ValueNumber:
		valueStr = fmt.Sprintf("%v", value)
	}
	return &TypeError{
		From:     from,
		Expected: exp,
		Actual:   valueType,
		Value:    valueStr,
	}
}

//------------------------------------------------------------------------------

type errFrom struct {
	from Function
	err  error
}

func (e *errFrom) Error() string {
	return fmt.Sprintf("%v: %v", e.from.Annotation(), e.err)
}

func (e *errFrom) Unwrap() error {
	return e.err
}

// ErrFrom wraps an error with the annotation of a function.
func ErrFrom(err error, from Function) error {
	if err == nil {
		return nil
	}
	if tErr, isTypeErr := err.(*TypeError); isTypeErr {
		if tErr.From == "" {
			tErr.From = from.Annotation()
		}
		return err
	}
	if _, isTypeMismatchErr := err.(*TypeMismatch); isTypeMismatchErr {
		return err
	}
	var fErr *errFrom
	if errors.As(err, &fErr) {
		return err
	}
	return &errFrom{from: from, err: err}
}

//------------------------------------------------------------------------------

// TypeMismatch represents an error where two values should be a comparable type
// but are not.
type TypeMismatch struct {
	Lfn       Function
	Rfn       Function
	Left      ValueType
	Right     ValueType
	Operation string
}

// Error implements the standard error interface.
func (t *TypeMismatch) Error() string {
	return fmt.Sprintf("cannot %v types %v (from %v) and %v (from %v)", t.Operation, t.Left, t.Lfn.Annotation(), t.Right, t.Rfn.Annotation())
}

// NewTypeMismatch creates a new type mismatch error.
func NewTypeMismatch(operation string, lfn, rfn Function, left, right any) *TypeMismatch {
	return &TypeMismatch{
		Lfn:       lfn,
		Rfn:       rfn,
		Left:      ITypeOf(left),
		Right:     ITypeOf(right),
		Operation: operation,
	}
}
