package query

import (
	"errors"
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/value"
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
	if tErr, isTypeErr := err.(*value.TypeError); isTypeErr {
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
	Left      value.Type
	Right     value.Type
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
		Left:      value.ITypeOf(left),
		Right:     value.ITypeOf(right),
		Operation: operation,
	}
}
