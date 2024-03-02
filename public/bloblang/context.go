package bloblang

import (
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/value"
)

// ExecContext is an optional context provided to advanced functions and methods
// that contains information about a given mapping at the time the
// function/method is executed. The vast majority of Bloblang plugins would not
// require this context and it is omitted by default.
type ExecContext struct {
	ctx query.FunctionContext
}

func newExecContext(ctx query.FunctionContext) *ExecContext {
	return &ExecContext{
		ctx: ctx,
	}
}

type (
	// ExecResultNothing represents a value yielded from an executed function
	// where the mapping resulted in nothing being enacted. For example, this
	// could mean the mapping was an if-statement that resolved to `false` and
	// had no alternative branches to execute.
	//
	// When nothing is returned it is up to the caller of the function to
	// interpret what this means. Usually it means the input data should be left
	// as it was.
	//
	// THERE IS NO CIRCUMSTANCE WHERE IT IS APPROPRIATE FOR PLUGIN AUTHORS TO
	// RETURN THIS VALUE.
	ExecResultNothing *struct{}

	// ExecResultDelete represents a value yielded from an executed function
	// where the mapping resulted in an instruction to delete the root entity.
	//
	// When a delete is returned it is up to the caller of the function to
	// interpret what this means. Usually it means the input data should be
	// deleted entirely.
	//
	// THERE IS NO CIRCUMSTANCE WHERE IT IS APPROPRIATE FOR PLUGIN AUTHORS TO
	// RETURN THIS VALUE.
	ExecResultDelete *struct{}
)

// Exec attempts to execute a provided ExecFunction, returning either a value or
// an error. Values returned depend on the function but typically fall within
// the standard scalar, any-map and any-slice values seen by most plugins.
//
// However, two exceptions exist which should be noted by Exec callers:
// ExecResultNothing and ExecResultDelete, as both of these values could be
// yielded and must be handled differently to typical values.
func (e *ExecContext) Exec(fn *ExecFunction) (any, error) {
	v, err := fn.fn.Exec(e.ctx)
	if err != nil {
		return nil, err
	}
	switch v.(type) {
	case value.Delete:
		return ExecResultDelete(nil), nil
	case value.Nothing:
		return ExecResultNothing(nil), nil
	}
	return v, nil
}

// ExecToInt64 attempts to execute a provided ExecFunction, returning either an
// integer value or an error if the execution failed or the value returned was
// not a valid integer.
func (e *ExecContext) ExecToInt64(fn *ExecFunction) (int64, error) {
	v, err := fn.fn.Exec(e.ctx)
	if err != nil {
		return 0, err
	}
	return value.IToInt(v)
}

// ExecFunction represents an active Bloblang function that can be executed by
// providing it an ExecContext. This is only relevant for advanced functions and
// methods that may wish to exhibit fully customised behaviours and/or mutate
// the contextual state of parameters and/or method targets.
//
// Most plugin authors will not have a need for interacting with any
// ExecFunction.
type ExecFunction struct {
	fn query.Function
}

func newExecFunction(fn query.Function) *ExecFunction {
	return &ExecFunction{fn: fn}
}
