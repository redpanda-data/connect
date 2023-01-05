package query

// Function takes a set of contextual arguments and returns the result of the
// query.
type Function interface {
	// Execute this function for a message of a batch.
	Exec(ctx FunctionContext) (any, error)

	// Annotation returns a string token to identify the function within error
	// messages. The returned token is not valid Bloblang and cannot be used to
	// recreate the function.
	Annotation() string

	// MarshalString returns a string representation of the function that could
	// be parsed back into the exact equivalent function. The result will be
	// normalized, which means the representation may not match the original
	// input from the user.
	// MarshalString() string

	// Returns a list of targets that this function attempts (or may attempt) to
	// access. A context must be provided that describes the current execution
	// context that this function will be executed upon, which is how it is able
	// to determine the full path and origin of values that it targets.
	//
	// A new context is returned which should be provided to methods that act
	// upon this function when querying their own targets.
	QueryTargets(ctx TargetsContext) (TargetsContext, []TargetPath)
}

// FunctionCtor constructs a new function from input arguments.
type FunctionCtor func(args *ParsedParams) (Function, error)

//------------------------------------------------------------------------------

// ClosureFunction allows you to define a Function using closures, this is a
// convenient constructor for function implementations that don't manage complex
// state.
func ClosureFunction(
	annotation string,
	exec func(ctx FunctionContext) (any, error),
	queryTargets func(ctx TargetsContext) (TargetsContext, []TargetPath),
) Function {
	if queryTargets == nil {
		queryTargets = func(ctx TargetsContext) (TargetsContext, []TargetPath) { return ctx, nil }
	}
	return closureFunction{annotation: annotation, exec: exec, queryTargets: queryTargets}
}

type closureFunction struct {
	annotation   string
	exec         func(ctx FunctionContext) (any, error)
	queryTargets func(ctx TargetsContext) (TargetsContext, []TargetPath)
}

func (f closureFunction) Annotation() string {
	return f.annotation
}

// Exec the underlying closure.
func (f closureFunction) Exec(ctx FunctionContext) (any, error) {
	return f.exec(ctx)
}

// QueryTargets returns nothing.
func (f closureFunction) QueryTargets(ctx TargetsContext) (TargetsContext, []TargetPath) {
	return f.queryTargets(ctx)
}
