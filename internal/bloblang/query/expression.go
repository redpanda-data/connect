package query

import (
	"fmt"
)

// MatchCase represents a single match case of a match expression, where a case
// query is checked and, if true, the underlying query is executed and returned.
type MatchCase struct {
	caseFn  Function
	queryFn Function
}

// NewMatchCase creates a single match case of a match expression, where a case
// query is checked and, if true, the underlying query is executed and returned.
func NewMatchCase(caseFn, queryFn Function) MatchCase {
	return MatchCase{
		caseFn, queryFn,
	}
}

// NewMatchFunction takes a contextual mapping and a list of MatchCases, when
// the function is executed
func NewMatchFunction(contextFn Function, cases ...MatchCase) Function {
	if contextFn == nil {
		contextFn = ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
			var value interface{}
			if v := ctx.Value(); v != nil {
				value = *v
			}
			return value, nil
		}, nil)
	}
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		ctxVal, err := contextFn.Exec(ctx)
		if err != nil {
			return nil, err
		}
		for i, c := range cases {
			caseCtx, err := c.caseFn.ContextCapture(ctx, ctxVal)
			if err != nil {
				return nil, fmt.Errorf("failed to capture match case %v context: %w", i, err)
			}
			var caseVal interface{}
			if caseVal, err = c.caseFn.Exec(caseCtx); err != nil {
				return nil, fmt.Errorf("failed to check match case %v: %w", i, err)
			}
			if matched, _ := caseVal.(bool); matched {
				return c.queryFn.Exec(caseCtx)
			}
		}
		return Nothing(nil), nil
	}, func(ctx TargetsContext) (TargetsContext, []TargetPath) {
		contextCtx, contextTargets := contextFn.QueryTargets(ctx)
		contextCtx = contextCtx.WithValues(contextTargets).WithValuesAsContext()

		var targets []TargetPath
		for _, c := range cases {
			_, caseTargets := c.caseFn.QueryTargets(contextCtx)
			targets = append(targets, caseTargets...)

			// TODO: Include new current targets in returned context
			_, queryTargets := c.queryFn.QueryTargets(contextCtx)
			targets = append(targets, queryTargets...)
		}

		targets = append(targets, contextTargets...)
		return ctx, targets
	})
}

// NewIfFunction creates a logical if expression from a query which should
// return a boolean value. If the returned boolean is true then the ifFn is
// executed and returned, otherwise elseFn is executed and returned.
func NewIfFunction(queryFn Function, ifFn Function, elseFn Function) Function {
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		queryVal, err := queryFn.Exec(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to check if condition: %w", err)
		}
		if queryRes, _ := queryVal.(bool); queryRes {
			return ifFn.Exec(ctx)
		}
		if elseFn != nil {
			return elseFn.Exec(ctx)
		}
		return Nothing(nil), nil
	}, aggregateTargetPaths(queryFn, ifFn, elseFn))
}

// NewNamedContextFunction wraps a function and ensures that when the function
// is executed with a new context the context is captured under a new name, with
// the "main" context left intact.
func NewNamedContextFunction(name string, fn Function) Function {
	return &namedContextFunction{name, fn}
}

type namedContextFunction struct {
	name string
	fn   Function
}

func (n *namedContextFunction) ContextCapture(ctx FunctionContext, value interface{}) (FunctionContext, error) {
	if n.name == "_" {
		// Special case means we totally disregard the context.
		return ctx, nil
	}
	ctx = ctx.WithNamedValue(n.name, value)
	return ctx, nil
}

func (n *namedContextFunction) Exec(ctx FunctionContext) (interface{}, error) {
	return n.fn.Exec(ctx)
}

func (n *namedContextFunction) QueryTargets(ctx TargetsContext) (TargetsContext, []TargetPath) {
	if n.name == "_" {
		ctx = ctx.PopContext()
	} else {
		ctx = ctx.WithContextAsNamed(n.name)
	}
	return n.fn.QueryTargets(ctx)
}
