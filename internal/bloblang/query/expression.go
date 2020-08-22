package query

import "fmt"

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
			if ctx.Value != nil {
				value = *ctx.Value
			}
			return value, nil
		}, nil)
	}
	return ClosureFunction(func(ctx FunctionContext) (interface{}, error) {
		ctxVal, err := contextFn.Exec(ctx)
		if err != nil {
			return nil, err
		}
		ctx.Value = &ctxVal
		for i, c := range cases {
			var caseVal interface{}
			if caseVal, err = c.caseFn.Exec(ctx); err != nil {
				return nil, fmt.Errorf("failed to check match case %v: %w", i, err)
			}
			if matched, _ := caseVal.(bool); matched {
				return c.queryFn.Exec(ctx)
			}
		}
		return Nothing(nil), nil
	}, func(ctx TargetsContext) []TargetPath {
		contextTargets := contextFn.QueryTargets(ctx)

		var targets []TargetPath
		for _, c := range cases {
			var cTargets []TargetPath
			cTargets = append(cTargets, c.caseFn.QueryTargets(ctx)...)
			cTargets = append(cTargets, c.queryFn.QueryTargets(ctx)...)
			targets = append(targets, rebaseTargetPaths(cTargets, contextTargets)...)
		}

		targets = append(targets, contextTargets...)
		return targets
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
