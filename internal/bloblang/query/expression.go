package query

import "fmt"

type matchCase struct {
	caseFn  Function
	queryFn Function
}

func matchFunction(contextFn Function, cases []matchCase) Function {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
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
	})
}

func ifFunction(queryFn Function, ifFn Function, elseFn Function) Function {
	return closureFn(func(ctx FunctionContext) (interface{}, error) {
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
	})
}
