package query

import (
	"errors"

	"golang.org/x/xerrors"
)

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
		var caseErr error
		for i, c := range cases {
			var caseVal interface{}
			if caseVal, err = c.caseFn.Exec(ctx); err != nil {
				if recover, ok := err.(*ErrRecoverable); ok {
					caseVal = recover.Recovered
				} else {
					caseErr = xerrors.Errorf("failed to check match case %v: %w", i, err)
				}
			}
			if matched, _ := caseVal.(bool); matched {
				return c.queryFn.Exec(ctx)
			}
		}
		if caseErr != nil {
			return nil, caseErr
		}
		return nil, &ErrRecoverable{
			Recovered: Nothing(nil),
			Err:       errors.New("no expressions matched"),
		}
	})
}
