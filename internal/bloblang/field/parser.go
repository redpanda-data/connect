package field

import (
	"errors"

	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

//------------------------------------------------------------------------------

func intoStaticResolver(p parser.Type) parser.Type {
	return func(input []rune) parser.Result {
		res := p(input)
		if str, ok := res.Payload.(string); ok {
			res.Payload = staticResolver(str)
		}
		return res
	}
}

func aFunction(input []rune) parser.Result {
	if len(input) < 3 || input[0] != '$' || input[1] != '{' || input[2] != '!' {
		return parser.Result{
			Err:       parser.NewError(input, "${!"),
			Remaining: input,
		}
	}
	i := 3
	for ; i < len(input); i++ {
		if input[i] == '}' {
			res := query.ParseDeprecated(input[3:i])
			if res.Err == nil {
				if len(res.Remaining) > 0 {
					pos := len(input[3:i]) - len(res.Remaining)
					return parser.Result{
						Err:       parser.NewFatalError(input[3+pos:], errors.New("required"), "end of expression"),
						Remaining: input,
					}
				}
				res.Remaining = input[i+1:]
				res.Payload = queryResolver{fn: res.Payload.(query.Function)}
			} else {
				pos := len(input[3:i]) - len(res.Err.Input)
				if !res.Err.IsFatal() {
					res.Err.Err = errors.New("required")
				}
				res.Err.Input = input[3+pos:]
				res.Remaining = input
			}
			return res
		}
	}
	return parser.Result{
		Payload:   staticResolver(string(input)),
		Err:       nil,
		Remaining: nil,
	}
}

func escapedBlock(input []rune) parser.Result {
	if len(input) < 4 || input[0] != '$' || input[1] != '{' || input[2] != '{' || input[3] != '!' {
		return parser.Result{
			Err:       parser.NewError(input, "${{!"),
			Remaining: input,
		}
	}
	i := 4
	for ; i < len(input)-1; i++ {
		if input[i] == '}' && input[i+1] == '}' {
			return parser.Result{
				Payload:   staticResolver("${!" + string(input[4:i]) + "}"),
				Err:       nil,
				Remaining: input[i+2:],
			}
		}
	}
	return parser.Result{
		Payload:   staticResolver(string(input)),
		Err:       nil,
		Remaining: nil,
	}
}

//------------------------------------------------------------------------------

func parse(expr string) (*expression, *parser.Error) {
	var resolvers []resolver

	p := parser.OneOf(
		escapedBlock,
		aFunction,
		intoStaticResolver(parser.Char('$')),
		intoStaticResolver(parser.NotChar('$')),
	)

	remaining := []rune(expr)
	for len(remaining) > 0 {
		res := p(remaining)
		if res.Err != nil {
			return nil, res.Err
		}
		remaining = res.Remaining
		resolvers = append(resolvers, res.Payload.(resolver))
	}

	return buildExpression(resolvers), nil
}

//------------------------------------------------------------------------------
