package parser

import (
	"errors"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

//------------------------------------------------------------------------------

func intoStaticResolver(p Func) Func {
	return func(input []rune) Result {
		res := p(input)
		if str, ok := res.Payload.(string); ok {
			res.Payload = field.StaticResolver(str)
		}
		return res
	}
}

func aFunction(input []rune) Result {
	if len(input) < 3 || input[0] != '$' || input[1] != '{' || input[2] != '!' {
		return Result{
			Err:       NewError(input, "${!"),
			Remaining: input,
		}
	}
	i := 3
	for ; i < len(input); i++ {
		if input[i] == '}' {
			res := ParseDeprecatedQuery(input[3:i])
			if res.Err == nil {
				if len(res.Remaining) > 0 {
					pos := len(input[3:i]) - len(res.Remaining)
					return Result{
						Err:       NewFatalError(input[3+pos:], errors.New("required"), "end of expression"),
						Remaining: input,
					}
				}
				res.Remaining = input[i+1:]
				res.Payload = field.NewQueryResolver(res.Payload.(query.Function))
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
	return Result{
		Payload:   field.StaticResolver(string(input)),
		Err:       nil,
		Remaining: nil,
	}
}

func escapedBlock(input []rune) Result {
	if len(input) < 4 || input[0] != '$' || input[1] != '{' || input[2] != '{' || input[3] != '!' {
		return Result{
			Err:       NewError(input, "${{!"),
			Remaining: input,
		}
	}
	i := 4
	for ; i < len(input)-1; i++ {
		if input[i] == '}' && input[i+1] == '}' {
			return Result{
				Payload:   field.StaticResolver("${!" + string(input[4:i]) + "}"),
				Err:       nil,
				Remaining: input[i+2:],
			}
		}
	}
	return Result{
		Payload:   field.StaticResolver(string(input)),
		Err:       nil,
		Remaining: nil,
	}
}

//------------------------------------------------------------------------------

func parseFieldResolvers(expr string) ([]field.Resolver, *Error) {
	var resolvers []field.Resolver

	p := OneOf(
		escapedBlock,
		aFunction,
		intoStaticResolver(Char('$')),
		intoStaticResolver(NotChar('$')),
	)

	remaining := []rune(expr)
	for len(remaining) > 0 {
		res := p(remaining)
		if res.Err != nil {
			return nil, res.Err
		}
		remaining = res.Remaining
		resolvers = append(resolvers, res.Payload.(field.Resolver))
	}

	return resolvers, nil
}

// ParseField attempts to parse a field expression.
func ParseField(expr string) (field.Expression, *Error) {
	resolvers, err := parseFieldResolvers(expr)
	if err != nil {
		return nil, err
	}
	return field.NewExpression(resolvers...), nil
}

//------------------------------------------------------------------------------
