package parser

import (
	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
)

func intoStaticResolver(p Func) Func {
	return func(input []rune) Result {
		res := p(input)
		if str, ok := res.Payload.(string); ok {
			res.Payload = field.StaticResolver(str)
		}
		return res
	}
}

func aFunction(pCtx Context) Func {
	return func(input []rune) Result {
		res := Sequence(
			Term("${!"),
			Optional(SpacesAndTabs()),
			MustBe(queryParser(pCtx)),
			Optional(SpacesAndTabs()),
			MustBe(Expect(Char('}'), "end of expression")),
		)(input)

		if res.Err != nil {
			return res
		}
		res.Payload = field.NewQueryResolver(res.Payload.([]any)[2].(query.Function))
		return res
	}
}

func escapedBlock(input []rune) Result {
	res := Sequence(
		Term("${{!"),
		MustBe(Expect(UntilTerm("}}"), "end of escaped expression")),
		Term("}}"),
	)(input)
	if res.Err != nil {
		return res
	}
	res.Payload = field.StaticResolver("${!" + res.Payload.([]any)[1].(string) + "}")
	return res
}

//------------------------------------------------------------------------------

func parseFieldResolvers(pCtx Context, expr string) ([]field.Resolver, *Error) {
	var resolvers []field.Resolver

	p := OneOf(
		escapedBlock,
		aFunction(pCtx),
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
func ParseField(pCtx Context, expr string) (*field.Expression, *Error) {
	resolvers, err := parseFieldResolvers(pCtx, expr)
	if err != nil {
		return nil, err
	}
	e := field.NewExpression(resolvers...)
	return e, nil
}
