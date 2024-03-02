package parser

import (
	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
)

func intoStaticResolver(p Func[string]) Func[field.Resolver] {
	return func(input []rune) Result[field.Resolver] {
		res := p(input)
		if res.Err != nil {
			return Fail[field.Resolver](res.Err, input)
		}
		return Success[field.Resolver](field.StaticResolver(res.Payload), res.Remaining)
	}
}

var interpStart = Term("${!")

func aFunction(pCtx Context) Func[field.Resolver] {
	pattern := TakeOnly(2, Sequence(
		strToQuery(interpStart),
		strToQuery(Optional(SpacesAndTabs)),
		MustBe(queryParser(pCtx)),
		strToQuery(Optional(SpacesAndTabs)),
		strToQuery(MustBe(Expect(charSquigClose, "end of expression"))),
	))
	return func(input []rune) Result[field.Resolver] {
		res := pattern(input)
		if res.Err != nil {
			return Fail[field.Resolver](res.Err, input)
		}
		return Success[field.Resolver](field.NewQueryResolver(res.Payload), res.Remaining)
	}
}

var interpEscapedStart = Term("${{!")
var interpEscapedEnd = Term("}}")
var untilInterpEscapedEnd = UntilTerm("}}")

var escapedBlock = func() Func[field.Resolver] {
	pattern := TakeOnly(1, Sequence(
		interpEscapedStart,
		MustBe(Expect(untilInterpEscapedEnd, "end of escaped expression")),
		interpEscapedEnd,
	))
	return func(input []rune) Result[field.Resolver] {
		res := pattern(input)
		if res.Err != nil {
			return Fail[field.Resolver](res.Err, input)
		}
		return Success[field.Resolver](field.StaticResolver("${!"+res.Payload+"}"), res.Remaining)
	}
}()

//------------------------------------------------------------------------------

func parseFieldResolvers(pCtx Context, expr string) ([]field.Resolver, *Error) {
	var resolvers []field.Resolver

	p := OneOf(
		escapedBlock,
		aFunction(pCtx),
		intoStaticResolver(charDollar),
		intoStaticResolver(NotChar('$')),
	)

	remaining := []rune(expr)
	for len(remaining) > 0 {
		res := p(remaining)
		if res.Err != nil {
			return nil, res.Err
		}
		remaining = res.Remaining
		resolvers = append(resolvers, res.Payload)
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
