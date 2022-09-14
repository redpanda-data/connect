package parser

import (
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
)

//------------------------------------------------------------------------------

func arithmeticOpParser() Func {
	opParser := OneOf(
		Char('+'),
		Char('-'),
		Char('/'),
		Char('*'),
		Char('%'),
		Term("&&"),
		Term("||"),
		Term("=="),
		Term("!="),
		Term(">="),
		Term("<="),
		Char('>'),
		Char('<'),
		Char('|'),
	)
	return func(input []rune) Result {
		res := opParser(input)
		if res.Err != nil {
			return res
		}
		switch res.Payload.(string) {
		case "+":
			res.Payload = query.ArithmeticAdd
		case "-":
			res.Payload = query.ArithmeticSub
		case "/":
			res.Payload = query.ArithmeticDiv
		case "*":
			res.Payload = query.ArithmeticMul
		case "%":
			res.Payload = query.ArithmeticMod
		case "==":
			res.Payload = query.ArithmeticEq
		case "!=":
			res.Payload = query.ArithmeticNeq
		case "&&":
			res.Payload = query.ArithmeticAnd
		case "||":
			res.Payload = query.ArithmeticOr
		case ">":
			res.Payload = query.ArithmeticGt
		case "<":
			res.Payload = query.ArithmeticLt
		case ">=":
			res.Payload = query.ArithmeticGte
		case "<=":
			res.Payload = query.ArithmeticLte
		case "|":
			res.Payload = query.ArithmeticPipe
		default:
			return Fail(
				NewFatalError(input, fmt.Errorf("operator not recognized: %v", res.Payload)),
				input,
			)
		}
		return res
	}
}

func arithmeticParser(fnParser Func) Func {
	whitespace := DiscardAll(
		OneOf(
			SpacesAndTabs(),
			NewlineAllowComment(),
		),
	)
	p := Delimited(
		Sequence(
			Optional(Sequence(Char('-'), whitespace)),
			fnParser,
		),
		Sequence(
			DiscardAll(SpacesAndTabs()),
			arithmeticOpParser(),
			whitespace,
		),
	)

	return func(input []rune) Result {
		var fns []query.Function
		var ops []query.ArithmeticOperator

		res := p(input)
		if res.Err != nil {
			return res
		}

		delimRes := res.Payload.(DelimitedResult)
		for _, primaryRes := range delimRes.Primary {
			fnSeq := primaryRes.([]any)
			fn := fnSeq[1].(query.Function)
			if fnSeq[0] != nil {
				var err error
				if fn, err = query.NewArithmeticExpression(
					[]query.Function{
						query.NewLiteralFunction("", int64(0)),
						fn,
					},
					[]query.ArithmeticOperator{
						query.ArithmeticSub,
					},
				); err != nil {
					return Fail(NewFatalError(input, err), input)
				}
			}
			fns = append(fns, fn)
		}
		for _, op := range delimRes.Delimiter {
			ops = append(ops, op.([]any)[1].(query.ArithmeticOperator))
		}

		fn, err := query.NewArithmeticExpression(fns, ops)
		if err != nil {
			return Fail(NewFatalError(input, err), input)
		}
		return Success(fn, res.Remaining)
	}
}

//------------------------------------------------------------------------------
