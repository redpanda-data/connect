package parser

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

//------------------------------------------------------------------------------

func arithmeticOpParser() Type {
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
			return Result{
				Remaining: input,
				Err:       NewFatalError(input, fmt.Errorf("operator not recognized: %v", res.Payload)),
			}
		}
		return res
	}
}

func arithmeticParser(fnParser Type) Type {
	whitespace := DiscardAll(
		OneOf(
			SpacesAndTabs(),
			NewlineAllowComment(),
		),
	)
	p := Delimited(
		fnParser,
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

		seqSlice := res.Payload.([]interface{})
		for _, fn := range seqSlice[0].([]interface{}) {
			fns = append(fns, fn.(query.Function))
		}
		for _, op := range seqSlice[1].([]interface{}) {
			ops = append(ops, op.([]interface{})[1].(query.ArithmeticOperator))
		}

		fn, err := query.NewArithmeticExpression(fns, ops)
		if err != nil {
			return Result{
				Err:       NewFatalError(input, err),
				Remaining: input,
			}
		}
		return Result{
			Payload:   fn,
			Remaining: res.Remaining,
		}
	}
}

//------------------------------------------------------------------------------
