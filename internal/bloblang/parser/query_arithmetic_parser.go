package parser

import (
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
)

var arithmeticOpPattern = OneOf(
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

func arithmeticOpParser(input []rune) Result[query.ArithmeticOperator] {
	res := arithmeticOpPattern(input)
	if res.Err != nil {
		return Fail[query.ArithmeticOperator](res.Err, input)
	}

	outRes := ResultInto[query.ArithmeticOperator](res)
	switch res.Payload {
	case "+":
		outRes.Payload = query.ArithmeticAdd
	case "-":
		outRes.Payload = query.ArithmeticSub
	case "/":
		outRes.Payload = query.ArithmeticDiv
	case "*":
		outRes.Payload = query.ArithmeticMul
	case "%":
		outRes.Payload = query.ArithmeticMod
	case "==":
		outRes.Payload = query.ArithmeticEq
	case "!=":
		outRes.Payload = query.ArithmeticNeq
	case "&&":
		outRes.Payload = query.ArithmeticAnd
	case "||":
		outRes.Payload = query.ArithmeticOr
	case ">":
		outRes.Payload = query.ArithmeticGt
	case "<":
		outRes.Payload = query.ArithmeticLt
	case ">=":
		outRes.Payload = query.ArithmeticGte
	case "<=":
		outRes.Payload = query.ArithmeticLte
	case "|":
		outRes.Payload = query.ArithmeticPipe
	default:
		return Fail[query.ArithmeticOperator](
			NewFatalError(input, fmt.Errorf("operator not recognized: %v", res.Payload)),
			input,
		)
	}
	return outRes
}

func arithmeticParser(fnParser Func[query.Function]) Func[query.Function] {
	p := Delimited(
		Sequence(
			FuncAsAny(Optional(JoinStringPayloads(Sequence(charMinus, DiscardedWhitespaceNewlineComments)))),
			FuncAsAny(fnParser),
		),
		TakeOnly(1, Sequence(
			ZeroedFuncAs[string, query.ArithmeticOperator](DiscardAll(SpacesAndTabs)),
			arithmeticOpParser,
			ZeroedFuncAs[string, query.ArithmeticOperator](DiscardedWhitespaceNewlineComments),
		)),
	)

	return func(input []rune) Result[query.Function] {
		var fns []query.Function

		res := p(input)
		if res.Err != nil {
			return Fail[query.Function](res.Err, input)
		}

		delimRes := res.Payload
		for _, primaryRes := range delimRes.Primary {
			fn := primaryRes[1].(query.Function)
			if mStr, _ := primaryRes[0].(string); mStr == "-" {
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
					return Fail[query.Function](NewFatalError(input, err), input)
				}
			}
			fns = append(fns, fn)
		}

		fn, err := query.NewArithmeticExpression(fns, delimRes.Delimiter)
		if err != nil {
			return Fail[query.Function](NewFatalError(input, err), input)
		}
		return Success(fn, res.Remaining)
	}
}
