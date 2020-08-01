package query

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
)

//------------------------------------------------------------------------------

func arithmeticOpParser() parser.Type {
	opParser := parser.OneOf(
		parser.Char('+'),
		parser.Char('-'),
		parser.Char('/'),
		parser.Char('*'),
		parser.Char('%'),
		parser.Term("&&"),
		parser.Term("||"),
		parser.Term("=="),
		parser.Term("!="),
		parser.Term(">="),
		parser.Term("<="),
		parser.Char('>'),
		parser.Char('<'),
		parser.Char('|'),
	)
	return func(input []rune) parser.Result {
		res := opParser(input)
		if res.Err != nil {
			return res
		}
		switch res.Payload.(string) {
		case "+":
			res.Payload = arithmeticAdd
		case "-":
			res.Payload = arithmeticSub
		case "/":
			res.Payload = arithmeticDiv
		case "*":
			res.Payload = arithmeticMul
		case "%":
			res.Payload = arithmeticMod
		case "==":
			res.Payload = arithmeticEq
		case "!=":
			res.Payload = arithmeticNeq
		case "&&":
			res.Payload = arithmeticAnd
		case "||":
			res.Payload = arithmeticOr
		case ">":
			res.Payload = arithmeticGt
		case "<":
			res.Payload = arithmeticLt
		case ">=":
			res.Payload = arithmeticGte
		case "<=":
			res.Payload = arithmeticLte
		case "|":
			res.Payload = arithmeticPipe
		default:
			return parser.Result{
				Remaining: input,
				Err:       fmt.Errorf("operator not recognized: %v", res.Payload),
			}
		}
		return res
	}
}

func arithmeticParser(fnParser parser.Type) parser.Type {
	whitespace := parser.DiscardAll(
		parser.OneOf(
			parser.SpacesAndTabs(),
			parser.NewlineAllowComment(),
		),
	)
	p := parser.Delimited(
		fnParser,
		parser.Sequence(
			parser.DiscardAll(parser.SpacesAndTabs()),
			arithmeticOpParser(),
			whitespace,
		),
	)

	return func(input []rune) parser.Result {
		var fns []Function
		var ops []arithmeticOp

		res := p(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]interface{})
		for _, fn := range seqSlice[0].([]interface{}) {
			fns = append(fns, fn.(Function))
		}
		for _, op := range seqSlice[1].([]interface{}) {
			ops = append(ops, op.([]interface{})[1].(arithmeticOp))
		}

		fn, err := resolveArithmetic(fns, ops)
		if err != nil {
			return parser.Result{
				Err:       err,
				Remaining: input,
			}
		}
		return parser.Result{
			Payload:   fn,
			Remaining: res.Remaining,
		}
	}
}

//------------------------------------------------------------------------------
