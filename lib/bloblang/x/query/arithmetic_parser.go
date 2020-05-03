package query

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/bloblang/x/parser"
)

//------------------------------------------------------------------------------

func arithmeticOpParser() parser.Type {
	opParser := parser.AnyOf(
		parser.Char('+'),
		parser.Char('-'),
		parser.Char('/'),
		parser.Char('*'),
		parser.Match("&&"),
		parser.Match("||"),
		parser.Match("=="),
		parser.Match("!="),
		parser.Match(">="),
		parser.Match("<="),
		parser.Char('>'),
		parser.Char('<'),
		parser.Char('|'),
	)
	return func(input []rune) parser.Result {
		res := opParser(input)
		if res.Err != nil {
			return res
		}
		switch res.Result.(string) {
		case "+":
			res.Result = arithmeticAdd
		case "-":
			res.Result = arithmeticSub
		case "/":
			res.Result = arithmeticDiv
		case "*":
			res.Result = arithmeticMul
		case "==":
			res.Result = arithmeticEq
		case "!=":
			res.Result = arithmeticNeq
		case "&&":
			res.Result = arithmeticAnd
		case "||":
			res.Result = arithmeticOr
		case ">":
			res.Result = arithmeticGt
		case "<":
			res.Result = arithmeticLt
		case ">=":
			res.Result = arithmeticGte
		case "<=":
			res.Result = arithmeticLte
		case "|":
			res.Result = arithmeticPipe
		default:
			return parser.Result{
				Remaining: input,
				Err:       fmt.Errorf("operator not recognized: %v", res.Result),
			}
		}
		return res
	}
}

func arithmeticParser(fnParser parser.Type) parser.Type {
	whitespace := parser.DiscardAll(
		parser.AnyOf(
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

		seqSlice := res.Result.([]interface{})
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
			Result:    fn,
			Remaining: res.Remaining,
		}
	}
}

//------------------------------------------------------------------------------
