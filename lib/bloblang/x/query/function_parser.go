package query

import (
	"fmt"
	"sort"

	"github.com/Jeffail/benthos/v3/lib/bloblang/x/parser"
)

//------------------------------------------------------------------------------

type badFunctionErr string

func (e badFunctionErr) Error() string {
	return fmt.Sprintf("unrecognised function '%v'", string(e))
}

func (e badFunctionErr) ToExpectedErr() parser.ExpectedError {
	exp := []string{}
	for k := range functions {
		exp = append(exp, k)
	}
	sort.Strings(exp)
	return parser.ExpectedError(exp)
}

type badMethodErr string

func (e badMethodErr) Error() string {
	return fmt.Sprintf("unrecognised method '%v'", string(e))
}

func (e badMethodErr) ToExpectedErr() parser.ExpectedError {
	exp := []string{}
	for k := range methods {
		exp = append(exp, k)
	}
	sort.Strings(exp)
	return parser.ExpectedError(exp)
}

//------------------------------------------------------------------------------

func functionArgsParser(allowFunctions bool) parser.Type {
	open, comma, close := parser.Char('('), parser.Char(','), parser.Char(')')
	whitespace := parser.DiscardAll(
		parser.AnyOf(
			parser.SpacesAndTabs(),
			parser.NewlineAllowComment(),
		),
	)

	paramTypes := []parser.Type{
		parser.Boolean(),
		parser.Number(),
		parser.QuotedString(),
	}

	return func(input []rune) parser.Result {
		tmpParamTypes := paramTypes
		if allowFunctions {
			tmpParamTypes = append([]parser.Type{}, paramTypes...)
			tmpParamTypes = append(tmpParamTypes, Parse)
		}
		return parser.DelimitedPattern(
			parser.InterceptExpectedError(
				parser.Sequence(
					open,
					whitespace,
				),
				"function-parameters",
			),
			parser.MustBe(parser.AnyOf(tmpParamTypes...)),
			parser.Sequence(
				parser.Discard(parser.SpacesAndTabs()),
				comma,
				whitespace,
			),
			parser.Sequence(
				whitespace,
				close,
			),
			false, false,
		)(input)
	}
}

func literalValueParser() parser.Type {
	parseLiteral := parser.LiteralValue()
	return func(input []rune) parser.Result {
		res := parseLiteral(input)
		if res.Err == nil {
			res.Result = literalFunction(res.Result)
		}
		return res
	}
}

func parseFunctionTail(fn Function) parser.Type {
	openBracket := parser.Char('(')
	closeBracket := parser.Char(')')

	whitespace := parser.DiscardAll(
		parser.AnyOf(
			parser.SpacesAndTabs(),
			parser.NewlineAllowComment(),
		),
	)

	return func(input []rune) parser.Result {
		res := parser.AnyOf(
			parser.Sequence(
				parser.InterceptExpectedError(openBracket, "method"),
				whitespace,
				Parse,
				whitespace,
				closeBracket,
			),
			methodParser(fn),
			fieldLiteralMapParser(fn),
		)(input)
		if seqSlice, isSlice := res.Result.([]interface{}); isSlice {
			res.Result, res.Err = mapMethod(fn, seqSlice[2].(Function))
		}
		return res
	}
}

func parseWithTails(fnParser parser.Type) parser.Type {
	delim := parser.Sequence(
		parser.Char('.'),
		parser.Discard(
			parser.Sequence(
				parser.NewlineAllowComment(),
				parser.SpacesAndTabs(),
			),
		),
	)

	return func(input []rune) parser.Result {
		res := fnParser(input)
		if res.Err != nil {
			return res
		}

		fn := res.Result.(Function)
		for {
			if res = delim(res.Remaining); res.Err != nil {
				return parser.Result{
					Result:    fn,
					Remaining: res.Remaining,
				}
			}
			i := len(input) - len(res.Remaining)
			if res = parser.MustBe(parseFunctionTail(fn))(res.Remaining); res.Err != nil {
				return parser.Result{
					Err:       parser.ErrAtPosition(i, res.Err),
					Remaining: res.Remaining,
				}
			}
			fn = res.Result.(Function)
		}
	}
}

func fieldLiteralMapParser(ctxFn Function) parser.Type {
	fieldPathParser := parser.JoinStringSliceResult(
		parser.InterceptExpectedError(
			parser.AllOf(
				parser.AnyOf(
					parser.InRange('a', 'z'),
					parser.InRange('A', 'Z'),
					parser.InRange('0', '9'),
					parser.InRange('*', '-'),
					parser.Char('_'),
					parser.Char('~'),
				),
			),
			"field-path",
		),
	)

	return func(input []rune) parser.Result {
		res := fieldPathParser(input)
		if res.Err != nil {
			return res
		}

		fn, err := fieldFunction(res.Result.(string))
		if err == nil {
			fn, err = mapMethod(ctxFn, fn)
		}
		if err != nil {
			return parser.Result{
				Remaining: input,
				Err:       err,
			}
		}

		return parser.Result{
			Remaining: res.Remaining,
			Result:    fn,
		}
	}
}

func fieldLiteralRootParser() parser.Type {
	fieldPathParser := parser.InterceptExpectedError(
		parser.JoinStringSliceResult(
			parser.AllOf(
				parser.AnyOf(
					parser.InRange('a', 'z'),
					parser.InRange('A', 'Z'),
					parser.InRange('0', '9'),
					parser.InRange('*', '-'),
					parser.Char('_'),
					parser.Char('~'),
				),
			),
		),
		"field-path",
	)

	return func(input []rune) parser.Result {
		res := fieldPathParser(input)
		if res.Err != nil {
			return res
		}

		var fn Function
		var err error

		path := res.Result.(string)
		if path == "this" {
			fn, err = fieldFunction()
		} else {
			fn, err = fieldFunction(path)
		}
		if err != nil {
			return parser.Result{
				Remaining: input,
				Err:       err,
			}
		}

		return parser.Result{
			Remaining: res.Remaining,
			Result:    fn,
		}
	}
}

func methodParser(fn Function) parser.Type {
	p := parser.Sequence(
		parser.InterceptExpectedError(
			parser.SnakeCase(),
			"method",
		),
		functionArgsParser(true),
	)

	return func(input []rune) parser.Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Result.([]interface{})

		targetMethod := seqSlice[0].(string)
		mtor, exists := methods[targetMethod]
		if !exists {
			return parser.Result{
				Err:       badMethodErr(targetMethod),
				Remaining: input,
			}
		}

		args := seqSlice[1].([]interface{})
		method, err := mtor(fn, args...)
		if err != nil {
			return parser.Result{
				Err:       err,
				Remaining: input,
			}
		}
		return parser.Result{
			Result:    method,
			Remaining: res.Remaining,
		}
	}
}

func functionParser() parser.Type {
	p := parser.Sequence(
		parser.InterceptExpectedError(
			parser.SnakeCase(),
			"function",
		),
		functionArgsParser(true),
	)

	return func(input []rune) parser.Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Result.([]interface{})

		targetFunc := seqSlice[0].(string)
		ctor, exists := functions[targetFunc]
		if !exists {
			return parser.Result{
				Err:       badFunctionErr(targetFunc),
				Remaining: input,
			}
		}

		args := seqSlice[1].([]interface{})
		fn, err := ctor(args...)
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

func parseDeprecatedFunction(input []rune) parser.Result {
	var targetFunc, arg string

	for i := 0; i < len(input); i++ {
		if input[i] == ':' {
			targetFunc = string(input[:i])
			arg = string(input[i+1:])
		}
	}
	if len(targetFunc) == 0 {
		targetFunc = string(input)
	}

	ftor, exists := deprecatedFunctions[targetFunc]
	if !exists {
		return parser.Result{
			// Make no suggestions, we want users to move off of these functions
			Err:       parser.ExpectedError{},
			Remaining: input,
		}
	}
	return parser.Result{
		Result:    wrapDeprecatedFunction(ftor(arg)),
		Err:       nil,
		Remaining: nil,
	}
}

//------------------------------------------------------------------------------
