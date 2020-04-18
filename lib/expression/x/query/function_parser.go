package query

import (
	"fmt"
	"sort"

	"github.com/Jeffail/benthos/v3/lib/expression/x/parser"
)

//------------------------------------------------------------------------------

type badFunctionErr string

func (e badFunctionErr) Error() string {
	exp := []string{}
	for k := range functions {
		exp = append(exp, k)
	}
	sort.Strings(exp)
	return fmt.Sprintf("unrecognised function '%v', expected one of: %v", string(e), exp)
}

type badMethodErr string

func (e badMethodErr) Error() string {
	exp := []string{}
	for k := range methods {
		exp = append(exp, k)
	}
	sort.Strings(exp)
	return fmt.Sprintf("unrecognised method '%v', expected one of: %v", string(e), exp)
}

//------------------------------------------------------------------------------

func functionArgsParser(allowFunctions bool) parser.Type {
	paramTypes := []parser.Type{
		parser.Boolean(),
		parser.Number(),
		parser.QuotedString(),
	}
	if allowFunctions {
		paramTypes = append(paramTypes, Parse)
	}

	parseParam := parser.AnyOf(paramTypes...)
	parseStart := parser.Char('(')
	parseEnd := parser.Char(')')
	parseNext := parser.AnyOf(
		parser.Char(')'),
		parser.Char(','),
	)

	return func(input []rune) parser.Result {
		res := parseStart(input)
		if res.Err != nil {
			return res
		}

		var params []interface{}
		if earlyExit := parseEnd(res.Remaining); earlyExit.Err == nil {
			earlyExit.Result = params
			return earlyExit
		}

		for {
			res = parser.SpacesAndTabs()(res.Remaining)
			i := len(input) - len(res.Remaining)
			res = parseParam(res.Remaining)
			if res.Err != nil {
				res.Err = parser.ErrAtPosition(i, res.Err)
				return res
			}
			params = append(params, res.Result)

			res = parser.SpacesAndTabs()(res.Remaining)
			i = len(input) - len(res.Remaining)
			res = parseNext(res.Remaining)
			if res.Err != nil {
				res.Err = parser.ErrAtPosition(i, res.Err)
				return res
			}

			if ")" == res.Result.(string) {
				break
			}
		}

		return parser.Result{
			Result:    params,
			Err:       nil,
			Remaining: res.Remaining,
		}
	}
}

func literalParser() parser.Type {
	parseLiteral := parser.AnyOf(
		parser.Boolean(),
		parser.Number(),
		parser.QuotedString(),
	)
	return func(input []rune) parser.Result {
		res := parseLiteral(input)
		if res.Err == nil {
			res.Result = literalFunction(res.Result)
		}
		return res
	}
}

func parseMethod(fn Function) parser.Type {
	argsParser := functionArgsParser(true)

	return func(input []rune) parser.Result {
		res := parser.CamelCase()(input)
		if res.Err != nil {
			return res
		}

		targetMethod := res.Result.(string)
		mtor, exists := methods[targetMethod]
		if !exists {
			return parser.Result{
				Err:       badMethodErr(targetMethod),
				Remaining: input,
			}
		}

		if len(res.Remaining) == 0 {
			return parser.Result{
				Err:       fmt.Errorf("expected params '()' after method: '%v'", targetMethod),
				Remaining: input,
			}
		}

		i := len(input) - len(res.Remaining)
		res = argsParser(res.Remaining)
		if res.Err != nil {
			return parser.Result{
				Err: parser.ErrAtPosition(i, res.Err).Expand(func(err error) error {
					return fmt.Errorf("failed to parse method arguments: %v", err)
				}),
				Remaining: input,
			}
		}
		args := res.Result.([]interface{})

		method, err := mtor(fn, args...)
		if err != nil {
			return parser.Result{
				Err:       parser.ErrAtPosition(i, err),
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
	argsParser := functionArgsParser(false)

	return func(input []rune) parser.Result {
		var targetFunc string
		var args []interface{}

		res := parser.CamelCase()(input)
		if res.Err != nil {
			return res
		}
		targetFunc = res.Result.(string)
		ftor, exists := functions[targetFunc]
		if !exists {
			return parser.Result{
				Err:       badFunctionErr(targetFunc),
				Remaining: input,
			}
		}

		if len(res.Remaining) == 0 {
			return parser.Result{
				Err: parser.ErrAtPosition(
					len(targetFunc),
					fmt.Errorf("expected params '()' after function: '%v'", targetFunc),
				),
				Remaining: input,
			}
		}

		i := len(input) - len(res.Remaining)
		res = argsParser(res.Remaining)
		if res.Err != nil {
			res.Err = parser.ErrAtPosition(i, res.Err).Expand(func(err error) error {
				return fmt.Errorf("failed to parse function arguments: %v", err)
			})
			res.Remaining = input
			return res
		}
		args = res.Result.([]interface{})

		fnResolver, err := ftor(args...)
		if err != nil {
			return parser.Result{
				Err:       parser.ErrAtPosition(i, err),
				Remaining: input,
			}
		}

		for {
			res = parser.Char('.')(res.Remaining)
			if res.Err != nil {
				break
			}

			i = len(input) - len(res.Remaining)
			res = parseMethod(fnResolver)(res.Remaining)
			if res.Err != nil {
				res.Err = parser.ErrAtPosition(i, res.Err)
				res.Remaining = input
				return res
			}
			fnResolver = res.Result.(Function)
		}

		return parser.Result{
			Result:    fnResolver,
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
