package query

import (
	"github.com/Jeffail/benthos/v3/lib/bloblang/x/parser"
)

func matchCaseParser() parser.Type {
	section := parser.Match("=>")
	whitespace := parser.SpacesAndTabs()

	return func(input []rune) parser.Result {
		res := parser.AnyOf(
			parser.Match("_ "),
			createParser(false),
		)(input)
		if res.Err != nil {
			return res
		}

		var caseFn Function
		switch t := res.Result.(type) {
		case Function:
			caseFn = t
		case string:
			caseFn = literalFunction(true)
		}

		res = whitespace(res.Remaining)
		i := len(input) - len(res.Remaining)
		if res = section(res.Remaining); res.Err != nil {
			return parser.Result{
				Err:       parser.ErrAtPosition(i, res.Err),
				Remaining: input,
			}
		}

		res = whitespace(res.Remaining)
		i = len(input) - len(res.Remaining)
		if res = createParser(false)(res.Remaining); res.Err != nil {
			return parser.Result{
				Err:       parser.ErrAtPosition(i, res.Err),
				Remaining: input,
			}
		}

		return parser.Result{
			Result: matchCase{
				caseFn:  caseFn,
				queryFn: res.Result.(Function),
			},
			Remaining: res.Remaining,
		}
	}
}

func lineBreak() parser.Type {
	lb := parser.Char('\n')
	return func(input []rune) parser.Result {
		res := lb(input)
		if res.Err != nil {
			if _, ok := res.Err.(parser.ExpectedError); ok {
				res.Err = parser.ExpectedError{"line-break"}
			}
		}
		return res
	}
}

func matchExpressionParser() parser.Type {
	matchWord := parser.Match("match")
	whitespace := parser.SpacesAndTabs()
	linebreak := lineBreak()
	caseParser := matchCaseParser()

	return func(input []rune) parser.Result {
		res := whitespace(input)
		if res = matchWord(res.Remaining); res.Err != nil {
			return res
		}
		res = whitespace(res.Remaining)

		i := len(input) - len(res.Remaining)
		if res = createParser(false)(res.Remaining); res.Err != nil {
			return parser.Result{
				Err:       parser.ErrAtPosition(i, res.Err),
				Remaining: input,
			}
		}

		contextFn := res.Result.(Function)
		cases := []matchCase{}

	caseLoop:
		for {
			res = whitespace(res.Remaining)
			i = len(input) - len(res.Remaining)
			if res = linebreak(res.Remaining); res.Err == nil {
				res = whitespace(res.Remaining)
				i = len(input) - len(res.Remaining)
				if res = caseParser(res.Remaining); res.Err == nil {
					cases = append(cases, res.Result.(matchCase))
				}
			}
			if res.Err != nil {
				if len(cases) == 0 {
					return parser.Result{
						Err:       parser.ErrAtPosition(i, res.Err),
						Remaining: input,
					}
				}
				break caseLoop
			}
		}

		res.Err = nil
		res.Result = matchFunction(contextFn, cases)
		return res
	}
}
