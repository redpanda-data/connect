package expression

import (
	"errors"
	"fmt"
)

//------------------------------------------------------------------------------

type expectedErr []string

func (e expectedErr) Error() string {
	return fmt.Sprintf("expected one of: %v", []string(e))
}

//------------------------------------------------------------------------------

type parserResult struct {
	Result    interface{}
	Err       error
	Remaining []rune
}

type parser func([]rune) parserResult

//------------------------------------------------------------------------------

func notEnd(p parser) parser {
	return func(input []rune) parserResult {
		if len(input) == 0 {
			return parserResult{
				Result:    nil,
				Err:       errors.New("unexpected end of input"),
				Remaining: input,
			}
		}
		return p(input)
	}
}

func char(c rune) parser {
	return notEnd(func(input []rune) parserResult {
		if input[0] != c {
			return parserResult{
				Result:    nil,
				Err:       expectedErr{string(c)},
				Remaining: input,
			}
		}
		return parserResult{
			Result:    string(c),
			Err:       nil,
			Remaining: input[1:],
		}
	})
}

func notChar(c rune) parser {
	return notEnd(func(input []rune) parserResult {
		if input[0] == c {
			return parserResult{
				Result:    nil,
				Err:       expectedErr{"not " + string(c)},
				Remaining: input,
			}
		}
		i := 0
		for ; i < len(input); i++ {
			if input[i] == c {
				return parserResult{
					Result:    string(input[:i]),
					Err:       nil,
					Remaining: input[i:],
				}
			}
		}
		return parserResult{
			Result:    string(input),
			Err:       nil,
			Remaining: nil,
		}
	})
}

func aFunction(input []rune) parserResult {
	if len(input) < 3 || input[0] != '$' || input[1] != '{' || input[2] != '!' {
		return parserResult{
			Result:    nil,
			Err:       expectedErr{"${!"},
			Remaining: input,
		}
	}
	i := 3
	for ; i < len(input); i++ {
		if input[i] == '}' {
			res := parseFunction(input[3:i])
			if res.Err == nil {
				res.Remaining = input[i+1:]
			} else {
				res.Remaining = input
			}
			return res
		}
	}
	/*
		return parserResult{
			Err:       errors.New("function expression started with `${!` but was not terminated with `}`"),
			Remaining: input,
		}
	*/
	return parserResult{
		Result:    staticResolver(string(input)),
		Err:       nil,
		Remaining: nil,
	}
}

func escapedBlock(input []rune) parserResult {
	if len(input) < 4 || input[0] != '$' || input[1] != '{' || input[2] != '{' || input[3] != '!' {
		return parserResult{
			Result:    nil,
			Err:       expectedErr{"${{!"},
			Remaining: input,
		}
	}
	i := 4
	for ; i < len(input)-1; i++ {
		if input[i] == '}' && input[i+1] == '}' {
			return parserResult{
				Result:    staticResolver("${!" + string(input[4:i]) + "}"),
				Err:       nil,
				Remaining: input[i+2:],
			}
		}
	}
	/*
		return parserResult{
			Err:       errors.New("function escape block started with `${{!` but was not terminated with `}}`"),
			Remaining: input,
		}
	*/
	return parserResult{
		Result:    staticResolver(string(input)),
		Err:       nil,
		Remaining: nil,
	}
}

func anyOf(parsers ...parser) parser {
	return func(input []rune) parserResult {
		var expectedStack expectedErr
	tryParsers:
		for _, p := range parsers {
			res := p(input)
			if res.Err != nil {
				switch t := res.Err.(type) {
				case expectedErr:
					expectedStack = append(expectedStack, t...)
					continue tryParsers
				}
			}
			return res
		}
		seen := map[string]struct{}{}
		var dedupeStack expectedErr
		for _, s := range expectedStack {
			if _, exists := seen[string(s)]; !exists {
				dedupeStack = append(dedupeStack, s)
				seen[string(s)] = struct{}{}
			}
		}
		return parserResult{
			Err:       dedupeStack,
			Remaining: input,
		}
	}
}

func intoStaticResolver(p parser) parser {
	return func(input []rune) parserResult {
		res := p(input)
		if str, ok := res.Result.(string); ok {
			res.Result = staticResolver(str)
		}
		return res
	}
}

//------------------------------------------------------------------------------

// Parse a string into a Benthos dynamic field expression.
func Parse(expr string) (*Expression, error) {
	var resolvers []resolver

	p := anyOf(
		escapedBlock,
		aFunction,
		intoStaticResolver(char('$')),
		intoStaticResolver(notChar('$')),
	)

	remaining := []rune(expr)
	i := 0
	for len(remaining) > 0 {
		res := p(remaining)
		if res.Err != nil {
			return nil, fmt.Errorf("failed to parse expression at char %v: %v", i, res.Err)
		}
		i = len(remaining) - len(res.Remaining)
		remaining = res.Remaining
		resolvers = append(resolvers, res.Result.(resolver))
	}

	return buildExpression(resolvers), nil
}

//------------------------------------------------------------------------------
