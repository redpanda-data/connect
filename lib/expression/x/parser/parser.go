package parser

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

//------------------------------------------------------------------------------

// ExpectedError represents a parser error where one of a list of possible
// tokens was expected but not found.
type ExpectedError []string

// Error returns a human readable error string.
func (e ExpectedError) Error() string {
	return fmt.Sprintf("expected one of: %v", []string(e))
}

// PositionalError represents an error that has occurred at a particular
// position in the input.
type PositionalError struct {
	Position int
	Err      error
}

// Error returns a human readable error string.
func (e PositionalError) Error() string {
	return fmt.Sprintf("char %v: %v", e.Position, e.Err)
}

// Expand the underlying error with more context.
func (e PositionalError) Expand(fn func(error) error) PositionalError {
	e.Err = fn(e.Err)
	return e
}

// ErrAtPosition takes an error and returns a positional wrapper. If the
// provided error is itself a positional type then the position is aggregated.
func ErrAtPosition(i int, err error) PositionalError {
	if p, ok := err.(PositionalError); ok {
		p.Position += i
		return p
	}
	return PositionalError{
		Position: i,
		Err:      err,
	}
}

//------------------------------------------------------------------------------

// Result represents the result of a parser given an input.
type Result struct {
	Result    interface{}
	Err       error
	Remaining []rune
}

// Type is a general parser method.
type Type func([]rune) Result

//------------------------------------------------------------------------------

// NotEnd parses zero characters from an input and expects it to not have ended.
func NotEnd(p Type) Type {
	return func(input []rune) Result {
		if len(input) == 0 {
			return Result{
				Result:    nil,
				Err:       errors.New("unexpected end of input"),
				Remaining: input,
			}
		}
		return p(input)
	}
}

// Char parses a single character and expects it to match one candidate.
func Char(c rune) Type {
	return NotEnd(func(input []rune) Result {
		if input[0] != c {
			return Result{
				Result:    nil,
				Err:       ExpectedError{string(c)},
				Remaining: input,
			}
		}
		return Result{
			Result:    string(c),
			Err:       nil,
			Remaining: input[1:],
		}
	})
}

// NotChar parses any number of characters until they match a single candidate.
func NotChar(c rune) Type {
	return NotEnd(func(input []rune) Result {
		if input[0] == c {
			return Result{
				Result:    nil,
				Err:       ExpectedError{"not " + string(c)},
				Remaining: input,
			}
		}
		i := 0
		for ; i < len(input); i++ {
			if input[i] == c {
				return Result{
					Result:    string(input[:i]),
					Err:       nil,
					Remaining: input[i:],
				}
			}
		}
		return Result{
			Result:    string(input),
			Err:       nil,
			Remaining: nil,
		}
	})
}

// InSet parses any number of characters within a set of runes.
func InSet(set ...rune) Type {
	setMap := make(map[rune]struct{}, len(set))
	for _, r := range set {
		setMap[r] = struct{}{}
	}
	return NotEnd(func(input []rune) Result {
		i := 0
		for ; i < len(input); i++ {
			if _, exists := setMap[input[i]]; !exists {
				if i == 0 {
					return Result{
						Err:       ExpectedError{fmt.Sprintf("chars(%v)", string(set))},
						Remaining: input,
					}
				}
				break
			}
		}

		return Result{
			Result:    string(input[:i]),
			Err:       nil,
			Remaining: input[i:],
		}
	})
}

// SpacesAndTabs parses any number of space or tab characters.
func SpacesAndTabs() Type {
	inSet := InSet(' ', '\t')
	return func(input []rune) Result {
		res := inSet(input)
		if res.Err != nil {
			if _, ok := res.Err.(ExpectedError); ok {
				// Override potentially confused expected list.
				res.Err = ExpectedError{"whitespace"}
			}
		}
		return res
	}
}

// Match parses a single instance of a string.
func Match(str string) Type {
	return NotEnd(func(input []rune) Result {
		for i, c := range str {
			if len(input) <= i || input[i] != c {
				return Result{
					Result:    nil,
					Err:       ExpectedError{str},
					Remaining: input,
				}
			}
		}
		return Result{
			Result:    str,
			Err:       nil,
			Remaining: input[len(str):],
		}
	})
}

// Number parses any number of numerical characters into either an int64 or, if
// the number contains float characters, a float64.
func Number() Type {
	digitSet := InSet([]rune("0123456789.")...)
	minus := Char('-')
	return func(input []rune) Result {
		var negative bool
		res := minus(input)
		if res.Err == nil {
			negative = true
		}
		res = digitSet(res.Remaining)
		if res.Err != nil {
			if _, ok := res.Err.(ExpectedError); ok {
				// Override potentially confused expected list.
				res.Err = ExpectedError{"number"}
			}
			return res
		}
		resStr := res.Result.(string)
		if strings.Contains(resStr, ".") {
			f, err := strconv.ParseFloat(resStr, 64)
			if err != nil {
				return Result{
					Err:       fmt.Errorf("failed to parse '%v' as float: %v", resStr, err),
					Remaining: input,
				}
			}
			if negative {
				f = -f
			}
			res.Result = f
		} else {
			i, err := strconv.ParseInt(resStr, 10, 64)
			if err != nil {
				return Result{
					Err:       fmt.Errorf("failed to parse '%v' as integer: %v", resStr, err),
					Remaining: input,
				}
			}
			if negative {
				i = -i
			}
			res.Result = i
		}
		return res
	}
}

// Boolean parses either 'true' or 'false' into a boolean value.
func Boolean() Type {
	parser := AnyOf(Match("true"), Match("false"))
	return func(input []rune) Result {
		res := parser(input)
		if res.Err == nil {
			res.Result = res.Result.(string) == "true"
		} else if _, ok := res.Err.(ExpectedError); ok {
			// Override potentially confused expected list.
			res.Err = ExpectedError{"boolean"}
		}
		return res
	}
}

// QuotedString parses a single instance of a quoted string. The result is the
// inner contents unescaped.
func QuotedString() Type {
	return NotEnd(func(input []rune) Result {
		if input[0] != '"' {
			return Result{
				Result:    nil,
				Err:       ExpectedError{"quoted-string"},
				Remaining: input,
			}
		}
		i := 1
		escaped := false
		for ; i < len(input); i++ {
			if input[i] == '"' && !escaped {
				unquoted, err := strconv.Unquote(string(input[:i+1]))
				if err != nil {
					return Result{
						Err:       fmt.Errorf("failed to unescape quoted string contents: %v", err),
						Remaining: input,
					}
				}
				return Result{
					Result:    unquoted,
					Remaining: input[i+1:],
				}
			}
			if input[i] == '\\' {
				escaped = !escaped
			} else if escaped {
				escaped = false
			}
		}
		return Result{
			Err:       ExpectedError{"quoted-string"},
			Remaining: input,
		}
	})
}

// Newline parses a single character and expects it to match a line break.
func Newline() Type {
	nl := AnyOf(Match("\r\n"), Char('\n'))
	return func(input []rune) Result {
		res := nl(input)
		if res.Err != nil {
			if _, ok := res.Err.(ExpectedError); ok {
				// Override potentially confused expected list.
				res.Err = ExpectedError{"line break"}
			}
		}
		return res
	}
}

// AnyOf accepts one or more parsers and tries them in order against an input.
// If a parser returns an ExpectedErroror then the next parser is tried and so on.
// Otherwise, the result is returned.
func AnyOf(Types ...Type) Type {
	return func(input []rune) Result {
		var expectedStack ExpectedError
	tryParsers:
		for _, p := range Types {
			res := p(input)
			if res.Err != nil {
				switch t := res.Err.(type) {
				case ExpectedError:
					expectedStack = append(expectedStack, t...)
					continue tryParsers
				}
			}
			return res
		}
		seen := map[string]struct{}{}
		var dedupeStack ExpectedError
		for _, s := range expectedStack {
			if _, exists := seen[string(s)]; !exists {
				dedupeStack = append(dedupeStack, s)
				seen[string(s)] = struct{}{}
			}
		}
		return Result{
			Err:       dedupeStack,
			Remaining: input,
		}
	}
}

//------------------------------------------------------------------------------
