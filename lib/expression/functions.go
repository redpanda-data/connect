package expression

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

func parseFunction(expr []rune) parserResult {
	res := newFunction(expr)
	if res.Err != nil {
		resDeprecated := deprecatedFunction(expr)
		if resDeprecated.Err == nil {
			return resDeprecated
		}
	}
	return res
}

//------------------------------------------------------------------------------

func functionArgs(input []rune) parserResult {
	var argString string

	res := char('(')(input)
	if res.Err != nil {
		return res
	}
	res = notChar(')')(res.Remaining)
	if res.Err == nil {
		argString = res.Result.(string)
	}
	res = char(')')(res.Remaining)
	if res.Err != nil {
		return res
	}
	return parserResult{
		Result:    strings.Split(argString, ","),
		Err:       nil,
		Remaining: res.Remaining,
	}
}

//------------------------------------------------------------------------------

type badFunctionErr string

func (e badFunctionErr) Error() string {
	exp := []string{}
	for k := range newFunctions {
		exp = append(exp, k)
	}
	sort.Strings(exp)
	return fmt.Sprintf("unrecognised function '%v', expected one of: %v", string(e), exp)
}

func newFunction(input []rune) parserResult {
	var targetFunc string
	var args []string

	res := notChar('(')(input)
	if res.Err != nil {
		return res
	}
	targetFunc = res.Result.(string)
	if len(res.Remaining) == 0 {
		var err error
		if _, exists := newFunctions[targetFunc]; exists {
			err = fmt.Errorf("expected params '()' after function: '%v'", targetFunc)
		} else {
			err = badFunctionErr(targetFunc)
		}
		return parserResult{
			Err:       err,
			Remaining: res.Remaining,
		}
	}
	res = functionArgs(res.Remaining)
	if res.Err != nil {
		res.Err = fmt.Errorf("failed to parse function arguments: %v", res.Err)
		return res
	}
	if len(res.Remaining) > 0 {
		return parserResult{
			Err:       fmt.Errorf("unexpected expression after function: '%v'", string(res.Remaining)),
			Remaining: res.Remaining,
		}
	}
	args = res.Result.([]string)

	ftor, exists := newFunctions[targetFunc]
	if !exists {
		return parserResult{
			Err:       badFunctionErr(targetFunc),
			Remaining: input,
		}
	}

	for i := range args {
		args[i] = strings.TrimSpace(args[i])
	}

	fnResolver, err := ftor(args...)
	if err != nil {
		return parserResult{
			Err:       err,
			Remaining: input,
		}
	}
	return parserResult{
		Result:    dynamicResolver(fnResolver),
		Err:       nil,
		Remaining: nil,
	}
}

//------------------------------------------------------------------------------

func makeJSONFunction(argIndex *int, argPath string) dynamicResolverFunc {
	return func(index int, msg Message, escaped, _ bool) []byte {
		part := index
		if argIndex != nil {
			part = *argIndex
		}
		jPart, err := msg.Get(part).JSON()
		if err != nil {
			return []byte("null")
		}
		gPart := gabs.Wrap(jPart)
		if len(argPath) > 0 {
			gPart = gPart.Path(argPath)
		}
		switch t := gPart.Data().(type) {
		case string:
			return []byte(t)
		case nil:
			return []byte(`null`)
		}
		return gPart.Bytes()
	}
}

func jsonFunction(args ...string) (dynamicResolverFunc, error) {
	var argPath string
	if len(args) > 1 {
		return nil, fmt.Errorf("expected one or zero arguments, received: %v", args)
	}
	if len(args) == 1 {
		argPath = args[0]
	}
	return makeJSONFunction(nil, argPath), nil
}

func jsonFromFunction(args ...string) (dynamicResolverFunc, error) {
	var argIndex *int
	var argPath string
	if len(args) != 2 {
		return nil, fmt.Errorf("expected two arguments, received: %v", args)
	}

	i64, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse index arg '%v': %v", args[1], err)
	}
	i := int(i64)
	argIndex = &i
	argPath = args[1]
	return makeJSONFunction(argIndex, argPath), nil
}

var newFunctions = map[string]func(args ...string) (dynamicResolverFunc, error){
	"json":      jsonFunction,
	"json_from": jsonFromFunction,
}

//------------------------------------------------------------------------------
