package parser

import (
	"fmt"
)

var dotEnvParser = func() Func[map[string]string] {
	assignmentParser := Sequence(
		NotInSet('=', ' ', '\n', '#'),
		Optional(SpacesAndTabs),
		charEquals,
		Optional(SpacesAndTabs),
		Optional(OneOf(TripleQuoteString, QuotedString, NotInSet('#', ' ', '\n'))),
		Optional(SpacesAndTabs),
	)

	envFileParser := Delimited(
		Expect(OneOf(
			assignmentParser,
			ZeroedFuncAs[string, []string](SpacesAndTabs),
			ZeroedFuncAs[any, []string](EmptyLine),
			ZeroedFuncAs[any, []string](EndOfInput),
			ZeroedFuncAs[[]any, []string](Sequence(
				FuncAsAny(charHash),
				FuncAsAny(Optional(UntilFail(NotChar('\n')))),
			)),
		), "Environment variable assignment"),
		NewlineAllowComment,
	)

	return func(input []rune) Result[map[string]string] {
		res := envFileParser(input)
		if res.Err != nil {
			return Fail[map[string]string](res.Err, input)
		}
		vars := map[string]string{}
		for _, line := range res.Payload.Primary {
			if len(line) != 6 {
				continue
			}
			vars[line[0]] = line[4]
		}
		return Success(vars, res.Remaining)
	}
}()

// ParseDotEnvFile attempts to parse a .env file containing environment variable
// assignments, and returns either a map of key/value assignments or an error.
func ParseDotEnvFile(envFileBytes []byte) (map[string]string, error) {
	input := string(envFileBytes)
	res := dotEnvParser([]rune(input))
	if res.Err != nil {
		line, _ := LineAndColOf([]rune(input), res.Err.Input)
		return nil, fmt.Errorf("%v: %v", line, res.Err.ErrorAtPositionStructured("", []rune(input)))
	}
	return res.Payload, nil
}
