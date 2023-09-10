package parser

import (
	"fmt"
)

func dotEnvParser() Func {
	assignmentParser := Sequence(
		NotInSet('=', ' ', '\n', '#'),
		Optional(SpacesAndTabs()),
		Char('='),
		Optional(SpacesAndTabs()),
		Optional(OneOf(TripleQuoteString(), QuotedString(), NotInSet('#', ' ', '\n'))),
		Optional(SpacesAndTabs()),
	)

	envFileParser := Delimited(
		Expect(OneOf(
			assignmentParser,
			SpacesAndTabs(),
			EmptyLine(),
			EndOfInput(),
			Sequence(
				Char('#'),
				Optional(UntilFail(NotChar('\n'))),
			),
		), "Environment variable assignment"),
		NewlineAllowComment(),
	)

	return func(input []rune) Result {
		res := envFileParser(input)
		if res.Err != nil {
			return res
		}
		vars := map[string]string{}
		for _, line := range res.Payload.(DelimitedResult).Primary {
			sequence, _ := line.([]any)
			if len(sequence) == 6 {
				key := sequence[0].(string)
				value, _ := sequence[4].(string)
				vars[key] = value
			}
		}
		res.Payload = vars
		return res
	}
}

// ParseDotEnvFile attempts to parse a .env file containing environment variable
// assignments, and returns either a map of key/value assignments or an error.
func ParseDotEnvFile(envFileBytes []byte) (map[string]string, error) {
	input := string(envFileBytes)
	res := dotEnvParser()([]rune(input))
	if res.Err != nil {
		line, _ := LineAndColOf([]rune(input), res.Err.Input)
		return nil, fmt.Errorf("%v: %v", line, res.Err.ErrorAtPositionStructured("", []rune(input)))
	}
	return res.Payload.(map[string]string), nil
}
