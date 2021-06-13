package parser

func dotEnvParser() Func {
	assignmentParser := Sequence(
		NotInSet('=', ' ', '\n', '#'),
		Char('='),
		Optional(OneOf(QuotedString(), NotInSet('#', ' ', '\n'))),
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
			sequence, _ := line.([]interface{})
			if len(sequence) == 4 {
				key := sequence[0].(string)
				value, _ := sequence[2].(string)
				vars[key] = value
			}
		}
		res.Payload = vars
		return res
	}
}

// ParseDotEnv attempts to parse a .env file containing environment variable
// assignments, and returns either a map of key/value assignments or an error.
func ParseDotEnv(input string) (map[string]string, error) {
	res := dotEnvParser()([]rune(input))
	if res.Err != nil {
		return nil, res.Err
	}
	return res.Payload.(map[string]string), nil
}
