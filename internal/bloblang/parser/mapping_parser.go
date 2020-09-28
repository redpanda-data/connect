package parser

import (
	"errors"
	"fmt"
	"io/ioutil"
	"path"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/gabs/v2"
)

// ParseMapping parses a bloblang mapping and returns an executor to run it, or
// an error if the parsing fails.
//
// The filepath is optional and used for relative file imports and error
// messages.
func ParseMapping(filepath string, expr string) (*mapping.Executor, *Error) {
	in := []rune(expr)
	dir := ""
	if len(filepath) > 0 {
		dir = path.Dir(filepath)
	}
	res := BestMatch(
		parseExecutor(dir),
		singleRootMapping(),
	)(in)
	if res.Err != nil {
		return nil, res.Err
	}
	return res.Payload.(*mapping.Executor), nil
}

//------------------------------------------------------------------------------'

func parseExecutor(baseDir string) Func {
	newline := NewlineAllowComment()
	whitespace := SpacesAndTabs()
	allWhitespace := DiscardAll(OneOf(whitespace, newline))

	return func(input []rune) Result {
		maps := map[string]query.Function{}
		statements := []mapping.Statement{}

		statement := OneOf(
			importParser(baseDir, maps),
			mapParser(maps),
			letStatementParser(),
			metaStatementParser(false),
			plainMappingStatementParser(),
		)

		res := allWhitespace(input)

		res = statement(res.Remaining)
		if res.Err != nil {
			res.Remaining = input
			return res
		}
		if mStmt, ok := res.Payload.(mapping.Statement); ok {
			statements = append(statements, mStmt)
		}

		for {
			res = Discard(whitespace)(res.Remaining)
			if len(res.Remaining) == 0 {
				break
			}

			if res = newline(res.Remaining); res.Err != nil {
				return Fail(res.Err, input)
			}

			res = allWhitespace(res.Remaining)
			if len(res.Remaining) == 0 {
				break
			}

			if res = statement(res.Remaining); res.Err != nil {
				return Fail(res.Err, input)
			}
			if mStmt, ok := res.Payload.(mapping.Statement); ok {
				statements = append(statements, mStmt)
			}
		}
		return Success(mapping.NewExecutor(input, maps, statements...), res.Remaining)
	}
}

func singleRootMapping() Func {
	whitespace := SpacesAndTabs()
	allWhitespace := DiscardAll(OneOf(whitespace, Newline()))

	return func(input []rune) Result {
		res := ParseQuery(input)
		if res.Err != nil {
			return res
		}

		fn := res.Payload.(query.Function)

		// Remove all tailing whitespace and ensure no remaining input.
		res = allWhitespace(res.Remaining)
		if len(res.Remaining) > 0 {
			return Fail(NewError(res.Remaining, "end of input"), input)
		}

		stmt := mapping.NewStatement(input, mapping.NewJSONAssignment(), fn)
		return Success(mapping.NewExecutor(input, map[string]query.Function{}, stmt), nil)
	}
}

//------------------------------------------------------------------------------

func varNameParser() Func {
	return JoinStringPayloads(
		UntilFail(
			OneOf(
				InRange('a', 'z'),
				InRange('A', 'Z'),
				InRange('0', '9'),
				Char('_'),
				Char('-'),
			),
		),
	)
}

func importParser(baseDir string, maps map[string]query.Function) Func {
	p := Sequence(
		Term("import"),
		SpacesAndTabs(),
		MustBe(
			Expect(
				QuotedString(),
				"filepath",
			),
		),
	)

	return func(input []rune) Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		filepath := res.Payload.([]interface{})[2].(string)
		filepath = path.Join(baseDir, filepath)
		contents, err := ioutil.ReadFile(filepath)
		if err != nil {
			return Fail(NewFatalError(input, fmt.Errorf("failed to read import: %w", err)), input)
		}

		importContent := []rune(string(contents))
		execRes := parseExecutor(path.Dir(filepath))(importContent)
		if execRes.Err != nil {
			return Fail(NewFatalError(input, NewImportError(filepath, importContent, execRes.Err)), input)
		}

		exec := execRes.Payload.(*mapping.Executor)
		if len(exec.Maps()) == 0 {
			err := fmt.Errorf("no maps to import from '%v'", filepath)
			return Fail(NewFatalError(input, err), input)
		}

		collisions := []string{}
		for k, v := range exec.Maps() {
			if _, exists := maps[k]; exists {
				collisions = append(collisions, k)
			} else {
				maps[k] = v
			}
		}
		if len(collisions) > 0 {
			err := fmt.Errorf("map name collisions from import '%v': %v", filepath, collisions)
			return Fail(NewFatalError(input, err), input)
		}

		return Success(filepath, res.Remaining)
	}
}

func mapParser(maps map[string]query.Function) Func {
	newline := NewlineAllowComment()
	whitespace := SpacesAndTabs()
	allWhitespace := DiscardAll(OneOf(whitespace, newline))

	p := Sequence(
		Term("map"),
		whitespace,
		// Prevents a missing path from being captured by the next parser
		MustBe(
			Expect(
				OneOf(
					QuotedString(),
					varNameParser(),
				),
				"map name",
			),
		),
		SpacesAndTabs(),
		DelimitedPattern(
			Sequence(
				Char('{'),
				allWhitespace,
			),
			OneOf(
				letStatementParser(),
				metaStatementParser(true), // Prevented for now due to .from(int)
				plainMappingStatementParser(),
			),
			Sequence(
				Discard(whitespace),
				newline,
				allWhitespace,
			),
			Sequence(
				allWhitespace,
				Char('}'),
			),
			true,
		),
	)

	return func(input []rune) Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]interface{})
		ident := seqSlice[2].(string)
		stmtSlice := seqSlice[4].([]interface{})

		if _, exists := maps[ident]; exists {
			return Fail(NewFatalError(input, fmt.Errorf("map name collision: %v", ident)), input)
		}

		statements := make([]mapping.Statement, len(stmtSlice))
		for i, v := range stmtSlice {
			statements[i] = v.(mapping.Statement)
		}

		maps[ident] = mapping.NewExecutor(input, maps, statements...)

		return Success(ident, res.Remaining)
	}
}

func letStatementParser() Func {
	p := Sequence(
		Expect(Term("let"), "assignment"),
		SpacesAndTabs(),
		// Prevents a missing path from being captured by the next parser
		MustBe(
			Expect(
				OneOf(
					QuotedString(),
					varNameParser(),
				),
				"variable name",
			),
		),
		SpacesAndTabs(),
		Char('='),
		SpacesAndTabs(),
		ParseQuery,
	)

	return func(input []rune) Result {
		res := p(input)
		if res.Err != nil {
			return res
		}
		resSlice := res.Payload.([]interface{})
		return Success(
			mapping.NewStatement(
				input,
				mapping.NewVarAssignment(resSlice[2].(string)),
				resSlice[6].(query.Function),
			),
			res.Remaining,
		)
	}
}

func nameLiteralParser() Func {
	return JoinStringPayloads(
		UntilFail(
			OneOf(
				InRange('a', 'z'),
				InRange('A', 'Z'),
				InRange('0', '9'),
				InRange('*', '+'),
				Char('.'),
				Char('_'),
				Char('-'),
				Char('~'),
			),
		),
	)
}

func metaStatementParser(disabled bool) Func {
	p := Sequence(
		Expect(Term("meta"), "assignment"),
		SpacesAndTabs(),
		Optional(OneOf(
			QuotedString(),
			nameLiteralParser(),
		)),
		Optional(SpacesAndTabs()),
		Char('='),
		SpacesAndTabs(),
		ParseQuery,
	)

	return func(input []rune) Result {
		res := p(input)
		if res.Err != nil {
			return res
		}
		if disabled {
			return Fail(
				NewFatalError(input, errors.New("setting meta fields from within a map is not allowed")),
				input,
			)
		}
		resSlice := res.Payload.([]interface{})

		var keyPtr *string
		if key, set := resSlice[2].(string); set {
			keyPtr = &key
		}

		return Success(
			mapping.NewStatement(
				input,
				mapping.NewMetaAssignment(keyPtr),
				resSlice[6].(query.Function),
			),
			res.Remaining,
		)
	}
}

func pathLiteralSegmentParser() Func {
	return JoinStringPayloads(
		UntilFail(
			OneOf(
				InRange('a', 'z'),
				InRange('A', 'Z'),
				InRange('0', '9'),
				InRange('*', '+'),
				Char('_'),
				Char('-'),
				Char('~'),
			),
		),
	)
}

func quotedPathLiteralSegmentParser() Func {
	pattern := QuotedString()

	return func(input []rune) Result {
		res := pattern(input)
		if res.Err != nil {
			return res
		}

		rawSegment, _ := res.Payload.(string)

		// Convert into a JSON pointer style path string.
		rawSegment = strings.Replace(rawSegment, "~", "~0", -1)
		rawSegment = strings.Replace(rawSegment, ".", "~1", -1)

		return Success(rawSegment, res.Remaining)
	}
}

func pathParser() Func {
	p := Sequence(
		Expect(pathLiteralSegmentParser(), "assignment"),
		Optional(
			Sequence(
				Char('.'),
				Delimited(
					Expect(
						OneOf(
							quotedPathLiteralSegmentParser(),
							pathLiteralSegmentParser(),
						),
						"target path",
					),
					Char('.'),
				),
			),
		),
	)

	return func(input []rune) Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		sequence := res.Payload.([]interface{})
		path := []string{sequence[0].(string)}

		if sequence[1] != nil {
			pathParts := sequence[1].([]interface{})[1].(DelimitedResult).Primary
			for _, p := range pathParts {
				path = append(path, gabs.DotPathToSlice(p.(string))...)
			}
		}

		return Success(path, res.Remaining)
	}
}

func plainMappingStatementParser() Func {
	p := Sequence(
		pathParser(),
		SpacesAndTabs(),
		Char('='),
		SpacesAndTabs(),
		ParseQuery,
	)

	return func(input []rune) Result {
		res := p(input)
		if res.Err != nil {
			return res
		}
		resSlice := res.Payload.([]interface{})
		path := resSlice[0].([]string)

		if len(path) > 0 && path[0] == "root" {
			path = path[1:]
		}

		return Success(
			mapping.NewStatement(
				input,
				mapping.NewJSONAssignment(path...),
				resSlice[4].(query.Function),
			),
			res.Remaining,
		)
	}
}

//------------------------------------------------------------------------------
