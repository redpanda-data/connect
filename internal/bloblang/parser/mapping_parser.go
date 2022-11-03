package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Jeffail/gabs/v2"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
)

// ParseMapping parses a bloblang mapping and returns an executor to run it, or
// an error if the parsing fails.
//
// The filepath is optional and used for relative file imports and error
// messages.
func ParseMapping(pCtx Context, expr string) (*mapping.Executor, *Error) {
	in := []rune(expr)

	resDirectImport := singleRootImport(pCtx)(in)
	if resDirectImport.Err != nil && resDirectImport.Err.IsFatal() {
		return nil, resDirectImport.Err
	}
	if resDirectImport.Err == nil && len(resDirectImport.Remaining) == 0 {
		return resDirectImport.Payload.(*mapping.Executor), nil
	}

	resExe := parseExecutor(pCtx)(in)
	if resExe.Err != nil && resExe.Err.IsFatal() {
		return nil, resExe.Err
	}
	resSingle := singleRootMapping(pCtx)(in)

	res := bestMatch(resExe, resSingle)
	if res.Err != nil {
		return nil, res.Err
	}
	return res.Payload.(*mapping.Executor), nil
}

//------------------------------------------------------------------------------'

func parseExecutor(pCtx Context) Func {
	newline := NewlineAllowComment()
	whitespace := SpacesAndTabs()
	allWhitespace := DiscardAll(OneOf(whitespace, newline))

	return func(input []rune) Result {
		maps := map[string]query.Function{}
		statements := []mapping.Statement{}

		statement := OneOf(
			importParser(maps, pCtx),
			mapParser(maps, pCtx),
			letStatementParser(pCtx),
			metaStatementParser(false, pCtx),
			plainMappingStatementParser(pCtx),
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
		return Success(mapping.NewExecutor("", input, maps, statements...), res.Remaining)
	}
}

func singleRootImport(pCtx Context) Func {
	whitespace := SpacesAndTabs()
	allWhitespace := DiscardAll(OneOf(whitespace, Newline()))

	parser := Sequence(
		allWhitespace,
		Term("from"),
		whitespace,
		QuotedString(),
		allWhitespace,
	)

	return func(input []rune) Result {
		res := parser(input)
		if res.Err != nil {
			return res
		}

		fpath := res.Payload.([]any)[3].(string)
		contents, err := pCtx.importer.Import(fpath)
		if err != nil {
			return Fail(NewFatalError(input, fmt.Errorf("failed to read import: %w", err)), input)
		}

		nextCtx := pCtx.WithImporterRelativeToFile(fpath)

		importContent := []rune(string(contents))
		execRes := parseExecutor(nextCtx)(importContent)
		if execRes.Err != nil {
			return Fail(NewFatalError(input, NewImportError(fpath, importContent, execRes.Err)), input)
		}
		return Success(execRes.Payload.(*mapping.Executor), res.Remaining)
	}
}

func singleRootMapping(pCtx Context) Func {
	whitespace := SpacesAndTabs()
	allWhitespace := DiscardAll(OneOf(whitespace, Newline()))

	return func(input []rune) Result {
		res := queryParser(pCtx)(input)
		if res.Err != nil {
			return res
		}

		fn := res.Payload.(query.Function)
		assignmentRunes := input[:len(input)-len(res.Remaining)]

		// Remove all tailing whitespace and ensure no remaining input.
		res = allWhitespace(res.Remaining)
		if len(res.Remaining) > 0 {
			tmpRes := allWhitespace(assignmentRunes)
			assignmentRunes = tmpRes.Remaining

			var assignmentStr string
			if len(assignmentRunes) > 12 {
				assignmentStr = string(assignmentRunes[:12]) + "..."
			} else {
				assignmentStr = string(assignmentRunes)
			}

			expStr := fmt.Sprintf("the mapping to end here as the beginning is shorthand for `root = %v`, but this shorthand form cannot be followed with more assignments", assignmentStr)
			return Fail(NewError(res.Remaining, expStr), input)
		}

		stmt := mapping.NewStatement(input, mapping.NewJSONAssignment(), fn)
		return Success(mapping.NewExecutor("", input, map[string]query.Function{}, stmt), nil)
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
			),
		),
	)
}

func importParser(maps map[string]query.Function, pCtx Context) Func {
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

		fpath := res.Payload.([]any)[2].(string)
		contents, err := pCtx.importer.Import(fpath)
		if err != nil {
			return Fail(NewFatalError(input, fmt.Errorf("failed to read import: %w", err)), input)
		}

		nextCtx := pCtx.WithImporterRelativeToFile(fpath)

		importContent := []rune(string(contents))
		execRes := parseExecutor(nextCtx)(importContent)
		if execRes.Err != nil {
			return Fail(NewFatalError(input, NewImportError(fpath, importContent, execRes.Err)), input)
		}

		exec := execRes.Payload.(*mapping.Executor)
		if len(exec.Maps()) == 0 {
			err := fmt.Errorf("no maps to import from '%v'", fpath)
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
			err := fmt.Errorf("map name collisions from import '%v': %v", fpath, collisions)
			return Fail(NewFatalError(input, err), input)
		}

		return Success(fpath, res.Remaining)
	}
}

func mapParser(maps map[string]query.Function, pCtx Context) Func {
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
				letStatementParser(pCtx),
				metaStatementParser(true, pCtx), // Prevented for now due to .from(int)
				plainMappingStatementParser(pCtx),
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

		seqSlice := res.Payload.([]any)
		ident := seqSlice[2].(string)
		stmtSlice := seqSlice[4].([]any)

		if _, exists := maps[ident]; exists {
			return Fail(NewFatalError(input, fmt.Errorf("map name collision: %v", ident)), input)
		}

		statements := make([]mapping.Statement, len(stmtSlice))
		for i, v := range stmtSlice {
			statements[i] = v.(mapping.Statement)
		}

		maps[ident] = mapping.NewExecutor("map "+ident, input, maps, statements...)

		return Success(ident, res.Remaining)
	}
}

func letStatementParser(pCtx Context) Func {
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
		queryParser(pCtx),
	)

	return func(input []rune) Result {
		res := p(input)
		if res.Err != nil {
			return res
		}
		resSlice := res.Payload.([]any)
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
				Char('_'),
			),
		),
	)
}

func metaStatementParser(disabled bool, pCtx Context) Func {
	p := Sequence(
		Expect(Term("meta"), "assignment"),
		SpacesAndTabs(),
		Optional(OneOf(
			QuotedString(),
			nameLiteralParser(),
		)),
		// TODO: Break out root assignment so we can make this mandatory
		Optional(SpacesAndTabs()),
		Char('='),
		SpacesAndTabs(),
		queryParser(pCtx),
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
		resSlice := res.Payload.([]any)

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
				Char('_'),
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
		rawSegment = strings.ReplaceAll(rawSegment, "~", "~0")
		rawSegment = strings.ReplaceAll(rawSegment, ".", "~1")

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

		sequence := res.Payload.([]any)
		path := []string{sequence[0].(string)}

		if sequence[1] != nil {
			pathParts := sequence[1].([]any)[1].(DelimitedResult).Primary
			for _, p := range pathParts {
				path = append(path, gabs.DotPathToSlice(p.(string))...)
			}
		}

		return Success(path, res.Remaining)
	}
}

func plainMappingStatementParser(pCtx Context) Func {
	p := Sequence(
		pathParser(),
		SpacesAndTabs(),
		Char('='),
		SpacesAndTabs(),
		queryParser(pCtx),
	)

	return func(input []rune) Result {
		res := p(input)
		if res.Err != nil {
			return res
		}
		resSlice := res.Payload.([]any)
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
