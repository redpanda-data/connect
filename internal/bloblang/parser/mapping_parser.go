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
		return resDirectImport.Payload, nil
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
	return res.Payload, nil
}

//------------------------------------------------------------------------------

func mappingStatement(pCtx Context, enableMeta bool, maps map[string]query.Function) Func[mapping.Statement] {
	toNilStatement := ZeroedFuncAs[string, mapping.Statement]

	var enabledStatements []Func[mapping.Statement]
	if maps != nil {
		enabledStatements = []Func[mapping.Statement]{
			toNilStatement(importParser(pCtx, maps)),
			toNilStatement(mapParser(pCtx, maps)),
		}
	}
	enabledStatements = append(enabledStatements,
		letStatementParser(pCtx),
		metaStatementParser(pCtx, enableMeta),
		plainMappingStatementParser(pCtx),
		rootLevelIfExpressionParser(pCtx),
	)

	return OneOf(enabledStatements...)
}

func parseExecutor(pCtx Context) Func[*mapping.Executor] {
	return func(input []rune) Result[*mapping.Executor] {
		maps := map[string]query.Function{}
		statements := []mapping.Statement{}

		statementPattern := mappingStatement(pCtx, true, maps)

		res := statementPattern(DiscardedWhitespaceNewlineComments(input).Remaining)
		if res.Err != nil {
			res.Remaining = input
			return ResultInto[*mapping.Executor](res)
		}
		if res.Payload != nil {
			statements = append(statements, res.Payload)
		}

		for {
			if res.Remaining = Discard(SpacesAndTabs)(res.Remaining).Remaining; len(res.Remaining) == 0 {
				break
			}

			if tmpRes := NewlineAllowComment(res.Remaining); tmpRes.Err != nil {
				return Fail[*mapping.Executor](tmpRes.Err, input)
			} else {
				res.Remaining = tmpRes.Remaining
			}

			if res.Remaining = DiscardedWhitespaceNewlineComments(res.Remaining).Remaining; len(res.Remaining) == 0 {
				break
			}

			if res = statementPattern(res.Remaining); res.Err != nil {
				return Fail[*mapping.Executor](res.Err, input)
			}
			if res.Payload != nil {
				statements = append(statements, res.Payload)
			}
		}
		return Success(mapping.NewExecutor("", input, maps, statements...), res.Remaining)
	}
}

func singleRootImport(pCtx Context) Func[*mapping.Executor] {
	parser := TakeOnly(3, Sequence(
		DiscardedWhitespaceNewlineComments,
		Term("from"),
		SpacesAndTabs,
		QuotedString,
		DiscardedWhitespaceNewlineComments,
	))

	return func(input []rune) Result[*mapping.Executor] {
		res := parser(input)
		if res.Err != nil {
			return Fail[*mapping.Executor](res.Err, input)
		}

		fpath := res.Payload
		contents, err := pCtx.importer.Import(fpath)
		if err != nil {
			return Fail[*mapping.Executor](NewFatalError(input, fmt.Errorf("failed to read import: %w", err)), input)
		}

		nextCtx := pCtx.WithImporterRelativeToFile(fpath)

		importContent := []rune(string(contents))
		execRes := parseExecutor(nextCtx)(importContent)
		if execRes.Err != nil {
			return Fail[*mapping.Executor](NewFatalError(input, NewImportError(fpath, importContent, execRes.Err)), input)
		}
		if len(res.Remaining) > 0 {
			return Fail[*mapping.Executor](NewFatalError(input, fmt.Errorf("unexpected content after single root import: %s", string(res.Remaining))), input)
		}
		return Success(execRes.Payload, res.Remaining)
	}
}

func singleRootMapping(pCtx Context) Func[*mapping.Executor] {
	return func(input []rune) Result[*mapping.Executor] {
		res := queryParser(pCtx)(DiscardedWhitespaceNewlineComments(input).Remaining)
		if res.Err != nil {
			return Fail[*mapping.Executor](res.Err, input)
		}

		fn := res.Payload
		assignmentRunes := input[:len(input)-len(res.Remaining)]

		// Remove all tailing whitespace and ensure no remaining input.
		testRes := DiscardedWhitespaceNewlineComments(res.Remaining)
		if len(testRes.Remaining) > 0 {
			assignmentRunes := DiscardedWhitespaceNewlineComments(assignmentRunes).Remaining

			var assignmentStr string
			if len(assignmentRunes) > 12 {
				assignmentStr = string(assignmentRunes[:12]) + "..."
			} else {
				assignmentStr = string(assignmentRunes)
			}

			expStr := fmt.Sprintf("the mapping to end here as the beginning is shorthand for `root = %v`, but this shorthand form cannot be followed with more assignments", assignmentStr)
			return Fail[*mapping.Executor](NewError(testRes.Remaining, expStr), input)
		}

		stmt := mapping.NewSingleStatement(input, mapping.NewJSONAssignment(), fn)
		return Success(mapping.NewExecutor("", input, map[string]query.Function{}, stmt), nil)
	}
}

//------------------------------------------------------------------------------

var varNameParser = JoinStringPayloads(
	UntilFail(
		OneOf(
			InRange('a', 'z'),
			InRange('A', 'Z'),
			InRange('0', '9'),
			charUnderscore,
		),
	),
)

var importParserComb = TakeOnly(2, Sequence(
	Term("import"),
	SpacesAndTabs,
	MustBe(
		Expect(
			QuotedString,
			"filepath",
		),
	),
))

func importParser(pCtx Context, maps map[string]query.Function) Func[string] {
	return func(input []rune) Result[string] {
		res := importParserComb(input)
		if res.Err != nil {
			return res
		}

		if maps == nil {
			return Fail[string](
				NewFatalError(input, errors.New("importing mappings is not allowed within this block")),
				input,
			)
		}

		fpath := res.Payload
		contents, err := pCtx.importer.Import(fpath)
		if err != nil {
			return Fail[string](NewFatalError(input, fmt.Errorf("failed to read import: %w", err)), input)
		}

		nextCtx := pCtx.WithImporterRelativeToFile(fpath)

		importContent := []rune(string(contents))
		execRes := parseExecutor(nextCtx)(importContent)
		if execRes.Err != nil {
			return Fail[string](NewFatalError(input, NewImportError(fpath, importContent, execRes.Err)), input)
		}

		exec := execRes.Payload
		if len(exec.Maps()) == 0 {
			err := fmt.Errorf("no maps to import from '%v'", fpath)
			return Fail[string](NewFatalError(input, err), input)
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
			return Fail[string](NewFatalError(input, err), input)
		}

		return Success(fpath, res.Remaining)
	}
}

func mapParser(pCtx Context, maps map[string]query.Function) Func[string] {
	p := Sequence(
		FuncAsAny(Term("map")),
		FuncAsAny(SpacesAndTabs),
		// Prevents a missing path from being captured by the next parser
		FuncAsAny(MustBe(
			Expect(
				OneOf(
					QuotedString,
					varNameParser,
				),
				"map name",
			),
		)),
		FuncAsAny(SpacesAndTabs),
		FuncAsAny(DelimitedPattern(
			Sequence(
				charSquigOpen,
				DiscardedWhitespaceNewlineComments,
			),
			// Prevent imports, maps and metadata assignments.
			mappingStatement(pCtx, false, nil),
			Sequence(
				Discard(SpacesAndTabs),
				NewlineAllowComment,
				DiscardedWhitespaceNewlineComments,
			),
			Sequence(
				DiscardedWhitespaceNewlineComments,
				charSquigClose,
			),
		)),
	)

	return func(input []rune) Result[string] {
		res := p(input)
		if res.Err != nil {
			return Fail[string](res.Err, input)
		}

		if maps == nil {
			return Fail[string](
				NewFatalError(input, errors.New("defining maps is not allowed within this block")),
				input,
			)
		}

		seqSlice := res.Payload
		ident := seqSlice[2].(string)
		stmtSlice := seqSlice[4].([]mapping.Statement)

		if _, exists := maps[ident]; exists {
			return Fail[string](NewFatalError(input, fmt.Errorf("map name collision: %v", ident)), input)
		}

		maps[ident] = mapping.NewExecutor("map "+ident, input, maps, stmtSlice...)
		return Success(ident, res.Remaining)
	}
}

func letStatementParser(pCtx Context) Func[mapping.Statement] {
	p := Sequence(
		FuncAsAny(Expect(Term("let"), "assignment")),
		FuncAsAny(SpacesAndTabs),
		// Prevents a missing path from being captured by the next parser
		FuncAsAny(MustBe(
			Expect(
				OneOf(
					QuotedString,
					varNameParser,
				),
				"variable name",
			),
		)),
		FuncAsAny(SpacesAndTabs),
		FuncAsAny(charEquals),
		FuncAsAny(SpacesAndTabs),
		FuncAsAny(queryParser(pCtx)),
	)

	return func(input []rune) Result[mapping.Statement] {
		res := p(input)
		if res.Err != nil {
			return Fail[mapping.Statement](res.Err, input)
		}
		return Success[mapping.Statement](mapping.NewSingleStatement(
			input,
			mapping.NewVarAssignment(res.Payload[2].(string)),
			res.Payload[6].(query.Function),
		), res.Remaining)
	}
}

var nameLiteralParser = JoinStringPayloads(
	UntilFail(
		OneOf(
			InRange('a', 'z'),
			InRange('A', 'Z'),
			InRange('0', '9'),
			charUnderscore,
		),
	),
)

func metaStatementParser(pCtx Context, enabled bool) Func[mapping.Statement] {
	p := Sequence(
		FuncAsAny(Expect(Term("meta"), "assignment")),
		FuncAsAny(SpacesAndTabs),
		FuncAsAny(OptionalPtr(OneOf(
			QuotedString,
			nameLiteralParser,
		))),
		// TODO: Break out root assignment so we can make this mandatory
		FuncAsAny(Optional(SpacesAndTabs)),
		FuncAsAny(charEquals),
		FuncAsAny(SpacesAndTabs),
		FuncAsAny(queryParser(pCtx)),
	)

	return func(input []rune) Result[mapping.Statement] {
		res := p(input)
		if res.Err != nil {
			return Fail[mapping.Statement](res.Err, input)
		}
		if !enabled {
			return Fail[mapping.Statement](
				NewFatalError(input, errors.New("setting meta fields is not allowed within this block")),
				input,
			)
		}
		resSlice := res.Payload

		return Success[mapping.Statement](mapping.NewSingleStatement(
			input,
			mapping.NewMetaAssignment(resSlice[2].(*string)),
			resSlice[6].(query.Function),
		), res.Remaining)
	}
}

var pathLiteralSegmentParser = JoinStringPayloads(
	UntilFail(
		OneOf(
			InRange('a', 'z'),
			InRange('A', 'Z'),
			InRange('0', '9'),
			charUnderscore,
		),
	),
)

func quotedPathLiteralSegmentParser(input []rune) Result[string] {
	res := QuotedString(input)
	if res.Err != nil {
		return res
	}

	// Convert into a JSON pointer style path string.
	rawSegment := strings.ReplaceAll(res.Payload, "~", "~0")
	rawSegment = strings.ReplaceAll(rawSegment, ".", "~1")

	return Success(rawSegment, res.Remaining)
}

var pathParserPattern = Sequence(
	FuncAsAny(Expect(pathLiteralSegmentParser, "assignment")),
	FuncAsAny(Optional(
		TakeOnly(1, Sequence(
			ZeroedFuncAs[string, DelimitedResult[string, string]](charDot),
			Delimited(
				Expect(
					OneOf(
						quotedPathLiteralSegmentParser,
						pathLiteralSegmentParser,
					),
					"target path",
				),
				charDot,
			),
		)),
	)),
)

func pathParser(input []rune) Result[[]string] {
	res := pathParserPattern(input)
	if res.Err != nil {
		return Fail[[]string](res.Err, input)
	}

	sequence := res.Payload
	path := []string{sequence[0].(string)}

	if sequence[1] != nil {
		pathParts := sequence[1].(DelimitedResult[string, string]).Primary
		for _, p := range pathParts {
			path = append(path, gabs.DotPathToSlice(p)...)
		}
	}

	return Success(path, res.Remaining)
}

func plainMappingStatementParser(pCtx Context) Func[mapping.Statement] {
	p := Sequence(
		FuncAsAny(pathParser),
		FuncAsAny(SpacesAndTabs),
		FuncAsAny(charEquals),
		FuncAsAny(SpacesAndTabs),
		FuncAsAny(queryParser(pCtx)),
	)

	return func(input []rune) Result[mapping.Statement] {
		res := p(input)
		if res.Err != nil {
			return Fail[mapping.Statement](res.Err, input)
		}

		resSlice := res.Payload
		path := resSlice[0].([]string)

		if len(path) > 0 && path[0] == "root" {
			path = path[1:]
		}

		return Success[mapping.Statement](mapping.NewSingleStatement(
			input,
			mapping.NewJSONAssignment(path...),
			resSlice[4].(query.Function),
		), res.Remaining)
	}
}
