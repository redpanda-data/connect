package mapping

import (
	"errors"
	"fmt"
	"io/ioutil"
	"path"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
	"golang.org/x/xerrors"
)

//------------------------------------------------------------------------------

// Message is an interface type to be given to a query function, it allows the
// function to resolve fields and metadata from a message.
type Message interface {
	Get(p int) types.Part
	Len() int
}

//------------------------------------------------------------------------------

type mappingStatement struct {
	input      []rune
	assignment Assignment
	query      query.Function
}

// Executor is a parsed bloblang mapping that can be executed on a Benthos
// message.
type Executor struct {
	input      []rune
	maps       map[string]query.Function
	statements []mappingStatement
}

// MapPart executes the bloblang mapping on a particular message index of a
// batch. The message is parsed as a JSON document in order to provide the
// mapping context. Returns an error if any stage of the mapping fails to
// execute.
//
// A resulting mapped message part is returned, unless the mapping results in a
// query.Delete value, in which case nil is returned and the part should be
// discarded.
func (e *Executor) MapPart(index int, msg Message) (types.Part, error) {
	vars := map[string]interface{}{}

	part := msg.Get(index).Copy()
	meta := part.Metadata()

	var valuePtr *interface{}
	if jObj, err := part.JSON(); err == nil {
		valuePtr = &jObj
	}

	var newObj interface{} = query.Nothing(nil)
	for _, stmt := range e.statements {
		res, err := stmt.query.Exec(query.FunctionContext{
			Maps:     e.maps,
			Value:    valuePtr,
			Vars:     vars,
			Index:    index,
			MsgBatch: msg,
		})
		if err != nil {
			var line int
			if len(e.input) > 0 && len(stmt.input) > 0 {
				line, _ = parser.LineAndColOf(e.input, stmt.input)
			}
			return nil, xerrors.Errorf("failed to execute mapping query at line %v: %v", line, err)
		}
		if _, isNothing := res.(query.Nothing); isNothing {
			// Skip assignment entirely
			continue
		}
		if err = stmt.assignment.Apply(res, AssignmentContext{
			Maps:  e.maps,
			Vars:  vars,
			Meta:  meta,
			Value: &newObj,
		}); err != nil {
			var line int
			if len(e.input) > 0 && len(stmt.input) > 0 {
				line, _ = parser.LineAndColOf(e.input, stmt.input)
			}
			return nil, xerrors.Errorf("failed to assign query result at line %v: %v", line, err)
		}
	}

	switch newObj.(type) {
	case query.Delete:
		// Return nil (filter the message part)
		return nil, nil
	case query.Nothing:
		// Do not change the original contents
	default:
		switch t := newObj.(type) {
		case string:
			part.Set([]byte(t))
		case []byte:
			part.Set(t)
		default:
			if err := part.SetJSON(newObj); err != nil {
				return nil, xerrors.Errorf("failed to set result of mapping: %w", err)
			}
		}
	}
	return part, nil
}

// Exec this function with a context struct.
func (e *Executor) Exec(ctx query.FunctionContext) (interface{}, error) {
	var newObj interface{} = query.Nothing(nil)
	for _, stmt := range e.statements {
		res, err := stmt.query.Exec(ctx)
		if err != nil {
			var line int
			if len(e.input) > 0 && len(stmt.input) > 0 {
				line, _ = parser.LineAndColOf(e.input, stmt.input)
			}
			return nil, xerrors.Errorf("failed to execute mapping assignment at line %v: %v", line, err)
		}
		if _, isNothing := res.(query.Nothing); isNothing {
			// Skip assignment entirely
			continue
		}
		if err = stmt.assignment.Apply(res, AssignmentContext{
			Maps: e.maps,
			Vars: ctx.Vars,
			// Meta: meta, Prevented for now due to .from(int)
			Value: &newObj,
		}); err != nil {
			var line int
			if len(e.input) > 0 && len(stmt.input) > 0 {
				line, _ = parser.LineAndColOf(e.input, stmt.input)
			}
			return nil, xerrors.Errorf("failed to assign mapping result at line %v: %v", line, err)
		}
	}

	return newObj, nil
}

// ToBytes executes this function for a message of a batch and returns the
// result marshalled into a byte slice.
func (e *Executor) ToBytes(ctx query.FunctionContext) []byte {
	v, err := e.Exec(ctx)
	if err != nil {
		if rec, ok := err.(*query.ErrRecoverable); ok {
			return query.IToBytes(rec.Recovered)
		}
		return nil
	}
	return query.IToBytes(v)
}

// ToString executes this function for a message of a batch and returns the
// result marshalled into a string.
func (e *Executor) ToString(ctx query.FunctionContext) string {
	v, err := e.Exec(ctx)
	if err != nil {
		if rec, ok := err.(*query.ErrRecoverable); ok {
			return query.IToString(rec.Recovered)
		}
		return ""
	}
	return query.IToString(v)
}

// NewExecutor parses a bloblang mapping and returns an executor to run it, or
// an error if the parsing fails.
//
// The filepath is optional and used for relative file imports and error
// messages.
func NewExecutor(filepath string, mapping string) (*Executor, *parser.Error) {
	in := []rune(mapping)
	dir := ""
	if len(filepath) > 0 {
		dir = path.Dir(filepath)
	}
	res := parser.BestMatch(
		parseExecutor(dir),
		singleRootMapping(),
	)(in)
	if res.Err != nil {
		return nil, res.Err
	}
	return res.Payload.(*Executor), nil
}

//------------------------------------------------------------------------------'

func parseExecutor(baseDir string) parser.Type {
	newline := parser.NewlineAllowComment()
	whitespace := parser.SpacesAndTabs()
	allWhitespace := parser.DiscardAll(parser.OneOf(whitespace, newline))

	return func(input []rune) parser.Result {
		maps := map[string]query.Function{}
		statements := []mappingStatement{}

		statement := parser.OneOf(
			importParser(baseDir, maps),
			mapParser(maps),
			letStatementParser(),
			metaStatementParser(false),
			plainMappingStatementParser(),
		)

		res := allWhitespace(input)

		statementInput := res.Remaining
		res = statement(res.Remaining)
		if res.Err != nil {
			res.Remaining = input
			return res
		}
		if mStmt, ok := res.Payload.(mappingStatement); ok {
			mStmt.input = statementInput
			statements = append(statements, mStmt)
		}

		for {
			res = parser.Discard(whitespace)(res.Remaining)
			if len(res.Remaining) == 0 {
				break
			}

			if res = newline(res.Remaining); res.Err != nil {
				return parser.Result{
					Err:       res.Err,
					Remaining: input,
				}
			}

			res = allWhitespace(res.Remaining)
			if len(res.Remaining) == 0 {
				break
			}

			statementInput = res.Remaining
			if res = statement(res.Remaining); res.Err != nil {
				return parser.Result{
					Err:       res.Err,
					Remaining: input,
				}
			}
			if mStmt, ok := res.Payload.(mappingStatement); ok {
				mStmt.input = statementInput
				statements = append(statements, mStmt)
			}
		}

		return parser.Result{
			Remaining: res.Remaining,
			Payload: &Executor{
				input, maps, statements,
			},
		}
	}
}

func singleRootMapping() parser.Type {
	whitespace := parser.SpacesAndTabs()
	allWhitespace := parser.DiscardAll(parser.OneOf(whitespace, parser.Newline()))

	return func(input []rune) parser.Result {
		res := query.Parse(input)
		if res.Err != nil {
			return res
		}

		fn := res.Payload.(query.Function)

		// Remove all tailing whitespace and ensure no remaining input.
		res = allWhitespace(res.Remaining)
		if len(res.Remaining) > 0 {
			return parser.Result{
				Remaining: input,
				Err:       parser.NewError(res.Remaining, "end of input"),
			}
		}

		stmt := mappingStatement{
			input: input,
			assignment: &jsonAssignment{
				Path: []string{},
			},
			query: fn,
		}

		return parser.Result{
			Remaining: nil,
			Payload: &Executor{
				maps:       map[string]query.Function{},
				statements: []mappingStatement{stmt},
			},
		}
	}
}

//------------------------------------------------------------------------------

func varNameParser() parser.Type {
	return parser.JoinStringPayloads(
		parser.UntilFail(
			parser.OneOf(
				parser.InRange('a', 'z'),
				parser.InRange('A', 'Z'),
				parser.InRange('0', '9'),
				parser.Char('_'),
				parser.Char('-'),
			),
		),
	)
}

func importParser(baseDir string, maps map[string]query.Function) parser.Type {
	p := parser.Sequence(
		parser.Term("import"),
		parser.SpacesAndTabs(),
		parser.MustBe(
			parser.Expect(
				parser.QuotedString(),
				"filepath",
			),
		),
	)

	return func(input []rune) parser.Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		filepath := res.Payload.([]interface{})[2].(string)
		filepath = path.Join(baseDir, filepath)
		contents, err := ioutil.ReadFile(filepath)
		if err != nil {
			return parser.Result{
				Err:       parser.NewFatalError(input, fmt.Errorf("failed to read import: %w", err)),
				Remaining: input,
			}
		}

		importContent := []rune(string(contents))
		execRes := parseExecutor(path.Dir(filepath))(importContent)
		if execRes.Err != nil {
			return parser.Result{
				Err:       parser.NewFatalError(input, parser.NewImportError(filepath, importContent, execRes.Err)),
				Remaining: input,
			}
		}

		exec := execRes.Payload.(*Executor)
		if len(exec.maps) == 0 {
			return parser.Result{
				Err:       parser.NewFatalError(input, fmt.Errorf("no maps to import from '%v'", filepath)),
				Remaining: input,
			}
		}

		collisions := []string{}
		for k, v := range exec.maps {
			if _, exists := maps[k]; exists {
				collisions = append(collisions, k)
			} else {
				maps[k] = v
			}
		}
		if len(collisions) > 0 {
			return parser.Result{
				Err:       parser.NewFatalError(input, fmt.Errorf("map name collisions from import '%v': %v", filepath, collisions)),
				Remaining: input,
			}
		}

		return parser.Result{
			Payload:   filepath,
			Remaining: res.Remaining,
		}
	}
}

func mapParser(maps map[string]query.Function) parser.Type {
	newline := parser.NewlineAllowComment()
	whitespace := parser.SpacesAndTabs()
	allWhitespace := parser.DiscardAll(parser.OneOf(whitespace, newline))

	p := parser.Sequence(
		parser.Term("map"),
		whitespace,
		// Prevents a missing path from being captured by the next parser
		parser.MustBe(
			parser.Expect(
				parser.OneOf(
					parser.QuotedString(),
					varNameParser(),
				),
				"map name",
			),
		),
		parser.SpacesAndTabs(),
		parser.DelimitedPattern(
			parser.Sequence(
				parser.Char('{'),
				allWhitespace,
			),
			parser.OneOf(
				letStatementParser(),
				metaStatementParser(true), // Prevented for now due to .from(int)
				plainMappingStatementParser(),
			),
			parser.Sequence(
				parser.Discard(whitespace),
				newline,
				allWhitespace,
			),
			parser.Sequence(
				allWhitespace,
				parser.Char('}'),
			),
			true, false,
		),
	)

	return func(input []rune) parser.Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]interface{})
		ident := seqSlice[2].(string)
		stmtSlice := seqSlice[4].([]interface{})

		if _, exists := maps[ident]; exists {
			return parser.Result{
				Err:       parser.NewFatalError(input, fmt.Errorf("map name collision: %v", ident)),
				Remaining: input,
			}
		}

		statements := make([]mappingStatement, len(stmtSlice))
		for i, v := range stmtSlice {
			statements[i] = v.(mappingStatement)
		}

		maps[ident] = &Executor{input, maps, statements}

		return parser.Result{
			Payload:   ident,
			Remaining: res.Remaining,
		}
	}
}

func letStatementParser() parser.Type {
	p := parser.Sequence(
		parser.Expect(parser.Term("let"), "assignment"),
		parser.SpacesAndTabs(),
		// Prevents a missing path from being captured by the next parser
		parser.MustBe(
			parser.Expect(
				parser.OneOf(
					parser.QuotedString(),
					varNameParser(),
				),
				"variable name",
			),
		),
		parser.SpacesAndTabs(),
		parser.Char('='),
		parser.SpacesAndTabs(),
		query.Parse,
	)

	return func(input []rune) parser.Result {
		res := p(input)
		if res.Err != nil {
			return res
		}
		resSlice := res.Payload.([]interface{})
		return parser.Result{
			Payload: mappingStatement{
				input: input,
				assignment: &varAssignment{
					Name: resSlice[2].(string),
				},
				query: resSlice[6].(query.Function),
			},
			Remaining: res.Remaining,
		}
	}
}

func nameLiteralParser() parser.Type {
	return parser.JoinStringPayloads(
		parser.UntilFail(
			parser.OneOf(
				parser.InRange('a', 'z'),
				parser.InRange('A', 'Z'),
				parser.InRange('0', '9'),
				parser.InRange('*', '+'),
				parser.Char('.'),
				parser.Char('_'),
				parser.Char('-'),
				parser.Char('~'),
			),
		),
	)
}

func metaStatementParser(disabled bool) parser.Type {
	p := parser.Sequence(
		parser.Expect(parser.Term("meta"), "assignment"),
		parser.SpacesAndTabs(),
		parser.Optional(parser.OneOf(
			parser.QuotedString(),
			nameLiteralParser(),
		)),
		parser.Optional(parser.SpacesAndTabs()),
		parser.Char('='),
		parser.SpacesAndTabs(),
		query.Parse,
	)

	return func(input []rune) parser.Result {
		res := p(input)
		if res.Err != nil {
			return res
		}
		if disabled {
			return parser.Result{
				Err:       parser.NewFatalError(input, errors.New("setting meta fields from within a map is not allowed")),
				Remaining: input,
			}
		}
		resSlice := res.Payload.([]interface{})

		var keyPtr *string
		if key, set := resSlice[2].(string); set {
			keyPtr = &key
		}

		return parser.Result{
			Payload: mappingStatement{
				input:      input,
				assignment: &metaAssignment{Key: keyPtr},
				query:      resSlice[6].(query.Function),
			},
			Remaining: res.Remaining,
		}
	}
}

func pathLiteralSegmentParser() parser.Type {
	return parser.JoinStringPayloads(
		parser.UntilFail(
			parser.OneOf(
				parser.InRange('a', 'z'),
				parser.InRange('A', 'Z'),
				parser.InRange('0', '9'),
				parser.InRange('*', '+'),
				parser.Char('_'),
				parser.Char('-'),
				parser.Char('~'),
			),
		),
	)
}

func quotedPathLiteralSegmentParser() parser.Type {
	pattern := parser.QuotedString()

	return func(input []rune) parser.Result {
		res := pattern(input)
		if res.Err != nil {
			return res
		}

		rawSegment, _ := res.Payload.(string)

		// Convert into a JSON pointer style path string.
		rawSegment = strings.Replace(rawSegment, "~", "~0", -1)
		rawSegment = strings.Replace(rawSegment, ".", "~1", -1)

		return parser.Result{
			Payload:   rawSegment,
			Remaining: res.Remaining,
		}
	}
}

func pathParser() parser.Type {
	p := parser.Sequence(
		parser.Expect(pathLiteralSegmentParser(), "assignment"),
		parser.Optional(
			parser.Sequence(
				parser.Char('.'),
				parser.Delimited(
					parser.Expect(
						parser.OneOf(
							quotedPathLiteralSegmentParser(),
							pathLiteralSegmentParser(),
						),
						"target path",
					),
					parser.Char('.'),
				),
			),
		),
	)

	return func(input []rune) parser.Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		sequence := res.Payload.([]interface{})
		path := []string{sequence[0].(string)}

		if sequence[1] != nil {
			pathParts := sequence[1].([]interface{})[1].([]interface{})[0].([]interface{})
			for _, p := range pathParts {
				path = append(path, gabs.DotPathToSlice(p.(string))...)
			}
		}

		return parser.Result{
			Payload:   path,
			Remaining: res.Remaining,
		}
	}
}

func plainMappingStatementParser() parser.Type {
	p := parser.Sequence(
		pathParser(),
		parser.SpacesAndTabs(),
		parser.Char('='),
		parser.SpacesAndTabs(),
		query.Parse,
	)

	return func(input []rune) parser.Result {
		res := p(input)
		if res.Err != nil {
			return res
		}
		resSlice := res.Payload.([]interface{})
		path := resSlice[0].([]string)

		if len(path) > 0 && path[0] == "root" {
			path = path[1:]
		}

		return parser.Result{
			Payload: mappingStatement{
				input: input,
				assignment: &jsonAssignment{
					Path: path,
				},
				query: resSlice[4].(query.Function),
			},
			Remaining: res.Remaining,
		}
	}
}

//------------------------------------------------------------------------------
