package mapping

import (
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/bloblang/x/parser"
	"github.com/Jeffail/benthos/v3/lib/bloblang/x/query"
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
	line       int
	assignment Assignment
	query      query.Function
}

// Executor is a parsed bloblang mapping that can be executed on a Benthos
// message.
type Executor struct {
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
			Maps:  e.maps,
			Value: valuePtr,
			Vars:  vars,
			Index: index,
			Msg:   msg,
		})
		if err != nil {
			return nil, xerrors.Errorf("failed to execute mapping assignment at line %v: %v", stmt.line+1, err)
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
			return nil, xerrors.Errorf("failed to assign mapping result at line %v: %v", stmt.line+1, err)
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
			return nil, xerrors.Errorf("failed to execute mapping assignment at line %v: %v", stmt.line, err)
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
			return nil, xerrors.Errorf("failed to assign mapping result at line %v: %v", stmt.line, err)
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
func NewExecutor(mapping string) (*Executor, error) {
	res := parseExecutor([]rune(mapping))
	if res.Err != nil {
		return nil, xerrors.Errorf("failed to parse mapping: %w", res.Err)
	}

	return res.Payload.(*Executor), nil
}

//------------------------------------------------------------------------------'

type mappingParseError struct {
	line   int
	column int
	err    error
}

func (e *mappingParseError) Error() string {
	return fmt.Sprintf("line %v char %v: %v", e.line+1, e.column+1, e.err)
}

func getLineCol(lines []int, char int) (int, int) {
	line, column := 0, char
	for i, index := range lines {
		if index > char {
			break
		}
		line = i + 1
		column = char - index
	}
	return line, column
}

func wrapParserErr(lines []int, err error) error {
	if p, ok := err.(parser.PositionalError); ok {
		line, column := getLineCol(lines, p.Position)
		return &mappingParseError{
			line:   line,
			column: column,
			err:    p.Err,
		}
	}
	return err
}

func parseExecutor(input []rune) parser.Result {
	maps := map[string]query.Function{}
	statements := []mappingStatement{}

	var i int
	var lineIndexes []int
	for _, l := range strings.Split(string(input), "\n") {
		i = i + len(l) + 1
		lineIndexes = append(lineIndexes, i)
	}

	newline := parser.NewlineAllowComment()
	whitespace := parser.SpacesAndTabs()
	allWhitespace := parser.DiscardAll(parser.OneOf(whitespace, newline))

	statement := parser.OneOf(
		importParser(maps),
		mapParser(maps),
		letStatementParser(),
		metaStatementParser(false),
		plainMappingStatementParser(),
	)

	res := allWhitespace(input)

	i = len(input) - len(res.Remaining)
	res = statement(res.Remaining)
	if res.Err != nil {
		res.Err = wrapParserErr(lineIndexes, parser.ErrAtPosition(i, res.Err))
		return res
	}
	if mStmt, ok := res.Payload.(mappingStatement); ok {
		mStmt.line, _ = getLineCol(lineIndexes, i)
		statements = append(statements, mStmt)
	}

	for {
		res = parser.Discard(whitespace)(res.Remaining)
		if len(res.Remaining) == 0 {
			break
		}

		i = len(input) - len(res.Remaining)
		if res = newline(res.Remaining); res.Err != nil {
			return parser.Result{
				Err:       wrapParserErr(lineIndexes, parser.ErrAtPosition(i, res.Err)),
				Remaining: input,
			}
		}

		res = allWhitespace(res.Remaining)
		if len(res.Remaining) == 0 {
			break
		}

		i = len(input) - len(res.Remaining)
		if res = statement(res.Remaining); res.Err != nil {
			return parser.Result{
				Err:       wrapParserErr(lineIndexes, parser.ErrAtPosition(i, res.Err)),
				Remaining: input,
			}
		}
		if mStmt, ok := res.Payload.(mappingStatement); ok {
			mStmt.line, _ = getLineCol(lineIndexes, i)
			statements = append(statements, mStmt)
		}
	}

	return parser.Result{
		Remaining: res.Remaining,
		Payload: &Executor{
			maps, statements,
		},
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

func pathLiteralParser() parser.Type {
	return parser.JoinStringPayloads(
		parser.UntilFail(
			parser.OneOf(
				parser.InRange('a', 'z'),
				parser.InRange('A', 'Z'),
				parser.InRange('0', '9'),
				parser.InRange('*', '+'),
				parser.Char('.'),
				parser.Char('_'),
				parser.Char('~'),
			),
		),
	)
}

func importParser(maps map[string]query.Function) parser.Type {
	p := parser.Sequence(
		parser.Term("import"),
		parser.SpacesAndTabs(),
		parser.MustBe(
			parser.Expect(
				parser.QuotedString(),
				"file-path",
			),
		),
	)

	return func(input []rune) parser.Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		filepath := res.Payload.([]interface{})[2].(string)
		contents, err := ioutil.ReadFile(filepath)
		if err != nil {
			return parser.Result{
				Err:       xerrors.Errorf("failed to read import: %w", err),
				Remaining: input,
			}
		}

		execRes := parseExecutor([]rune(string(contents)))
		if execRes.Err != nil {
			return parser.Result{
				Err:       xerrors.Errorf("failed to parse import '%v': %w", filepath, execRes.Err),
				Remaining: input,
			}
		}

		exec := execRes.Payload.(*Executor)
		if len(exec.maps) == 0 {
			return parser.Result{
				Err:       xerrors.Errorf("no maps to import from '%v'", filepath),
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
				Err:       fmt.Errorf("map name collisions from import '%v': %v", filepath, collisions),
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
				"map-name",
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
				Err:       fmt.Errorf("map name collision: %v", ident),
				Remaining: input,
			}
		}

		statements := make([]mappingStatement, len(stmtSlice))
		for i, v := range stmtSlice {
			statements[i] = v.(mappingStatement)
		}

		maps[ident] = &Executor{maps, statements}

		return parser.Result{
			Payload:   ident,
			Remaining: res.Remaining,
		}
	}
}

func letStatementParser() parser.Type {
	p := parser.Sequence(
		parser.Term("let"),
		parser.SpacesAndTabs(),
		// Prevents a missing path from being captured by the next parser
		parser.MustBe(
			parser.Expect(
				parser.OneOf(
					parser.QuotedString(),
					varNameParser(),
				),
				"variable-name",
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
				assignment: &varAssignment{
					Name: resSlice[2].(string),
				},
				query: resSlice[6].(query.Function),
			},
			Remaining: res.Remaining,
		}
	}
}

func metaStatementParser(disabled bool) parser.Type {
	p := parser.Sequence(
		parser.Term("meta"),
		parser.SpacesAndTabs(),
		parser.Optional(parser.OneOf(
			parser.QuotedString(),
			pathLiteralParser(),
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
				Err:       errors.New("setting meta fields from within a map is not allowed"),
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
				assignment: &metaAssignment{Key: keyPtr},
				query:      resSlice[6].(query.Function),
			},
			Remaining: res.Remaining,
		}
	}
}

func plainMappingStatementParser() parser.Type {
	p := parser.Sequence(
		parser.Expect(
			parser.OneOf(
				parser.QuotedString(),
				pathLiteralParser(),
			),
			"target-path",
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
		path := gabs.DotPathToSlice(resSlice[0].(string))
		if len(path) > 0 && path[0] == "root" {
			path = path[1:]
		}
		return parser.Result{
			Payload: mappingStatement{
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
