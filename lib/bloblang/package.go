package bloblang

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// Message is an interface type to be given to a function interpolator, it
// allows the function to resolve fields and metadata from a message.
type Message interface {
	Get(p int) types.Part
	Len() int
}

// Field represents a Benthos dynamic field expression, used to configure string
// fields where the contents should change based on the contents of messages and
// other factors.
//
// Each function here resolves the expression for a particular message of a
// batch, this is why an index is expected.
type Field interface {
	// Bytes returns a byte slice representing the expression resolved for a
	// message of a batch.
	Bytes(index int, msg Message) []byte

	// String returns a string representing the expression resolved for a
	// message of a batch.
	String(index int, msg Message) string
}

type fieldWrap struct {
	f field.Expression
}

func (w *fieldWrap) Bytes(index int, msg Message) []byte {
	return w.f.Bytes(index, field.Message(msg))
}

func (w *fieldWrap) String(index int, msg Message) string {
	return w.f.String(index, field.Message(msg))
}

// NewField attempts to parse and create a dynamic field expression from a
// string. If the expression is invalid an error is returned.
//
// When a parsing error occurs the returned error will be a *parser.Error type,
// which allows you to gain positional and structured error messages.
func NewField(expr string) (Field, error) {
	e, err := parser.ParseField(expr)
	if err != nil {
		return nil, err
	}
	return &fieldWrap{e}, nil
}

//------------------------------------------------------------------------------

// Mapping is a parsed Bloblang mapping.
type Mapping interface {
	// QueryPart executes a Bloblang mapping and expects a boolean result, which
	// is returned. If the execution fails or the result is not boolean an error
	// is returned.
	//
	// Bloblang is able to query other messages of a batch, and therefore this
	// function takes a message batch and index rather than a single message
	// part argument.
	QueryPart(index int, msg Message) (bool, error)

	// MapPart executes a Bloblang mapping on a message part and returns a new
	// resulting part, or an error if the execution fails.
	//
	// Bloblang is able to query other messages of a batch, and therefore this
	// function takes a message batch and index rather than a single message
	// part argument.
	MapPart(index int, msg Message) (types.Part, error)
}

type mappingWrap struct {
	e *mapping.Executor
}

func (w *mappingWrap) QueryPart(index int, msg Message) (bool, error) {
	return w.e.QueryPart(index, field.Message(msg))
}

func (w *mappingWrap) MapPart(index int, msg Message) (types.Part, error) {
	return w.e.MapPart(index, field.Message(msg))
}

// NewMapping attempts to parse and create a Bloblang mapping from a string. If
// the mapping was read from a file the path should be provided in order to
// resolve relative imports, otherwise the path can be left empty.
//
// When a parsing error occurs the returned error may be a *parser.Error type,
// which allows you to gain positional and structured error messages.
func NewMapping(expr string) (Mapping, error) {
	e, err := parser.ParseMapping("", expr, parser.Context{
		Functions: query.AllFunctions,
		Methods:   query.AllMethods,
	})
	if err != nil {
		return nil, err
	}
	return &mappingWrap{e}, nil
}
