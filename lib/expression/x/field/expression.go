package field

import (
	"bytes"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Message is an interface type to be given to a function interpolator, it
// allows the function to resolve fields and metadata from a message.
type Message interface {
	Get(p int) types.Part
	Len() int
}

//------------------------------------------------------------------------------

// Expression represents a Benthos dynamic field expression, used to configure
// string fields where the contents should change based on the contents of
// messages and other factors.
//
// Each function here resolves the expression for a particular message of a
// batch, this is why an index is expected.
type Expression interface {
	// Bytes returns a byte slice representing the expression resolved for a
	// message
	// of a batch.
	Bytes(index int, msg Message) []byte

	// BytesLegacy is DEPRECATED - Instructs deprecated functions to disregard
	// index information.
	// TODO V4: Remove this.
	BytesLegacy(index int, msg Message) []byte

	// BytesEscaped returns a byte slice representing the expression resolved
	// for a message of a batch with the contents of resolved expressions
	// escaped.
	BytesEscaped(index int, msg Message) []byte

	// BytesEscapedLegacy is DEPRECATED - Instructs deprecated functions to
	// disregard index information.
	// TODO V4: Remove this.
	BytesEscapedLegacy(index int, msg Message) []byte

	// String returns a string representing the expression resolved for a
	// message of a batch.
	String(index int, msg Message) string

	// StringLegacy is DEPRECATED - Instructs deprecated functions to disregard
	// index information.
	// TODO V4: Remove this.
	StringLegacy(index int, msg Message) string
}

// New attempts to parse and create an expression from a string. If the
// expression is invalid an error is returned.
func New(expr string) (Expression, error) {
	return parse(expr)
}

//------------------------------------------------------------------------------

func buildExpression(resolvers []resolver) *expression {
	e := &expression{
		resolvers: resolvers,
	}
	var staticBuf bytes.Buffer
	for _, r := range resolvers {
		if s, is := r.(staticResolver); is {
			staticBuf.Write(s.ResolveBytes(0, message.New(nil), false, false))
		} else {
			return e
		}
	}
	if staticBuf.Len() == 0 {
		return e
	}
	return &expression{
		static: staticBuf.String(),
	}
}

//------------------------------------------------------------------------------

type expression struct {
	static    string
	resolvers []resolver
}

func (e *expression) resolve(index int, msg Message, escaped, legacy bool) []byte {
	if len(e.resolvers) == 1 {
		return e.resolvers[0].ResolveBytes(index, msg, escaped, legacy)
	}
	var buf bytes.Buffer
	for _, r := range e.resolvers {
		buf.Write(r.ResolveBytes(index, msg, escaped, legacy))
	}
	return buf.Bytes()
}

// Bytes returns a byte slice representing the expression resolved for a message
// of a batch.
func (e *expression) Bytes(index int, msg Message) []byte {
	if len(e.resolvers) == 0 {
		return []byte(e.static)
	}
	return e.resolve(index, msg, false, false)
}

// BytesLegacy is DEPRECATED - Instructs deprecated functions to disregard index
// information.
// TODO V4: Remove this.
func (e *expression) BytesLegacy(index int, msg Message) []byte {
	if len(e.resolvers) == 0 {
		return []byte(e.static)
	}
	return e.resolve(index, msg, false, true)
}

// BytesEscaped returns a byte slice representing the expression resolved for a
// message of a batch with the contents of resolved expressions escaped.
func (e *expression) BytesEscaped(index int, msg Message) []byte {
	if len(e.resolvers) == 0 {
		return []byte(e.static)
	}
	return e.resolve(index, msg, true, false)
}

// BytesEscapedLegacy is DEPRECATED - Instructs deprecated functions to
// disregard index information.
// TODO V4: Remove this.
func (e *expression) BytesEscapedLegacy(index int, msg Message) []byte {
	if len(e.resolvers) == 0 {
		return []byte(e.static)
	}
	return e.resolve(index, msg, true, true)
}

// String returns a string representing the expression resolved for a message of
// a batch.
func (e *expression) String(index int, msg Message) string {
	if len(e.resolvers) == 0 {
		return e.static
	}
	return string(e.Bytes(index, msg))
}

// StringLegacy is DEPRECATED - Instructs deprecated functions to disregard
// index information.
// TODO V4: Remove this.
func (e *expression) StringLegacy(index int, msg Message) string {
	if len(e.resolvers) == 0 {
		return e.static
	}
	return string(e.BytesLegacy(index, msg))
}

//------------------------------------------------------------------------------
