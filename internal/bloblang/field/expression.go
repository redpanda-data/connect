package field

import (
	"bytes"

	"github.com/benthosdev/benthos/v4/internal/message"
)

// Message is an interface type to be given to a function interpolator, it
// allows the function to resolve fields and metadata from a message.
type Message interface {
	Get(p int) *message.Part
	Len() int
}

//------------------------------------------------------------------------------

// NewExpression creates a field expression from a slice of resolvers.
func NewExpression(resolvers ...Resolver) *Expression {
	e := &Expression{
		resolvers: resolvers,
	}
	var staticBuf bytes.Buffer
	for _, r := range resolvers {
		if s, is := r.(StaticResolver); is {
			staticBuf.Write([]byte(s))
		} else {
			e.dynamicExpressions++
		}
	}
	if e.dynamicExpressions > 0 || staticBuf.Len() == 0 {
		return e
	}
	return &Expression{
		static: staticBuf.String(),
	}
}

//------------------------------------------------------------------------------

// Expression represents a Benthos dynamic field expression, used to configure
// string fields where the contents should change based on the contents of
// messages and other factors.
//
// Each function here resolves the expression for a particular message of a
// batch, this is why an index is expected.
type Expression struct {
	static             string
	resolvers          []Resolver
	dynamicExpressions int
}

func (e *Expression) resolve(index int, msg Message, escaped bool) ([]byte, error) {
	if len(e.resolvers) == 1 {
		return e.resolvers[0].ResolveBytes(index, msg, escaped)
	}
	var buf bytes.Buffer
	for _, r := range e.resolvers {
		b, err := r.ResolveBytes(index, msg, escaped)
		if err != nil {
			return nil, err
		}
		buf.Write(b)
	}
	return buf.Bytes(), nil
}

// NumDynamicExpressions returns the number of dynamic interpolation functions
// within the expression.
func (e *Expression) NumDynamicExpressions() int {
	return e.dynamicExpressions
}

// Bytes returns a byte slice representing the expression resolved for a message
// of a batch.
func (e *Expression) Bytes(index int, msg Message) ([]byte, error) {
	if len(e.resolvers) == 0 {
		return []byte(e.static), nil
	}
	return e.resolve(index, msg, false)
}

// String returns a string representing the expression resolved for a message of
// a batch.
func (e *Expression) String(index int, msg Message) (string, error) {
	if len(e.resolvers) == 0 {
		return e.static, nil
	}
	b, err := e.Bytes(index, msg)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
