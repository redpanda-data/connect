package service

import (
	"github.com/benthosdev/benthos/v4/internal/bloblang"
	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// InterpolatedString resolves a string containing dynamic interpolation
// functions for a given message.
type InterpolatedString struct {
	expr *field.Expression
}

// NewInterpolatedString parses an interpolated string expression.
func NewInterpolatedString(expr string) (*InterpolatedString, error) {
	e, err := bloblang.GlobalEnvironment().NewField(expr)
	if err != nil {
		return nil, err
	}
	return &InterpolatedString{expr: e}, nil
}

type fauxOldMessage struct {
	p *message.Part
}

func (f fauxOldMessage) Get(i int) *message.Part {
	return f.p
}

func (f fauxOldMessage) Len() int {
	return 1
}

// Static returns the underlying contents of the interpolated string only if it
// contains zero dynamic expressions, and is therefore static, otherwise an
// empty string is returned. A second boolean parameter is also returned
// indicating whether the string was static, helping to distinguish between a
// static empty string versus a non-static string.
func (i *InterpolatedString) Static() (string, bool) {
	if i.expr.NumDynamicExpressions() > 0 {
		return "", false
	}
	s, _ := i.expr.String(0, nil)
	return s, true
}

// TryString resolves the interpolated field for a given message as a string,
// returns an error if any interpolation functions fail.
func (i *InterpolatedString) TryString(m *Message) (string, error) {
	return i.expr.String(0, fauxOldMessage{m.part})
}

// TryBytes resolves the interpolated field for a given message as a slice of
// bytes, returns an error if any interpolation functions fail.
func (i *InterpolatedString) TryBytes(m *Message) ([]byte, error) {
	return i.expr.Bytes(0, fauxOldMessage{m.part})
}

// String resolves the interpolated field for a given message as a string.
// Deprecated: Use TryString instead in order to capture errors.
func (i *InterpolatedString) String(m *Message) string {
	s, _ := i.expr.String(0, fauxOldMessage{m.part})
	return s
}

// Bytes resolves the interpolated field for a given message as a byte slice.
// Deprecated: Use TryBytes instead in order to capture errors.
func (i *InterpolatedString) Bytes(m *Message) []byte {
	b, _ := i.expr.Bytes(0, fauxOldMessage{m.part})
	return b
}
