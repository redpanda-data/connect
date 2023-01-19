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
