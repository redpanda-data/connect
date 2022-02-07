package service

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/lib/message"
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

// String resolves the interpolated field for a given message as a string.
func (i *InterpolatedString) String(m *Message) string {
	return i.expr.String(0, fauxOldMessage{m.part})
}

// Bytes resolves the interpolated field for a given message as a byte slice.
func (i *InterpolatedString) Bytes(m *Message) []byte {
	return i.expr.Bytes(0, fauxOldMessage{m.part})
}
