package service

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// InterpolatedField resolves a string containing dynamic interpolation
// functions for a given message.
type InterpolatedField struct {
	expr *field.Expression
}

// NewInterpolatedField parses an interpolated string expression.
func NewInterpolatedField(expr string) (*InterpolatedField, error) {
	e, err := bloblang.NewField(expr)
	if err != nil {
		return nil, err
	}
	return &InterpolatedField{expr: e}, nil
}

type fauxOldMessage struct {
	p types.Part
}

func (f fauxOldMessage) Get(i int) types.Part {
	return f.p
}

func (f fauxOldMessage) Len() int {
	return 1
}

// String resolves the interpolated field for a given message as a string.
func (i *InterpolatedField) String(m *Message) string {
	return i.expr.String(0, fauxOldMessage{m.part})
}

// Bytes resolves the interpolated field for a given message as a byte slice.
func (i *InterpolatedField) Bytes(m *Message) []byte {
	return i.expr.Bytes(0, fauxOldMessage{m.part})
}
