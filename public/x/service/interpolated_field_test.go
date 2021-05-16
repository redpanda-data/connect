package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInterpolatedField(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		msg      *Message
		expected string
	}{
		{
			name:     "content interpolation",
			expr:     `foo ${! content() } bar`,
			msg:      NewMessage([]byte("hello world")),
			expected: `foo hello world bar`,
		},
		{
			name:     "no interpolation",
			expr:     `foo bar`,
			msg:      NewMessage([]byte("hello world")),
			expected: `foo bar`,
		},
		{
			name: "metadata interpolation",
			expr: `foo ${! meta("var1") } bar`,
			msg: func() *Message {
				m := NewMessage([]byte("hello world"))
				m.MetaSet("var1", "value1")
				return m
			}(),
			expected: `foo value1 bar`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			i, err := NewInterpolatedField(test.expr)
			require.NoError(t, err)
			assert.Equal(t, test.expected, i.String(test.msg))
			assert.Equal(t, test.expected, string(i.Bytes(test.msg)))
		})
	}
}
