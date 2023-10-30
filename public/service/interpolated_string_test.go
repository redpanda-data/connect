package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInterpolatedString(t *testing.T) {
	t.Parallel()

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
		test := test

		t.Run("deprecated api/"+test.name, func(t *testing.T) {
			t.Parallel()

			i, err := NewInterpolatedString(test.expr)
			require.NoError(t, err)
			assert.Equal(t, test.expected, i.String(test.msg))
			assert.Equal(t, test.expected, string(i.Bytes(test.msg)))
		})

		t.Run("recommended api/"+test.name, func(t *testing.T) {
			t.Parallel()

			i, err := NewInterpolatedString(test.expr)
			require.NoError(t, err)

			{
				got, err := i.TryString(test.msg)
				require.NoError(t, err)

				assert.Equal(t, test.expected, got)
			}

			{
				got, err := i.TryBytes(test.msg)
				require.NoError(t, err)

				assert.Equal(t, test.expected, string(got))
			}
		})
	}
}

func TestInterpolatedStringCtor(t *testing.T) {
	t.Parallel()

	i, err := NewInterpolatedString(`foo ${! meta("var1")  bar`)

	assert.EqualError(t, err, "required: expected end of expression, got: bar")
	assert.Nil(t, i)
}

func TestInterpolatedStringMethods(t *testing.T) {
	t.Parallel()

	i, err := NewInterpolatedString(`foo ${! meta("var1") + 1 } bar`)
	require.NoError(t, err)

	m := NewMessage([]byte("hello world"))
	m.MetaSet("var1", "value1")

	{
		got, err := i.TryString(m)
		require.EqualError(t, err, "cannot add types string (from meta field var1) and number (from number literal)")
		require.Empty(t, got)
	}

	{
		got, err := i.TryBytes(m)
		require.EqualError(t, err, "cannot add types string (from meta field var1) and number (from number literal)")
		require.Empty(t, got)
	}
}
