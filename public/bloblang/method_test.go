package bloblang

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypedMethods(t *testing.T) {
	testCases := []struct {
		name string
		fn   Method
		in   any
		exp  any
		err  string
	}{
		{
			name: "bad int64",
			fn: Int64Method(func(i int64) (any, error) {
				return i, nil
			}),
			in:  "not an int",
			err: `expected number value, got string ("not an int")`,
		},
		{
			name: "good int64",
			fn: Int64Method(func(i int64) (any, error) {
				return i * 2, nil
			}),
			in:  5,
			exp: int64(10),
		},
		{
			name: "bad float64",
			fn: Float64Method(func(f float64) (any, error) {
				return f, nil
			}),
			in:  "not a float",
			err: `expected number value, got string ("not a float")`,
		},
		{
			name: "good float64",
			fn: Float64Method(func(f float64) (any, error) {
				return f * 2, nil
			}),
			in:  5.0,
			exp: 10.0,
		},
		{
			name: "bad string",
			fn: StringMethod(func(s string) (any, error) {
				return s, nil
			}),
			in:  5,
			err: "expected string value, got number (5)",
		},
		{
			name: "good string",
			fn: StringMethod(func(s string) (any, error) {
				return "yep: " + s, nil
			}),
			in:  "hey",
			exp: "yep: hey",
		},
		{
			name: "bad bytes",
			fn: BytesMethod(func(s []byte) (any, error) {
				return s, nil
			}),
			in:  5,
			err: "expected bytes value, got number (5)",
		},
		{
			name: "good bytes",
			fn: BytesMethod(func(s []byte) (any, error) {
				return append([]byte("yep: "), s...), nil
			}),
			in:  []byte("hey"),
			exp: []byte("yep: hey"),
		},
		{
			name: "bad bool",
			fn: BoolMethod(func(b bool) (any, error) {
				return b, nil
			}),
			in:  "nope",
			err: `expected bool value, got string ("nope")`,
		},
		{
			name: "good bool",
			fn: BoolMethod(func(b bool) (any, error) {
				return !b, nil
			}),
			in:  true,
			exp: false,
		},
		{
			name: "bad object",
			fn: ObjectMethod(func(o map[string]any) (any, error) {
				return o, nil
			}),
			in:  5,
			err: "expected object value, got number (5)",
		},
		{
			name: "bad array",
			fn: ArrayMethod(func(a []any) (any, error) {
				return a, nil
			}),
			in:  5,
			err: "expected array value, got number (5)",
		},
		{
			name: "bad timestamp",
			fn: TimestampMethod(func(t time.Time) (any, error) {
				return t, nil
			}),
			in:  "not a timestamp",
			err: "parsing time \"not a timestamp\" as \"2006-01-02T15:04:05.999999999Z07:00\": cannot parse \"not a timestamp\" as \"2006\"",
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			out, err := test.fn(test.in)
			if test.err != "" {
				assert.EqualError(t, err, test.err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.exp, out)
			}
		})
	}
}
