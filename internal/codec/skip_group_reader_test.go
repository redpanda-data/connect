package codec

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadUpTo(t *testing.T) {
	for _, test := range []struct {
		name     string
		input    string
		skips    [][]byte
		expected string
	}{
		{
			name:     "no groups",
			input:    "foo",
			expected: "foo",
		},
		{
			name:  "no input",
			input: "",
			skips: [][]byte{
				[]byte("hello "),
			},
			expected: "",
		},
		{
			name:  "an empty group",
			input: "hello world",
			skips: [][]byte{
				[]byte("not this"),
				[]byte(""),
			},
			expected: "hello world",
		},
		{
			name:     "easy match",
			input:    "hello world",
			expected: "world",
			skips: [][]byte{
				[]byte("hello "),
			},
		},
		{
			name:     "exact skip match",
			input:    "foo",
			expected: "",
			skips: [][]byte{
				[]byte("foo"),
			},
		},
		{
			name:     "max is bigger",
			input:    "foa",
			expected: "a",
			skips: [][]byte{
				[]byte("fo"),
				[]byte("what this is huge"),
			},
		},
		{
			name:     "order doesnt matter",
			input:    "helloworld",
			expected: "ld",
			skips: [][]byte{
				[]byte("hellowoa"),
				[]byte("hella"),
				[]byte("hello"),
				[]byte("hellowor"),
				[]byte("hea"),
			},
		},
	} {
		test := test
		for _, readWrapper := range []struct {
			name string
			fn   func(io.Reader) io.Reader
		}{
			{"full", func(r io.Reader) io.Reader { return r }},
			{"one_byte", iotest.OneByteReader},
		} {
			t.Run(fmt.Sprintf("%v_%v", test.name, readWrapper.name), func(t *testing.T) {
				testReader := skipGroup(bytes.NewReader([]byte(test.input)), test.skips...)
				output, err := io.ReadAll(testReader)
				require.NoError(t, err)
				assert.Equal(t, test.expected, string(output))
			})
		}
	}
}
