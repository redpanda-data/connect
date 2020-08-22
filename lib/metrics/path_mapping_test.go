package metrics

import (
	"strconv"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPathMapping(t *testing.T) {
	tests := []struct {
		mapping      string
		inputOutputs [][2]string
	}{
		{
			mapping: `if this.contains("foo") { deleted() }`,
			inputOutputs: [][2]string{
				{"foo", ""},
				{"hello foo world", ""},
				{"hello world", "hello world"},
			},
		},
		{
			mapping: `root = this.foo.bar.not_null()`,
			inputOutputs: [][2]string{
				{"foo", "foo"},
			},
		},
		{
			mapping: `this.replace("foo","bar")`,
			inputOutputs: [][2]string{
				{"foo", "bar"},
				{"hello foo world", "hello bar world"},
				{"hello world", "hello world"},
			},
		},
		{
			mapping: `10`,
			inputOutputs: [][2]string{
				{"foo", "foo"},
				{"hello foo world", "hello foo world"},
				{"hello world", "hello world"},
			},
		},
		{
			mapping: ``,
			inputOutputs: [][2]string{
				{"foo", "foo"},
				{"hello foo world", "hello foo world"},
				{"hello world", "hello world"},
			},
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			m, err := newPathMapping(test.mapping, log.Noop())
			require.NoError(t, err)
			for j, io := range test.inputOutputs {
				assert.Equal(t, io[1], m.mapPath(io[0]), strconv.Itoa(j))
			}
		})
	}
}
