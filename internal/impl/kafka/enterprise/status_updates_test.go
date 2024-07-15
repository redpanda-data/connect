package enterprise

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPathConversion(t *testing.T) {
	tests := []struct {
		path     []string
		expected string
	}{
		{
			path:     []string{},
			expected: "",
		},
		{
			path:     []string{"foo"},
			expected: "foo",
		},
		{
			path:     []string{"foo", "bar"},
			expected: "foo.bar",
		},
		{
			path:     []string{"foo.bar", "baz"},
			expected: "foo~1bar.baz",
		},
		{
			path:     []string{"foo.bar", "baz~buz"},
			expected: "foo~1bar.baz~0buz",
		},
		{
			path:     []string{"foo.bar.~baz~~buz", "meow", "woof"},
			expected: "foo~1bar~1~0baz~0~0buz.meow.woof",
		},
	}
	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			act := sliceToDotPath(test.path)
			assert.Equal(t, test.expected, act)
		})
	}
}
