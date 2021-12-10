package filter

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataFilter(t *testing.T) {
	tests := []struct {
		name       string
		inputMeta  map[string]string
		outputMeta map[string]string
		conf       Config
		isNotSet   bool
	}{
		{
			name: "no filter",
			inputMeta: map[string]string{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			outputMeta: map[string]string{},
			conf:       NewConfig(),
			isNotSet:   true,
		},
		{
			name: "foo prefix filter",
			inputMeta: map[string]string{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			outputMeta: map[string]string{
				"foo": "foo1",
			},
			conf: Config{
				IncludePrefixes: []string{"f"},
			},
		},
		{
			name: "ar$ pattern filter",
			inputMeta: map[string]string{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			outputMeta: map[string]string{
				"bar": "bar1",
			},
			conf: Config{
				IncludePatterns: []string{"ar$"},
			},
		},
		{
			name: "empty prefix filter",
			inputMeta: map[string]string{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			outputMeta: map[string]string{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			conf: Config{
				IncludePrefixes: []string{""},
			},
		},
		{
			name: "empty pattern filter",
			inputMeta: map[string]string{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			outputMeta: map[string]string{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			conf: Config{
				IncludePatterns: []string{""},
			},
		},
		{
			name: "foo prefix filter and bar pattern filter",
			inputMeta: map[string]string{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			outputMeta: map[string]string{
				"foo": "foo1",
				"bar": "bar1",
			},
			conf: Config{
				IncludePrefixes: []string{"foo"},
				IncludePatterns: []string{"bar"},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			meta := metadata.New(test.inputMeta)

			filter, err := test.conf.CreateFilter()
			require.NoError(t, err)
			require.Equal(t, test.isNotSet, !filter.IsSet())

			outputMeta := map[string]string{}
			require.NoError(t, meta.Iter(func(k, v string) error {
				if filter.Match(k) {
					outputMeta[k] = v
					return nil
				}
				return nil
			}))

			assert.Equal(t, test.outputMeta, outputMeta)
		})
	}
}
