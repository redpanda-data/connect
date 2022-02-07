package metadata

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataFilter(t *testing.T) {
	tests := []struct {
		name       string
		inputMeta  map[string]string
		outputMeta map[string]string
		conf       IncludeFilterConfig
	}{
		{
			name: "no filter",
			inputMeta: map[string]string{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			outputMeta: map[string]string{},
			conf:       NewIncludeFilterConfig(),
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
			conf: IncludeFilterConfig{
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
			conf: IncludeFilterConfig{
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
			conf: IncludeFilterConfig{
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
			conf: IncludeFilterConfig{
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
			conf: IncludeFilterConfig{
				IncludePrefixes: []string{"foo"},
				IncludePatterns: []string{"bar"},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			part := message.NewPart(nil)
			for k, v := range test.inputMeta {
				part.MetaSet(k, v)
			}

			filter, err := test.conf.CreateFilter()
			require.NoError(t, err)

			outputMeta := map[string]string{}
			require.NoError(t, part.MetaIter(func(k, v string) error {
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
