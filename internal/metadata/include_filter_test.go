package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestMetadataFilter(t *testing.T) {
	tests := []struct {
		name       string
		inputMeta  map[string]any
		outputMeta map[string]any
		conf       IncludeFilterConfig
	}{
		{
			name: "no filter",
			inputMeta: map[string]any{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			outputMeta: map[string]any{},
			conf:       NewIncludeFilterConfig(),
		},
		{
			name: "foo prefix filter",
			inputMeta: map[string]any{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			outputMeta: map[string]any{
				"foo": "foo1",
			},
			conf: IncludeFilterConfig{
				IncludePrefixes: []string{"f"},
			},
		},
		{
			name: "ar$ pattern filter",
			inputMeta: map[string]any{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			outputMeta: map[string]any{
				"bar": "bar1",
			},
			conf: IncludeFilterConfig{
				IncludePatterns: []string{"ar$"},
			},
		},
		{
			name: "empty prefix filter",
			inputMeta: map[string]any{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			outputMeta: map[string]any{
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
			inputMeta: map[string]any{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			outputMeta: map[string]any{
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
			inputMeta: map[string]any{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			outputMeta: map[string]any{
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
				part.MetaSetMut(k, v)
			}

			filter, err := test.conf.CreateFilter()
			require.NoError(t, err)

			outputMeta := map[string]any{}
			require.NoError(t, filter.Iter(part, func(k string, v any) error {
				outputMeta[k] = v
				return nil
			}))

			assert.Equal(t, test.outputMeta, outputMeta)
		})
	}
}
