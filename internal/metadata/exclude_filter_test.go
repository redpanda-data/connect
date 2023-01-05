package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestExcludeFilter(t *testing.T) {
	tests := []struct {
		name       string
		inputMeta  map[string]any
		outputMeta map[string]any
		conf       ExcludeFilterConfig
	}{
		{
			name: "no filter",
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
			conf: NewExcludeFilterConfig(),
		},
		{
			name: "foo filter",
			inputMeta: map[string]any{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			outputMeta: map[string]any{
				"bar": "bar1",
				"baz": "baz1",
			},
			conf: ExcludeFilterConfig{
				ExcludePrefixes: []string{"f"},
			},
		},
		{
			name: "empty filter",
			inputMeta: map[string]any{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			outputMeta: map[string]any{},
			conf: ExcludeFilterConfig{
				ExcludePrefixes: []string{""},
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
			filter, err := test.conf.Filter()
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
