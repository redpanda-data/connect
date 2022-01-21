package metadata

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExcludeFilter(t *testing.T) {
	tests := []struct {
		name       string
		inputMeta  map[string]string
		outputMeta map[string]string
		conf       ExcludeFilterConfig
	}{
		{
			name: "no filter",
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
			conf: NewExcludeFilterConfig(),
		},
		{
			name: "foo filter",
			inputMeta: map[string]string{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			outputMeta: map[string]string{
				"bar": "bar1",
				"baz": "baz1",
			},
			conf: ExcludeFilterConfig{
				ExcludePrefixes: []string{"f"},
			},
		},
		{
			name: "empty filter",
			inputMeta: map[string]string{
				"foo": "foo1",
				"bar": "bar1",
				"baz": "baz1",
			},
			outputMeta: map[string]string{},
			conf: ExcludeFilterConfig{
				ExcludePrefixes: []string{""},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			meta := metadata.New(test.inputMeta)
			filter, err := test.conf.Filter()
			require.NoError(t, err)

			outputMeta := map[string]string{}
			require.NoError(t, filter.Iter(meta, func(k, v string) error {
				outputMeta[k] = v
				return nil
			}))

			assert.Equal(t, test.outputMeta, outputMeta)
		})
	}
}
