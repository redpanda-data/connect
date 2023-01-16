package batchconfig_test

import (
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigFromAny(t *testing.T) {
	fromDefault := func(fn func(o *batchconfig.Config)) batchconfig.Config {
		c := batchconfig.NewConfig()
		fn(&c)
		return c
	}

	tests := []struct {
		name   string
		input  any
		output batchconfig.Config
	}{
		{
			name: "map values",
			input: map[string]any{
				"count":  11,
				"period": "1s",
			},
			output: fromDefault(func(o *batchconfig.Config) {
				o.Count = 11
				o.Period = "1s"
			}),
		},
		{
			name: "map values with processors",
			input: map[string]any{
				"count": 12,
				"processors": []any{
					map[string]any{
						"label": "fooproc",
						"noop":  struct{}{},
					},
					map[string]any{
						"label": "barproc",
						"noop":  struct{}{},
					},
				},
			},
			output: fromDefault(func(o *batchconfig.Config) {
				o.Count = 12

				fooConf := processor.NewConfig()
				fooConf.Label = "fooproc"
				fooConf.Type = "noop"

				barConf := processor.NewConfig()
				barConf.Label = "barproc"
				barConf.Type = "noop"

				o.Processors = append(o.Processors, fooConf, barConf)
			}),
		},
		{
			name: "node values",
			input: func() any {
				var n yaml.Node
				require.NoError(t, n.Encode(map[string]any{
					"count":  11,
					"period": "1s",
				}))
				return &n
			}(),
			output: fromDefault(func(o *batchconfig.Config) {
				o.Count = 11
				o.Period = "1s"
			}),
		},
		{
			name: "map values with node processors",
			input: func() any {
				var fooProcNode, barProcNode yaml.Node
				require.NoError(t, fooProcNode.Encode(map[string]any{
					"label": "fooproc",
					"noop":  struct{}{},
				}))
				require.NoError(t, barProcNode.Encode(map[string]any{
					"label": "barproc",
					"noop":  struct{}{},
				}))
				return map[string]any{
					"count":      12,
					"processors": []any{&fooProcNode, &barProcNode},
				}
			}(),
			output: fromDefault(func(o *batchconfig.Config) {
				o.Count = 12

				fooConf := processor.NewConfig()
				fooConf.Label = "fooproc"
				fooConf.Type = "noop"

				barConf := processor.NewConfig()
				barConf.Label = "barproc"
				barConf.Type = "noop"

				o.Processors = append(o.Processors, fooConf, barConf)
			}),
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			res, err := batchconfig.FromAny(test.input)
			require.NoError(t, err)
			assert.Equal(t, test.output, res)
		})
	}
}
