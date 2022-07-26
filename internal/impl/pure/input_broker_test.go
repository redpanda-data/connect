package pure_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestBrokerConfigs(t *testing.T) {
	for _, test := range []struct {
		name   string
		config string
		output map[string]struct{}
	}{
		{
			name: "simple inputs",
			config: `
broker:
  inputs:
    - generate:
        count: 1
        interval: ""
        mapping: 'root = "hello world 1"'
    - generate:
        count: 1
        interval: ""
        mapping: 'root = "hello world 2"'
`,
			output: map[string]struct{}{
				"hello world 1": {},
				"hello world 2": {},
			},
		},
		{
			name: "input processors",
			config: `
broker:
  inputs:
    - generate:
        count: 1
        interval: ""
        mapping: 'root = "hello world 1"'
      processors:
        - bloblang: 'root = content().uppercase()'
processors:
  - bloblang: 'root = "meow " + content().string()'
`,
			output: map[string]struct{}{
				"meow HELLO WORLD 1": {},
			},
		},
		{
			name: "input processors to batcher",
			config: `
broker:
  inputs:
    - generate:
        count: 3
        interval: ""
        mapping: 'root = "hello world 1"'
      processors:
        - bloblang: 'root = content().uppercase()'
  batching:
    count: 3
    processors:
      - archive:
          format: lines
      - bloblang: 'root = content() + " woof"'
processors:
  - bloblang: 'root = "meow " + content().string()'
`,
			output: map[string]struct{}{
				"meow HELLO WORLD 1\nHELLO WORLD 1\nHELLO WORLD 1 woof": {},
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			builder := service.NewEnvironment().NewStreamBuilder()
			require.NoError(t, builder.AddInputYAML(test.config))
			require.NoError(t, builder.SetLoggerYAML(`level: none`))

			outputMsgs := map[string]struct{}{}
			require.NoError(t, builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
				mBytes, _ := msg.AsBytes()
				outputMsgs[string(mBytes)] = struct{}{}
				return nil
			}))

			strm, err := builder.Build()
			require.NoError(t, err)

			tCtx, done := context.WithTimeout(context.Background(), time.Minute)
			defer done()

			require.NoError(t, strm.Run(tCtx))
			assert.Equal(t, test.output, outputMsgs)
		})
	}
}
