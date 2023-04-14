package manager_test

import (
	"context"
	"testing"
	"time"

	_ "github.com/benthosdev/benthos/v4/public/components/pure"
	"github.com/benthosdev/benthos/v4/public/service"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagerStreamPipelines(t *testing.T) {
	for _, test := range []struct {
		name     string
		conf     string
		closeErr string
	}{
		{
			name: "basic pipeline all resource types",
			conf: `
input:
  resource: fooinput

pipeline:
  processors:
    - rate_limit:
        resource: fooratelimit
    - resource: fooproc

output:
  resource: foooutput

input_resources:
  - label: fooinput
    generate:
      interval: 1ms
      mapping: |
        meta = {"foo":"foo value","bar":"bar value"}
        root.id = uuid_v4()

processor_resources:
  - label: fooproc
    cache:
      operator: set
      resource: foocache
      key: fookey
      value: '${! this.id }'

cache_resources:
  - label: foocache
    memory: {}

rate_limit_resources:
  - label: fooratelimit
    local:
      count: 10
      interval: 1s

output_resources:
  - label: foooutput
    drop: {}
`,
		},
		{
			name: "blocked pipeline",
			conf: `
input:
  resource: fooinput

pipeline:
  processors:
    - resource: fooproc

output:
  resource: foooutput

input_resources:
  - label: fooinput
    generate:
      interval: 1ms
      mapping: |
        meta = {"foo":"foo value","bar":"bar value"}
        root.id = uuid_v4()

processor_resources:
  - label: fooproc
    rate_limit:
      resource: fooratelimit

rate_limit_resources:
  - label: fooratelimit
    local:
      count: 100
      interval: 1s

output_resources:
  - label: foooutput
    broker:
      batching:
        period: 10s
      outputs:
        - drop: {}
`,
			closeErr: "failed to cleanly shutdown",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			builder := service.NewStreamBuilder()
			require.NoError(t, builder.SetYAML(test.conf))
			require.NoError(t, builder.SetLoggerYAML(`level: none`))

			strm, err := builder.Build()
			require.NoError(t, err)

			cancelledCtx, done := context.WithCancel(context.Background())
			done()

			assert.Equal(t, cancelledCtx.Err(), strm.Run(cancelledCtx))

			stopErr := strm.StopWithin(time.Millisecond * 100)
			if test.closeErr == "" {
				require.NoError(t, stopErr)
			} else {
				require.Error(t, stopErr)
				assert.ErrorContains(t, stopErr, test.closeErr)
			}
		})
	}
}
