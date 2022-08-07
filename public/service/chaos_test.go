package service_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"

	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

type chaosOutput struct {
	t        *testing.T
	expected string
	eRate    float64
	seen     int64
}

func (c *chaosOutput) Connect(ctx context.Context) error {
	return nil
}

func (c *chaosOutput) Write(ctx context.Context, m *service.Message) error {
	mBytes, err := m.AsBytes()
	require.NoError(c.t, err)
	assert.Equal(c.t, c.expected, string(mBytes))

	_ = atomic.AddInt64(&c.seen, 1)

	// Whether or not we acknowledge is random
	if f := rand.Float64(); f <= c.eRate {
		return errors.New("chaos output chose you")
	}
	return nil
}

func (c *chaosOutput) Close(ctx context.Context) error {
	assert.Greater(c.t, atomic.LoadInt64(&c.seen), int64(0), c.expected)
	return nil
}

func TestChaosConfig(t *testing.T) {
	env := service.NewEnvironment()

	require.NoError(t, env.RegisterOutput("chaos", service.NewConfigSpec().
		Field(service.NewStringField("expected")).
		Field(service.NewFloatField("error_rate").Description("A number [0.0,1.0) representing the rate of errors.").Default(0.1)),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			exp, err := conf.FieldString("expected")
			if err != nil {
				return nil, 0, err
			}
			eRate, err := conf.FieldFloat("error_rate")
			if err != nil {
				return nil, 0, err
			}
			if eRate >= 1.0 || eRate < 0 {
				return nil, 0, fmt.Errorf("error_rate must be >=0 and <1, got %v", eRate)
			}
			return &chaosOutput{
				t:        t,
				expected: exp,
				eRate:    eRate,
			}, 10, nil
		}))

	strmBuilder := env.NewStreamBuilder()
	require.NoError(t, strmBuilder.SetYAML(`
logger:
  level: NONE

input:
  generate:
    count: 1_000
    interval: ""
    mapping: 'root.seen = []'
  processors:
    - mutation: 'root.seen = this.seen.append("a")'
    - mutation: 'root.seen = this.seen.append("b")'

pipeline:
  processors:
    - mutation: 'root.seen = this.seen.append("c")'

output:
  processors:
    - mutation: 'root.seen = this.seen.append("d")'
  fallback:
    - processors:
        - mutation: 'root.seen = this.seen.append("e1")'
      chaos:
        expected: '{"seen":["a","b","c","d","e1"]}'
        error_rate: 0.25

    - processors:
        - mutation: 'root.seen = this.seen.append("e2")'
      switch:
        retry_until_success: false
        cases:
          - output:
              chaos:
                expected: '{"seen":["a","b","c","d","e2","f1"]}'
                error_rate: 0.10
              processors:
                - mutation: 'root.seen = this.seen.append("f1")'
            continue: true
          - output:
              chaos:
                expected: '{"seen":["a","b","c","d","e2","f2"]}'
                error_rate: 0.10
              processors:
                - mutation: 'root.seen = this.seen.append("f2")'
            continue: true

    - processors:
        - mutation: 'root.seen = this.seen.append("e3")'
      broker:
        pattern: fan_out
        outputs:
          - processors:
              - mutation: 'root.seen = this.seen.append("f3")'
            chaos:
              expected: '{"seen":["a","b","c","d","e3","f3"]}'
              error_rate: 0.01
          - processors:
              - mutation: 'root.seen = this.seen.append("f4")'
            chaos:
              expected: '{"seen":["a","b","c","d","e3","f4"]}'
              error_rate: 0.01
`))

	strm, err := strmBuilder.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	assert.NoError(t, strm.Run(ctx))
}
