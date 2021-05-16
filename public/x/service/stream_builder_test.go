package service_test

import (
	"testing"

	"github.com/Jeffail/benthos/v3/public/x/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamBuilderDefault(t *testing.T) {
	b := service.NewStreamBuilder()

	act, err := b.AsYAML()
	require.NoError(t, err)

	exp := []string{
		`input:
    label: ""
    stdin:`,
		`buffer:
    none: {}`,
		`pipeline:
    threads: 0
    processors: []`,
		`output:
    label: ""
    stdout:`,
		`logger:
    level: INFO`,
		`metrics:
    http_server:`,
	}

	for _, str := range exp {
		assert.Contains(t, act, str)
	}
}

func TestStreamBuilderCustomLogger(t *testing.T) {
	b := service.NewStreamBuilder()
	b.SetPrintLogger(nil)

	act, err := b.AsYAML()
	require.NoError(t, err)

	exp := `logger:
    level: INFO`

	assert.NotContains(t, act, exp)
}

func TestStreamBuilderSetYAML(t *testing.T) {
	b := service.NewStreamBuilder()
	b.SetThreads(10)
	require.NoError(t, b.AddCacheYAML(`label: foocache
type: memory`))
	require.NoError(t, b.AddInputYAML(`type: kafka`))
	require.NoError(t, b.AddOutputYAML(`type: nats`))
	require.NoError(t, b.AddProcessorYAML(`type: bloblang`))
	require.NoError(t, b.AddProcessorYAML(`type: jmespath`))
	require.NoError(t, b.AddRateLimitYAML(`label: foorl
type: local`))
	require.NoError(t, b.SetMetricsYAML(`type: prometheus`))
	require.NoError(t, b.SetLoggerYAML(`level: DEBUG`))

	act, err := b.AsYAML()
	require.NoError(t, err)

	exp := []string{
		`input:
    label: ""
    kafka:`,
		`buffer:
    none: {}`,
		`pipeline:
    threads: 10
    processors:`,
		`
        - label: ""
          bloblang: ""`,
		`
        - label: ""
          jmespath:
            query: ""`,
		`output:
    label: ""
    nats:`,
		`metrics:
    prometheus:`,
		`cache_resources:
    - label: foocache
      memory:`,
		`rate_limit_resources:
    - label: foorl
      local:`,
		`  level: DEBUG`,
	}

	for _, str := range exp {
		assert.Contains(t, act, str)
	}
}

func TestStreamBuilderSetYAMLBrokers(t *testing.T) {
	b := service.NewStreamBuilder()
	b.SetThreads(10)
	require.NoError(t, b.AddInputYAML(`type: kafka`))
	require.NoError(t, b.AddInputYAML(`type: amqp_0_9`))
	require.NoError(t, b.AddOutputYAML(`type: nats`))
	require.NoError(t, b.AddOutputYAML(`type: file`))

	act, err := b.AsYAML()
	require.NoError(t, err)

	exp := []string{
		`input:
    label: ""
    broker:
        copies: 1
        inputs:`,
		`            - label: ""
              kafka:`,
		`            - label: ""
              amqp_0_9:`,
		`output:
    label: ""
    broker:
        copies: 1
        pattern: fan_out
        max_in_flight: 1
        outputs:`,
		`            - label: ""
              nats:`,
		`            - label: ""
              file:`,
	}

	for _, str := range exp {
		assert.Contains(t, act, str)
	}
}

func TestStreamBuilderYAMLErrors(t *testing.T) {
	b := service.NewStreamBuilder()

	err := b.AddCacheYAML(`{ label: "", type: memory }`)
	require.Error(t, err)
	assert.EqualError(t, err, "a label must be specified for cache resources")

	err = b.AddInputYAML(`not valid ! yaml 34324`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal errors")

	err = b.SetCoreYAML(`not valid ! yaml 34324`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal errors")

	err = b.SetCoreYAML(`input: { foo: nope }`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unable to infer")

	err = b.SetCoreYAML(`input: { kafka: { not_a_field: nope } }`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field not_a_field not recognised")

	err = b.AddInputYAML(`not_a_field: nah`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unable to infer")

	err = b.AddInputYAML(`kafka: { not_a_field: nah }`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field not_a_field not recognised")

	err = b.SetLoggerYAML(`not_a_field: nah`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field not_a_field not recognised")

	err = b.AddRateLimitYAML(`{ label: "", local: {} }`)
	require.Error(t, err)
	assert.EqualError(t, err, "a label must be specified for rate limit resources")
}

func TestStreamBuilderSetCoreYAML(t *testing.T) {
	b := service.NewStreamBuilder()
	b.SetThreads(10)
	require.NoError(t, b.SetCoreYAML(`
input:
  kafka: {}

pipeline:
  threads: 5
  processors:
    - type: bloblang
    - type: jmespath

output:
  nats: {}
`))

	act, err := b.AsYAML()
	require.NoError(t, err)

	exp := []string{
		`input:
    label: ""
    kafka:`,
		`buffer:
    none: {}`,
		`pipeline:
    threads: 5
    processors:`,
		`
        - label: ""
          bloblang: ""`,
		`
        - label: ""
          jmespath:
            query: ""`,
		`output:
    label: ""
    nats:`,
	}

	for _, str := range exp {
		assert.Contains(t, act, str)
	}
}
