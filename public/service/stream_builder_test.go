package service_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/public/service"
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

func TestStreamBuilderProducerFunc(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "stream_builder_producer_test")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	outFilePath := filepath.Join(tmpDir, "out.txt")

	b := service.NewStreamBuilder()
	require.NoError(t, b.SetLoggerYAML("level: NONE"))
	require.NoError(t, b.AddProcessorYAML(`bloblang: 'root = content().uppercase()'`))
	require.NoError(t, b.AddOutputYAML(fmt.Sprintf(`
file:
  codec: lines
  path: %v`, outFilePath)))

	pushFn, err := b.AddProducerFunc()
	require.NoError(t, err)

	// Fails on second call.
	_, err = b.AddProducerFunc()
	require.Error(t, err)

	// Don't allow input overrides now.
	err = b.SetYAML(`input: {}`)
	require.Error(t, err)

	strm, err := b.Build()
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		ctx, done := context.WithTimeout(context.Background(), time.Second*10)
		defer done()

		require.NoError(t, pushFn(ctx, service.NewMessage([]byte("hello world 1"))))
		require.NoError(t, pushFn(ctx, service.NewMessage([]byte("hello world 2"))))
		require.NoError(t, pushFn(ctx, service.NewMessage([]byte("hello world 3"))))

		require.NoError(t, strm.StopWithin(time.Second*5))
	}()

	require.NoError(t, strm.Run(context.Background()))
	wg.Wait()

	outBytes, err := ioutil.ReadFile(outFilePath)
	require.NoError(t, err)

	assert.Equal(t, "HELLO WORLD 1\nHELLO WORLD 2\nHELLO WORLD 3\n", string(outBytes))
}

func TestStreamBuilderConsumerFunc(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "stream_builder_consumer_test")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	inFilePath := filepath.Join(tmpDir, "in.txt")
	require.NoError(t, ioutil.WriteFile(inFilePath, []byte(`HELLO WORLD 1
HELLO WORLD 2
HELLO WORLD 3`), 0755))

	b := service.NewStreamBuilder()
	require.NoError(t, b.SetLoggerYAML("level: NONE"))
	require.NoError(t, b.AddInputYAML(fmt.Sprintf(`
file:
  codec: lines
  paths: [ %v ]`, inFilePath)))
	require.NoError(t, b.AddProcessorYAML(`bloblang: 'root = content().lowercase()'`))

	outMsgs := map[string]struct{}{}
	var outMut sync.Mutex
	handler := func(_ context.Context, m *service.Message) error {
		outMut.Lock()
		defer outMut.Unlock()

		b, err := m.AsBytes()
		assert.NoError(t, err)

		outMsgs[string(b)] = struct{}{}
		return nil
	}
	require.NoError(t, b.AddConsumerFunc(handler))

	// Fails on second call.
	require.Error(t, b.AddConsumerFunc(handler))

	// Don't allow output overrides now.
	err = b.SetYAML(`output: {}`)
	require.Error(t, err)

	strm, err := b.Build()
	require.NoError(t, err)

	require.NoError(t, strm.Run(context.Background()))

	outMut.Lock()
	assert.Equal(t, map[string]struct{}{
		"hello world 1": {},
		"hello world 2": {},
		"hello world 3": {},
	}, outMsgs)
	outMut.Unlock()
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

func TestStreamBuilderSetResourcesYAML(t *testing.T) {
	b := service.NewStreamBuilder()
	require.NoError(t, b.AddResourcesYAML(`
cache_resources:
  - label: foocache
    type: memory

rate_limit_resources:
  - label: foorl
    type: local

processor_resources:
  - label: fooproc1
    type: bloblang
  - label: fooproc2
    type: jmespath

input_resources:
  - label: fooinput
    type: kafka

output_resources:
  - label: foooutput
    type: nats
`))

	act, err := b.AsYAML()
	require.NoError(t, err)

	exp := []string{
		`cache_resources:
    - label: foocache
      memory:`,
		`rate_limit_resources:
    - label: foorl
      local:`,
		`processor_resources:
    - label: fooproc1
      bloblang:`,
		`    - label: fooproc2
      jmespath:`,
		`input_resources:
    - label: fooinput
      kafka:`,
		`output_resources:
    - label: foooutput
      nats:`,
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

	err = b.SetYAML(`not valid ! yaml 34324`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal errors")

	err = b.SetYAML(`input: { foo: nope }`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unable to infer")

	err = b.SetYAML(`input: { kafka: { not_a_field: nope } }`)
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
	require.NoError(t, b.SetYAML(`
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
