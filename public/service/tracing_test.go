package service_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/Jeffail/benthos/v3/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func TestTracing(t *testing.T) {
	config := `
input:
  generate:
    count: 5
    interval: 1us
    mapping: |
      root.id = count("counting the number of messages in my tracing test")

pipeline:
  processors:
    - bloblang: |
        root.count = if this.id % 2 == 0 { throw("nah %v".format(this.id)) } else { this.id }

output:
  drop: {}

logger:
  level: OFF
`

	strmBuilder := service.NewStreamBuilder()
	require.NoError(t, strmBuilder.SetYAML(config))

	strm, trace, err := strmBuilder.BuildTraced()
	require.NoError(t, err)

	require.NoError(t, strm.Run(context.Background()))

	assert.Equal(t, 5, int(trace.TotalInput()))
	assert.Equal(t, 5, int(trace.TotalOutput()))
	assert.Equal(t, 2, int(trace.TotalProcessorErrors()))

	assert.Equal(t, map[string][]service.TracingEvent{
		"root.input": {
			{Type: service.TracingEventProduce, Content: `{"id":1}`},
			{Type: service.TracingEventProduce, Content: `{"id":2}`},
			{Type: service.TracingEventProduce, Content: `{"id":3}`},
			{Type: service.TracingEventProduce, Content: `{"id":4}`},
			{Type: service.TracingEventProduce, Content: `{"id":5}`},
		},
	}, trace.InputEvents())

	assert.Equal(t, map[string][]service.TracingEvent{
		"root.pipeline.processors.0": {
			{Type: service.TracingEventConsume, Content: `{"id":1}`},
			{Type: service.TracingEventProduce, Content: `{"count":1}`},
			{Type: service.TracingEventConsume, Content: `{"id":2}`},
			{Type: service.TracingEventProduce, Content: `{"id":2}`},
			{Type: service.TracingEventError, Content: `failed assignment (line 1): nah 2`},
			{Type: service.TracingEventConsume, Content: `{"id":3}`},
			{Type: service.TracingEventProduce, Content: `{"count":3}`},
			{Type: service.TracingEventConsume, Content: `{"id":4}`},
			{Type: service.TracingEventProduce, Content: `{"id":4}`},
			{Type: service.TracingEventError, Content: `failed assignment (line 1): nah 4`},
			{Type: service.TracingEventConsume, Content: `{"id":5}`},
			{Type: service.TracingEventProduce, Content: `{"count":5}`},
		},
	}, trace.ProcessorEvents())

	assert.Equal(t, map[string][]service.TracingEvent{
		"root.output": {
			{Type: service.TracingEventConsume, Content: `{"count":1}`},
			{Type: service.TracingEventConsume, Content: `{"id":2}`},
			{Type: service.TracingEventConsume, Content: `{"count":3}`},
			{Type: service.TracingEventConsume, Content: `{"id":4}`},
			{Type: service.TracingEventConsume, Content: `{"count":5}`},
		},
	}, trace.OutputEvents())
}

func BenchmarkStreamTracing(b *testing.B) {
	config := `
input:
  generate:
    count: 5
    interval: ""
    mapping: |
      root.id = uuid_v4()

pipeline:
  processors:
    - bloblang: 'root = this'

output:
  drop: {}

logger:
  level: OFF
`

	strmBuilder := service.NewStreamBuilder()
	strmBuilder.SetHTTPMux(disabledMux{})
	require.NoError(b, strmBuilder.SetYAML(config))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		strm, trace, err := strmBuilder.BuildTraced()
		require.NoError(b, err)

		require.NoError(b, strm.Run(context.Background()))

		assert.Equal(b, 5, int(trace.TotalInput()))
		assert.Equal(b, 5, int(trace.TotalOutput()))
		assert.Equal(b, 0, int(trace.TotalProcessorErrors()))
	}
}

func BenchmarkStreamTracingOutputN1(b *testing.B) {
	benchmarkStreamTracingOutputNX(b, 1)
}

func BenchmarkStreamTracingOutputN10(b *testing.B) {
	benchmarkStreamTracingOutputNX(b, 10)
}

func BenchmarkStreamTracingOutputN100(b *testing.B) {
	benchmarkStreamTracingOutputNX(b, 100)
}

func benchmarkStreamTracingOutputNX(b *testing.B, size int) {
	var outputsBuf bytes.Buffer
	for i := 0; i < size; i++ {
		outputsBuf.WriteString("      - custom: {}\n")
	}

	config := fmt.Sprintf(`
input:
  generate:
    count: 5
    interval: ""
    mapping: |
      root.id = uuid_v4()

pipeline:
  processors:
    - bloblang: 'root = this'

output:
  broker:
    outputs:
%v

logger:
  level: OFF
`, outputsBuf.String())

	env := service.NewEnvironment()
	require.NoError(b, env.RegisterOutput(
		"custom",
		service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			return &noopOutput{}, 1, nil
		},
	))

	strmBuilder := env.NewStreamBuilder()
	strmBuilder.SetHTTPMux(disabledMux{})
	require.NoError(b, strmBuilder.SetYAML(config))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		strm, trace, err := strmBuilder.BuildTraced()
		require.NoError(b, err)

		require.NoError(b, strm.Run(context.Background()))

		assert.Equal(b, 5, int(trace.TotalInput()))
	}
}
