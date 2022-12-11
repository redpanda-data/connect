package service_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/public/service"

	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestOtelTracingPlugin(t *testing.T) {
	env := service.NewEnvironment()
	confSpec := service.NewConfigSpec().Field(service.NewStringField("foo"))

	var testValue string

	require.NoError(t, env.RegisterOtelTracerProvider(
		"meow", confSpec,
		func(conf *service.ParsedConfig) (trace.TracerProvider, error) {
			testStr, err := conf.FieldString("foo")
			if err != nil {
				return nil, err
			}
			testValue = testStr
			return trace.NewNoopTracerProvider(), nil
		}))

	builder := env.NewStreamBuilder()
	require.NoError(t, builder.SetYAML(`
input:
  label: fooinput
  generate:
    count: 2
    interval: 1ns
    mapping: 'root.id = uuid_v4()'

output:
  label: foooutput
  drop: {}

tracer:
  meow:
    foo: foo value from config
`))

	strm, err := builder.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	require.NoError(t, strm.Run(ctx))

	assert.Equal(t, "foo value from config", testValue)
}

func TestTracing(t *testing.T) {
	u, err := uuid.NewV4()
	require.NoError(t, err)

	config := `
input:
  generate:
    count: 5
    interval: 1us
    mapping: |
      root.id = count("` + u.String() + `")

pipeline:
  threads: 1
  processors:
    - bloblang: |
        root.count = if this.id % 2 == 0 { throw("nah %v".format(this.id)) } else { this.id }
        meta foo = this.id

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

	type tMap = map[string]any

	assert.Equal(t, map[string][]service.TracingEvent{
		"root.input": {
			{Type: service.TracingEventProduce, Content: `{"id":1}`, Meta: tMap{}},
			{Type: service.TracingEventProduce, Content: `{"id":2}`, Meta: tMap{}},
			{Type: service.TracingEventProduce, Content: `{"id":3}`, Meta: tMap{}},
			{Type: service.TracingEventProduce, Content: `{"id":4}`, Meta: tMap{}},
			{Type: service.TracingEventProduce, Content: `{"id":5}`, Meta: tMap{}},
		},
	}, trace.InputEvents())

	assert.Equal(t, map[string][]service.TracingEvent{
		"root.pipeline.processors.0": {
			{Type: service.TracingEventConsume, Content: `{"id":1}`, Meta: tMap{}},
			{Type: service.TracingEventProduce, Content: `{"count":1}`, Meta: tMap{"foo": int64(1)}},
			{Type: service.TracingEventConsume, Content: `{"id":2}`, Meta: tMap{}},
			{Type: service.TracingEventProduce, Content: `{"id":2}`, Meta: tMap{}},
			{Type: service.TracingEventError, Content: `failed assignment (line 1): nah 2`},
			{Type: service.TracingEventConsume, Content: `{"id":3}`, Meta: tMap{}},
			{Type: service.TracingEventProduce, Content: `{"count":3}`, Meta: tMap{"foo": int64(3)}},
			{Type: service.TracingEventConsume, Content: `{"id":4}`, Meta: tMap{}},
			{Type: service.TracingEventProduce, Content: `{"id":4}`, Meta: tMap{}},
			{Type: service.TracingEventError, Content: `failed assignment (line 1): nah 4`},
			{Type: service.TracingEventConsume, Content: `{"id":5}`, Meta: tMap{}},
			{Type: service.TracingEventProduce, Content: `{"count":5}`, Meta: tMap{"foo": int64(5)}},
		},
	}, trace.ProcessorEvents())

	assert.Equal(t, map[string][]service.TracingEvent{
		"root.output": {
			{Type: service.TracingEventConsume, Content: `{"count":1}`, Meta: tMap{"foo": int64(1)}},
			{Type: service.TracingEventConsume, Content: `{"id":2}`, Meta: tMap{}},
			{Type: service.TracingEventConsume, Content: `{"count":3}`, Meta: tMap{"foo": int64(3)}},
			{Type: service.TracingEventConsume, Content: `{"id":4}`, Meta: tMap{}},
			{Type: service.TracingEventConsume, Content: `{"count":5}`, Meta: tMap{"foo": int64(5)}},
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
  threads: 1
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
  threads: 1
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
