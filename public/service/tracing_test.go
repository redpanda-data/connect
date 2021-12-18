package service_test

import (
	"context"
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
		"input": {
			{Type: service.TracingEventProduce, Content: `{"id":1}`},
			{Type: service.TracingEventProduce, Content: `{"id":2}`},
			{Type: service.TracingEventProduce, Content: `{"id":3}`},
			{Type: service.TracingEventProduce, Content: `{"id":4}`},
			{Type: service.TracingEventProduce, Content: `{"id":5}`},
		},
	}, trace.InputEvents())

	assert.Equal(t, map[string][]service.TracingEvent{
		"pipeline.processor.0": {
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
		"output": {
			{Type: service.TracingEventConsume, Content: `{"count":1}`},
			{Type: service.TracingEventConsume, Content: `{"id":2}`},
			{Type: service.TracingEventConsume, Content: `{"count":3}`},
			{Type: service.TracingEventConsume, Content: `{"id":4}`},
			{Type: service.TracingEventConsume, Content: `{"count":5}`},
		},
	}, trace.OutputEvents())
}
