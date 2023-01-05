package tracing

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestInitSpansFromParentTextMap(t *testing.T) {
	t.Run("it will update the context for each message in the batch", func(t *testing.T) {
		textMap := map[string]any{
			"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		}

		msgOne := message.NewPart([]byte("hello"))
		msgTwo := message.NewPart([]byte("world"))

		batch := message.Batch([]*message.Part{msgOne, msgTwo})

		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}))
		tp := trace.NewNoopTracerProvider()

		err := InitSpansFromParentTextMap(tp, "test", textMap, batch)
		assert.Nil(t, err)

		spanOne := trace.SpanFromContext(batch[0].GetContext())
		assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", spanOne.SpanContext().TraceID().String())
		assert.Equal(t, "00f067aa0ba902b7", spanOne.SpanContext().SpanID().String())

		spanTwo := trace.SpanFromContext(batch[1].GetContext())
		assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", spanTwo.SpanContext().TraceID().String())
		assert.Equal(t, "00f067aa0ba902b7", spanTwo.SpanContext().SpanID().String())
	})
}
