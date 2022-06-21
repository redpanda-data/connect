package component

import (
	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/log"
)

// Observability is an interface implemented by components that provide a range
// of observability APIs to components. This is primarily done the service-wide
// managers.
type Observability interface {
	Metrics() metrics.Type
	Logger() log.Modular
	Tracer() trace.TracerProvider
}

type mockObs struct{}

func (m mockObs) Metrics() metrics.Type {
	return metrics.Noop()
}

func (m mockObs) Logger() log.Modular {
	return log.Noop()
}

func (m mockObs) Tracer() trace.TracerProvider {
	return trace.NewNoopTracerProvider()
}

// NoopObservability returns an implementation of Observability that does
// nothing.
func NoopObservability() Observability {
	return mockObs{}
}
