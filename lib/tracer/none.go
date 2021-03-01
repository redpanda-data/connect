package tracer

import "github.com/Jeffail/benthos/v3/internal/docs"

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNone] = TypeSpec{
		constructor: NewNone,
		Summary: `
Do not send opentracing events anywhere.`,
		config: docs.FieldComponent().Map(),
	}
}

//------------------------------------------------------------------------------

type noopTracer struct{}

func (n noopTracer) Close() error {
	return nil
}

// Noop returns a tracer implementation that does nothing.
func Noop() Type {
	return noopTracer{}
}

// NewNone creates a noop tracer.
func NewNone(config Config, opts ...func(Type)) (Type, error) {
	return Noop(), nil
}

//------------------------------------------------------------------------------
