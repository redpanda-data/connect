package tracer

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNone] = TypeSpec{
		constructor: NewNone,
		Description: `
Do not send opentracing events anywhere.`,
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
