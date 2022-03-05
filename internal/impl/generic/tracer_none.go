package generic

import (
	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/component/tracer"
	"github.com/Jeffail/benthos/v3/internal/docs"
)

func init() {
	_ = bundle.AllTracers.Add(func(c tracer.Config) (tracer.Type, error) {
		return noopTracer{}, nil
	}, docs.ComponentSpec{
		Name:    "none",
		Type:    docs.TypeTracer,
		Status:  docs.StatusStable,
		Summary: `Do not send tracing events anywhere.`,
		Config:  docs.FieldComponent().HasType(docs.FieldTypeObject),
	})
}

//------------------------------------------------------------------------------

type noopTracer struct{}

func (n noopTracer) Close() error {
	return nil
}
