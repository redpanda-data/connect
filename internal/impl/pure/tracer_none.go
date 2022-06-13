package pure

import (
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/tracer"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

func init() {
	_ = bundle.AllTracers.Add(func(c tracer.Config, nm bundle.NewManagement) (tracer.Type, error) {
		return noopTracer{}, nil
	}, docs.ComponentSpec{
		Name:    "none",
		Type:    docs.TypeTracer,
		Status:  docs.StatusStable,
		Summary: `Do not send tracing events anywhere.`,
		Config:  docs.FieldObject("", ""),
	})
}

//------------------------------------------------------------------------------

type noopTracer struct{}

func (n noopTracer) Close() error {
	return nil
}
