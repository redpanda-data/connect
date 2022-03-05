package generic

import (
	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/component/metrics"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/log"
)

func init() {
	_ = bundle.AllMetrics.Add(func(metrics.Config, log.Modular) (metrics.Type, error) {
		return metrics.Noop(), nil
	}, docs.ComponentSpec{
		Name:    "none",
		Type:    docs.TypeMetrics,
		Summary: `Disable metrics entirely.`,
		Config:  docs.FieldComponent().HasType(docs.FieldTypeObject),
	})
}
