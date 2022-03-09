package generic

import (
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
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
