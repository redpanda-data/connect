package pure

import (
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

func init() {
	_ = bundle.AllMetrics.Add(func(metrics.Config, bundle.NewManagement) (metrics.Type, error) {
		return metrics.Noop(), nil
	}, docs.ComponentSpec{
		Name:    "none",
		Type:    docs.TypeMetrics,
		Summary: `Disable metrics entirely.`,
		Config:  docs.FieldObject("", "").HasDefault(struct{}{}),
	})
}
