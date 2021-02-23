package interop

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/gofrs/uuid"
)

func unwrapMetric(t metrics.Type) metrics.Type {
	u, ok := t.(interface {
		Unwrap() metrics.Type
	})
	if ok {
		t = u.Unwrap()
	}
	return t
}

// LabelRoot replaces the label of the provided observability components.
func LabelRoot(label string, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Manager, log.Modular, metrics.Type) {
	if m, ok := mgr.(interface {
		ForComponent(string) types.Manager
	}); ok {
		newMgr := m.ForComponent(label)
		if m2, ok := newMgr.(interface {
			Logger() log.Modular
			Metrics() metrics.Type
		}); ok {
			return newMgr, m2.Logger(), m2.Metrics()
		}
	}
	newLog := logger.WithFields(map[string]string{
		"component": label,
	})
	newStats := metrics.Namespaced(unwrapMetric(stats), label)
	return mgr, newLog, newStats
}

// LabelChild expands the label of the provided observability components.
func LabelChild(label string, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Manager, log.Modular, metrics.Type) {
	if m, ok := mgr.(interface {
		ForChildComponent(string) types.Manager
	}); ok {
		newMgr := m.ForChildComponent(label)
		if m2, ok := newMgr.(interface {
			Logger() log.Modular
			Metrics() metrics.Type
		}); ok {
			return newMgr, m2.Logger(), m2.Metrics()
		}
	}
	newLog := logger.NewModule("." + label)
	newStats := metrics.Namespaced(stats, label)
	return mgr, newLog, newStats
}

// GetLabel attempts the extract the current label of a component by obtaining
// it from a manager. If the manager does not support label methods then it
// instead falls back to a UUID, and somehow failing that returns an empty
// string.
func GetLabel(mgr types.Manager) string {
	if m, ok := mgr.(interface {
		Label() string
	}); ok {
		return m.Label()
	}
	b, err := uuid.NewV4()
	if err == nil {
		return b.String()
	}
	return ""
}
