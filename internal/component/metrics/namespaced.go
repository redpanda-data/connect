package metrics

import (
	"sort"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

// Namespaced wraps a child metrics exporter and exposes a metrics.Type API that
// adds namespacing labels and name prefixes to new metrics.
type Namespaced struct {
	prefix   string
	labels   map[string]string
	mappings []*Mapping
	child    metrics.Type
}

// NewNamespaced wraps a metrics exporter and adds prefixes and custom labels.
func NewNamespaced(child metrics.Type) *Namespaced {
	return &Namespaced{
		child: child,
	}
}

// WithPrefix returns a namespaced metrics exporter with a new prefix.
func (n *Namespaced) WithPrefix(str string) *Namespaced {
	newNs := *n
	newNs.prefix = str
	return &newNs
}

// WithLabels returns a namespaced metrics exporter with a new set of labels,
// which are added to any prior labels.
func (n *Namespaced) WithLabels(labels ...string) *Namespaced {
	newLabels := map[string]string{}
	for k, v := range n.labels {
		newLabels[k] = v
	}
	for i := 0; i < len(labels)-1; i += 2 {
		newLabels[labels[i]] = labels[i+1]
	}
	newNs := *n
	newNs.labels = newLabels
	return &newNs
}

// WithMapping returns a namespaced metrics exporter with a new mapping.
// Mappings are applied _before_ the prefix and static labels are applied.
// Mappings already added are executed after this new mapping.
func (n *Namespaced) WithMapping(m *Mapping) *Namespaced {
	newNs := *n
	newMappings := make([]*Mapping, 0, len(n.mappings)+1)
	newMappings = append(newMappings, m)
	newMappings = append(newMappings, n.mappings...)
	newNs.mappings = newMappings
	return &newNs
}

// Unwrap to the underlying metrics type.
func (n *Namespaced) Unwrap() metrics.Type {
	u, ok := n.child.(interface {
		Unwrap() metrics.Type
	})
	if ok {
		return u.Unwrap()
	}
	return n.child
}

//------------------------------------------------------------------------------

func (n *Namespaced) getPathAndLabels(path string) (newPath string, labelKeys, labelValues []string) {
	newPath = path
	// TODO: V4 Don't do this
	if n.prefix != "" {
		newPath = n.prefix + "." + path
	}
	if n.labels != nil && len(n.labels) > 0 {
		labelKeys = make([]string, 0, len(n.labels))
		for k := range n.labels {
			labelKeys = append(labelKeys, k)
		}
		sort.Strings(labelKeys)
		labelValues = make([]string, 0, len(n.labels))
		for _, k := range labelKeys {
			labelValues = append(labelValues, n.labels[k])
		}
	}
	for _, mapping := range n.mappings {
		newPath, labelKeys, labelValues = mapping.mapPath(newPath, labelKeys, labelValues)
	}
	return
}

type counterVecWithStatic struct {
	staticValues []string
	child        metrics.StatCounterVec
}

func (c *counterVecWithStatic) With(values ...string) metrics.StatCounter {
	newValues := make([]string, 0, len(c.staticValues)+len(values))
	newValues = append(newValues, c.staticValues...)
	newValues = append(newValues, values...)
	return c.child.With(newValues...)
}

type timerVecWithStatic struct {
	staticValues []string
	child        metrics.StatTimerVec
}

func (c *timerVecWithStatic) With(values ...string) metrics.StatTimer {
	newValues := make([]string, 0, len(c.staticValues)+len(values))
	newValues = append(newValues, c.staticValues...)
	newValues = append(newValues, values...)
	return c.child.With(newValues...)
}

type gaugeVecWithStatic struct {
	staticValues []string
	child        metrics.StatGaugeVec
}

func (c *gaugeVecWithStatic) With(values ...string) metrics.StatGauge {
	newValues := make([]string, 0, len(c.staticValues)+len(values))
	newValues = append(newValues, c.staticValues...)
	newValues = append(newValues, values...)
	return c.child.With(newValues...)
}

//------------------------------------------------------------------------------

// GetCounter returns an editable counter stat for a given path.
func (n *Namespaced) GetCounter(path string) metrics.StatCounter {
	path, labelKeys, labelValues := n.getPathAndLabels(path)
	if len(labelKeys) > 0 {
		return n.child.GetCounterVec(path, labelKeys).With(labelValues...)
	}
	return n.child.GetCounter(path)
}

// GetCounterVec returns an editable counter stat for a given path with labels,
// these labels must be consistent with any other metrics registered on the same
// path.
func (n *Namespaced) GetCounterVec(path string, labelNames []string) metrics.StatCounterVec {
	path, staticKeys, staticValues := n.getPathAndLabels(path)
	if len(staticKeys) > 0 {
		newNames := make([]string, 0, len(staticKeys)+len(labelNames))
		newNames = append(newNames, staticKeys...)
		newNames = append(newNames, labelNames...)
		return &counterVecWithStatic{
			staticValues: staticValues,
			child:        n.child.GetCounterVec(path, newNames),
		}
	}
	return n.child.GetCounterVec(path, labelNames)
}

// GetTimer returns an editable timer stat for a given path.
func (n *Namespaced) GetTimer(path string) metrics.StatTimer {
	path, labelKeys, labelValues := n.getPathAndLabels(path)
	if len(labelKeys) > 0 {
		return n.child.GetTimerVec(path, labelKeys).With(labelValues...)
	}
	return n.child.GetTimer(path)
}

// GetTimerVec returns an editable timer stat for a given path with labels,
// these labels must be consistent with any other metrics registered on the same
// path.
func (n *Namespaced) GetTimerVec(path string, labelNames []string) metrics.StatTimerVec {
	path, staticKeys, staticValues := n.getPathAndLabels(path)
	if len(staticKeys) > 0 {
		newNames := make([]string, 0, len(staticKeys)+len(labelNames))
		newNames = append(newNames, staticKeys...)
		newNames = append(newNames, labelNames...)
		return &timerVecWithStatic{
			staticValues: staticValues,
			child:        n.child.GetTimerVec(path, newNames),
		}
	}
	return n.child.GetTimerVec(path, labelNames)
}

// GetGauge returns an editable gauge stat for a given path.
func (n *Namespaced) GetGauge(path string) metrics.StatGauge {
	path, labelKeys, labelValues := n.getPathAndLabels(path)
	if len(labelKeys) > 0 {
		return n.child.GetGaugeVec(path, labelKeys).With(labelValues...)
	}
	return n.child.GetGauge(path)
}

// GetGaugeVec returns an editable gauge stat for a given path with labels,
// these labels must be consistent with any other metrics registered on the same
// path.
func (n *Namespaced) GetGaugeVec(path string, labelNames []string) metrics.StatGaugeVec {
	path, staticKeys, staticValues := n.getPathAndLabels(path)
	if len(staticKeys) > 0 {
		newNames := make([]string, 0, len(staticKeys)+len(labelNames))
		newNames = append(newNames, staticKeys...)
		newNames = append(newNames, labelNames...)
		return &gaugeVecWithStatic{
			staticValues: staticValues,
			child:        n.child.GetGaugeVec(path, newNames),
		}
	}
	return n.child.GetGaugeVec(path, labelNames)
}

// SetLogger sets the logging mechanism of the metrics type.
func (n *Namespaced) SetLogger(log log.Modular) {
	n.child.SetLogger(log)
}

// Close stops aggregating stats and cleans up resources.
func (n *Namespaced) Close() error {
	return n.child.Close()
}
