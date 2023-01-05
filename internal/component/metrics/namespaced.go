package metrics

import (
	"net/http"
	"sort"
)

// Namespaced wraps a child metrics exporter and exposes a Type API that
// adds namespacing labels and name prefixes to new.
type Namespaced struct {
	labels   map[string]string
	mappings []*Mapping
	child    Type
}

// NewNamespaced wraps a metrics exporter and adds prefixes and custom labels.
func NewNamespaced(child Type) *Namespaced {
	return &Namespaced{
		child: child,
	}
}

// Noop returns a namespaced metrics aggregator with a noop child.
func Noop() *Namespaced {
	return &Namespaced{
		child: DudType{},
	}
}

// WithStats returns a namespaced metrics exporter with a different stats
// implementation.
func (n *Namespaced) WithStats(s Type) *Namespaced {
	newNs := *n
	newNs.child = s
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

//------------------------------------------------------------------------------

// Child returns the underlying metrics type.
func (n *Namespaced) Child() Type {
	return n.child
}

// HandlerFunc returns the http handler of the child.
func (n *Namespaced) HandlerFunc() http.HandlerFunc {
	return n.child.HandlerFunc()
}

//------------------------------------------------------------------------------

func (n *Namespaced) getPathAndLabels(path string) (newPath string, labelKeys, labelValues []string) {
	newPath = path
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
		if newPath, labelKeys, labelValues = mapping.mapPath(newPath, labelKeys, labelValues); newPath == "" {
			return
		}
	}
	return
}

type counterVecWithStatic struct {
	staticValues []string
	child        StatCounterVec
}

func (c *counterVecWithStatic) With(values ...string) StatCounter {
	newValues := make([]string, 0, len(c.staticValues)+len(values))
	newValues = append(newValues, c.staticValues...)
	newValues = append(newValues, values...)
	return c.child.With(newValues...)
}

type timerVecWithStatic struct {
	staticValues []string
	child        StatTimerVec
}

func (c *timerVecWithStatic) With(values ...string) StatTimer {
	newValues := make([]string, 0, len(c.staticValues)+len(values))
	newValues = append(newValues, c.staticValues...)
	newValues = append(newValues, values...)
	return c.child.With(newValues...)
}

type gaugeVecWithStatic struct {
	staticValues []string
	child        StatGaugeVec
}

func (c *gaugeVecWithStatic) With(values ...string) StatGauge {
	newValues := make([]string, 0, len(c.staticValues)+len(values))
	newValues = append(newValues, c.staticValues...)
	newValues = append(newValues, values...)
	return c.child.With(newValues...)
}

//------------------------------------------------------------------------------

// GetCounter returns an editable counter stat for a given path.
func (n *Namespaced) GetCounter(path string) StatCounter {
	path, labelKeys, labelValues := n.getPathAndLabels(path)
	if path == "" {
		return DudStat{}
	}
	if len(labelKeys) > 0 {
		return n.child.GetCounterVec(path, labelKeys...).With(labelValues...)
	}
	return n.child.GetCounter(path)
}

// GetCounterVec returns an editable counter stat for a given path with labels,
// these labels must be consistent with any other metrics registered on the same
// path.
func (n *Namespaced) GetCounterVec(path string, labelNames ...string) StatCounterVec {
	path, staticKeys, staticValues := n.getPathAndLabels(path)
	if path == "" {
		return FakeCounterVec(func(...string) StatCounter {
			return DudStat{}
		})
	}
	if len(staticKeys) > 0 {
		newNames := make([]string, 0, len(staticKeys)+len(labelNames))
		newNames = append(newNames, staticKeys...)
		newNames = append(newNames, labelNames...)
		return &counterVecWithStatic{
			staticValues: staticValues,
			child:        n.child.GetCounterVec(path, newNames...),
		}
	}
	return n.child.GetCounterVec(path, labelNames...)
}

// GetTimer returns an editable timer stat for a given path.
func (n *Namespaced) GetTimer(path string) StatTimer {
	path, labelKeys, labelValues := n.getPathAndLabels(path)
	if path == "" {
		return DudStat{}
	}
	if len(labelKeys) > 0 {
		return n.child.GetTimerVec(path, labelKeys...).With(labelValues...)
	}
	return n.child.GetTimer(path)
}

// GetTimerVec returns an editable timer stat for a given path with labels,
// these labels must be consistent with any other metrics registered on the same
// path.
func (n *Namespaced) GetTimerVec(path string, labelNames ...string) StatTimerVec {
	path, staticKeys, staticValues := n.getPathAndLabels(path)
	if path == "" {
		return FakeTimerVec(func(...string) StatTimer {
			return DudStat{}
		})
	}
	if len(staticKeys) > 0 {
		newNames := make([]string, 0, len(staticKeys)+len(labelNames))
		newNames = append(newNames, staticKeys...)
		newNames = append(newNames, labelNames...)
		return &timerVecWithStatic{
			staticValues: staticValues,
			child:        n.child.GetTimerVec(path, newNames...),
		}
	}
	return n.child.GetTimerVec(path, labelNames...)
}

// GetGauge returns an editable gauge stat for a given path.
func (n *Namespaced) GetGauge(path string) StatGauge {
	path, labelKeys, labelValues := n.getPathAndLabels(path)
	if path == "" {
		return DudStat{}
	}
	if len(labelKeys) > 0 {
		return n.child.GetGaugeVec(path, labelKeys...).With(labelValues...)
	}
	return n.child.GetGauge(path)
}

// GetGaugeVec returns an editable gauge stat for a given path with labels,
// these labels must be consistent with any other metrics registered on the same
// path.
func (n *Namespaced) GetGaugeVec(path string, labelNames ...string) StatGaugeVec {
	path, staticKeys, staticValues := n.getPathAndLabels(path)
	if path == "" {
		return FakeGaugeVec(func(...string) StatGauge {
			return DudStat{}
		})
	}
	if len(staticKeys) > 0 {
		newNames := make([]string, 0, len(staticKeys)+len(labelNames))
		newNames = append(newNames, staticKeys...)
		newNames = append(newNames, labelNames...)
		return &gaugeVecWithStatic{
			staticValues: staticValues,
			child:        n.child.GetGaugeVec(path, newNames...),
		}
	}
	return n.child.GetGaugeVec(path, labelNames...)
}

// Close stops aggregating stats and cleans up resources.
func (n *Namespaced) Close() error {
	return n.child.Close()
}
