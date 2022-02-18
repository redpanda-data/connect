package bundle

import (
	"sort"

	imetrics "github.com/Jeffail/benthos/v3/internal/component/metrics"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

// AllMetrics is a set containing every single metrics that has been imported.
var AllMetrics = &MetricsSet{
	specs: map[string]metricsSpec{},
}

//------------------------------------------------------------------------------

// MetricConstructor constructs an metrics component.
type MetricConstructor metrics.ConstructorFunc

type metricsSpec struct {
	constructor MetricConstructor
	spec        docs.ComponentSpec
}

// MetricsSet contains an explicit set of metrics available to a Benthos
// service.
type MetricsSet struct {
	specs map[string]metricsSpec
}

// Add a new metrics to this set by providing a spec (name, documentation, and
// constructor).
func (s *MetricsSet) Add(constructor MetricConstructor, spec docs.ComponentSpec) error {
	if s.specs == nil {
		s.specs = map[string]metricsSpec{}
	}
	s.specs[spec.Name] = metricsSpec{
		constructor: constructor,
		spec:        spec,
	}
	docs.RegisterDocs(spec)
	return nil
}

// Init attempts to initialise an metrics from a config.
func (s *MetricsSet) Init(conf metrics.Config, log log.Modular) (*imetrics.Namespaced, error) {
	spec, exists := s.specs[conf.Type]
	if !exists {
		return nil, metrics.ErrInvalidMetricOutputType
	}

	m, err := spec.constructor(conf, log)
	if err != nil {
		return nil, err
	}

	ns := imetrics.NewNamespaced(m)
	if conf.Mapping != "" {
		mmap, err := imetrics.NewMapping(conf.Mapping, log)
		if err != nil {
			return nil, err
		}
		ns = ns.WithMapping(mmap)
	}
	return ns, nil
}

// Docs returns a slice of metrics specs, which document each method.
func (s *MetricsSet) Docs() []docs.ComponentSpec {
	var docs []docs.ComponentSpec
	for _, v := range s.specs {
		docs = append(docs, v.spec)
	}
	sort.Slice(docs, func(i, j int) bool {
		return docs[i].Name < docs[j].Name
	})
	return docs
}

// DocsFor returns the documentation for a given component name, returns a
// boolean indicating whether the component name exists.
func (s *MetricsSet) DocsFor(name string) (docs.ComponentSpec, bool) {
	c, ok := s.specs[name]
	if !ok {
		return docs.ComponentSpec{}, false
	}
	return c.spec, true
}
