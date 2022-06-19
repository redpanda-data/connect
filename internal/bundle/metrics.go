package bundle

import (
	"fmt"
	"sort"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

// AllMetrics is a set containing every single metrics that has been imported.
var AllMetrics = &MetricsSet{
	specs: map[string]metricsSpec{},
}

//------------------------------------------------------------------------------

// MetricsAdd adds a new metrics exporter to this environment by providing a
// constructor and documentation.
func (e *Environment) MetricsAdd(constructor MetricConstructor, spec docs.ComponentSpec) error {
	return e.metrics.Add(constructor, spec)
}

// MetricsInit attempts to initialise a metrics exporter from a config.
func (e *Environment) MetricsInit(conf metrics.Config, nm NewManagement) (*metrics.Namespaced, error) {
	return e.metrics.Init(conf, nm)
}

// MetricsDocs returns a slice of metrics exporter specs.
func (e *Environment) MetricsDocs() []docs.ComponentSpec {
	return e.metrics.Docs()
}

//------------------------------------------------------------------------------

// MetricConstructor constructs an metrics component.
type MetricConstructor func(conf metrics.Config, nm NewManagement) (metrics.Type, error)

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
	if !nameRegexp.MatchString(spec.Name) {
		return fmt.Errorf("component name '%v' does not match the required regular expression /%v/", spec.Name, nameRegexpRaw)
	}
	if s.specs == nil {
		s.specs = map[string]metricsSpec{}
	}
	spec.Type = docs.TypeMetrics
	s.specs[spec.Name] = metricsSpec{
		constructor: constructor,
		spec:        spec,
	}
	docs.DeprecatedProvider.RegisterDocs(spec)
	return nil
}

// Init attempts to initialise an metrics from a config.
func (s *MetricsSet) Init(conf metrics.Config, nm NewManagement) (*metrics.Namespaced, error) {
	spec, exists := s.specs[conf.Type]
	if !exists {
		return nil, component.ErrInvalidType("metric", conf.Type)
	}

	m, err := spec.constructor(conf, nm)
	if err != nil {
		return nil, err
	}

	ns := metrics.NewNamespaced(m)
	if conf.Mapping != "" {
		mmap, err := metrics.NewMapping(conf.Mapping, nm.Logger())
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
