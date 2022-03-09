package bundle

import (
	"fmt"
	"sort"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/tracer"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

// AllTracers is a set containing every single tracer that has been imported.
var AllTracers = &TracerSet{
	specs: map[string]tracerSpec{},
}

//------------------------------------------------------------------------------

// TracerConstructor constructs an tracer component.
type TracerConstructor func(tracer.Config) (tracer.Type, error)

type tracerSpec struct {
	constructor TracerConstructor
	spec        docs.ComponentSpec
}

// TracerSet contains an explicit set of tracers available to a Benthos service.
type TracerSet struct {
	specs map[string]tracerSpec
}

// Add a new tracer to this set by providing a spec (name, documentation, and
// constructor).
func (s *TracerSet) Add(constructor TracerConstructor, spec docs.ComponentSpec) error {
	if !nameRegexp.MatchString(spec.Name) {
		return fmt.Errorf("component name '%v' does not match the required regular expression /%v/", spec.Name, nameRegexpRaw)
	}
	if s.specs == nil {
		s.specs = map[string]tracerSpec{}
	}
	s.specs[spec.Name] = tracerSpec{
		constructor: constructor,
		spec:        spec,
	}
	docs.RegisterDocs(spec)
	return nil
}

// Init attempts to initialise an tracer from a config.
func (s *TracerSet) Init(conf tracer.Config) (tracer.Type, error) {
	spec, exists := s.specs[conf.Type]
	if !exists {
		return nil, component.ErrInvalidTracerType
	}
	return spec.constructor(conf)
}

// Docs returns a slice of tracer specs, which document each method.
func (s *TracerSet) Docs() []docs.ComponentSpec {
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
func (s *TracerSet) DocsFor(name string) (docs.ComponentSpec, bool) {
	c, ok := s.specs[name]
	if !ok {
		return docs.ComponentSpec{}, false
	}
	return c.spec, true
}
