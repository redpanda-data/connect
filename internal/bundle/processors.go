package bundle

import (
	"sort"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// AllProcessors is a set containing every single processor that has been
// imported.
var AllProcessors = &ProcessorSet{
	specs: map[string]processorSpec{},
}

//------------------------------------------------------------------------------

// ProcessorAdd adds a new processor to this environment by providing a
// constructor and documentation.
func (e *Environment) ProcessorAdd(constructor ProcessorConstructor, spec docs.ComponentSpec) error {
	return e.processors.Add(constructor, spec)
}

// ProcessorInit attempts to initialise a processor from a config.
func (e *Environment) ProcessorInit(conf processor.Config, mgr NewManagement) (types.Processor, error) {
	return e.processors.Init(conf, mgr)
}

// ProcessorDocs returns a slice of processor specs, which document each method.
func (e *Environment) ProcessorDocs() []docs.ComponentSpec {
	return e.processors.Docs()
}

//------------------------------------------------------------------------------

// ProcessorConstructor constructs an processor component.
type ProcessorConstructor func(processor.Config, NewManagement) (processor.Type, error)

type processorSpec struct {
	constructor ProcessorConstructor
	spec        docs.ComponentSpec
}

// ProcessorSet contains an explicit set of processors available to a Benthos
// service.
type ProcessorSet struct {
	specs map[string]processorSpec
}

// Add a new processor to this set by providing a spec (name, documentation, and
// constructor).
func (s *ProcessorSet) Add(constructor ProcessorConstructor, spec docs.ComponentSpec) error {
	if s.specs == nil {
		s.specs = map[string]processorSpec{}
	}
	s.specs[spec.Name] = processorSpec{
		constructor: constructor,
		spec:        spec,
	}
	docs.RegisterDocs(spec)
	return nil
}

// Init attempts to initialise an processor from a config.
func (s *ProcessorSet) Init(conf processor.Config, mgr NewManagement) (types.Processor, error) {
	spec, exists := s.specs[conf.Type]
	if !exists {
		return nil, types.ErrInvalidProcessorType
	}
	return spec.constructor(conf, mgr)
}

// Docs returns a slice of processor specs, which document each method.
func (s *ProcessorSet) Docs() []docs.ComponentSpec {
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
func (s *ProcessorSet) DocsFor(name string) (docs.ComponentSpec, bool) {
	c, ok := s.specs[name]
	if !ok {
		return docs.ComponentSpec{}, false
	}
	return c.spec, true
}
