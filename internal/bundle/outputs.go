package bundle

import (
	"fmt"
	"sort"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// AllOutputs is a set containing every single output that has been imported.
var AllOutputs = &OutputSet{
	specs: map[string]outputSpec{},
}

//------------------------------------------------------------------------------

// OutputAdd adds a new output to this environment by providing a constructor
// and documentation.
func (e *Environment) OutputAdd(constructor OutputConstructor, spec docs.ComponentSpec) error {
	return e.outputs.Add(constructor, spec)
}

// OutputInit attempts to initialise a output from a config.
func (e *Environment) OutputInit(
	conf output.Config,
	mgr NewManagement,
	pipelines ...types.PipelineConstructorFunc,
) (types.Output, error) {
	return e.outputs.Init(conf, mgr, pipelines...)
}

// OutputDocs returns a slice of output specs, which document each method.
func (e *Environment) OutputDocs() []docs.ComponentSpec {
	return e.outputs.Docs()
}

//------------------------------------------------------------------------------

// OutputConstructorFromSimple provides a way to define an output constructor
// without manually initializing processors of the config.
func OutputConstructorFromSimple(fn func(output.Config, NewManagement) (output.Type, error)) OutputConstructor {
	return func(c output.Config, nm NewManagement, pcf ...types.PipelineConstructorFunc) (output.Type, error) {
		o, err := fn(c, nm)
		if err != nil {
			return nil, fmt.Errorf("failed to create output '%v': %w", c.Type, err)
		}
		pcf = output.AppendProcessorsFromConfig(c, nm, nm.Logger(), nm.Metrics(), pcf...)
		return output.WrapWithPipelines(o, pcf...)
	}
}

//------------------------------------------------------------------------------

// OutputConstructor constructs an output component.
type OutputConstructor func(output.Config, NewManagement, ...types.PipelineConstructorFunc) (output.Type, error)

type outputSpec struct {
	constructor OutputConstructor
	spec        docs.ComponentSpec
}

// OutputSet contains an explicit set of outputs available to a Benthos service.
type OutputSet struct {
	specs map[string]outputSpec
}

// Add a new output to this set by providing a spec (name, documentation, and
// constructor).
func (s *OutputSet) Add(constructor OutputConstructor, spec docs.ComponentSpec) error {
	if s.specs == nil {
		s.specs = map[string]outputSpec{}
	}
	s.specs[spec.Name] = outputSpec{
		constructor: constructor,
		spec:        spec,
	}
	docs.RegisterDocs(spec)
	return nil
}

// Init attempts to initialise an output from a config.
func (s *OutputSet) Init(
	conf output.Config,
	mgr NewManagement,
	pipelines ...types.PipelineConstructorFunc,
) (types.Output, error) {
	spec, exists := s.specs[conf.Type]
	if !exists {
		return nil, types.ErrInvalidOutputType
	}
	return spec.constructor(conf, mgr, pipelines...)
}

// Docs returns a slice of output specs, which document each method.
func (s *OutputSet) Docs() []docs.ComponentSpec {
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
func (s *OutputSet) DocsFor(name string) (docs.ComponentSpec, bool) {
	c, ok := s.specs[name]
	if !ok {
		return docs.ComponentSpec{}, false
	}
	return c.spec, true
}
