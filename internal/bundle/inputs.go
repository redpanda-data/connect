package bundle

import (
	"fmt"
	"sort"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// AllInputs is a set containing every single input that has been imported.
var AllInputs = &InputSet{
	specs: map[string]inputSpec{},
}

//------------------------------------------------------------------------------

// InputAdd adds a new input to this environment by providing a constructor and
// documentation.
func (e *Environment) InputAdd(constructor InputConstructor, spec docs.ComponentSpec) error {
	return e.inputs.Add(constructor, spec)
}

// InputInit attempts to initialise an input from a config.
func (e *Environment) InputInit(
	hasBatchProc bool,
	conf input.Config,
	mgr NewManagement,
	pipelines ...types.PipelineConstructorFunc,
) (types.Input, error) {
	return e.inputs.Init(hasBatchProc, conf, mgr, pipelines...)
}

// InputDocs returns a slice of input specs, which document each method.
func (e *Environment) InputDocs() []docs.ComponentSpec {
	return e.inputs.Docs()
}

//------------------------------------------------------------------------------

// InputConstructorFromSimple provides a way to define an input constructor
// without manually initializing processors of the config.
func InputConstructorFromSimple(fn func(input.Config, NewManagement) (input.Type, error)) InputConstructor {
	return func(b bool, c input.Config, nm NewManagement, pcf ...types.PipelineConstructorFunc) (input.Type, error) {
		i, err := fn(c, nm)
		if err != nil {
			return nil, fmt.Errorf("failed to create input '%v': %w", c.Type, err)
		}
		pcf = input.AppendProcessorsFromConfig(c, nm, nm.Logger(), nm.Metrics(), pcf...)
		return input.WrapWithPipelines(i, pcf...)
	}
}

//------------------------------------------------------------------------------

// InputConstructor constructs an input component.
type InputConstructor func(bool, input.Config, NewManagement, ...types.PipelineConstructorFunc) (input.Type, error)

type inputSpec struct {
	constructor InputConstructor
	spec        docs.ComponentSpec
}

// InputSet contains an explicit set of inputs available to a Benthos service.
type InputSet struct {
	specs map[string]inputSpec
}

// Add a new input to this set by providing a constructor and documentation.
func (s *InputSet) Add(constructor InputConstructor, spec docs.ComponentSpec) error {
	if s.specs == nil {
		s.specs = map[string]inputSpec{}
	}
	s.specs[spec.Name] = inputSpec{
		constructor: constructor,
		spec:        spec,
	}
	docs.RegisterDocs(spec)
	return nil
}

// Init attempts to initialise an input from a config.
func (s *InputSet) Init(
	hasBatchProc bool,
	conf input.Config,
	mgr NewManagement,
	pipelines ...types.PipelineConstructorFunc,
) (types.Input, error) {
	spec, exists := s.specs[conf.Type]
	if !exists {
		return nil, types.ErrInvalidInputType
	}
	return spec.constructor(hasBatchProc, conf, mgr, pipelines...)
}

// Docs returns a slice of input specs, which document each method.
func (s *InputSet) Docs() []docs.ComponentSpec {
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
func (s *InputSet) DocsFor(name string) (docs.ComponentSpec, bool) {
	c, ok := s.specs[name]
	if !ok {
		return docs.ComponentSpec{}, false
	}
	return c.spec, true
}
