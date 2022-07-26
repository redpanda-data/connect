package bundle

import (
	"fmt"
	"sort"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/docs"
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
func (e *Environment) InputInit(conf input.Config, mgr NewManagement) (input.Streamed, error) {
	return e.inputs.Init(conf, mgr)
}

// InputDocs returns a slice of input specs, which document each method.
func (e *Environment) InputDocs() []docs.ComponentSpec {
	return e.inputs.Docs()
}

//------------------------------------------------------------------------------

// InputConstructor constructs an input component.
type InputConstructor func(input.Config, NewManagement) (input.Streamed, error)

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
	if !nameRegexp.MatchString(spec.Name) {
		return fmt.Errorf("component name '%v' does not match the required regular expression /%v/", spec.Name, nameRegexpRaw)
	}
	if s.specs == nil {
		s.specs = map[string]inputSpec{}
	}
	spec.Type = docs.TypeInput
	s.specs[spec.Name] = inputSpec{
		constructor: constructor,
		spec:        spec,
	}
	docs.DeprecatedProvider.RegisterDocs(spec)
	return nil
}

// Init attempts to initialise an input from a config.
func (s *InputSet) Init(conf input.Config, mgr NewManagement) (input.Streamed, error) {
	spec, exists := s.specs[conf.Type]
	if !exists {
		return nil, component.ErrInvalidType("input", conf.Type)
	}
	c, err := spec.constructor(conf, mgr)
	err = wrapComponentErr(mgr, "input", err)
	return c, err
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
