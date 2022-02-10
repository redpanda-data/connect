package bundle

import (
	"sort"

	"github.com/Jeffail/benthos/v3/internal/component"
	ibuffer "github.com/Jeffail/benthos/v3/internal/component/buffer"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/buffer"
)

// AllBuffers is a set containing every single buffer that has been imported.
var AllBuffers = &BufferSet{
	specs: map[string]bufferSpec{},
}

//------------------------------------------------------------------------------

// BufferAdd adds a new buffer to this environment by providing a constructor
// and documentation.
func (e *Environment) BufferAdd(constructor BufferConstructor, spec docs.ComponentSpec) error {
	return e.buffers.Add(constructor, spec)
}

// BufferInit attempts to initialise a buffer from a config.
func (e *Environment) BufferInit(conf buffer.Config, mgr NewManagement) (ibuffer.Streamed, error) {
	return e.buffers.Init(conf, mgr)
}

// BufferDocs returns a slice of buffer specs, which document each method.
func (e *Environment) BufferDocs() []docs.ComponentSpec {
	return e.buffers.Docs()
}

//------------------------------------------------------------------------------

// BufferConstructor constructs an buffer component.
type BufferConstructor func(buffer.Config, NewManagement) (ibuffer.Streamed, error)

type bufferSpec struct {
	constructor BufferConstructor
	spec        docs.ComponentSpec
}

// BufferSet contains an explicit set of buffers available to a Benthos service.
type BufferSet struct {
	specs map[string]bufferSpec
}

// Add a new buffer to this set by providing a spec (name, documentation, and
// constructor).
func (s *BufferSet) Add(constructor BufferConstructor, spec docs.ComponentSpec) error {
	if s.specs == nil {
		s.specs = map[string]bufferSpec{}
	}
	s.specs[spec.Name] = bufferSpec{
		constructor: constructor,
		spec:        spec,
	}
	docs.RegisterDocs(spec)
	return nil
}

// Init attempts to initialise an buffer from a config.
func (s *BufferSet) Init(conf buffer.Config, mgr NewManagement) (ibuffer.Streamed, error) {
	spec, exists := s.specs[conf.Type]
	if !exists {
		return nil, component.ErrInvalidBufferType
	}
	return spec.constructor(conf, mgr)
}

// Docs returns a slice of buffer specs, which document each method.
func (s *BufferSet) Docs() []docs.ComponentSpec {
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
func (s *BufferSet) DocsFor(name string) (docs.ComponentSpec, bool) {
	c, ok := s.specs[name]
	if !ok {
		return docs.ComponentSpec{}, false
	}
	return c.spec, true
}
