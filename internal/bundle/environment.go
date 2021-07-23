package bundle

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
)

// Environment is a collection of Benthos component plugins that can be used in
// order to build and run streaming pipelines with access to different sets of
// plugins. This is useful for sandboxing, testing, etc.
type Environment struct {
	Buffers    *BufferSet
	Caches     *CacheSet
	Inputs     *InputSet
	Outputs    *OutputSet
	Processors *ProcessorSet
	RateLimits *RateLimitSet
}

// NewEnvironment creates an empty environment.
func NewEnvironment() *Environment {
	return &Environment{
		Buffers:    &BufferSet{},
		Caches:     &CacheSet{},
		Inputs:     &InputSet{},
		Outputs:    &OutputSet{},
		Processors: &ProcessorSet{},
		RateLimits: &RateLimitSet{},
	}
}

// Clone an existing environment to a new one that can be modified
// independently.
func (e *Environment) Clone() *Environment {
	newEnv := NewEnvironment()
	for _, v := range e.Buffers.specs {
		newEnv.Buffers.Add(v.constructor, v.spec)
	}
	for _, v := range e.Caches.specs {
		newEnv.Caches.Add(v.constructor, v.spec)
	}
	for _, v := range e.Inputs.specs {
		newEnv.Inputs.Add(v.constructor, v.spec)
	}
	for _, v := range e.Outputs.specs {
		newEnv.Outputs.Add(v.constructor, v.spec)
	}
	for _, v := range e.Processors.specs {
		newEnv.Processors.Add(v.constructor, v.spec)
	}
	for _, v := range e.RateLimits.specs {
		newEnv.RateLimits.Add(v.constructor, v.spec)
	}
	return newEnv
}

// GetDocs returns a documentation spec for an implementation of a component.
func (e *Environment) GetDocs(name string, ctype docs.Type) (docs.ComponentSpec, bool) {
	var spec docs.ComponentSpec
	var ok bool

	switch ctype {
	case docs.TypeBuffer:
		spec, ok = e.Buffers.DocsFor(name)
	case docs.TypeCache:
		spec, ok = e.Caches.DocsFor(name)
	case docs.TypeInput:
		spec, ok = e.Inputs.DocsFor(name)
	case docs.TypeOutput:
		spec, ok = e.Outputs.DocsFor(name)
	case docs.TypeProcessor:
		spec, ok = e.Processors.DocsFor(name)
	case docs.TypeRateLimit:
		spec, ok = e.RateLimits.DocsFor(name)
	default:
		return docs.GetDocs(nil, name, ctype)
	}

	return spec, ok
}

// GlobalEnvironment contains service-wide singleton bundles.
var GlobalEnvironment = &Environment{
	Buffers:    AllBuffers,
	Caches:     AllCaches,
	Inputs:     AllInputs,
	Outputs:    AllOutputs,
	Processors: AllProcessors,
	RateLimits: AllRateLimits,
}
