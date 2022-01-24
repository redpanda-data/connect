package bundle

import (
	"sort"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// AllRateLimits is a set containing every single ratelimit that has been imported.
var AllRateLimits = &RateLimitSet{
	specs: map[string]rateLimitSpec{},
}

//------------------------------------------------------------------------------

// RateLimitAdd adds a new ratelimit to this environment by providing a
// constructor and documentation.
func (e *Environment) RateLimitAdd(constructor RateLimitConstructor, spec docs.ComponentSpec) error {
	return e.rateLimits.Add(constructor, spec)
}

// RateLimitInit attempts to initialise a ratelimit from a config.
func (e *Environment) RateLimitInit(conf ratelimit.Config, mgr NewManagement) (types.RateLimit, error) {
	return e.rateLimits.Init(conf, mgr)
}

// RateLimitDocs returns a slice of ratelimit specs, which document each method.
func (e *Environment) RateLimitDocs() []docs.ComponentSpec {
	return e.rateLimits.Docs()
}

//------------------------------------------------------------------------------

// RateLimitConstructor constructs an ratelimit component.
type RateLimitConstructor func(ratelimit.Config, NewManagement) (types.RateLimit, error)

type rateLimitSpec struct {
	constructor RateLimitConstructor
	spec        docs.ComponentSpec
}

// RateLimitSet contains an explicit set of ratelimits available to a Benthos service.
type RateLimitSet struct {
	specs map[string]rateLimitSpec
}

// Add a new ratelimit to this set by providing a spec (name, documentation, and
// constructor).
func (s *RateLimitSet) Add(constructor RateLimitConstructor, spec docs.ComponentSpec) error {
	if s.specs == nil {
		s.specs = map[string]rateLimitSpec{}
	}
	s.specs[spec.Name] = rateLimitSpec{
		constructor: constructor,
		spec:        spec,
	}
	docs.RegisterDocs(spec)
	return nil
}

// Init attempts to initialise an ratelimit from a config.
func (s *RateLimitSet) Init(conf ratelimit.Config, mgr NewManagement) (types.RateLimit, error) {
	spec, exists := s.specs[conf.Type]
	if !exists {
		return nil, types.ErrInvalidRateLimitType
	}
	return spec.constructor(conf, mgr)
}

// Docs returns a slice of ratelimit specs, which document each method.
func (s *RateLimitSet) Docs() []docs.ComponentSpec {
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
func (s *RateLimitSet) DocsFor(name string) (docs.ComponentSpec, bool) {
	c, ok := s.specs[name]
	if !ok {
		return docs.ComponentSpec{}, false
	}
	return c.spec, true
}
