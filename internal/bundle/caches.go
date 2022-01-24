package bundle

import (
	"sort"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// AllCaches is a set containing every single cache that has been imported.
var AllCaches = &CacheSet{
	specs: map[string]cacheSpec{},
}

//------------------------------------------------------------------------------

// CacheAdd adds a new cache to this environment by providing a constructor
// and documentation.
func (e *Environment) CacheAdd(constructor CacheConstructor, spec docs.ComponentSpec) error {
	return e.caches.Add(constructor, spec)
}

// CacheInit attempts to initialise a cache from a config.
func (e *Environment) CacheInit(conf cache.Config, mgr NewManagement) (types.Cache, error) {
	return e.caches.Init(conf, mgr)
}

// CacheDocs returns a slice of cache specs, which document each method.
func (e *Environment) CacheDocs() []docs.ComponentSpec {
	return e.caches.Docs()
}

//------------------------------------------------------------------------------

// CacheConstructor constructs an cache component.
type CacheConstructor func(cache.Config, NewManagement) (types.Cache, error)

type cacheSpec struct {
	constructor CacheConstructor
	spec        docs.ComponentSpec
}

// CacheSet contains an explicit set of caches available to a Benthos service.
type CacheSet struct {
	specs map[string]cacheSpec
}

// Add a new cache to this set by providing a spec (name, documentation, and
// constructor).
func (s *CacheSet) Add(constructor CacheConstructor, spec docs.ComponentSpec) error {
	if s.specs == nil {
		s.specs = map[string]cacheSpec{}
	}
	s.specs[spec.Name] = cacheSpec{
		constructor: constructor,
		spec:        spec,
	}
	docs.RegisterDocs(spec)
	return nil
}

// Init attempts to initialise an cache from a config.
func (s *CacheSet) Init(conf cache.Config, mgr NewManagement) (types.Cache, error) {
	spec, exists := s.specs[conf.Type]
	if !exists {
		return nil, types.ErrInvalidCacheType
	}
	return spec.constructor(conf, mgr)
}

// Docs returns a slice of cache specs, which document each method.
func (s *CacheSet) Docs() []docs.ComponentSpec {
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
func (s *CacheSet) DocsFor(name string) (docs.ComponentSpec, bool) {
	c, ok := s.specs[name]
	if !ok {
		return docs.ComponentSpec{}, false
	}
	return c.spec, true
}
