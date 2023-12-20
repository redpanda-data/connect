package bundle

import (
	"fmt"
	"sort"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/scanner"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

// AllScanners is a set containing every single scanner that has been imported.
var AllScanners = &ScannerSet{
	specs: map[string]scannerSpec{},
}

//------------------------------------------------------------------------------

// ScannerAdd adds a new scanner to this environment by providing a constructor
// and documentation.
func (e *Environment) ScannerAdd(constructor ScannerConstructor, spec docs.ComponentSpec) error {
	return e.scanners.Add(constructor, spec)
}

// ScannerInit attempts to initialise a scanner creator from a config.
func (e *Environment) ScannerInit(conf scanner.Config, nm NewManagement) (scanner.Creator, error) {
	return e.scanners.Init(conf, nm)
}

// ScannerDocs returns a slice of scanner specs.
func (e *Environment) ScannerDocs() []docs.ComponentSpec {
	return e.scanners.Docs()
}

//------------------------------------------------------------------------------

// ScannerConstructor constructs a scanner component.
type ScannerConstructor func(scanner.Config, NewManagement) (scanner.Creator, error)

type scannerSpec struct {
	constructor ScannerConstructor
	spec        docs.ComponentSpec
}

// ScannerSet contains an explicit set of scanners available to a Benthos
// service.
type ScannerSet struct {
	specs map[string]scannerSpec
}

// Add a new scanner to this set by providing a spec (name, documentation, and
// constructor).
func (s *ScannerSet) Add(constructor ScannerConstructor, spec docs.ComponentSpec) error {
	if !nameRegexp.MatchString(spec.Name) {
		return fmt.Errorf("component name '%v' does not match the required regular expression /%v/", spec.Name, nameRegexpRaw)
	}
	if s.specs == nil {
		s.specs = map[string]scannerSpec{}
	}
	spec.Type = docs.TypeScanner
	s.specs[spec.Name] = scannerSpec{
		constructor: constructor,
		spec:        spec,
	}
	docs.DeprecatedProvider.RegisterDocs(spec)
	return nil
}

// Init attempts to initialise a scanner from a config.
func (s *ScannerSet) Init(conf scanner.Config, nm NewManagement) (scanner.Creator, error) {
	spec, exists := s.specs[conf.Type]
	if !exists {
		return nil, component.ErrInvalidType("scanner", conf.Type)
	}
	return spec.constructor(conf, nm)
}

// Docs returns a slice of scanner specs, which document each method.
func (s *ScannerSet) Docs() []docs.ComponentSpec {
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
func (s *ScannerSet) DocsFor(name string) (docs.ComponentSpec, bool) {
	c, ok := s.specs[name]
	if !ok {
		return docs.ComponentSpec{}, false
	}
	return c.spec, true
}
