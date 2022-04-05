package docs

import (
	"sync"
)

// Provider stores the component spec definitions of various component
// implementations.
type Provider interface {
	GetDocs(name string, ctype Type) (ComponentSpec, bool)
}

// DeprecatedProvider is a globally declared docs provider that enables the old
// config parse style to work with dynamic plugins. Eventually we can eliminate
// all the UnmarshalYAML methods on config structs and remove this as well.
var DeprecatedProvider = NewMappedDocsProvider()

//------------------------------------------------------------------------------

// MappedDocsProvider stores component documentation in maps, protected by a
// mutex, allowing safe concurrent use.
type MappedDocsProvider struct {
	bufferMap     map[string]ComponentSpec
	cacheMap      map[string]ComponentSpec
	inputMap      map[string]ComponentSpec
	metricsMap    map[string]ComponentSpec
	outputMap     map[string]ComponentSpec
	processorMap  map[string]ComponentSpec
	rateLimitMap  map[string]ComponentSpec
	tracerMap     map[string]ComponentSpec
	componentLock sync.Mutex
}

// NewMappedDocsProvider creates a new (empty) provider of component docs.
func NewMappedDocsProvider() *MappedDocsProvider {
	return &MappedDocsProvider{
		bufferMap:    map[string]ComponentSpec{},
		cacheMap:     map[string]ComponentSpec{},
		inputMap:     map[string]ComponentSpec{},
		metricsMap:   map[string]ComponentSpec{},
		outputMap:    map[string]ComponentSpec{},
		processorMap: map[string]ComponentSpec{},
		rateLimitMap: map[string]ComponentSpec{},
		tracerMap:    map[string]ComponentSpec{},
	}
}

// Clone returns a copied version of the provider that can be modified
// independently.
func (m *MappedDocsProvider) Clone() *MappedDocsProvider {
	newM := &MappedDocsProvider{
		bufferMap:    map[string]ComponentSpec{},
		cacheMap:     map[string]ComponentSpec{},
		inputMap:     map[string]ComponentSpec{},
		metricsMap:   map[string]ComponentSpec{},
		outputMap:    map[string]ComponentSpec{},
		processorMap: map[string]ComponentSpec{},
		rateLimitMap: map[string]ComponentSpec{},
		tracerMap:    map[string]ComponentSpec{},
	}

	for k, v := range m.bufferMap {
		newM.bufferMap[k] = v
	}
	for k, v := range m.cacheMap {
		newM.cacheMap[k] = v
	}
	for k, v := range m.inputMap {
		newM.inputMap[k] = v
	}
	for k, v := range m.metricsMap {
		newM.metricsMap[k] = v
	}
	for k, v := range m.outputMap {
		newM.outputMap[k] = v
	}
	for k, v := range m.processorMap {
		newM.processorMap[k] = v
	}
	for k, v := range m.rateLimitMap {
		newM.rateLimitMap[k] = v
	}
	for k, v := range m.tracerMap {
		newM.tracerMap[k] = v
	}
	return newM
}

// RegisterDocs adds the documentation of a component implementation.
func (m *MappedDocsProvider) RegisterDocs(spec ComponentSpec) {
	m.componentLock.Lock()
	defer m.componentLock.Unlock()

	switch spec.Type {
	case TypeBuffer:
		m.bufferMap[spec.Name] = spec
	case TypeCache:
		m.cacheMap[spec.Name] = spec
	case TypeInput:
		m.inputMap[spec.Name] = spec
	case TypeMetrics:
		m.metricsMap[spec.Name] = spec
	case TypeOutput:
		m.outputMap[spec.Name] = spec
	case TypeProcessor:
		m.processorMap[spec.Name] = spec
	case TypeRateLimit:
		m.rateLimitMap[spec.Name] = spec
	case TypeTracer:
		m.tracerMap[spec.Name] = spec
	}
}

// GetDocs attempts to obtain component implementation docs.
func (m *MappedDocsProvider) GetDocs(name string, ctype Type) (ComponentSpec, bool) {
	m.componentLock.Lock()
	defer m.componentLock.Unlock()

	var spec ComponentSpec
	var ok bool

	switch ctype {
	case TypeBuffer:
		spec, ok = m.bufferMap[name]
	case TypeCache:
		spec, ok = m.cacheMap[name]
	case TypeInput:
		spec, ok = m.inputMap[name]
	case TypeMetrics:
		spec, ok = m.metricsMap[name]
	case TypeOutput:
		spec, ok = m.outputMap[name]
	case TypeProcessor:
		spec, ok = m.processorMap[name]
	case TypeRateLimit:
		spec, ok = m.rateLimitMap[name]
	case TypeTracer:
		spec, ok = m.tracerMap[name]
	}

	return spec, ok
}
