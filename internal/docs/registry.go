package docs

import (
	"sync"
)

// Provider stores the component spec definitions of various component
// implementations.
type Provider interface {
	GetDocs(name string, ctype Type) (ComponentSpec, bool)
}

var globalProvider = NewMappedDocsProvider()

// RegisterDocs stores the documentation spec for a component.
func RegisterDocs(spec ComponentSpec) {
	globalProvider.componentLock.Lock()
	switch spec.Type {
	case TypeBuffer:
		globalProvider.bufferMap[spec.Name] = spec
	case TypeCache:
		globalProvider.cacheMap[spec.Name] = spec
	case TypeInput:
		globalProvider.inputMap[spec.Name] = spec
	case TypeMetrics:
		globalProvider.metricsMap[spec.Name] = spec
	case TypeOutput:
		globalProvider.outputMap[spec.Name] = spec
	case TypeProcessor:
		globalProvider.processorMap[spec.Name] = spec
	case TypeRateLimit:
		globalProvider.rateLimitMap[spec.Name] = spec
	case TypeTracer:
		globalProvider.tracerMap[spec.Name] = spec
	}
	globalProvider.componentLock.Unlock()
}

// GetDocs attempts to locate a documentation spec for a component identified by
// a unique name and type combination.
func GetDocs(prov Provider, name string, ctype Type) (ComponentSpec, bool) {
	if prov == nil {
		prov = globalProvider
	}
	return prov.GetDocs(name, ctype)
}

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
