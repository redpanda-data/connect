package docs

import (
	"sync"
)

var bufferMap = map[string]ComponentSpec{}
var cacheMap = map[string]ComponentSpec{}
var inputMap = map[string]ComponentSpec{}
var metricsMap = map[string]ComponentSpec{}
var outputMap = map[string]ComponentSpec{}
var processorMap = map[string]ComponentSpec{}
var rateLimitMap = map[string]ComponentSpec{}
var tracerMap = map[string]ComponentSpec{}
var componentLock sync.Mutex

// RegisterDocs stores the documentation spec for a component.
func RegisterDocs(spec ComponentSpec) {
	componentLock.Lock()
	switch spec.Type {
	case TypeBuffer:
		bufferMap[spec.Name] = spec
	case TypeCache:
		cacheMap[spec.Name] = spec
	case TypeInput:
		inputMap[spec.Name] = spec
	case TypeMetrics:
		metricsMap[spec.Name] = spec
	case TypeOutput:
		outputMap[spec.Name] = spec
	case TypeProcessor:
		processorMap[spec.Name] = spec
	case TypeRateLimit:
		rateLimitMap[spec.Name] = spec
	case TypeTracer:
		tracerMap[spec.Name] = spec
	}
	componentLock.Unlock()
}

// GetDocs attempts to locate a documentation spec for a component identified by
// a unique name and type combination.
func GetDocs(name string, ctype Type) (ComponentSpec, bool) {
	componentLock.Lock()
	defer componentLock.Unlock()

	var spec ComponentSpec
	var ok bool

	switch ctype {
	case TypeBuffer:
		spec, ok = bufferMap[name]
	case TypeCache:
		spec, ok = cacheMap[name]
	case TypeInput:
		spec, ok = inputMap[name]
	case TypeMetrics:
		spec, ok = metricsMap[name]
	case TypeOutput:
		spec, ok = outputMap[name]
	case TypeProcessor:
		spec, ok = processorMap[name]
	case TypeRateLimit:
		spec, ok = rateLimitMap[name]
	case TypeTracer:
		spec, ok = tracerMap[name]
	}

	return spec, ok
}
