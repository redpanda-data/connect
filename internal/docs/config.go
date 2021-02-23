package docs

import (
	"fmt"
	"sort"

	"github.com/Jeffail/benthos/v3/internal/interop/plugins"
)

func ignoreFieldsByType(t Type) map[string]struct{} {
	switch t {
	case TypeInput:
		return map[string]struct{}{
			"processors": {},
		}
	case TypeOutput:
		return map[string]struct{}{
			"processors": {},
		}
	}
	return map[string]struct{}{}
}

func refreshOldPlugins() {
	plugins.FlushNameTypes(func(nt [2]string) {
		RegisterDocs(ComponentSpec{
			Name:   nt[0],
			Type:   Type(nt[1]),
			Status: StatusPlugin,
		})
	})
}

// GetInferenceCandidate checks a generic config structure for a component and
// returns either the inferred type name or an error if one cannot be inferred.
func GetInferenceCandidate(t Type, defaultType string, raw interface{}) (string, ComponentSpec, error) {
	refreshOldPlugins()

	m, ok := raw.(map[string]interface{})
	if !ok {
		return "", ComponentSpec{}, fmt.Errorf("invalid config value %T, expected object", raw)
	}

	if tStr, ok := m["type"].(string); ok {
		spec, exists := GetDocs(tStr, t)
		if !exists {
			return "", ComponentSpec{}, fmt.Errorf("%v type '%v' was not recognised", string(t), tStr)
		}
		return tStr, spec, nil
	}

	ignore := ignoreFieldsByType(t)

	var candidates []string
	var inferred string
	var inferredSpec ComponentSpec
	for k := range m {
		if _, exists := ignore[k]; exists {
			continue
		}
		candidates = append(candidates, k)
		if spec, exists := GetDocs(k, t); exists {
			if len(inferred) > 0 {
				candidates = []string{inferred, k}
				sort.Strings(candidates)
				return "", ComponentSpec{}, fmt.Errorf(
					"unable to infer %v type, multiple candidates '%v' and '%v'", string(t), candidates[0], candidates[1],
				)
			}
			inferred = k
			inferredSpec = spec
		}
	}

	if len(candidates) == 0 && len(defaultType) > 0 {
		// A totally empty component config results in the default.
		// TODO: V4 Disable this
		if spec, exists := GetDocs(defaultType, t); exists {
			return defaultType, spec, nil
		}
	}

	if len(inferred) == 0 {
		sort.Strings(candidates)
		return "", ComponentSpec{}, fmt.Errorf("unable to infer %v type, candidates were: %v", string(t), candidates)
	}
	return inferred, inferredSpec, nil
}
