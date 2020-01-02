package config

import "sort"

//------------------------------------------------------------------------------

// GetInferenceCandidates checks a generic config structure (YAML or JSON) and,
// if an explicit type value is not found, returns a list of candidate types by
// walking the root objects field names.
func GetInferenceCandidates(raw interface{}) []string {
	switch t := raw.(type) {
	case map[string]interface{}:
		if _, exists := t["type"]; exists {
			return nil
		}
		candidates := make([]string, 0, len(t))
		for k := range t {
			candidates = append(candidates, k)
		}
		sort.Strings(candidates)
		return candidates
	case map[interface{}]interface{}:
		if _, exists := t["type"]; exists {
			return nil
		}
		candidates := make([]string, 0, len(t))
		for k := range t {
			if kStr, isStr := k.(string); isStr {
				candidates = append(candidates, kStr)
			}
		}
		sort.Strings(candidates)
		return candidates
	}
	return nil
}

//------------------------------------------------------------------------------
