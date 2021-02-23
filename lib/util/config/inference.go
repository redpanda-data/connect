package config

import "sort"

//------------------------------------------------------------------------------

// GetInferenceCandidates checks a generic config structure (YAML or JSON) and,
// if an explicit type value is not found, returns a list of candidate types by
// walking the root objects field names.
func GetInferenceCandidates(raw interface{}, ignoreFields ...string) []string {
	ignore := make(map[string]struct{}, len(ignoreFields))
	for _, k := range ignoreFields {
		ignore[k] = struct{}{}
	}
	switch t := raw.(type) {
	case map[string]interface{}:
		if _, exists := t["type"]; exists {
			return nil
		}
		candidates := make([]string, 0, len(t))
		for k := range t {
			if _, exists := ignore[k]; !exists {
				candidates = append(candidates, k)
			}
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
				if _, exists := ignore[kStr]; !exists {
					candidates = append(candidates, kStr)
				}
			}
		}
		sort.Strings(candidates)
		return candidates
	}
	return nil
}

//------------------------------------------------------------------------------
