package message

import (
	"encoding/json"
	"fmt"
)

// GetAllBytes returns a 2D byte slice representing the raw byte content of the
// parts of a message.
func GetAllBytes(m Batch) [][]byte {
	if len(m) == 0 {
		return nil
	}
	parts := make([][]byte, len(m))
	_ = m.Iter(func(i int, p *Part) error {
		parts[i] = p.AsBytes()
		return nil
	})
	return parts
}

//------------------------------------------------------------------------------

func cloneMap(oldMap map[string]any) (map[string]any, error) {
	var err error
	newMap := make(map[string]any, len(oldMap))
	for k, v := range oldMap {
		if newMap[k], err = cloneGeneric(v); err != nil {
			return nil, err
		}
	}
	return newMap, nil
}

func cloneCheekyMap(oldMap map[any]any) (map[any]any, error) {
	var err error
	newMap := make(map[any]any, len(oldMap))
	for k, v := range oldMap {
		if newMap[k], err = cloneGeneric(v); err != nil {
			return nil, err
		}
	}
	return newMap, nil
}

func cloneSlice(oldSlice []any) ([]any, error) {
	var err error
	newSlice := make([]any, len(oldSlice))
	for i, v := range oldSlice {
		if newSlice[i], err = cloneGeneric(v); err != nil {
			return nil, err
		}
	}
	return newSlice, nil
}

// cloneGeneric is a utility function that recursively copies a generic
// structure usually resulting from a JSON parse.
func cloneGeneric(root any) (any, error) {
	switch t := root.(type) {
	case map[string]any:
		return cloneMap(t)
	case map[any]any:
		return cloneCheekyMap(t)
	case []any:
		return cloneSlice(t)
	case string, []byte, json.Number, uint64, int, int64, float64, bool, json.RawMessage:
		return t, nil
	default:
		// Oops, this means we have 'dirty' types within the JSON object. Our
		// only way to fallback is to marshal/unmarshal the structure, gross!
		if b, err := json.Marshal(root); err == nil {
			var rootCopy any
			if err = json.Unmarshal(b, &rootCopy); err == nil {
				return rootCopy, nil
			}
		}
		return nil, fmt.Errorf("unrecognised type: %T", t)
	}
}

// CopyJSON recursively creates a deep copy of a JSON structure extracted from a
// message part.
func CopyJSON(root any) (any, error) {
	return cloneGeneric(root)
}
