package message

import (
	"encoding/json"
	"fmt"
)

//------------------------------------------------------------------------------

// SetAllMetadata sets the metadata of all message parts to match a provided
// metadata implementation.
func SetAllMetadata(m *Batch, meta map[string]string) {
	_ = m.Iter(func(i int, p *Part) error {
		for k, v := range meta {
			p.MetaSet(k, v)
		}
		return nil
	})
}

// GetAllBytes returns a 2D byte slice representing the raw byte content of the
// parts of a message.
func GetAllBytes(m *Batch) [][]byte {
	if m.Len() == 0 {
		return nil
	}
	parts := make([][]byte, m.Len())
	_ = m.Iter(func(i int, p *Part) error {
		parts[i] = p.Get()
		return nil
	})
	return parts
}

// GetAllBytesLen returns total length of message content in bytes
func GetAllBytesLen(m *Batch) int {
	if m.Len() == 0 {
		return 0
	}
	length := 0
	_ = m.Iter(func(i int, p *Part) error {
		length += len(p.Get())
		return nil
	})
	return length
}

//------------------------------------------------------------------------------

func cloneMap(oldMap map[string]interface{}) (map[string]interface{}, error) {
	var err error
	newMap := make(map[string]interface{}, len(oldMap))
	for k, v := range oldMap {
		if newMap[k], err = cloneGeneric(v); err != nil {
			return nil, err
		}
	}
	return newMap, nil
}

func cloneCheekyMap(oldMap map[interface{}]interface{}) (map[interface{}]interface{}, error) {
	var err error
	newMap := make(map[interface{}]interface{}, len(oldMap))
	for k, v := range oldMap {
		if newMap[k], err = cloneGeneric(v); err != nil {
			return nil, err
		}
	}
	return newMap, nil
}

func cloneSlice(oldSlice []interface{}) ([]interface{}, error) {
	var err error
	newSlice := make([]interface{}, len(oldSlice))
	for i, v := range oldSlice {
		if newSlice[i], err = cloneGeneric(v); err != nil {
			return nil, err
		}
	}
	return newSlice, nil
}

// cloneGeneric is a utility function that recursively copies a generic
// structure usually resulting from a JSON parse.
func cloneGeneric(root interface{}) (interface{}, error) {
	switch t := root.(type) {
	case map[string]interface{}:
		return cloneMap(t)
	case map[interface{}]interface{}:
		return cloneCheekyMap(t)
	case []interface{}:
		return cloneSlice(t)
	case string, []byte, json.Number, uint64, int, int64, float64, bool, json.RawMessage:
		return t, nil
	default:
		// Oops, this means we have 'dirty' types within the JSON object. Our
		// only way to fallback is to marshal/unmarshal the structure, gross!
		if b, err := json.Marshal(root); err == nil {
			var rootCopy interface{}
			if err = json.Unmarshal(b, &rootCopy); err == nil {
				return rootCopy, nil
			}
		}
		return nil, fmt.Errorf("unrecognised type: %T", t)
	}
}

// CopyJSON recursively creates a deep copy of a JSON structure extracted from a
// message part.
func CopyJSON(root interface{}) (interface{}, error) {
	return cloneGeneric(root)
}

//------------------------------------------------------------------------------
