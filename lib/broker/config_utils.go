package broker

import (
	"fmt"
)

//------------------------------------------------------------------------------

// GetGenericType returns the type of a generically parsed config structure.
func GetGenericType(boxedConfig interface{}) string {
	switch unboxed := boxedConfig.(type) {
	case map[string]interface{}:
		if t, ok := unboxed["type"].(string); ok {
			return t
		}
	case map[interface{}]interface{}:
		if t, ok := unboxed["type"].(string); ok {
			return t
		}
	}
	return ""
}

// RemoveGenericType removes the type of a generically parsed config structure.
func RemoveGenericType(boxedConfig interface{}) {
	switch unboxed := boxedConfig.(type) {
	case map[string]interface{}:
		delete(unboxed, "type")
	case map[interface{}]interface{}:
		delete(unboxed, "type")
	}
}

// ComplementGenericConfig copies fields from one generic config to another, but
// avoids overriding existing values in the destination config.
func ComplementGenericConfig(target, complement interface{}) error {
	switch t := target.(type) {
	case map[string]interface{}:
		cMap, ok := complement.(map[string]interface{})
		if !ok {
			return fmt.Errorf("ditto config type mismatch: %T != %T", target, complement)
		}
		for k, v := range cMap {
			if tv, exists := t[k]; !exists {
				t[k] = v
			} else {
				ComplementGenericConfig(tv, v)
			}
		}
	case map[interface{}]interface{}:
		cMap, ok := complement.(map[interface{}]interface{})
		if !ok {
			return fmt.Errorf("ditto config type mismatch: %T != %T", target, complement)
		}
		for k, v := range cMap {
			if tv, exists := t[k]; !exists {
				t[k] = v
			} else {
				ComplementGenericConfig(tv, v)
			}
		}
	}
	return nil
}

//------------------------------------------------------------------------------
