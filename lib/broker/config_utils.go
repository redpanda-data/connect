// Copyright (c) 2019 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
