// Copyright (c) 2018 Ashley Jeffs
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

package metadata

import (
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Type is an implementation of types.Metadata representing the metadata of a
// message part within a batch.
type Type struct {
	m map[string]string
}

// New creates a new metadata implementation from a map[string]string. It is
// safe to provide a nil map.
func New(m map[string]string) *Type {
	return &Type{
		m: m,
	}
}

//------------------------------------------------------------------------------

// Copy returns a copy of the metadata object that can be edited without
// changing the contents of the original.
func (m *Type) Copy() types.Metadata {
	var newMap map[string]string
	if m.m != nil {
		newMap = make(map[string]string, len(m.m))
		for k, v := range m.m {
			newMap[k] = v
		}
	}
	return New(newMap)
}

// Get returns a metadata value if a key exists, otherwise an empty string.
func (m *Type) Get(key string) string {
	if m.m == nil {
		return ""
	}
	return m.m[key]
}

// Set sets the value of a metadata key.
func (m *Type) Set(key, value string) types.Metadata {
	if m.m == nil {
		m.m = map[string]string{
			key: value,
		}
		return m
	}
	m.m[key] = value
	return m
}

// Delete removes the value of a metadata key.
func (m *Type) Delete(key string) types.Metadata {
	if m.m == nil {
		return m
	}
	delete(m.m, key)
	return m
}

// Iter iterates each metadata key/value pair.
func (m *Type) Iter(f func(k, v string) error) error {
	if m.m == nil {
		return nil
	}
	for ak, av := range m.m {
		if err := f(ak, av); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------
