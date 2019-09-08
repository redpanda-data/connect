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

// lazyCopy is a types.Metadata implementation that takes an existing metadata
// object and lazily returns its values. If a call is made to edit the contents
// of the metadata then a copy is made of the original before doing so.
type lazyCopy struct {
	copied bool
	m      types.Metadata
}

func (l *lazyCopy) ensureCopied() {
	if l.copied {
		return
	}
	var newMap map[string]string
	if t, ok := l.m.(*Type); ok {
		newMap = make(map[string]string, len(t.m))
	} else {
		newMap = map[string]string{}
	}
	l.m.Iter(func(k, v string) error {
		newMap[k] = v
		return nil
	})
	l.m = New(newMap)
	l.copied = true
}

// Get returns a metadata value if a key exists, otherwise an empty string.
func (l *lazyCopy) Get(key string) string {
	return l.m.Get(key)
}

// Set sets the value of a metadata key.
func (l *lazyCopy) Set(key, value string) types.Metadata {
	l.ensureCopied()
	l.m.Set(key, value)
	return l
}

// Delete removes the value of a metadata key.
func (l *lazyCopy) Delete(key string) types.Metadata {
	l.ensureCopied()
	l.m.Delete(key)
	return l
}

// Iter iterates each metadata key/value pair.
func (l *lazyCopy) Iter(f func(k, v string) error) error {
	return l.m.Iter(f)
}

// Copy returns a copy of the metadata object that can be edited without
// changing the contents of the original.
func (l *lazyCopy) Copy() types.Metadata {
	return l.m.Copy()
}

//------------------------------------------------------------------------------

// LazyCopy takes an existing metadata object and returns a new implementation
// that lazily returns its values. If a call is made to edit the contents of the
// metadata then a copy is made of the original before doing so.
func LazyCopy(m types.Metadata) types.Metadata {
	return &lazyCopy{
		m: m,
	}
}

//------------------------------------------------------------------------------
