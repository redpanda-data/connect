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

import "github.com/Jeffail/benthos/lib/types"

//------------------------------------------------------------------------------

// Noop is a types.Metadata implementation that does nothing.
type Noop struct{}

// Get returns a metadata value if a key exists, otherwise an empty string.
func (l Noop) Get(key string) string {
	return ""
}

// Set sets the value of a metadata key.
func (l Noop) Set(key, value string) {
}

// Delete removes the value of a metadata key.
func (l Noop) Delete(key string) {
}

// Iter iterates each metadata key/value pair.
func (l Noop) Iter(f func(k, v string) error) error {
	return nil
}

// Copy returns a copy of the metadata object that can be edited without
// changing the contents of the original.
func (l Noop) Copy() types.Metadata {
	return l
}

//------------------------------------------------------------------------------
