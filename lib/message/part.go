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

package message

import (
	"encoding/json"
	"errors"

	"github.com/Jeffail/benthos/lib/message/metadata"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

// Part is an implementation of types.Part, containing the contents and metadata
// of a message part.
type Part struct {
	data      []byte
	metadata  types.Metadata
	jsonCache interface{}
}

// NewPart initializes a new message part.
func NewPart(data []byte) *Part {
	return &Part{
		data: data,
	}
}

//------------------------------------------------------------------------------

// Copy creates a shallow copy of the message part.
func (p *Part) Copy() types.Part {
	var clonedMeta types.Metadata
	if p.metadata != nil {
		clonedMeta = p.metadata.Copy()
	}
	var clonedJSON interface{}
	if p.jsonCache != nil {
		var err error
		if clonedJSON, err = cloneGeneric(p.jsonCache); err != nil {
			clonedJSON = nil
		}
	}
	return &Part{
		data:      p.data,
		metadata:  clonedMeta,
		jsonCache: clonedJSON,
	}
}

// DeepCopy creates a new deep copy of the message part.
func (p *Part) DeepCopy() types.Part {
	var clonedMeta types.Metadata
	if p.metadata != nil {
		clonedMeta = p.metadata.Copy()
	}
	var clonedJSON interface{}
	if p.jsonCache != nil {
		var err error
		if clonedJSON, err = cloneGeneric(p.jsonCache); err != nil {
			clonedJSON = nil
		}
	}
	np := make([]byte, len(p.data))
	copy(np, p.data)
	return &Part{
		data:      np,
		metadata:  clonedMeta,
		jsonCache: clonedJSON,
	}
}

//------------------------------------------------------------------------------

// Get returns the body of the message part.
func (p *Part) Get() []byte {
	return p.data
}

// Metadata returns the metadata of the message part.
func (p *Part) Metadata() types.Metadata {
	if p.metadata == nil {
		p.metadata = metadata.New(nil)
	}
	return p.metadata
}

// JSON attempts to parse the message part as a JSON document and returns the
// result.
func (p *Part) JSON() (interface{}, error) {
	if p.jsonCache != nil {
		return p.jsonCache, nil
	}
	if p.data == nil {
		return nil, errors.New("part is nil")
	}
	if err := json.Unmarshal(p.data, &p.jsonCache); err != nil {
		return nil, err
	}
	return p.jsonCache, nil
}

// Set the value of the message part.
func (p *Part) Set(data []byte) types.Part {
	p.data = data
	p.jsonCache = nil
	return p
}

// SetMetadata sets the metadata of a message part
func (p *Part) SetMetadata(meta types.Metadata) types.Part {
	p.metadata = meta
	return p
}

// SetJSON attempts to marshal a JSON document into a byte slice and stores the
// result as the contents of the message part.
func (p *Part) SetJSON(jObj interface{}) error {
	partBytes, err := json.Marshal(jObj)
	if err != nil {
		return err
	}

	p.data = partBytes
	p.jsonCache = jObj
	return nil
}

//------------------------------------------------------------------------------
