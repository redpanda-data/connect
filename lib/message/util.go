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
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/message/metadata"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// SetAllMetadata sets the metadata of all message parts to match a provided
// metadata implementation.
func SetAllMetadata(m types.Message, meta types.Metadata) {
	lazy := metadata.LazyCopy(meta)
	m.Iter(func(i int, p types.Part) error {
		p.SetMetadata(lazy)
		return nil
	})
}

// GetAllBytes returns a 2D byte slice representing the raw byte content of the
// parts of a message.
func GetAllBytes(m types.Message) [][]byte {
	if m.Len() == 0 {
		return nil
	}
	parts := make([][]byte, m.Len())
	m.Iter(func(i int, p types.Part) error {
		parts[i] = p.Get()
		return nil
	})
	return parts
}

// GetAllBytesLen returns total length of message content in bytes
func GetAllBytesLen(m types.Message) int {
	if m.Len() == 0 {
		return 0
	}
	length := 0
	m.Iter(func(i int, p types.Part) error {
		length += len(p.Get())
		return nil
	})
	return length
}

//------------------------------------------------------------------------------

// MetaPartCopy creates a new empty message part by copying any meta fields
// (metadata, context, etc) from a reference part.
func MetaPartCopy(p types.Part) types.Part {
	newPart := WithContext(GetContext(p), NewPart(nil))
	newPart.SetMetadata(p.Metadata().Copy())
	return newPart
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
	case string, json.Number, int, int64, float64, bool, json.RawMessage:
		return t, nil
	default:
		// Oops, this means we have 'dirty' types within the JSON object. Our
		// only way to fallback is to marshal/unmarshal the structure, gross!
		if b, err := json.Marshal(root); err == nil {
			var copy interface{}
			if err = json.Unmarshal(b, &copy); err == nil {
				return copy, nil
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
