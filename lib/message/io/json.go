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

package io

import (
	"encoding/json"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/metadata"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

type partJSONStruct struct {
	Metadata map[string]string `json:"metadata"`
	Value    string            `json:"value"`
}

func partToJSONStruct(p types.Part) partJSONStruct {
	meta := map[string]string{}
	p.Metadata().Iter(func(k, v string) error {
		meta[k] = v
		return nil
	})
	return partJSONStruct{
		Metadata: meta,
		Value:    string(p.Get()),
	}
}

func partFromJSONStruct(p partJSONStruct) types.Part {
	part := message.NewPart([]byte(p.Value))
	part.SetMetadata(metadata.New(p.Metadata))
	return part
}

//------------------------------------------------------------------------------

// MessageToJSON converts a message into raw JSON bytes of the form:
// [{"value":"foo","metadata":{"bar":"baz"}}]
func MessageToJSON(msg types.Message) ([]byte, error) {
	message := []partJSONStruct{}
	msg.Iter(func(i int, part types.Part) error {
		message = append(message, partToJSONStruct(part))
		return nil
	})
	return json.Marshal(message)
}

// MessageFromJSON parses JSON bytes into a message type of the form:
// [{"value":"foo","metadata":{"bar":"baz"}}]
func MessageFromJSON(jsonBytes []byte) (types.Message, error) {
	var jsonParts []partJSONStruct
	if err := json.Unmarshal(jsonBytes, &jsonParts); err != nil {
		return nil, err
	}
	msg := message.New(nil)
	for _, v := range jsonParts {
		msg.Append(partFromJSONStruct(v))
	}
	return msg, nil
}

//------------------------------------------------------------------------------
