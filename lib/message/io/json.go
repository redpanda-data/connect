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
