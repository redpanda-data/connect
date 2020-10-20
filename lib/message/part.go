package message

import (
	"bytes"
	"encoding/json"
	"os"

	"github.com/Jeffail/benthos/v3/lib/message/metadata"
	"github.com/Jeffail/benthos/v3/lib/types"
)

var useNumber bool = true

func init() {
	if os.Getenv("BENTHOS_USE_NUMBER") == "false" {
		useNumber = false
	}
}

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
	return &Part{
		data:      p.data,
		metadata:  clonedMeta,
		jsonCache: p.jsonCache,
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
	var np []byte
	if p.data != nil {
		np = make([]byte, len(p.data))
		copy(np, p.data)
	}
	return &Part{
		data:      np,
		metadata:  clonedMeta,
		jsonCache: clonedJSON,
	}
}

//------------------------------------------------------------------------------

// Get returns the body of the message part.
func (p *Part) Get() []byte {
	if p.data == nil && p.jsonCache != nil {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		err := enc.Encode(p.jsonCache)
		if err != nil {
			return nil
		}
		if buf.Len() > 1 {
			p.data = buf.Bytes()[:buf.Len()-1]
		}
	}
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
		return nil, ErrMessagePartNotExist
	}
	dec := json.NewDecoder(bytes.NewReader(p.data))
	if useNumber {
		dec.UseNumber()
	}
	if err := dec.Decode(&p.jsonCache); err != nil {
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

// SetMetadata sets the metadata of a message part.
func (p *Part) SetMetadata(meta types.Metadata) types.Part {
	p.metadata = meta
	return p
}

// SetJSON attempts to marshal a JSON document into a byte slice and stores the
// result as the contents of the message part.
func (p *Part) SetJSON(jObj interface{}) error {
	p.data = nil
	if jObj == nil {
		p.data = []byte(`null`)
	}
	p.jsonCache = jObj
	return nil
}

//------------------------------------------------------------------------------

// IsEmpty returns true if the message part is empty.
func (p *Part) IsEmpty() bool {
	return len(p.data) == 0 && p.jsonCache == nil
}

//------------------------------------------------------------------------------
