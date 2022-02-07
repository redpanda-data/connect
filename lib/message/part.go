package message

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
)

var useNumber = true

func init() {
	if os.Getenv("BENTHOS_USE_NUMBER") == "false" {
		useNumber = false
	}
}

//------------------------------------------------------------------------------

type rwData struct {
	rawBytes  []byte
	jsonCache interface{}
	metadata  map[string]string
}

// Part represents a single Benthos message.
type Part struct {
	data *rwData
	ctx  context.Context
}

// NewPart initializes a new message part.
func NewPart(data []byte) *Part {
	return &Part{
		data: &rwData{
			rawBytes: data,
		},
		ctx: context.Background(),
	}
}

//------------------------------------------------------------------------------

// Copy creates a shallow copy of the message part.
func (p *Part) Copy() *Part {
	var clonedMeta map[string]string
	if p.data.metadata != nil {
		clonedMeta = make(map[string]string, len(p.data.metadata))
		for k, v := range p.data.metadata {
			clonedMeta[k] = v
		}
	}
	return &Part{
		data: &rwData{
			rawBytes:  p.data.rawBytes,
			metadata:  clonedMeta,
			jsonCache: p.data.jsonCache,
		},
		ctx: p.ctx,
	}
}

// DeepCopy creates a new deep copy of the message part.
func (p *Part) DeepCopy() *Part {
	var clonedMeta map[string]string
	if p.data.metadata != nil {
		clonedMeta = make(map[string]string, len(p.data.metadata))
		for k, v := range p.data.metadata {
			clonedMeta[k] = v
		}
	}
	var clonedJSON interface{}
	if p.data.jsonCache != nil {
		var err error
		if clonedJSON, err = cloneGeneric(p.data.jsonCache); err != nil {
			clonedJSON = nil
		}
	}
	var np []byte
	if p.data != nil {
		np = make([]byte, len(p.data.rawBytes))
		copy(np, p.data.rawBytes)
	}
	return &Part{
		data: &rwData{
			rawBytes:  np,
			metadata:  clonedMeta,
			jsonCache: clonedJSON,
		},
		ctx: p.ctx,
	}
}

//------------------------------------------------------------------------------

// GetContext either returns a context attached to the message part, or
// context.Background() if one hasn't been previously attached.
func GetContext(p *Part) context.Context {
	return p.ctx
}

// WithContext returns the same message part wrapped with a context, this
// context can subsequently be received with GetContext.
func WithContext(ctx context.Context, p *Part) *Part {
	return p.WithContext(ctx)
}

// GetContext returns the underlying context attached to this message part.
func (p *Part) GetContext() context.Context {
	return p.ctx
}

// WithContext returns the underlying message part with a different context
// attached.
func (p *Part) WithContext(ctx context.Context) *Part {
	newP := *p
	newP.ctx = ctx
	return &newP
}

//------------------------------------------------------------------------------

// Get returns the body of the message part.
func (p *Part) Get() []byte {
	if len(p.data.rawBytes) == 0 && p.data.jsonCache != nil {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		err := enc.Encode(p.data.jsonCache)
		if err != nil {
			return nil
		}
		if buf.Len() > 1 {
			p.data.rawBytes = buf.Bytes()[:buf.Len()-1]
		}
	}
	return p.data.rawBytes
}

// JSON attempts to parse the message part as a JSON document and returns the
// result.
func (p *Part) JSON() (interface{}, error) {
	if p.data.jsonCache != nil {
		return p.data.jsonCache, nil
	}
	if p.data.rawBytes == nil {
		return nil, ErrMessagePartNotExist
	}

	dec := json.NewDecoder(bytes.NewReader(p.data.rawBytes))
	if useNumber {
		dec.UseNumber()
	}

	err := dec.Decode(&p.data.jsonCache)
	if err != nil {
		return nil, err
	}

	var dummy json.RawMessage
	if err = dec.Decode(&dummy); err == io.EOF {
		return p.data.jsonCache, nil
	}

	p.data.jsonCache = nil
	if err = dec.Decode(&dummy); err == nil || err == io.EOF {
		err = errors.New("message contains multiple valid documents")
	}
	return nil, err
}

// Set the value of the message part.
func (p *Part) Set(data []byte) *Part {
	p.data.rawBytes = data
	p.data.jsonCache = nil
	return p
}

// SetJSON attempts to marshal a JSON document into a byte slice and stores the
// result as the contents of the message part.
func (p *Part) SetJSON(jObj interface{}) error {
	p.data.rawBytes = nil
	if jObj == nil {
		p.data.rawBytes = []byte(`null`)
	}
	p.data.jsonCache = jObj
	return nil
}

//------------------------------------------------------------------------------

// MetaGet returns a metadata value if a key exists, otherwise an empty string.
func (p *Part) MetaGet(key string) string {
	if p.data.metadata == nil {
		return ""
	}
	return p.data.metadata[key]
}

// MetaSet sets the value of a metadata key.
func (p *Part) MetaSet(key, value string) {
	if p.data.metadata == nil {
		p.data.metadata = map[string]string{
			key: value,
		}
		return
	}
	p.data.metadata[key] = value
}

// MetaDelete removes the value of a metadata key.
func (p *Part) MetaDelete(key string) {
	if p.data.metadata == nil {
		return
	}
	delete(p.data.metadata, key)
}

// MetaIter iterates each metadata key/value pair.
func (p *Part) MetaIter(f func(k, v string) error) error {
	if p.data.metadata == nil {
		return nil
	}
	for ak, av := range p.data.metadata {
		if err := f(ak, av); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------

// IsEmpty returns true if the message part is empty.
func (p *Part) IsEmpty() bool {
	return len(p.data.rawBytes) == 0 && p.data.jsonCache == nil
}
