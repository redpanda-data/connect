// Copyright (c) 2014 Ashley Jeffs
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
	"time"

	"github.com/Jeffail/benthos/lib/message/metadata"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

// Type is the standard implementation of types.Message, containing a multiple
// part message.
type Type struct {
	createdAt   time.Time
	parts       [][]byte
	metadata    []types.Metadata
	partCaches  []*partCache
	resultCache map[string]bool
}

// New initializes a new message.
func New(parts [][]byte) *Type {
	return &Type{
		createdAt: time.Now(),
		parts:     parts,
	}
}

//------------------------------------------------------------------------------

// partCache is a cache of operations performed on message parts, a part cache
// becomes invalid when the contents of a part is changed.
type partCache struct {
	json interface{}
}

// Clone a partCache, the cloned result can be edited without changing the
// original.
func (p *partCache) Clone() (*partCache, error) {
	cloned, err := cloneGeneric(p.json)
	if err != nil {
		return nil, err
	}
	return &partCache{
		json: cloned,
	}, nil
}

//------------------------------------------------------------------------------

// ShallowCopy creates a new shallow copy of the message. Parts can be
// re-arranged in the new copy and JSON parts can be get/set without impacting
// other message copies. However, it is still unsafe to edit the raw content of
// message parts.
func (m *Type) ShallowCopy() types.Message {
	var metadata []types.Metadata
	if len(m.metadata) > 0 {
		metadata = make([]types.Metadata, len(m.metadata))
		for i, md := range m.metadata {
			if md != nil {
				metadata[i] = md.Copy()
			}
		}
	}
	var newPartCaches []*partCache
	if len(m.partCaches) > 0 {
		newPartCaches = make([]*partCache, len(m.partCaches))
		for i, c := range m.partCaches {
			if c == nil {
				continue
			}
			newPartCaches[i], _ = c.Clone()
		}
	}
	var newResultCache map[string]bool
	if len(m.resultCache) > 0 {
		newResultCache = make(map[string]bool, len(m.resultCache))
		for k, v := range m.resultCache {
			newResultCache[k] = v
		}
	}
	return &Type{
		createdAt:   m.createdAt,
		parts:       append([][]byte(nil), m.parts...),
		metadata:    metadata,
		partCaches:  newPartCaches,
		resultCache: newResultCache,
	}
}

// DeepCopy creates a new deep copy of the message. This can be considered an
// entirely new object that is safe to use anywhere.
func (m *Type) DeepCopy() types.Message {
	var metadata []types.Metadata
	if len(m.metadata) > 0 {
		metadata = make([]types.Metadata, len(m.metadata))
		for i, md := range m.metadata {
			if md != nil {
				metadata[i] = md.Copy()
			}
		}
	}
	var newPartCaches []*partCache
	if len(m.partCaches) > 0 {
		newPartCaches = make([]*partCache, len(m.partCaches))
		for i, c := range m.partCaches {
			if c == nil {
				continue
			}
			newPartCaches[i], _ = c.Clone()
		}
	}
	newParts := make([][]byte, len(m.parts))
	for i, p := range m.parts {
		np := make([]byte, len(p))
		copy(np, p)
		newParts[i] = np
	}
	var newResultCache map[string]bool
	if len(m.resultCache) > 0 {
		newResultCache = make(map[string]bool, len(m.resultCache))
		for k, v := range m.resultCache {
			newResultCache[k] = v
		}
	}
	return &Type{
		createdAt:   m.createdAt,
		parts:       newParts,
		metadata:    metadata,
		partCaches:  newPartCaches,
		resultCache: newResultCache,
	}
}

//------------------------------------------------------------------------------

// Get returns a message part at a particular index, indexes can be negative.
func (m *Type) Get(index int) []byte {
	if index < 0 {
		index = len(m.parts) + index
	}
	if index < 0 || index >= len(m.parts) {
		return nil
	}
	return m.parts[index]
}

// GetAll returns all message parts as a 2D byte slice, the contents of this
// slice should NOT be modified.
func (m *Type) GetAll() [][]byte {
	return m.parts
}

// GetMetadata returns the metadata of a message part. If the index is negative
// then the part is found by counting backwards from the last part starting at
// -1. If the index is out of bounds then a no-op metadata object is returned.
func (m *Type) GetMetadata(index int) types.Metadata {
	if index < 0 {
		index = len(m.parts) + index
	}
	if index < 0 || index >= len(m.parts) {
		return metadata.New(nil)
	}
	m.expandMetadata()
	if m.metadata[index] == nil {
		m.metadata[index] = metadata.New(nil)
	}
	return m.metadata[index]
}

// SetMetadata sets the metadata of message parts by their index. Multiple
// indexes can be specified in order to set the same metadata values to each
// part. If zero indexes are specified the metadata is set for all message
// parts. If an index is negative then the part is found by counting backwards
// from the last part starting at -1. If an index is out of bounds then nothing
// is done.
func (m *Type) SetMetadata(meta types.Metadata, indexes ...int) {
	m.expandMetadata()
	if len(indexes) == 0 {
		for i := range m.metadata {
			m.metadata[i] = metadata.LazyCopy(meta)
		}
		return
	}
	for _, index := range indexes {
		if index < 0 {
			index = len(m.parts) + index
		}
		if index < 0 || index >= len(m.parts) {
			continue
		}
		if len(indexes) > 1 {
			m.metadata[index] = metadata.LazyCopy(meta)
		} else {
			m.metadata[index] = meta
		}
	}
}

// Set the value of a message part by its index. Indexes can be negative.
func (m *Type) Set(index int, b []byte) {
	if index < 0 {
		index = len(m.parts) + index
	}
	if index < 0 || index >= len(m.parts) {
		return
	}
	if len(m.partCaches) > index {
		// Remove now invalid part cache.
		m.partCaches[index] = nil
	}
	m.clearGeneralCaches()
	m.parts[index] = b
}

// SetAll changes the entire set of message parts.
func (m *Type) SetAll(p [][]byte) {
	m.parts = p
	m.clearAllCaches()
}

// Append adds a new message part to the message.
func (m *Type) Append(b ...[]byte) int {
	if len(m.metadata) > 0 {
		m.metadata = append(m.metadata, make([]types.Metadata, len(b))...)
	}
	for _, p := range b {
		m.parts = append(m.parts, p)
	}
	m.clearGeneralCaches()
	return len(m.parts) - 1
}

// Len returns the length of the message in parts.
func (m *Type) Len() int {
	return len(m.parts)
}

// Iter will iterate all parts of the message, calling f for each.
func (m *Type) Iter(f func(i int, b []byte) error) error {
	for i, p := range m.parts {
		if err := f(i, p); err != nil {
			return err
		}
	}
	return nil
}

func (m *Type) expandMetadata() {
	if len(m.metadata) < len(m.parts) {
		mParts := make([]types.Metadata, len(m.parts))
		copy(mParts, m.metadata)
		m.metadata = mParts
	}
}

func (m *Type) expandCache(index int) {
	if len(m.partCaches) > index {
		return
	}
	cParts := make([]*partCache, index+1)
	copy(cParts, m.partCaches)
	m.partCaches = cParts
}

func (m *Type) clearGeneralCaches() {
	m.resultCache = nil
}

func (m *Type) clearAllCaches() {
	m.resultCache = nil
	m.partCaches = nil
}

// GetJSON attempts to parse a message part as JSON and returns the result.
func (m *Type) GetJSON(part int) (interface{}, error) {
	if part < 0 {
		part = len(m.parts) + part
	}
	if part < 0 || part >= len(m.parts) {
		return nil, ErrMessagePartNotExist
	}
	m.expandCache(part)
	cPart := m.partCaches[part]
	if cPart == nil {
		cPart = &partCache{}
		m.partCaches[part] = cPart
	}
	if err := json.Unmarshal(m.Get(part), &cPart.json); err != nil {
		return nil, err
	}
	return cPart.json, nil
}

// SetJSON attempts to marshal an object into a JSON string and sets a message
// part to the result.
func (m *Type) SetJSON(part int, jObj interface{}) error {
	if part < 0 {
		part = len(m.parts) + part
	}
	if part < 0 || part >= len(m.parts) {
		return ErrMessagePartNotExist
	}
	m.expandCache(part)

	partBytes, err := json.Marshal(jObj)
	if err != nil {
		return err
	}

	m.Set(part, partBytes)
	m.partCaches[part] = &partCache{
		json: jObj,
	}
	m.clearGeneralCaches()
	return nil
}

// LazyCondition resolves a particular condition on the message, if the
// condition has already been applied to this message the cached result is
// returned instead. When a message is altered in any way the conditions cache
// is cleared.
func (m *Type) LazyCondition(label string, cond types.Condition) bool {
	if m.resultCache == nil {
		m.resultCache = map[string]bool{}
	} else if res, exists := m.resultCache[label]; exists {
		return res
	}

	res := cond.Check(m)
	m.resultCache[label] = res
	return res
}

// CreatedAt returns a timestamp whereby the message was created.
func (m *Type) CreatedAt() time.Time {
	return m.createdAt
}

//------------------------------------------------------------------------------

/*
Internal message blob format:

- Four bytes containing number of message parts in big endian
- For each message part:
    + Four bytes containing length of message part in big endian
    + Content of message part

                                         # Of bytes in message part 2
                                         |
# Of message parts (u32 big endian)      |           Content of message part 2
|                                        |           |
v                                        v           v
| 0| 0| 0| 2| 0| 0| 0| 5| h| e| l| l| o| 0| 0| 0| 5| w| o| r| l| d|
  0  1  2  3  4  5  6  7  8  9 10 11 13 14 15 16 17 18 19 20 21 22
              ^           ^
              |           |
              |           Content of message part 1
              |
              # Of bytes in message part 1 (u32 big endian)
*/

// Reserve bytes for our length counter (4 * 8 = 32 bit)
var intLen uint32 = 4

// Bytes serialises the message into a single byte array.
func (m *Type) Bytes() []byte {
	lenParts := uint32(len(m.parts))

	l := (lenParts + 1) * intLen
	for i := range m.parts {
		l += uint32(len(m.parts[i]))
	}
	b := make([]byte, l)

	b[0] = byte(lenParts >> 24)
	b[1] = byte(lenParts >> 16)
	b[2] = byte(lenParts >> 8)
	b[3] = byte(lenParts)

	b2 := b[intLen:]
	for i := range m.parts {
		le := uint32(len(m.parts[i]))

		b2[0] = byte(le >> 24)
		b2[1] = byte(le >> 16)
		b2[2] = byte(le >> 8)
		b2[3] = byte(le)

		b2 = b2[intLen:]

		copy(b2, m.parts[i])
		b2 = b2[len(m.parts[i]):]
	}
	return b
}

// FromBytes deserialises a Message from a byte array.
func FromBytes(b []byte) (*Type, error) {
	if len(b) < 4 {
		return nil, ErrBadMessageBytes
	}

	numParts := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
	if numParts >= uint32(len(b)) {
		return nil, ErrBadMessageBytes
	}

	b = b[4:]

	m := New(nil)
	for i := uint32(0); i < numParts; i++ {
		if len(b) < 4 {
			return nil, ErrBadMessageBytes
		}
		partSize := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
		b = b[4:]

		if uint32(len(b)) < partSize {
			return nil, ErrBadMessageBytes
		}
		m.Append(b[:partSize])
		b = b[partSize:]
	}
	return m, nil
}

//------------------------------------------------------------------------------
