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

package types

import (
	"encoding/json"
	"time"
)

//------------------------------------------------------------------------------

// NewMessage initializes a new message.
func NewMessage(parts [][]byte) Message {
	return &messageImpl{
		createdAt: time.Now(),
		parts:     parts,
	}
}

// FromBytes deserialises a Message from a byte array.
func FromBytes(b []byte) (Message, error) {
	if len(b) < 4 {
		return nil, ErrBadMessageBytes
	}

	numParts := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
	if numParts >= uint32(len(b)) {
		return nil, ErrBadMessageBytes
	}

	b = b[4:]

	m := NewMessage(nil)
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

// partCache is a cache of operations performed on message parts, a part cache
// becomes invalid when the contents of a part is changed.
type partCache struct {
	json interface{}
}

//------------------------------------------------------------------------------

// messageImpl is a struct containing any relevant fields of a benthos message
// and
// helper functions.
type messageImpl struct {
	createdAt   time.Time
	parts       [][]byte
	partCaches  []*partCache
	resultCache map[string]bool
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
func (m *messageImpl) Bytes() []byte {
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

//------------------------------------------------------------------------------

// ShallowCopy creates a new shallow copy of the message. Parts can be
// re-arranged in the new copy and JSON parts can be get/set without impacting
// other message copies. However, it is still unsafe to edit the content of
// parts.
func (m *messageImpl) ShallowCopy() Message {
	// NOTE: JSON parts are not copied here, as even though we can safely copy
	// the hash and len fields we cannot safely copy the content as it may
	// contain pointers or ref types.
	return &messageImpl{
		createdAt:   m.createdAt,
		parts:       append([][]byte(nil), m.parts...),
		resultCache: m.resultCache,
	}
}

// DeepCopy creates a new deep copy of the message. This can be considered an
// entirely new object that is safe to use anywhere.
func (m *messageImpl) DeepCopy() Message {
	newParts := make([][]byte, len(m.parts))
	for i, p := range m.parts {
		np := make([]byte, len(p))
		copy(np, p)
		newParts[i] = np
	}
	return &messageImpl{
		createdAt: m.createdAt,
		parts:     newParts,
	}
}

//------------------------------------------------------------------------------

func (m *messageImpl) Get(index int) []byte {
	if index < 0 {
		index = len(m.parts) + index
	}
	if index < 0 || index >= len(m.parts) {
		return nil
	}
	return m.parts[index]
}

func (m *messageImpl) GetAll() [][]byte {
	return m.parts
}

func (m *messageImpl) Set(index int, b []byte) {
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

func (m *messageImpl) SetAll(p [][]byte) {
	m.parts = p
	m.clearAllCaches()
}

func (m *messageImpl) Append(b ...[]byte) int {
	for _, p := range b {
		m.parts = append(m.parts, p)
	}
	m.clearGeneralCaches()
	return len(m.parts) - 1
}

func (m *messageImpl) Len() int {
	return len(m.parts)
}

func (m *messageImpl) Iter(f func(i int, b []byte) error) error {
	for i, p := range m.parts {
		if err := f(i, p); err != nil {
			return err
		}
	}
	return nil
}

func (m *messageImpl) expandCache(index int) {
	if len(m.partCaches) > index {
		return
	}
	cParts := make([]*partCache, index+1)
	copy(cParts, m.partCaches)
	m.partCaches = cParts
}

func (m *messageImpl) clearGeneralCaches() {
	m.resultCache = nil
}

func (m *messageImpl) clearAllCaches() {
	m.resultCache = nil
	m.partCaches = nil
}

func (m *messageImpl) GetJSON(part int) (interface{}, error) {
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

func (m *messageImpl) SetJSON(part int, jObj interface{}) error {
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

func (m *messageImpl) LazyCondition(label string, cond Condition) bool {
	if m.resultCache == nil {
		m.resultCache = map[string]bool{}
	} else if res, exists := m.resultCache[label]; exists {
		return res
	}

	res := cond.Check(m)
	m.resultCache[label] = res
	return res
}

func (m *messageImpl) CreatedAt() time.Time {
	return m.createdAt
}

//------------------------------------------------------------------------------
