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
)

//------------------------------------------------------------------------------

// Message is an interface representing a payload of data that was received from
// an input. Messages contain multiple parts, where each part is a byte array.
// If an input supports only single part messages they will still be read as
// multipart messages with one part.
type Message interface {
	// Get attempts to access a message part from an index. If the index is
	// negative then the part is found by counting backwards from the last part
	// starting at -1. If the index is out of bounds then nil is returned. It is
	// not safe to edit the contents of a message directly.
	Get(p int) []byte

	// GetAll returns all message parts as a two-dimensional byte array. It is
	// NOT safe to edit the contents of the result.
	GetAll() [][]byte

	// Set edits the contents of an existing message part. If the index is
	// negative then the part is found by counting backwards from the last part
	// starting at -1. If the index is out of bounds then nothing is done.
	Set(p int, b []byte)

	// SetAll replaces all parts of a message with a new set.
	SetAll(p [][]byte)

	// GetJSON returns a message part parsed as JSON into an `interface{}` type.
	// This is lazily evaluated and the result is cached. If multiple layers of
	// a pipeline extract the same part as JSON it will only be unmarshalled
	// once, unless the content of the part has changed. If the index is
	// negative then the part is found by counting backwards from the last part
	// starting at -1.
	GetJSON(p int) (interface{}, error)

	// SetJSON sets a message part to the marshalled bytes of a JSON object, but
	// also caches the object itself. If the JSON contents of a part is
	// subsequently queried it will receive the cached version as long as the
	// raw content has not changed. If the index is negative then the part is
	// found by counting backwards from the last part starting at -1.
	SetJSON(p int, jObj interface{}) error

	// Append appends new message parts to the message and returns the index of
	// last part to be added.
	Append(b ...[]byte) int

	// Len returns the number of parts this message contains.
	Len() int

	// Iter will iterate each message part in order, calling the closure
	// argument with the index and contents of the message part.
	Iter(f func(i int, b []byte))

	// Bytes returns a binary representation of the message, which can be later
	// parsed back into a multipart message with `FromBytes`. The result of this
	// call can itself be the part of a new message, which is a useful way of
	// transporting multiple part messages across protocols that only support
	// single parts.
	Bytes() []byte

	// LazyCondition lazily evaluates conditions on the message by caching the
	// results as per a label to identify the condition. The cache of results is
	// cleared whenever the contents of the message is changed.
	LazyCondition(label string, cond Condition) bool

	// ShallowCopy creates a shallow copy of the message, where the list of
	// message parts can be edited independently from the original version.
	// However, editing the byte array contents of a message part will still
	// alter the contents of the original.
	ShallowCopy() Message

	// DeepCopy creates a deep copy of the message, where the message part
	// contents are entirely copied and are therefore safe to edit without
	// altering the original.
	DeepCopy() Message
}

//------------------------------------------------------------------------------

// NewMessage initializes a new message.
func NewMessage(parts [][]byte) Message {
	return &messageImpl{
		parts: parts,
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
		parts: newParts,
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

func (m *messageImpl) Iter(f func(i int, b []byte)) {
	for i, p := range m.parts {
		f(i, p)
	}
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

//------------------------------------------------------------------------------
