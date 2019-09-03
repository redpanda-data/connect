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
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Type is the standard implementation of types.Message, containing a multiple
// part message.
type Type struct {
	createdAt time.Time
	parts     []types.Part
}

// New initializes a new message from a 2D byte slice, the slice can be nil.
func New(bslice [][]byte) *Type {
	parts := make([]types.Part, len(bslice))
	for i, v := range bslice {
		parts[i] = NewPart(v)
	}
	return &Type{
		createdAt: time.Now(),
		parts:     parts,
	}
}

//------------------------------------------------------------------------------

// Copy creates a new shallow copy of the message. Parts can be re-arranged in
// the new copy and JSON parts can be get/set without impacting other message
// copies. However, it is still unsafe to edit the raw content of message parts.
func (m *Type) Copy() types.Message {
	parts := make([]types.Part, len(m.parts))
	for i, v := range m.parts {
		parts[i] = v.Copy()
	}
	return &Type{
		createdAt: m.createdAt,
		parts:     parts,
	}
}

// DeepCopy creates a new deep copy of the message. This can be considered an
// entirely new object that is safe to use anywhere.
func (m *Type) DeepCopy() types.Message {
	parts := make([]types.Part, len(m.parts))
	for i, v := range m.parts {
		parts[i] = v.DeepCopy()
	}
	return &Type{
		createdAt: m.createdAt,
		parts:     parts,
	}
}

//------------------------------------------------------------------------------

// Get returns a message part at a particular index, indexes can be negative.
func (m *Type) Get(index int) types.Part {
	if index < 0 {
		index = len(m.parts) + index
	}
	if index < 0 || index >= len(m.parts) {
		return NewPart(nil)
	}
	if m.parts[index] == nil {
		m.parts[index] = NewPart(nil)
	}
	return m.parts[index]
}

// SetAll changes the entire set of message parts.
func (m *Type) SetAll(parts []types.Part) {
	m.parts = parts
}

// Append adds a new message part to the message.
func (m *Type) Append(b ...types.Part) int {
	m.parts = append(m.parts, b...)
	return len(m.parts) - 1
}

// Len returns the length of the message in parts.
func (m *Type) Len() int {
	return len(m.parts)
}

// Iter will iterate all parts of the message, calling f for each.
func (m *Type) Iter(f func(i int, p types.Part) error) error {
	for i, p := range m.parts {
		if p == nil {
			p = NewPart(nil)
			m.parts[i] = p
		}
		if err := f(i, p); err != nil {
			return err
		}
	}
	return nil
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

// ToBytes serialises a message into a single byte array.
func ToBytes(m types.Message) []byte {
	lenParts := uint32(m.Len())

	l := (lenParts + 1) * intLen
	m.Iter(func(i int, p types.Part) error {
		l += uint32(len(p.Get()))
		return nil
	})
	b := make([]byte, l)

	b[0] = byte(lenParts >> 24)
	b[1] = byte(lenParts >> 16)
	b[2] = byte(lenParts >> 8)
	b[3] = byte(lenParts)

	b2 := b[intLen:]

	m.Iter(func(i int, p types.Part) error {
		le := uint32(len(p.Get()))

		b2[0] = byte(le >> 24)
		b2[1] = byte(le >> 16)
		b2[2] = byte(le >> 8)
		b2[3] = byte(le)

		b2 = b2[intLen:]

		copy(b2, p.Get())
		b2 = b2[len(p.Get()):]
		return nil
	})

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
		m.Append(NewPart(b[:partSize]))
		b = b[partSize:]
	}
	return m, nil
}

//------------------------------------------------------------------------------
