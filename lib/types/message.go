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

//------------------------------------------------------------------------------

// Message is a struct containing any relevant fields of a benthos message and
// helper functions.
type Message struct {
	Parts [][]byte `json:"parts"`
}

// NewMessage initializes an empty message.
func NewMessage() Message {
	return Message{
		Parts: [][]byte{},
	}
}

//------------------------------------------------------------------------------

/*
Internal message blob format:

- Four bytes containing number of message parts in big endian
- For each message part:
    + Four bytes containing length of message part in big endian
    + Content of message part

                                         # Of bytes in message 2
                                         |
# Of message parts (big endian)          |           Content of message 2
|                                        |           |
v                                        v           v
| 0| 0| 0| 2| 0| 0| 0| 5| h| e| l| l| o| 0| 0| 0| 5| w| o| r| l| d|
  0  1  2  3  4  5  6  7  8  9 10 11 13 14 15 16 17 18 19 20 21 22
              ^           ^
              |           |
              |           Content of message 1
              |
              # Of bytes in message 1 (big endian)
*/

// Reserve bytes for our length counter (4 * 8 = 32 bit)
var intLen uint32 = 4

// Bytes serialises the message into a single byte array.
func (m *Message) Bytes() []byte {
	lenParts := uint32(len(m.Parts))

	l := (lenParts + 1) * intLen
	for i := range m.Parts {
		l += uint32(len(m.Parts[i]))
	}
	b := make([]byte, l)

	b[0] = byte(lenParts >> 24)
	b[1] = byte(lenParts >> 16)
	b[2] = byte(lenParts >> 8)
	b[3] = byte(lenParts)

	b2 := b[intLen:]
	for i := range m.Parts {
		le := uint32(len(m.Parts[i]))

		b2[0] = byte(le >> 24)
		b2[1] = byte(le >> 16)
		b2[2] = byte(le >> 8)
		b2[3] = byte(le)

		b2 = b2[intLen:]

		copy(b2, m.Parts[i])
		b2 = b2[len(m.Parts[i]):]
	}
	return b
}

// FromBytes deserialises a Message from a byte array.
func FromBytes(b []byte) (Message, error) {
	var m Message

	if len(b) < 4 {
		return m, ErrBadMessageBytes
	}

	numParts := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
	if numParts >= uint32(len(b)) {
		return m, ErrBadMessageBytes
	}

	m.Parts = make([][]byte, numParts)

	b = b[4:]

	for i := uint32(0); i < numParts; i++ {
		if len(b) < 4 {
			return m, ErrBadMessageBytes
		}
		partSize := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
		b = b[4:]

		if uint32(len(b)) < partSize {
			return m, ErrBadMessageBytes
		}
		m.Parts[i] = b[:partSize]
		b = b[partSize:]
	}
	return m, nil
}

//------------------------------------------------------------------------------
