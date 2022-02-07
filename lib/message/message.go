package message

// Batch represents zero or more messages.
type Batch struct {
	parts []*Part
}

// QuickBatch initializes a new message batch from a 2D byte slice, the slice
// can be nil, in which case the batch will start empty.
func QuickBatch(bslice [][]byte) *Batch {
	parts := make([]*Part, len(bslice))
	for i, v := range bslice {
		parts[i] = NewPart(v)
	}
	return &Batch{
		parts: parts,
	}
}

//------------------------------------------------------------------------------

// Copy creates a new shallow copy of the message. Parts can be re-arranged in
// the new copy and JSON parts can be get/set without impacting other message
// copies. However, it is still unsafe to edit the raw content of message parts.
func (m *Batch) Copy() *Batch {
	parts := make([]*Part, len(m.parts))
	for i, v := range m.parts {
		parts[i] = v.Copy()
	}
	return &Batch{
		parts: parts,
	}
}

// DeepCopy creates a new deep copy of the message. This can be considered an
// entirely new object that is safe to use anywhere.
func (m *Batch) DeepCopy() *Batch {
	parts := make([]*Part, len(m.parts))
	for i, v := range m.parts {
		parts[i] = v.DeepCopy()
	}
	return &Batch{
		parts: parts,
	}
}

//------------------------------------------------------------------------------

// Get returns a message part at a particular index, indexes can be negative.
func (m *Batch) Get(index int) *Part {
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
func (m *Batch) SetAll(parts []*Part) {
	m.parts = parts
}

// Append adds a new message part to the message.
func (m *Batch) Append(b ...*Part) int {
	m.parts = append(m.parts, b...)
	return len(m.parts) - 1
}

// Len returns the length of the message in parts.
func (m *Batch) Len() int {
	return len(m.parts)
}

// Iter will iterate all parts of the message, calling f for each.
func (m *Batch) Iter(f func(i int, p *Part) error) error {
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
func ToBytes(m *Batch) []byte {
	lenParts := uint32(m.Len())

	l := (lenParts + 1) * intLen
	_ = m.Iter(func(i int, p *Part) error {
		l += uint32(len(p.Get()))
		return nil
	})
	b := make([]byte, l)

	b[0] = byte(lenParts >> 24)
	b[1] = byte(lenParts >> 16)
	b[2] = byte(lenParts >> 8)
	b[3] = byte(lenParts)

	b2 := b[intLen:]

	_ = m.Iter(func(i int, p *Part) error {
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
func FromBytes(b []byte) (*Batch, error) {
	if len(b) < 4 {
		return nil, ErrBadMessageBytes
	}

	numParts := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
	if numParts >= uint32(len(b)) {
		return nil, ErrBadMessageBytes
	}

	b = b[4:]

	m := QuickBatch(nil)
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
