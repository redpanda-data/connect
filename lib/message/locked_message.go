package message

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Lock wraps a message into a read only type that restricts access to only a
// single message part, accessible either by index 0 or -1.
func Lock(msg types.Message, part int) types.Message {
	return &lockedMessage{
		m:    msg,
		part: part,
	}
}

//------------------------------------------------------------------------------

// lockedMessage wraps a message in a read only restricted type.
type lockedMessage struct {
	m    types.Message
	part int
}

// Copy creates a shallow copy of the locked message, meaning it has only one
// part.
func (m *lockedMessage) Copy() types.Message {
	parts := []types.Part{
		m.m.Get(m.part).Copy(),
	}
	msg := New(nil)
	msg.SetAll(parts)
	return msg
}

// DeepCopy creates a deep copy of the locked message, meaning it has only one
// part.
func (m *lockedMessage) DeepCopy() types.Message {
	parts := []types.Part{
		m.m.Get(m.part).DeepCopy(),
	}
	msg := New(nil)
	msg.SetAll(parts)
	return msg
}

func (m *lockedMessage) Get(index int) types.Part {
	if index != 0 && index != -1 {
		return NewPart(nil)
	}
	return m.m.Get(m.part)
}

func (m *lockedMessage) SetAll(p []types.Part) {
}

func (m *lockedMessage) Append(b ...types.Part) int {
	return 0
}

func (m *lockedMessage) Len() int {
	if m.m.Len() == 0 {
		return 0
	}
	return 1
}

func (m *lockedMessage) Iter(f func(i int, b types.Part) error) error {
	return f(0, m.m.Get(m.part).Copy())
}

func (m *lockedMessage) CreatedAt() time.Time {
	return m.m.CreatedAt()
}

//------------------------------------------------------------------------------
