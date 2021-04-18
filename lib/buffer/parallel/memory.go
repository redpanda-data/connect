package parallel

import (
	"sync"

	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Memory is a parallel buffer implementation that allows multiple parallel
// consumers to read and purge messages from the buffer asynchronously.
type Memory struct {
	messages     []types.Message
	bytes        int
	pendingBytes int

	cap  int
	cond *sync.Cond

	closed bool
}

// NewMemory creates a memory based parallel buffer.
func NewMemory(capacity int) *Memory {
	return &Memory{
		bytes: 0,
		cap:   capacity,
		cond:  sync.NewCond(&sync.Mutex{}),
	}
}

//------------------------------------------------------------------------------

// NextMessage reads the next oldest message, the message is preserved until the
// returned AckFunc is called.
func (m *Memory) NextMessage() (types.Message, AckFunc, error) {
	m.cond.L.Lock()
	for len(m.messages) == 0 && !m.closed {
		m.cond.Wait()
	}

	if m.closed {
		m.cond.L.Unlock()
		return nil, nil, types.ErrTypeClosed
	}

	msg := m.messages[0]

	m.messages[0] = nil
	m.messages = m.messages[1:]

	messageSize := 0
	msg.Iter(func(i int, b types.Part) error {
		messageSize += len(b.Get())
		return nil
	})
	m.pendingBytes += messageSize

	m.cond.Broadcast()
	m.cond.L.Unlock()

	return msg, func(ack bool) (int, error) {
		m.cond.L.Lock()
		if m.closed {
			m.cond.L.Unlock()
			return 0, types.ErrTypeClosed
		}
		m.pendingBytes -= messageSize
		if ack {
			m.bytes -= messageSize
		} else {
			m.messages = append([]types.Message{msg}, m.messages...)
		}
		m.cond.Broadcast()

		backlog := m.bytes
		m.cond.L.Unlock()

		return backlog, nil
	}, nil
}

// PushMessage adds a new message to the stack. Returns the backlog in bytes.
func (m *Memory) PushMessage(msg types.Message) (int, error) {
	extraBytes := 0
	msg.Iter(func(i int, b types.Part) error {
		extraBytes += len(b.Get())
		return nil
	})

	if extraBytes > m.cap {
		return 0, types.ErrMessageTooLarge
	}

	m.cond.L.Lock()

	if m.closed {
		m.cond.L.Unlock()
		return 0, types.ErrTypeClosed
	}

	for (m.bytes + extraBytes) > m.cap {
		m.cond.Wait()
		if m.closed {
			m.cond.L.Unlock()
			return 0, types.ErrTypeClosed
		}
	}

	m.messages = append(m.messages, msg.DeepCopy())
	m.bytes += extraBytes

	backlog := m.bytes

	m.cond.Broadcast()
	m.cond.L.Unlock()

	return backlog, nil
}

// CloseOnceEmpty closes the Buffer once the buffer has been emptied, this is a
// way for a writer to signal to a reader that it is finished writing messages,
// and therefore the reader can close once it is caught up. This call blocks
// until the close is completed.
func (m *Memory) CloseOnceEmpty() {
	m.cond.L.Lock()
	// TODO: We include pendingBytes here even though they're not acked, which
	// means if those pending messages fail we have message loss. However, if we
	// don't count them then we don't have any way to signal to a batcher at the
	// upper level that it should flush the final batch. We need a cleaner
	// mechanism here.
	for (m.bytes-m.pendingBytes > 0) && !m.closed {
		m.cond.Wait()
	}
	if !m.closed {
		m.closed = true
		m.cond.Broadcast()
	}
	m.cond.L.Unlock()
}

// Close closes the Buffer so that blocked readers or writers become
// unblocked.
func (m *Memory) Close() {
	m.cond.L.Lock()
	m.closed = true
	m.cond.Broadcast()
	m.cond.L.Unlock()
}

//------------------------------------------------------------------------------
