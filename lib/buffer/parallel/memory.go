// Copyright (c) 2018 Ashley Jeffs
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
func NewMemory(cap int) *Memory {
	return &Memory{
		bytes: 0,
		cap:   cap,
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
