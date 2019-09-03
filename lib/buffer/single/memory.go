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

package single

import (
	"sync"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// MemoryConfig is config values for a purely memory based ring buffer type.
type MemoryConfig struct {
	Limit int `json:"limit" yaml:"limit"`
}

// NewMemoryConfig creates a new MemoryConfig with default values.
func NewMemoryConfig() MemoryConfig {
	return MemoryConfig{
		Limit: 1024 * 1024 * 500, // 500MB
	}
}

// Memory is a purely memory based ring buffer. This buffer blocks when the
// buffer is full.
type Memory struct {
	config MemoryConfig

	block     []byte
	readFrom  int
	writtenTo int

	closed bool

	cond *sync.Cond
}

// NewMemory creates a new memory based ring buffer.
func NewMemory(config MemoryConfig) *Memory {
	return &Memory{
		config:    config,
		block:     make([]byte, config.Limit),
		readFrom:  0,
		writtenTo: 0,
		closed:    false,
		cond:      sync.NewCond(&sync.Mutex{}),
	}
}

//------------------------------------------------------------------------------

// backlog reads the current backlog of messages stored.
func (m *Memory) backlog() int {
	if m.writtenTo >= m.readFrom {
		return m.writtenTo - m.readFrom
	}
	return m.config.Limit - m.readFrom + m.writtenTo
}

// readMessageSize reads the size in bytes of a serialised message block
// starting at index.
func readMessageSize(block []byte, index int) int {
	if index+3 >= len(block) {
		return 0
	}
	return int(block[0+index])<<24 |
		int(block[1+index])<<16 |
		int(block[2+index])<<8 |
		int(block[3+index])
}

// writeMessageSize writes the size in bytes of a serialised message block
// starting at index.
func writeMessageSize(block []byte, index int, size int) {
	block[index+0] = byte(size >> 24)
	block[index+1] = byte(size >> 16)
	block[index+2] = byte(size >> 8)
	block[index+3] = byte(size)
}

//------------------------------------------------------------------------------

// CloseOnceEmpty closes the memory buffer once the backlog reaches 0.
func (m *Memory) CloseOnceEmpty() {
	defer func() {
		m.cond.L.Unlock()
		m.Close()
	}()
	m.cond.L.Lock()

	// Until the backlog is cleared.
	for m.backlog() > 0 {
		// Wait for a broadcast from our reader.
		m.cond.Wait()
	}
}

// Close unblocks any blocked calls and prevents further writing to the block.
func (m *Memory) Close() {
	m.cond.L.Lock()
	m.closed = true
	m.cond.Broadcast()
	m.cond.L.Unlock()
}

// ShiftMessage removes the last message from the block. Returns the backlog
// count.
func (m *Memory) ShiftMessage() (int, error) {
	m.cond.L.Lock()
	defer func() {
		m.cond.Broadcast()
		m.cond.L.Unlock()
	}()

	msgSize := readMessageSize(m.block, m.readFrom)

	// Messages are written in a contiguous array of bytes, therefore when the
	// writer reaches the end it will zero the next four bytes (zero size
	// message) to indicate to the reader that it has looped back to index 0.
	if msgSize <= 0 {
		m.readFrom = 0
		msgSize = readMessageSize(m.block, m.readFrom)
	}

	// Set new read from position to next message start.
	m.readFrom = m.readFrom + int(msgSize) + 4

	return m.backlog(), nil
}

// NextMessage reads the next message, this call blocks until there's something
// to read.
func (m *Memory) NextMessage() (types.Message, error) {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	index := m.readFrom

	for index == m.writtenTo && !m.closed {
		m.cond.Wait()
	}
	if m.closed {
		return nil, types.ErrTypeClosed
	}

	msgSize := readMessageSize(m.block, index)

	// Messages are written in a contiguous array of bytes, therefore when the
	// writer reaches the end it will zero the next four bytes (zero size
	// message) to indicate to the reader that it has looped back to index 0.
	if msgSize <= 0 {
		index = 0
		for index == m.writtenTo && !m.closed {
			m.cond.Wait()
		}
		if m.closed {
			return nil, types.ErrTypeClosed
		}

		msgSize = readMessageSize(m.block, index)
	}

	index = index + 4
	if index+int(msgSize) > m.config.Limit {
		return nil, types.ErrBlockCorrupted
	}

	return message.FromBytes(m.block[index : index+int(msgSize)])
}

// PushMessage pushes a new message onto the block, returns the backlog count.
func (m *Memory) PushMessage(msg types.Message) (int, error) {
	m.cond.L.Lock()
	defer func() {
		m.cond.Broadcast()
		m.cond.L.Unlock()
	}()

	block := message.ToBytes(msg)
	index := m.writtenTo

	if len(block)+4 > m.config.Limit {
		return 0, types.ErrMessageTooLarge
	}

	// Block while the reader is catching up.
	for m.readFrom > index && m.readFrom <= index+len(block)+4 {
		m.cond.Wait()
	}
	if m.closed {
		return 0, types.ErrTypeClosed
	}

	// If we can't fit our next message in the remainder of the buffer we will
	// loop back to index 0. In order to prevent the reader from reading garbage
	// we set the next message size to 0, which tells the reader to loop back to
	// index 0.
	if len(block)+4+index > m.config.Limit {

		// If the reader is currently at 0 then we avoid looping over it.
		for m.readFrom <= len(block)+4 && !m.closed {
			m.cond.Wait()
		}
		if m.closed {
			return 0, types.ErrTypeClosed
		}
		for i := index; i < m.config.Limit && i < index+4; i++ {
			m.block[i] = byte(0)
		}
		index = 0
	}

	// Block again if the reader is catching up.
	for m.readFrom > index && m.readFrom <= index+len(block)+4 && !m.closed {
		m.cond.Wait()
	}
	if m.closed {
		return 0, types.ErrTypeClosed
	}

	writeMessageSize(m.block, index, len(block))
	copy(m.block[index+4:], block)

	// Move writtenTo index ahead. If writtenTo becomes m.config.Limit we want
	// it to wrap back to 0
	m.writtenTo = (index + len(block) + 4) % m.config.Limit

	return m.backlog(), nil
}

//------------------------------------------------------------------------------
