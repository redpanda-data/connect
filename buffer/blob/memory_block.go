/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package blob

import (
	"sync"

	"github.com/jeffail/benthos/types"
)

//--------------------------------------------------------------------------------------------------

// MemoryBlockConfig - Config values for a MemoryBlock type.
type MemoryBlockConfig struct {
	Limit int `json:"limit" yaml:"limit"`
}

// NewMemoryBlockConfig - Create a new MemoryBlockConfig with default values.
func NewMemoryBlockConfig() MemoryBlockConfig {
	return MemoryBlockConfig{
		Limit: 1024 * 1024 * 500, // 500MB
	}
}

/*
MemoryBlock - A memory block of serialized messages. All messages are written contiguously, when
the writer is unable to write a full message to the end of the block it will loop back to index 0.

Both writing and reading operations will block until the operation is possible. When Close is called
all blocked operations are escaped.
*/
type MemoryBlock struct {
	config MemoryBlockConfig

	block     []byte
	readFrom  int
	writtenTo int

	closed bool

	cond *sync.Cond
}

// NewMemoryBlock - Creates a block for buffering serialized messages.
func NewMemoryBlock(config MemoryBlockConfig) *MemoryBlock {
	return &MemoryBlock{
		config:    config,
		block:     make([]byte, config.Limit),
		readFrom:  0,
		writtenTo: 0,
		closed:    false,
		cond:      sync.NewCond(&sync.Mutex{}),
	}
}

//--------------------------------------------------------------------------------------------------

// backlog - Reads the current backlog of messages stored.
func (m *MemoryBlock) backlog() int {
	if m.writtenTo >= m.readFrom {
		return m.writtenTo - m.readFrom
	}
	return m.config.Limit - m.readFrom + m.writtenTo
}

// readMessageSize - Reads the size in bytes of a serialised message block starting at index.
func readMessageSize(block []byte, index int) int {
	if index+3 >= len(block) {
		return 0
	}
	return int(block[0+index])<<24 |
		int(block[1+index])<<16 |
		int(block[2+index])<<8 |
		int(block[3+index])
}

// writeMessageSize - Writes the size in bytes of a serialised message block starting at index.
func writeMessageSize(block []byte, index int, size int) {
	block[index+0] = byte(size >> 24)
	block[index+1] = byte(size >> 16)
	block[index+2] = byte(size >> 8)
	block[index+3] = byte(size)
}

//--------------------------------------------------------------------------------------------------

// Close - Unblocks any blocked calls and prevents further writing to the block.
func (m *MemoryBlock) Close() {
	m.cond.L.Lock()
	m.closed = true
	m.cond.Broadcast()
	m.cond.L.Unlock()
}

// ShiftMessage - Removes the last message from the block. Returns the backlog count.
func (m *MemoryBlock) ShiftMessage() int {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()
	defer m.cond.Broadcast()

	msgSize := readMessageSize(m.block, m.readFrom)

	// Messages are written in a contiguous array of bytes, therefore when the writer reaches the
	// end it will zero the next four bytes (zero size message) to indicate to the reader that it
	// has looped back to index 0.
	if msgSize <= 0 {
		m.readFrom = 0
		msgSize = readMessageSize(m.block, m.readFrom)
	}

	// Set new read from position to next message start.
	m.readFrom = m.readFrom + int(msgSize) + 4

	return m.backlog()
}

// NextMessage - Reads the next message, this call blocks until there's something to read.
func (m *MemoryBlock) NextMessage() (types.Message, error) {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	index := m.readFrom

	for index == m.writtenTo && !m.closed {
		m.cond.Wait()
	}
	if m.closed {
		return types.Message{}, types.ErrTypeClosed
	}

	msgSize := readMessageSize(m.block, index)

	// Messages are written in a contiguous array of bytes, therefore when the writer reaches the
	// end it will zero the next four bytes (zero size message) to indicate to the reader that it
	// has looped back to index 0.
	if msgSize <= 0 {
		index = 0
		for index == m.writtenTo && !m.closed {
			m.cond.Wait()
		}
		if m.closed {
			return types.Message{}, types.ErrTypeClosed
		}

		msgSize = readMessageSize(m.block, index)
	}

	index = index + 4
	if index+int(msgSize) > m.config.Limit {
		return types.Message{}, types.ErrBlockCorrupted
	}

	return types.FromBytes(m.block[index : index+int(msgSize)])
}

// PushMessage - Pushes a new message onto the block, returns the backlog count.
func (m *MemoryBlock) PushMessage(msg types.Message) int {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()
	defer m.cond.Broadcast()

	block := msg.Bytes()
	index := m.writtenTo

	// Block while the reader is catching up.
	for m.readFrom > index && m.readFrom <= index+len(block)+4 {
		m.cond.Wait()
	}

	// If we can't fit our next message in the remainder of the buffer we will loop back to index 0.
	// In order to prevent the reader from reading garbage we set the next message size to 0, which
	// tells the reader to loop back to index 0.
	if len(block)+4+index > m.config.Limit {

		// If the reader is currently at 0 then we need to avoid looping over it.
		for m.readFrom <= len(block)+4 {
			m.cond.Wait()
		}
		for i := index; i < m.config.Limit && i < index+4; i++ {
			m.block[i] = byte(0)
		}
		index = 0
	}

	// Block again if the reader is catching up.
	for m.readFrom > index && m.readFrom <= index+len(block)+4 {
		m.cond.Wait()
	}

	writeMessageSize(m.block, index, len(block))
	copy(m.block[index+4:], block)

	// Move writtenTo index ahead. If writtenTo becomes m.config.Limit we want it to wrap back to 0
	m.writtenTo = (index + len(block) + 4) % m.config.Limit

	return m.backlog()
}

//--------------------------------------------------------------------------------------------------
