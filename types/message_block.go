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

package types

import "sync"

//--------------------------------------------------------------------------------------------------

/*
MessageBlock - A memory block of serialized messages. All messages are written contiguously, when
the writer is unable to write a full message to the end of the block it will loop back to index 0.

Both writing and reading operations will block until the operation is possible. When Close is called
all blocked operations are escaped.
*/
type MessageBlock struct {
	block     []byte
	size      int
	readFrom  int
	writtenTo int

	closed bool

	cond *sync.Cond
}

// NewMessageBlock - Creates a block for buffering serialized messages.
func NewMessageBlock(limit int) *MessageBlock {
	return &MessageBlock{
		block:     make([]byte, limit),
		size:      limit,
		readFrom:  0,
		writtenTo: 0,
		closed:    false,
		cond:      sync.NewCond(&sync.Mutex{}),
	}
}

//--------------------------------------------------------------------------------------------------

// readMessageSize - Reads the size in bytes of a serialised message block starting at index.
func (m *MessageBlock) readMessageSize(index int) uint32 {
	if index+3 >= m.size {
		return 0
	}
	return uint32(m.block[0+index])<<24 |
		uint32(m.block[1+index])<<16 |
		uint32(m.block[2+index])<<8 |
		uint32(m.block[3+index])
}

// writeMessageSize - Writes the size in bytes of a serialised message block starting at index.
func (m *MessageBlock) writeMessageSize(index int, size uint32) {
	m.block[index+0] = byte(size >> 24)
	m.block[index+1] = byte(size >> 16)
	m.block[index+2] = byte(size >> 8)
	m.block[index+3] = byte(size)
}

//--------------------------------------------------------------------------------------------------

// Close - Unblocks any blocked calls and prevents further writing to the block.
func (m *MessageBlock) Close() {
	m.cond.L.Lock()
	m.closed = true
	m.cond.Broadcast()
	m.cond.L.Unlock()
}

// ShiftMessage - Removes the last message from the block.
func (m *MessageBlock) ShiftMessage() {
	m.cond.L.Lock()
	defer m.cond.Broadcast()
	defer m.cond.L.Unlock()

	msgSize := m.readMessageSize(m.readFrom)

	// Messages are written in a contiguous array of bytes, therefore when the writer reaches the
	// end it will zero the next four bytes (zero size message) to indicate to the reader that it
	// has looped back to index 0.
	if msgSize <= 0 {
		m.readFrom = 0
		msgSize = m.readMessageSize(m.readFrom)
	}

	// Set new read from position to next message start.
	m.readFrom = m.readFrom + int(msgSize) + 4
}

// NextMessage - Reads the next message, this call blocks until there's something to read.
func (m *MessageBlock) NextMessage() (Message, error) {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	for m.readFrom == m.writtenTo {
		if m.closed {
			return Message{}, ErrTypeClosed
		}
		m.cond.Wait()
	}

	index := m.readFrom
	msgSize := m.readMessageSize(index)

	// Messages are written in a contiguous array of bytes, therefore when the writer reaches the
	// end it will zero the next four bytes (zero size message) to indicate to the reader that it
	// has looped back to index 0.
	if msgSize <= 0 {
		index = 0
		msgSize = m.readMessageSize(index)
	}

	index = index + 4
	if index+int(msgSize) > m.size {
		return Message{}, ErrBlockCorrupted
	}

	return FromBytes(m.block[index : index+int(msgSize)])
}

// PushMessage - Pushes a new message onto the block, this call blocks until space is available.
func (m *MessageBlock) PushMessage(msg Message) {
	m.cond.L.Lock()
	defer m.cond.Broadcast()
	defer m.cond.L.Unlock()

	block := msg.Bytes()

	// Block while the reader is catching up.
	for m.readFrom > m.writtenTo && m.readFrom <= m.writtenTo+len(block)+4 {
		m.cond.Wait()
	}

	index := m.writtenTo

	// If we can't fit our next message in the remainder of the buffer we will loop back to index 0.
	// In order to prevent the reader from reading garbage we set the next message size to 0, which
	// tells the reader to loop back to index 0.
	if len(block)+4+index > m.size {

		// If the reader is currently at 0 then we need to avoid looping over it.
		for m.readFrom <= len(block)+4 {
			m.cond.Wait()
		}
		for i := index; i < m.size && i < index+4; i++ {
			m.block[i] = byte(0)
		}
		index = 0
	}

	// Block again if the reader is catching up.
	for m.readFrom > index && m.readFrom <= index+len(block)+4 {
		m.cond.Wait()
	}

	m.writeMessageSize(index, uint32(len(block)))
	copy(m.block[index+4:], block)

	// Move writtenTo index ahead. If writtenTo becomes m.size we want it to wrap back to 0
	m.writtenTo = (index + len(block) + 4) % m.size
}

//--------------------------------------------------------------------------------------------------
