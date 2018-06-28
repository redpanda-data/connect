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

package types

import (
	"time"
)

//------------------------------------------------------------------------------

// LockMessage wraps a message into a read only type that restricts access to
// only a single message part, accessible either by index 0 or -1.
func LockMessage(msg Message, part int) Message {
	return &lockedMessage{
		m:    msg,
		part: part,
	}
}

//------------------------------------------------------------------------------

// lockedMessage wraps a message in a read only restricted type.
type lockedMessage struct {
	m    Message
	part int
}

// Bytes serialises the message into a single byte array.
func (m *lockedMessage) Bytes() []byte {
	return m.m.Bytes()
}

// ShallowCopy simply returns the same type, it's read only and therefore
// doesn't need copying.
func (m *lockedMessage) ShallowCopy() Message {
	return m
}

// DeepCopy simply returns the same type, it's read only and therefore doesn't
// need copying.
func (m *lockedMessage) DeepCopy() Message {
	return m
}

func (m *lockedMessage) Get(index int) []byte {
	if index != 0 && index != -1 {
		return nil
	}
	return m.m.Get(m.part)
}

func (m *lockedMessage) GetAll() [][]byte {
	return [][]byte{m.m.Get(m.part)}
}

func (m *lockedMessage) Set(index int, b []byte) {
}

func (m *lockedMessage) SetAll(p [][]byte) {
}

func (m *lockedMessage) Append(b ...[]byte) int {
	return 0
}

func (m *lockedMessage) Len() int {
	if m.m.Len() == 0 {
		return 0
	}
	return 1
}

func (m *lockedMessage) Iter(f func(i int, b []byte) error) error {
	return f(0, m.m.Get(m.part))
}

func (m *lockedMessage) GetJSON(part int) (interface{}, error) {
	if part != 0 && part != -1 {
		return nil, ErrMessagePartNotExist
	}
	return m.m.GetJSON(m.part)
}

func (m *lockedMessage) SetJSON(part int, jObj interface{}) error {
	return nil
}

func (m *lockedMessage) LazyCondition(label string, cond Condition) bool {
	return m.m.LazyCondition(label, cond)
}

func (m *lockedMessage) CreatedAt() time.Time {
	return m.m.CreatedAt()
}

//------------------------------------------------------------------------------
