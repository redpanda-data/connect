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
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestMemoryBasic(t *testing.T) {
	t.Skip("DEPRECATED")

	n := 100

	block := NewMemory(MemoryConfig{Limit: 100000})

	for i := 0; i < n; i++ {
		if _, err := block.PushMessage(message.New(
			[][]byte{
				[]byte("hello"),
				[]byte("world"),
				[]byte("12345"),
				[]byte(fmt.Sprintf("test%v", i)),
			},
		)); err != nil {
			t.Error(err)
		}
	}

	for i := 0; i < n; i++ {
		m, err := block.NextMessage()
		if err != nil {
			t.Error(err)
			return
		}
		if m.Len() != 4 {
			t.Errorf("Wrong # parts, %v != %v", m.Len(), 4)
		} else if expected, actual := fmt.Sprintf("test%v", i), string(m.Get(3).Get()); expected != actual {
			t.Errorf("Wrong order of messages, %v != %v", expected, actual)
		}
		if _, err := block.ShiftMessage(); err != nil {
			t.Error(err)
		}
	}
}

func TestMemoryBacklogCounter(t *testing.T) {
	t.Skip("DEPRECATED")

	block := NewMemory(MemoryConfig{Limit: 100000})

	if _, err := block.PushMessage(message.New(
		[][]byte{[]byte("1234")}, // 4 bytes + 4 bytes
	)); err != nil {
		t.Error(err)
		return
	}

	if expected, actual := 16, block.backlog(); expected != actual {
		t.Errorf("Wrong backlog count: %v != %v", expected, actual)
	}

	if _, err := block.PushMessage(message.New(
		[][]byte{
			[]byte("1234"),
			[]byte("1234"),
		}, // ( 4 bytes + 4 bytes ) * 2
	)); err != nil {
		t.Error(err)
		return
	}

	if expected, actual := 40, block.backlog(); expected != actual {
		t.Errorf("Wrong backlog count: %v != %v", expected, actual)
	}

	if _, err := block.ShiftMessage(); err != nil {
		t.Error(err)
	}

	if expected, actual := 24, block.backlog(); expected != actual {
		t.Errorf("Wrong backlog count: %v != %v", expected, actual)
	}

	if _, err := block.ShiftMessage(); err != nil {
		t.Error(err)
	}

	if expected, actual := 0, block.backlog(); expected != actual {
		t.Errorf("Wrong backlog count: %v != %v", expected, actual)
	}
}

func TestMemoryNearLimit(t *testing.T) {
	t.Skip("DEPRECATED")

	n, iter := 50, 5

	block := NewMemory(MemoryConfig{Limit: 2285})

	for j := 0; j < iter; j++ {
		for i := 0; i < n; i++ {
			if _, err := block.PushMessage(message.New(
				[][]byte{
					[]byte("hello"),
					[]byte("world"),
					[]byte("12345"),
					[]byte(fmt.Sprintf("test%v", i)),
				},
			)); err != nil {
				t.Error(err)
				return
			}
		}

		for i := 0; i < n; i++ {
			m, err := block.NextMessage()
			if err != nil {
				t.Error(err)
				return
			}
			if m.Len() != 4 {
				t.Errorf("Wrong # parts, %v != %v", m.Len(), 4)
			} else if expected, actual := fmt.Sprintf("test%v", i), string(m.Get(3).Get()); expected != actual {
				t.Errorf("Wrong order of messages, %v != %v", expected, actual)
			}
			if _, err := block.ShiftMessage(); err != nil {
				t.Error(err)
			}
		}
	}
}

func TestMemoryLoopingRandom(t *testing.T) {
	t.Skip("DEPRECATED")

	n, iter := 50, 5

	block := NewMemory(MemoryConfig{Limit: 8000})

	for j := 0; j < iter; j++ {
		for i := 0; i < n; i++ {
			b := make([]byte, rand.Int()%100)
			for k := range b {
				b[k] = '0'
			}
			if _, err := block.PushMessage(message.New(
				[][]byte{
					b,
					[]byte(fmt.Sprintf("test%v", i)),
				},
			)); err != nil {
				t.Error(err)
			}
		}

		for i := 0; i < n; i++ {
			m, err := block.NextMessage()
			if err != nil {
				t.Error(err)
				return
			}
			if m.Len() != 2 {
				t.Errorf("Wrong # parts, %v != %v", m.Len(), 4)
				return
			} else if expected, actual := fmt.Sprintf("test%v", i), string(m.Get(1).Get()); expected != actual {
				t.Errorf("Wrong order of messages, %v != %v", expected, actual)
				return
			}
			if _, err := block.ShiftMessage(); err != nil {
				t.Error(err)
			}
		}
	}
}

func TestMemoryLockStep(t *testing.T) {
	t.Skip("DEPRECATED")

	n := 10000

	block := NewMemory(MemoryConfig{Limit: 1000})

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			m, err := block.NextMessage()
			if err != nil {
				t.Error(err)
			}
			if m.Len() != 4 {
				t.Errorf("Wrong # parts, %v != %v", m.Len(), 4)
				return
			} else if expected, actual := fmt.Sprintf("test%v", i), string(m.Get(3).Get()); expected != actual {
				t.Errorf("Wrong order of messages, %v != %v", expected, actual)
				return
			}
			if _, err := block.ShiftMessage(); err != nil {
				t.Error(err)
			}
		}
	}()

	go func() {
		for i := 0; i < n; i++ {
			if _, err := block.PushMessage(message.New(
				[][]byte{
					[]byte("hello"),
					[]byte("world"),
					[]byte("12345"),
					[]byte(fmt.Sprintf("test%v", i)),
				},
			)); err != nil {
				t.Error(err)
			}
		}
	}()

	wg.Wait()
}

func TestMemoryClose(t *testing.T) {
	t.Skip("DEPRECATED")

	// Test reader block

	block := NewMemory(MemoryConfig{Limit: 20})
	doneChan := make(chan struct{})

	go func() {
		_, err := block.NextMessage()
		if err != types.ErrTypeClosed {
			t.Errorf("Wrong error returned: %v != %v", err, types.ErrTypeClosed)
		}
		close(doneChan)
	}()

	<-time.After(100 * time.Millisecond)
	block.Close()

	select {
	case <-doneChan:
	case <-time.After(time.Second):
		t.Errorf("Timed out after block close on reader")
	}

	// Test writer block

	block = NewMemory(MemoryConfig{Limit: 100})
	doneChan = make(chan struct{})

	go func() {
		for i := 0; i < 100; i++ {
			if _, err := block.PushMessage(message.New(
				[][]byte{
					[]byte("hello"),
					[]byte("world"),
					[]byte("12345"),
					[]byte(fmt.Sprintf("test%v", i)),
				},
			)); err != nil {
				t.Error(err)
			}
		}
		close(doneChan)
	}()

	go func() {
		for {
			_, err := block.NextMessage()
			if err == types.ErrTypeClosed {
				return
			} else if err != nil {
				t.Error(err)
			}
			if _, err := block.ShiftMessage(); err != nil {
				t.Error(err)
			}
		}
	}()

	<-time.After(100 * time.Millisecond)
	block.Close()

	select {
	case <-doneChan:
	case <-time.After(time.Second * 1):
		t.Errorf("Timed out after block close on writer")
	}
}

func TestMemoryRejectLargeMessage(t *testing.T) {
	t.Skip("DEPRECATED")

	tMsg := message.New(make([][]byte, 1))
	tMsg.Get(0).Set([]byte("hello world this message is too long!"))

	block := NewMemory(MemoryConfig{Limit: 10})

	_, err := block.PushMessage(tMsg)
	if exp, actual := types.ErrMessageTooLarge, err; exp != actual {
		t.Errorf("Unexpected error: %v != %v", exp, actual)
	}
}
