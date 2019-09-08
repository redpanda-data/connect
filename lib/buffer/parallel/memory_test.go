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
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestMemoryBasic(t *testing.T) {
	n := 100

	block := NewMemory(100000)

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
		m, ackFunc, err := block.NextMessage()
		if err != nil {
			t.Error(err)
			return
		}
		if m.Len() != 4 {
			t.Errorf("Wrong # parts, %v != %v", m.Len(), 4)
		} else if expected, actual := fmt.Sprintf("test%v", i), string(m.Get(3).Get()); expected != actual {
			t.Errorf("Wrong order of messages, %v != %v", expected, actual)
		}
		if _, err := ackFunc(true); err != nil {
			t.Error(err)
		}
	}
}

func TestMemoryNearLimit(t *testing.T) {
	n, iter := 50, 5

	block := NewMemory(2285)

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
			m, ackFunc, err := block.NextMessage()
			if err != nil {
				t.Error(err)
				return
			}
			if m.Len() != 4 {
				t.Errorf("Wrong # parts, %v != %v", m.Len(), 4)
			} else if expected, actual := fmt.Sprintf("test%v", i), string(m.Get(3).Get()); expected != actual {
				t.Errorf("Wrong order of messages, %v != %v", expected, actual)
			}
			if _, err := ackFunc(true); err != nil {
				t.Error(err)
			}
		}
	}
}

func TestMemoryLoopingRandom(t *testing.T) {
	n, iter := 50, 5

	block := NewMemory(8000)

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
			m, ackFunc, err := block.NextMessage()
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
			if _, err := ackFunc(true); err != nil {
				t.Error(err)
			}
		}
	}
}

func TestMemoryLockStep(t *testing.T) {
	n := 10000

	block := NewMemory(1000)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			m, ackFunc, err := block.NextMessage()
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
			if _, err := ackFunc(true); err != nil {
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

func TestMemoryAck(t *testing.T) {
	block := NewMemory(1000)

	block.PushMessage(message.New([][]byte{
		[]byte("1"),
	}))
	block.PushMessage(message.New([][]byte{
		[]byte("2"),
	}))

	m, ackFunc, err := block.NextMessage()
	if err != nil {
		t.Error(err)
	} else {
		if expected, actual := "1", string(m.Get(0).Get()); expected != actual {
			t.Fatalf("Wrong message contents, %v != %v", expected, actual)
		}
		if _, err := ackFunc(false); err != nil {
			t.Error(err)
		}
	}

	m, ackFunc, err = block.NextMessage()
	if err != nil {
		t.Error(err)
	} else {
		if expected, actual := "1", string(m.Get(0).Get()); expected != actual {
			t.Fatalf("Wrong message contents, %v != %v", expected, actual)
		}
		if _, err := ackFunc(true); err != nil {
			t.Error(err)
		}
	}

	m, ackFunc, err = block.NextMessage()
	if err != nil {
		t.Error(err)
	} else {
		if expected, actual := "2", string(m.Get(0).Get()); expected != actual {
			t.Fatalf("Wrong message contents, %v != %v", expected, actual)
		}
		if _, err := ackFunc(true); err != nil {
			t.Error(err)
		}
	}

	block.Close()

	if _, err = block.PushMessage(message.New(nil)); err != types.ErrTypeClosed {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrTypeClosed)
	}
	if _, _, err = block.NextMessage(); err != types.ErrTypeClosed {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrTypeClosed)
	}
}

func TestMemoryClose(t *testing.T) {
	block := NewMemory(1000)

	for i := 0; i < 10; i++ {
		block.PushMessage(message.New([][]byte{
			[]byte("hello world"),
		}))
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		block.CloseOnceEmpty()
		wg.Done()
	}()

	<-time.After(time.Millisecond * 100)
	for i := 0; i < 10; i++ {
		m, ackFunc, err := block.NextMessage()
		if err != nil {
			t.Error(err)
		} else {
			if expected, actual := "hello world", string(m.Get(0).Get()); expected != actual {
				t.Errorf("Wrong message contents, %v != %v", expected, actual)
			}
			if _, err := ackFunc(true); err != nil {
				t.Error(err)
			}
		}
	}

	if _, _, err := block.NextMessage(); err != types.ErrTypeClosed {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrTypeClosed)
	}

	wg.Wait()
}

func TestMemoryCloseWithPending(t *testing.T) {
	block := NewMemory(1000)

	for i := 0; i < 10; i++ {
		block.PushMessage(message.New([][]byte{
			[]byte("hello world"),
		}))
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		block.CloseOnceEmpty()
		wg.Done()
	}()

	<-time.After(time.Millisecond * 100)
	for i := 0; i < 10; i++ {
		m, ackFunc, err := block.NextMessage()
		if err != nil {
			t.Error(err)
		} else {
			if expected, actual := "hello world", string(m.Get(0).Get()); expected != actual {
				t.Errorf("Wrong message contents, %v != %v", expected, actual)
			}
			if i < 9 {
				if _, err := ackFunc(true); err != nil {
					t.Error(err)
				}
			}
		}
	}

	if _, _, err := block.NextMessage(); err != types.ErrTypeClosed {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrTypeClosed)
	}

	wg.Wait()
}

func TestMemoryRejectLargeMessage(t *testing.T) {
	tMsg := message.New(make([][]byte, 1))
	tMsg.Get(0).Set([]byte("hello world this message is too long!"))

	block := NewMemory(10)

	_, err := block.PushMessage(tMsg)
	if exp, actual := types.ErrMessageTooLarge, err; exp != actual {
		t.Errorf("Unexpected error: %v != %v", exp, actual)
	}
}
