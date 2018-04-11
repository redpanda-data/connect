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
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/types"
)

func TestBadgerBasic(t *testing.T) {
	dir, err := ioutil.TempDir("", "benthos_badger_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	conf := NewBadgerConfig()
	conf.Directory = dir

	block, err := NewBadger(conf)
	if err != nil {
		t.Fatal(err)
	}

	n := 100

	for i := 0; i < n; i++ {
		if _, err := block.PushMessage(types.NewMessage(
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
		} else if expected, actual := fmt.Sprintf("test%v", i), string(m.Get(3)); expected != actual {
			t.Errorf("Wrong order of messages, %v != %v", expected, actual)
		}
		if _, err := ackFunc(true); err != nil {
			t.Error(err)
		}
	}
}

func TestBadgerLoopingRandom(t *testing.T) {
	dir, err := ioutil.TempDir("", "benthos_badger_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	conf := NewBadgerConfig()
	conf.Directory = dir

	block, err := NewBadger(conf)
	if err != nil {
		t.Fatal(err)
	}

	n, iter := 10, 5

	for j := 0; j < iter; j++ {
		for i := 0; i < n; i++ {
			b := make([]byte, rand.Int()%100)
			for k := range b {
				b[k] = '0'
			}
			if _, err := block.PushMessage(types.NewMessage(
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
			} else if expected, actual := fmt.Sprintf("test%v", i), string(m.Get(1)); expected != actual {
				t.Errorf("Wrong order of messages, %v != %v", expected, actual)
				return
			}
			if _, err := ackFunc(true); err != nil {
				t.Error(err)
			}
		}
	}
}

func TestBadgerLockStep(t *testing.T) {
	dir, err := ioutil.TempDir("", "benthos_badger_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	conf := NewBadgerConfig()
	conf.Directory = dir

	block, err := NewBadger(conf)
	if err != nil {
		t.Fatal(err)
	}

	n := 100

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
			} else if expected, actual := fmt.Sprintf("test%v", i), string(m.Get(3)); expected != actual {
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
			if _, err := block.PushMessage(types.NewMessage(
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

func TestBadgerRestart(t *testing.T) {
	dir, err := ioutil.TempDir("", "benthos_badger_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	conf := NewBadgerConfig()
	conf.Directory = dir

	block, err := NewBadger(conf)
	if err != nil {
		t.Fatal(err)
	}

	n := 100

	expKeys := map[string]struct{}{}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("test%v", i)
		if _, err := block.PushMessage(types.NewMessage(
			[][]byte{
				[]byte("hello"),
				[]byte("world"),
				[]byte("12345"),
				[]byte(key),
			},
		)); err != nil {
			t.Error(err)
		}
		expKeys[key] = struct{}{}
	}

	block.Close()
	block = nil

	block, err = NewBadger(conf)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < n; i++ {
		m, ackFunc, err := block.NextMessage()
		if err != nil {
			t.Fatal(err)
		}
		if m.Len() != 4 {
			t.Errorf("Wrong # parts, %v != %v", m.Len(), 4)
		} else {
			key := string(m.Get(3))
			if _, exists := expKeys[key]; !exists {
				t.Errorf("Unexpected key: %v", key)
			}
			delete(expKeys, key)
		}
		if _, err := ackFunc(true); err != nil {
			t.Error(err)
		}
	}

	if len(expKeys) > 0 {
		t.Errorf("Some expected keys were not received: %v", expKeys)
	}

	block.CloseOnceEmpty()
}

func TestBadgerAck(t *testing.T) {
	dir, err := ioutil.TempDir("", "benthos_badger_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	conf := NewBadgerConfig()
	conf.Directory = dir

	block, err := NewBadger(conf)
	if err != nil {
		t.Fatal(err)
	}

	block.PushMessage(types.NewMessage([][]byte{
		[]byte("1"),
	}))
	block.PushMessage(types.NewMessage([][]byte{
		[]byte("2"),
	}))

	m, ackFunc, err := block.NextMessage()
	if err != nil {
		t.Error(err)
	} else {
		if expected, actual := "1", string(m.Get(0)); expected != actual {
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
		if expected, actual := "1", string(m.Get(0)); expected != actual {
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
		if expected, actual := "2", string(m.Get(0)); expected != actual {
			t.Fatalf("Wrong message contents, %v != %v", expected, actual)
		}
		if _, err := ackFunc(true); err != nil {
			t.Error(err)
		}
	}

	block.Close()

	if _, err = block.PushMessage(types.NewMessage(nil)); err != types.ErrTypeClosed {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrTypeClosed)
	}
	if _, _, err = block.NextMessage(); err != types.ErrTypeClosed {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrTypeClosed)
	}
}

func TestBadgerClose(t *testing.T) {
	dir, err := ioutil.TempDir("", "benthos_badger_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	conf := NewBadgerConfig()
	conf.Directory = dir

	block, err := NewBadger(conf)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		block.PushMessage(types.NewMessage([][]byte{
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
			if expected, actual := "hello world", string(m.Get(0)); expected != actual {
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

func BenchmarkParallelBadgerSync(b *testing.B) {
	dir, err := ioutil.TempDir("", "benthos_badger_test")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	conf := NewBadgerConfig()
	conf.Directory = dir

	bgr, err := NewBadger(conf)
	if err != nil {
		b.Fatal(err)
	}

	contents := [][]byte{
		make([]byte, 1024*1024*1),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err = bgr.PushMessage(types.NewMessage(contents)); err != nil {
			b.Fatal(err)
		}
		_, ackFunc, err := bgr.NextMessage()
		if err != nil {
			b.Fatal(err)
		}
		ackFunc(true)
	}
	b.StopTimer()

	bgr.Close()
}

func BenchmarkParallelBadgerNoSync(b *testing.B) {
	dir, err := ioutil.TempDir("", "benthos_badger_test")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	conf := NewBadgerConfig()
	conf.Directory = dir
	conf.SyncWrites = false

	bgr, err := NewBadger(conf)
	if err != nil {
		b.Fatal(err)
	}

	contents := [][]byte{
		make([]byte, 1024*1024*1),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err = bgr.PushMessage(types.NewMessage(contents)); err != nil {
			b.Fatal(err)
		}
		_, ackFunc, err := bgr.NextMessage()
		if err != nil {
			b.Fatal(err)
		}
		ackFunc(true)
	}
	b.StopTimer()

	bgr.Close()
}
