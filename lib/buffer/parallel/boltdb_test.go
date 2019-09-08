// Copyright (c) 2019 Ashley Jeffs
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

// +build !wasm

package parallel

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func initBoltDB(t *testing.T) (*BoltDB, func()) {
	t.Helper()
	return initBoltDBConfig(NewBoltDBConfig(), t)
}

func initBoltDBConfig(conf BoltDBConfig, t *testing.T) (*BoltDB, func()) {
	t.Helper()
	d, err := ioutil.TempDir("", "benthos_test")
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(d, "test.db")
	conf.File = path
	boltDB, err := NewBoltDB(conf)
	if err != nil {
		os.RemoveAll(d)
		t.Fatal(err)
	}

	return boltDB, func() {
		os.RemoveAll(d)
	}
}

func TestBoltDBBasic(t *testing.T) {
	n := 20

	conf := NewBoltDBConfig()
	conf.PrefetchCount = 5

	block, fn := initBoltDBConfig(conf, t)
	defer fn()

	expected := map[string]struct{}{}

	for i := 0; i < n; i++ {
		uniquePayload := fmt.Sprintf("test%v", i)
		if _, err := block.PushMessages([]types.Message{
			message.New(
				[][]byte{
					[]byte("hello"),
					[]byte("world"),
					[]byte("12345"),
					[]byte(uniquePayload),
				},
			),
		}); err != nil {
			t.Fatal(err)
		}
		expected[uniquePayload] = struct{}{}
	}

	for len(expected) > 0 {
		m, ackFunc, err := block.NextMessage()
		if err != nil {
			t.Fatal(err)
		}
		if m.Len() != 4 {
			t.Errorf("Wrong # parts, %v != %v", m.Len(), 4)
			continue
		}
		actual := string(m.Get(3).Get())
		if _, ok := expected[actual]; !ok {
			t.Errorf("Unexpected payload: %v", actual)
		} else {
			delete(expected, actual)
		}

		if _, err := ackFunc(true); err != nil {
			t.Error(err)
		}
	}
}

func TestBoltDBLockStep(t *testing.T) {
	n := 50

	conf := NewBoltDBConfig()
	conf.PrefetchCount = 5

	block, fn := initBoltDBConfig(conf, t)
	defer fn()

	expected := map[string]struct{}{}
	actual := map[string]struct{}{}

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			m, ackFunc, err := block.NextMessage()
			if err != nil {
				t.Fatal(err)
			}
			if m.Len() != 4 {
				t.Fatalf("Wrong # parts, %v != %v", m.Len(), 4)
			}
			actualPayload := string(m.Get(3).Get())
			actual[actualPayload] = struct{}{}
			if _, err := ackFunc(true); err != nil {
				t.Error(err)
			}
		}
	}()

	go func() {
		msgs := []types.Message{}
		for i := 0; i < n; i++ {
			uniquePayload := fmt.Sprintf("test%v", i)
			msgs = append(msgs, message.New(
				[][]byte{
					[]byte("hello"),
					[]byte("world"),
					[]byte("12345"),
					[]byte(uniquePayload),
				},
			))
			expected[uniquePayload] = struct{}{}
			if len(msgs) == 10 || i == (n-1) {
				if _, err := block.PushMessages(msgs); err != nil {
					t.Fatal(err)
				}
				msgs = nil
			}
		}
	}()

	wg.Wait()
	if act, exp := len(actual), len(expected); act != exp {
		t.Errorf("Wrong count of results: %v != %v", act, exp)
	}
	for k := range expected {
		if _, ok := actual[k]; !ok {
			t.Errorf("Missing payload: %v", k)
		}
	}
	for k := range actual {
		if _, ok := expected[k]; !ok {
			t.Errorf("Unexpected payload: %v", k)
		}
	}
}

func TestBoltDBAck(t *testing.T) {
	block, fn := initBoltDB(t)
	defer fn()

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

func BenchmarkBoltDBWrites1(b *testing.B) {
	d, err := ioutil.TempDir("", "benthos_test")
	if err != nil {
		b.Fatal(err)
	}

	path := filepath.Join(d, "test.db")
	conf := NewBoltDBConfig()
	conf.File = path
	block, err := NewBoltDB(conf)
	if err != nil {
		os.RemoveAll(d)
		b.Fatal(err)
	}
	defer os.RemoveAll(d)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		uniquePayload := fmt.Sprintf("test%v", i)
		if _, err := block.PushMessages([]types.Message{
			message.New(
				[][]byte{
					[]byte("hello"),
					[]byte("world"),
					[]byte("12345"),
					[]byte(uniquePayload),
				},
			),
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBoltDBWrites10(b *testing.B) {
	d, err := ioutil.TempDir("", "benthos_test")
	if err != nil {
		b.Fatal(err)
	}

	path := filepath.Join(d, "test.db")
	conf := NewBoltDBConfig()
	conf.File = path
	block, err := NewBoltDB(conf)
	if err != nil {
		os.RemoveAll(d)
		b.Fatal(err)
	}
	defer os.RemoveAll(d)

	b.ResetTimer()
	payloads := []types.Message{}
	for i := 0; i < b.N; i++ {
		uniquePayload := fmt.Sprintf("test%v", i)
		payloads = append(payloads, message.New(
			[][]byte{
				[]byte("hello"),
				[]byte("world"),
				[]byte("12345"),
				[]byte(uniquePayload),
			},
		))
		if len(payloads) == 10 || i == (b.N-1) {
			if _, err := block.PushMessages(payloads); err != nil {
				b.Fatal(err)
			}
			payloads = nil
		}
	}
}

func BenchmarkBoltDBWrites100(b *testing.B) {
	d, err := ioutil.TempDir("", "benthos_test")
	if err != nil {
		b.Fatal(err)
	}

	path := filepath.Join(d, "test.db")
	conf := NewBoltDBConfig()
	conf.File = path
	block, err := NewBoltDB(conf)
	if err != nil {
		os.RemoveAll(d)
		b.Fatal(err)
	}
	defer os.RemoveAll(d)

	b.ResetTimer()
	payloads := []types.Message{}
	for i := 0; i < b.N; i++ {
		uniquePayload := fmt.Sprintf("test%v", i)
		payloads = append(payloads, message.New(
			[][]byte{
				[]byte("hello"),
				[]byte("world"),
				[]byte("12345"),
				[]byte(uniquePayload),
			},
		))
		if len(payloads) == 100 || i == (b.N-1) {
			if _, err := block.PushMessages(payloads); err != nil {
				b.Fatal(err)
			}
			payloads = nil
		}
	}
}
