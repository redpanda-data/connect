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

package processor

import (
	"os"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestSplitToSingleParts(t *testing.T) {
	conf := NewConfig()

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewSplit(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	tests := [][][]byte{
		{},
		{
			[]byte("foo"),
		},
		{
			[]byte("foo"),
			[]byte("bar"),
		},
		{
			[]byte("foo"),
			[]byte("bar"),
			[]byte("baz"),
		},
	}

	for _, tIn := range tests {
		inMsg := message.New(tIn)
		inMsg.Iter(func(i int, p types.Part) error {
			p.Metadata().Set("foo", "bar")
			return nil
		})
		msgs, _ := proc.ProcessMessage(inMsg)
		if exp, act := len(tIn), len(msgs); exp != act {
			t.Errorf("Wrong count of messages: %v != %v", act, exp)
			continue
		}
		for i, expBytes := range tIn {
			if act, exp := string(msgs[i].Get(0).Get()), string(expBytes); act != exp {
				t.Errorf("Wrong contents: %v != %v", act, exp)
			}
			if act, exp := msgs[i].Get(0).Metadata().Get("foo"), "bar"; act != exp {
				t.Errorf("Wrong metadata: %v != %v", act, exp)
			}
		}
	}
}

func TestSplitToMultipleParts(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeSplit
	conf.Split.Size = 2

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	inMsg := message.New([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
	})
	msgs, _ := proc.ProcessMessage(inMsg)
	if exp, act := 2, len(msgs); exp != act {
		t.Fatalf("Wrong message count: %v != %v", act, exp)
	}
	if exp, act := 2, msgs[0].Len(); exp != act {
		t.Fatalf("Wrong message count: %v != %v", act, exp)
	}
	if exp, act := 1, msgs[1].Len(); exp != act {
		t.Fatalf("Wrong message count: %v != %v", act, exp)
	}
	if exp, act := "foo", string(msgs[0].Get(0).Get()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
	if exp, act := "bar", string(msgs[0].Get(1).Get()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
	if exp, act := "baz", string(msgs[1].Get(0).Get()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
}

func TestSplitByBytes(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeSplit
	conf.Split.Size = 0
	conf.Split.ByteSize = 6

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	inMsg := message.New([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
	})
	msgs, _ := proc.ProcessMessage(inMsg)
	if exp, act := 2, len(msgs); exp != act {
		t.Fatalf("Wrong batch count: %v != %v", act, exp)
	}
	if exp, act := 2, msgs[0].Len(); exp != act {
		t.Fatalf("Wrong message 1 count: %v != %v", act, exp)
	}
	if exp, act := 1, msgs[1].Len(); exp != act {
		t.Fatalf("Wrong message 2 count: %v != %v", act, exp)
	}
	if exp, act := "foo", string(msgs[0].Get(0).Get()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
	if exp, act := "bar", string(msgs[0].Get(1).Get()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
	if exp, act := "baz", string(msgs[1].Get(0).Get()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
}

func TestSplitByBytesTooLarge(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeSplit
	conf.Split.Size = 0
	conf.Split.ByteSize = 2

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	inMsg := message.New([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
	})
	msgs, _ := proc.ProcessMessage(inMsg)
	if exp, act := 3, len(msgs); exp != act {
		t.Fatalf("Wrong batch count: %v != %v", act, exp)
	}
	if exp, act := 1, msgs[0].Len(); exp != act {
		t.Fatalf("Wrong message 1 count: %v != %v", act, exp)
	}
	if exp, act := 1, msgs[1].Len(); exp != act {
		t.Fatalf("Wrong message 2 count: %v != %v", act, exp)
	}
	if exp, act := 1, msgs[2].Len(); exp != act {
		t.Fatalf("Wrong message 3 count: %v != %v", act, exp)
	}
	if exp, act := "foo", string(msgs[0].Get(0).Get()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
	if exp, act := "bar", string(msgs[1].Get(0).Get()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
	if exp, act := "baz", string(msgs[2].Get(0).Get()); act != exp {
		t.Errorf("Wrong contents: %v != %v", act, exp)
	}
}
