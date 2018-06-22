// Copyright (c) 2017 Ashley Jeffs
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
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

func TestCombineTwoParts(t *testing.T) {
	conf := NewConfig()
	conf.Combine.Parts = 2

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewCombine(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}

	msgs, res := proc.ProcessMessage(types.NewMessage(exp))
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msgs[0].GetAll()) {
		t.Errorf("Wrong result: %s != %s", msgs[0].GetAll(), exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}
}

func BenchmarkCombineMultiMessagesSharedBuffer(b *testing.B) {
	conf := NewConfig()
	conf.Combine.Parts = 3

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewCombine(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		b.Fatal(err)
	}

	var expParts [][]byte

	blobSizes := []int{100 * 1024, 200 * 1024}
	for _, bSize := range blobSizes {
		dataBlob := make([]byte, bSize)
		for i := range dataBlob {
			dataBlob[i] = byte(rand.Int())
		}
		expParts = append(expParts, dataBlob)
	}

	inputMsg := types.NewMessage(expParts)

	b.ReportAllocs()
	b.ResetTimer()

	// For each loop we add three parts of a message.
	for i := 0; i < b.N; i++ {
		msgs, res := proc.ProcessMessage(inputMsg)
		if len(msgs) != 1 {
			if err := res.Error(); err != nil {
				b.Error(err)
			}
		} else if exp, act := 3, len(msgs[0].GetAll()); exp != act {
			b.Errorf("Wrong parts count: %v != %v\n", act, exp)
		}
	}
}

func TestCombineLotsOfParts(t *testing.T) {
	conf := NewConfig()
	conf.Combine.Parts = 2

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewCombine(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	input := [][]byte{
		[]byte("foo"), []byte("bar"), []byte("bar2"),
		[]byte("bar3"), []byte("bar4"), []byte("bar5"),
	}

	msgs, res := proc.ProcessMessage(types.NewMessage(input))
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(input, msgs[0].GetAll()) {
		t.Errorf("Wrong result: %s != %s", msgs[0].GetAll(), input)
	}
	if res != nil {
		t.Error("Expected nil res")
	}
}

func TestCombineTwoSingleParts(t *testing.T) {
	conf := NewConfig()
	conf.Combine.Parts = 2

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewCombine(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{[]byte("foo1"), []byte("bar1")}

	msgs, res := proc.ProcessMessage(types.NewMessage([][]byte{exp[0]}))
	if len(msgs) != 0 {
		t.Error("Expected fail on one part")
	}
	if !res.SkipAck() {
		t.Error("Expected skip ack")
	}

	msgs, res = proc.ProcessMessage(types.NewMessage([][]byte{exp[1]}))
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msgs[0].GetAll()) {
		t.Errorf("Wrong result: %s != %s", msgs[0].GetAll(), exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}

	exp = [][]byte{[]byte("foo2"), []byte("bar2")}

	msgs, res = proc.ProcessMessage(types.NewMessage([][]byte{exp[0]}))
	if len(msgs) != 0 {
		t.Error("Expected fail on one part")
	}
	if !res.SkipAck() {
		t.Error("Expected skip ack")
	}

	msgs, res = proc.ProcessMessage(types.NewMessage([][]byte{exp[1]}))
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msgs[0].GetAll()) {
		t.Errorf("Wrong result: %s != %s", msgs[0].GetAll(), exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}
}

func TestCombineTwoDiffParts(t *testing.T) {
	conf := NewConfig()
	conf.Combine.Parts = 2

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewCombine(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	input := [][]byte{[]byte("foo1"), []byte("bar1")}
	exp := [][]byte{[]byte("foo1"), []byte("bar1"), []byte("foo1")}

	msgs, res := proc.ProcessMessage(types.NewMessage([][]byte{input[0]}))
	if len(msgs) != 0 {
		t.Error("Expected fail on one part")
	}
	if !res.SkipAck() {
		t.Error("Expected skip ack")
	}

	msgs, res = proc.ProcessMessage(types.NewMessage([][]byte{input[1], input[0]}))
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msgs[0].GetAll()) {
		t.Errorf("Wrong result: %s != %s", msgs[0].GetAll(), exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}

	exp = [][]byte{[]byte("bar1"), []byte("foo1")}

	msgs, res = proc.ProcessMessage(types.NewMessage([][]byte{input[1], input[0]}))
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msgs[0].GetAll()) {
		t.Errorf("Wrong result: %s != %s", msgs[0].GetAll(), exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}

	exp = [][]byte{[]byte("bar1"), []byte("foo1"), []byte("bar1")}

	msgs, res = proc.ProcessMessage(types.NewMessage([][]byte{input[1], input[0], input[1]}))
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msgs[0].GetAll()) {
		t.Errorf("Wrong result: %s != %s", msgs[0].GetAll(), exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}
}
