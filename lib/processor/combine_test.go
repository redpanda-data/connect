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
	"bytes"
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

func TestCombineTwoParts(t *testing.T) {
	conf := NewConfig()
	conf.Combine.Parts = 2

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	proc, err := NewCombine(conf, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}

	msgs, res := proc.ProcessMessage(&types.Message{Parts: exp})
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msgs[0].Parts) {
		t.Errorf("Wrong result: %s != %s", msgs[0].Parts, exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}
}

func TestCombineMessagesSharedBuffer(t *testing.T) {
	conf := NewConfig()
	conf.Combine.Parts = 2

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	proc, err := NewCombine(conf, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}

	buf := &bytes.Buffer{}
	buf.Write([]byte("foo"))

	msgs, res := proc.ProcessMessage(&types.Message{Parts: [][]byte{buf.Bytes()}})
	if len(msgs) > 0 {
		t.Error("Expected non success on first part")
	}

	buf.Reset()
	buf.Write([]byte("bar"))

	msgs, res = proc.ProcessMessage(&types.Message{Parts: [][]byte{buf.Bytes()}})
	if len(msgs) != 1 {
		t.Error("Expected success on second part")
	}
	if !reflect.DeepEqual(exp, msgs[0].Parts) {
		t.Errorf("Wrong result: %s != %s", msgs[0].Parts, exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}
}

func TestCombineMultiMessagesSharedBuffer(t *testing.T) {
	conf := NewConfig()
	conf.Combine.Parts = 3

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	proc, err := NewCombine(conf, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	inputs := [][]byte{[]byte("foo 1 2 3"), []byte("bar")}

	buf := &bytes.Buffer{}

	expEven := [][]byte{[]byte("foo 1 2 3"), []byte("bar"), []byte("foo 1 2 3")}
	expOdd := [][]byte{[]byte("bar"), []byte("foo 1 2 3"), []byte("bar")}

	results := 0

	// For each loop we add three parts of a message.
	for i := 0; i < 10; i++ {
		buf.Reset()
		bIndex := 0

		inputMsg := types.NewMessage()

		for _, input := range inputs {
			written, _ := buf.Write(input)
			inputMsg.Parts = append(inputMsg.Parts, buf.Bytes()[bIndex:bIndex+written])
			bIndex += written
		}

		msgs, res := proc.ProcessMessage(&inputMsg)
		if i%3 != 0 {
			exp := expEven
			if results%2 != 0 {
				exp = expOdd
			}

			if len(msgs) != 1 {
				t.Error("Expected success")
			} else {
				if !reflect.DeepEqual(exp, msgs[0].Parts) {
					t.Errorf("Wrong result: %s != %s", msgs[0].Parts, exp)
				}
				if res != nil {
					t.Error("Expected nil res")
				}
			}
			results++
		} else if len(msgs) > 0 {
			t.Error("Expected non-success on first send")
		}
	}
}

func BenchmarkCombineMultiMessagesSharedBuffer(b *testing.B) {
	conf := NewConfig()
	conf.Combine.Parts = 3

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	proc, err := NewCombine(conf, testLog, metrics.DudType{})
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

	inputMsg := &types.Message{
		Parts: expParts,
	}

	b.ReportAllocs()
	b.ResetTimer()

	// For each loop we add three parts of a message.
	for i := 0; i < b.N; i++ {
		msgs, res := proc.ProcessMessage(inputMsg)
		if len(msgs) != 1 {
			if err := res.Error(); err != nil {
				b.Error(err)
			}
		} else if exp, act := 3, len(msgs[0].Parts); exp != act {
			b.Errorf("Wrong parts count: %v != %v\n", act, exp)
		}
	}
}

func TestCombineLotsOfParts(t *testing.T) {
	conf := NewConfig()
	conf.Combine.Parts = 2

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	proc, err := NewCombine(conf, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	input := [][]byte{
		[]byte("foo"), []byte("bar"), []byte("bar2"),
		[]byte("bar3"), []byte("bar4"), []byte("bar5"),
	}

	msgs, res := proc.ProcessMessage(&types.Message{Parts: input})
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(input, msgs[0].Parts) {
		t.Errorf("Wrong result: %s != %s", msgs[0].Parts, input)
	}
	if res != nil {
		t.Error("Expected nil res")
	}
}

func TestCombineTwoSingleParts(t *testing.T) {
	conf := NewConfig()
	conf.Combine.Parts = 2

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	proc, err := NewCombine(conf, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{[]byte("foo1"), []byte("bar1")}

	msgs, res := proc.ProcessMessage(&types.Message{Parts: [][]byte{exp[0]}})
	if len(msgs) != 0 {
		t.Error("Expected fail on one part")
	}
	if !res.SkipAck() {
		t.Error("Expected skip ack")
	}

	msgs, res = proc.ProcessMessage(&types.Message{Parts: [][]byte{exp[1]}})
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msgs[0].Parts) {
		t.Errorf("Wrong result: %s != %s", msgs[0].Parts, exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}

	exp = [][]byte{[]byte("foo2"), []byte("bar2")}

	msgs, res = proc.ProcessMessage(&types.Message{Parts: [][]byte{exp[0]}})
	if len(msgs) != 0 {
		t.Error("Expected fail on one part")
	}
	if !res.SkipAck() {
		t.Error("Expected skip ack")
	}

	msgs, res = proc.ProcessMessage(&types.Message{Parts: [][]byte{exp[1]}})
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msgs[0].Parts) {
		t.Errorf("Wrong result: %s != %s", msgs[0].Parts, exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}
}

func TestCombineTwoDiffParts(t *testing.T) {
	conf := NewConfig()
	conf.Combine.Parts = 2

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	proc, err := NewCombine(conf, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{[]byte("foo1"), []byte("bar1")}

	msgs, res := proc.ProcessMessage(&types.Message{Parts: [][]byte{exp[0]}})
	if len(msgs) != 0 {
		t.Error("Expected fail on one part")
	}
	if !res.SkipAck() {
		t.Error("Expected skip ack")
	}

	msgs, res = proc.ProcessMessage(&types.Message{Parts: [][]byte{exp[1], exp[0]}})
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msgs[0].Parts) {
		t.Errorf("Wrong result: %s != %s", msgs[0].Parts, exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}

	msgs, res = proc.ProcessMessage(&types.Message{Parts: [][]byte{exp[1], exp[0]}})
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msgs[0].Parts) {
		t.Errorf("Wrong result: %s != %s", msgs[0].Parts, exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}

	msgs, res = proc.ProcessMessage(&types.Message{Parts: [][]byte{exp[1]}})
	if len(msgs) != 1 {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msgs[0].Parts) {
		t.Errorf("Wrong result: %s != %s", msgs[0].Parts, exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}
}
