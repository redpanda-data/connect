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
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/processor/condition"
	"github.com/Jeffail/benthos/lib/types"
)

func TestBatchTwoParts(t *testing.T) {
	conf := NewConfig()
	conf.Batch.ByteSize = 6

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewBatch(conf, nil, testLog, metrics.DudType{})
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

func TestBatchLotsOfParts(t *testing.T) {
	conf := NewConfig()
	conf.Batch.ByteSize = 1

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewBatch(conf, nil, testLog, metrics.DudType{})
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

func TestBatchTwoSingleParts(t *testing.T) {
	conf := NewConfig()
	conf.Batch.ByteSize = 5

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewBatch(conf, nil, testLog, metrics.DudType{})
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

func TestBatchTwoDiffParts(t *testing.T) {
	conf := NewConfig()
	conf.Batch.ByteSize = 5

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewBatch(conf, nil, testLog, metrics.DudType{})
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

func TestBatchCondition(t *testing.T) {
	condConf := condition.NewConfig()
	condConf.Type = "text"
	condConf.Text.Operator = "contains"
	condConf.Text.Arg = "end_batch"

	conf := NewConfig()
	conf.Batch.ByteSize = 1000
	conf.Batch.Condition = condConf

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewBatch(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	msgs, res := proc.ProcessMessage(types.NewMessage([][]byte{[]byte("foo")}))
	if len(msgs) != 0 {
		t.Error("Expected no batch")
	}
	if !res.SkipAck() {
		t.Error("Expected skip ack")
	}

	msgs, res = proc.ProcessMessage(types.NewMessage([][]byte{[]byte("bar")}))
	if len(msgs) != 0 {
		t.Error("Expected no batch")
	}
	if !res.SkipAck() {
		t.Error("Expected skip ack")
	}

	msgs, res = proc.ProcessMessage(types.NewMessage([][]byte{[]byte("baz: end_batch")}))
	if len(msgs) != 1 {
		t.Fatal("Expected batch")
	}

	exp := [][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz: end_batch"),
	}
	if act := msgs[0].GetAll(); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong batch contents: %s != %s", act, exp)
	}
}
