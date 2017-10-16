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
	"os"
	"reflect"
	"testing"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
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

	msg, res, check := proc.ProcessMessage(&types.Message{Parts: exp})
	if !check {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msg.Parts) {
		t.Errorf("Wrong result: %s != %s", msg.Parts, exp)
	}
	if res != nil {
		t.Error("Expected nil res")
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
	exp := [][]byte{[]byte("foo"), []byte("bar")}

	msg, res, check := proc.ProcessMessage(&types.Message{Parts: input})
	if !check {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msg.Parts) {
		t.Errorf("Wrong result: %s != %s", msg.Parts, exp)
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

	msg, res, check := proc.ProcessMessage(&types.Message{Parts: [][]byte{exp[0]}})
	if check {
		t.Error("Expected fail on one part")
	}
	if msg != nil {
		t.Error("Expected nil msg")
	}
	if !res.SkipAck() {
		t.Error("Expected skip ack")
	}

	msg, res, check = proc.ProcessMessage(&types.Message{Parts: [][]byte{exp[1]}})
	if !check {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msg.Parts) {
		t.Errorf("Wrong result: %s != %s", msg.Parts, exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}

	exp = [][]byte{[]byte("foo2"), []byte("bar2")}

	msg, res, check = proc.ProcessMessage(&types.Message{Parts: [][]byte{exp[0]}})
	if check {
		t.Error("Expected fail on one part")
	}
	if msg != nil {
		t.Error("Expected nil msg")
	}
	if !res.SkipAck() {
		t.Error("Expected skip ack")
	}

	msg, res, check = proc.ProcessMessage(&types.Message{Parts: [][]byte{exp[1]}})
	if !check {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msg.Parts) {
		t.Errorf("Wrong result: %s != %s", msg.Parts, exp)
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

	msg, res, check := proc.ProcessMessage(&types.Message{Parts: [][]byte{exp[0]}})
	if check {
		t.Error("Expected fail on one part")
	}
	if msg != nil {
		t.Error("Expected nil msg")
	}
	if !res.SkipAck() {
		t.Error("Expected skip ack")
	}

	msg, res, check = proc.ProcessMessage(&types.Message{Parts: [][]byte{exp[1], exp[0]}})
	if !check {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msg.Parts) {
		t.Errorf("Wrong result: %s != %s", msg.Parts, exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}

	msg, res, check = proc.ProcessMessage(&types.Message{Parts: [][]byte{exp[1], exp[0]}})
	if !check {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msg.Parts) {
		t.Errorf("Wrong result: %s != %s", msg.Parts, exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}

	msg, res, check = proc.ProcessMessage(&types.Message{Parts: [][]byte{exp[1]}})
	if !check {
		t.Error("Expected success")
	}
	if !reflect.DeepEqual(exp, msg.Parts) {
		t.Errorf("Wrong result: %s != %s", msg.Parts, exp)
	}
	if res != nil {
		t.Error("Expected nil res")
	}
}
