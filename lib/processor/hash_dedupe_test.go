// Copyright (c) 2018 Lorenzo Alberton
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

// +build integration

package processor

import (
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

func init() {
    rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")


func TestHashDedupe(t *testing.T) {
	rndText1 := randStringRunes(20)
	rndText2 := randStringRunes(15)
	doc1 := []byte(rndText1)
	doc2 := []byte(rndText1) // duplicate
	doc3 := []byte(rndText2)

	conf := NewConfig()
	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	proc, err1 := NewHashDedupe(conf, nil, testLog, metrics.DudType{})
	if err1 != nil {
		t.Error(err1)
		return
	}

	msgIn := types.NewMessage([][]byte{doc1})
	msgOut, err := proc.ProcessMessage(msgIn)
	if nil != err && nil != err.Error() {
		t.Error("Message 1 told not to propagate even if it was expected to propagate. Cache error:", err.Error())
	}
	if nil == msgOut {
		t.Error("Message 1 told not to propagate even if it was expected to propagate")
	}

	msgIn = types.NewMessage([][]byte{doc2})
	msgOut, err = proc.ProcessMessage(msgIn)
	if nil != err && nil != err.Error() {
		t.Error("Message 1 told to propagate even if it was expected not to propagate. Cache error:", err.Error())
	}
	if nil != msgOut {
		t.Error("Message 2 told to propagate even if it was expected not to propagate")
	}

	msgIn = types.NewMessage([][]byte{doc3})
	msgOut, err = proc.ProcessMessage(msgIn)
	if nil != err && nil != err.Error() {
		t.Error("Message 1 told not to propagate even if it was expected to propagate. Cache error:", err.Error())
	}
	if nil == msgOut {
		t.Error("Message 3 told not to propagate even if it was expected to propagate")
	}
}

func TestHashDedupePartSelection(t *testing.T) {
	hdr := []byte(`some header`)
	rndText1 := randStringRunes(20)
	rndText2 := randStringRunes(15)
	doc1 := []byte(rndText1)
	doc2 := []byte(rndText1) // duplicate
	doc3 := []byte(rndText2)

	conf := NewConfig()
	conf.HashDedupe.Parts = []int{1} // only take the 2nd part
	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	proc, err1 := NewHashDedupe(conf, nil, testLog, metrics.DudType{})
	if err1 != nil {
		t.Error(err1)
		return
	}

	msgIn := types.NewMessage([][]byte{hdr, doc1})
	msgOut, err := proc.ProcessMessage(msgIn)
	if nil != err && nil != err.Error() {
		t.Error("Message 1 told not to propagate even if it was expected to propagate. Cache error:", err.Error())
	}
	if nil == msgOut {
		t.Error("Message 1 told not to propagate even if it was expected to propagate")
	}

	msgIn = types.NewMessage([][]byte{hdr, doc2})
	msgOut, err = proc.ProcessMessage(msgIn)
	if nil != err && nil != err.Error() {
		t.Error("Message 1 told to propagate even if it was expected not to propagate. Cache error:", err.Error())
	}
	if nil != msgOut {
		t.Error("Message 2 told to propagate even if it was expected not to propagate")
	}

	msgIn = types.NewMessage([][]byte{hdr, doc3})
	msgOut, err = proc.ProcessMessage(msgIn)
	if nil != err && nil != err.Error() {
		t.Error("Message 1 told not to propagate even if it was expected to propagate. Cache error:", err.Error())
	}
	if nil == msgOut {
		t.Error("Message 3 told not to propagate even if it was expected to propagate")
	}
}

func TestHashDedupeBoundsCheck(t *testing.T) {
	conf := NewConfig()
	conf.HashDedupe.Parts = []int{5}

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	proc, err1 := NewHashDedupe(conf, nil, testLog, metrics.DudType{})
	if err1 != nil {
		t.Fatal(err1)
	}

	msgIn := types.NewMessage([][]byte{})
	msgs, res := proc.ProcessMessage(msgIn)
	if len(msgs) > 0 {
		t.Error("OOB message told to propagate")
	}

	if exp, act := types.NewSimpleResponse(nil), res; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong response returned: %v != %v", act, exp)
	}
}

func TestHashDedupeNegBoundsCheck(t *testing.T) {
	conf := NewConfig()
	conf.HashDedupe.Parts = []int{-5}

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	proc, err1 := NewHashDedupe(conf, nil, testLog, metrics.DudType{})
	if err1 != nil {
		t.Fatal(err1)
	}

	msgIn := types.NewMessage([][]byte{})
	msgs, res := proc.ProcessMessage(msgIn)
	if len(msgs) > 0 {
		t.Error("OOB message told to propagate")
	}

	if exp, act := types.NewSimpleResponse(nil), res; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong response returned: %v != %v", act, exp)
	}
}

func randStringRunes(n int) string {
    b := make([]rune, n)
    for i := range b {
        b[i] = letterRunes[rand.Intn(len(letterRunes))]
    }
    return string(b)
}