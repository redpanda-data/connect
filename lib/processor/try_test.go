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
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

//------------------------------------------------------------------------------

func TestTryEmpty(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeTry

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{
		[]byte("foo bar baz"),
	}
	msgs, res := proc.ProcessMessage(message.New(exp))
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
}

func TestTryBasic(t *testing.T) {
	encodeConf := NewConfig()
	encodeConf.Type = "encode"
	encodeConf.Encode.Parts = []int{0}

	conf := NewConfig()
	conf.Type = TypeTry
	conf.Try = append(conf.Try, encodeConf)

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte("foo bar baz"),
		[]byte("1 2 3 4"),
		[]byte("hello foo world"),
	}
	exp := [][]byte{
		[]byte("Zm9vIGJhciBiYXo="),
		[]byte("MSAyIDMgNA=="),
		[]byte("aGVsbG8gZm9vIHdvcmxk"),
	}
	msgs, res := proc.ProcessMessage(message.New(parts))
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
}

func TestTryFilterSome(t *testing.T) {
	cond := condition.NewConfig()
	cond.Type = "text"
	cond.Text.Arg = "foo"
	cond.Text.Operator = "contains"

	filterConf := NewConfig()
	filterConf.Type = "filter"
	filterConf.Filter.Config = cond

	conf := NewConfig()
	conf.Type = TypeTry
	conf.Try = append(conf.Try, filterConf)

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte("foo bar baz"),
		[]byte("1 2 3 4"),
		[]byte("hello foo world"),
	}
	exp := [][]byte{
		[]byte("foo bar baz"),
		[]byte("hello foo world"),
	}
	msgs, res := proc.ProcessMessage(message.New(parts))
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
}

func TestTryMultiProcs(t *testing.T) {
	encodeConf := NewConfig()
	encodeConf.Type = "encode"
	encodeConf.Encode.Parts = []int{0}

	cond := condition.NewConfig()
	cond.Type = "text"
	cond.Text.Arg = "foo"
	cond.Text.Operator = "contains"

	filterConf := NewConfig()
	filterConf.Type = "filter"
	filterConf.Filter.Config = cond

	conf := NewConfig()
	conf.Type = TypeTry
	conf.Try = append(conf.Try, filterConf)
	conf.Try = append(conf.Try, encodeConf)

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte("foo bar baz"),
		[]byte("1 2 3 4"),
		[]byte("hello foo world"),
	}
	exp := [][]byte{
		[]byte("Zm9vIGJhciBiYXo="),
		[]byte("aGVsbG8gZm9vIHdvcmxk"),
	}
	msgs, res := proc.ProcessMessage(message.New(parts))
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
}

func TestTryFailJSON(t *testing.T) {
	encodeConf := NewConfig()
	encodeConf.Type = "encode"
	encodeConf.Encode.Parts = []int{0}

	jmespathConf := NewConfig()
	jmespathConf.Type = TypeJMESPath
	jmespathConf.JMESPath.Query = "foo"

	conf := NewConfig()
	conf.Type = TypeTry
	conf.Try = append(conf.Try, jmespathConf)
	conf.Try = append(conf.Try, encodeConf)

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte(`{"foo":{"bar":"baz"}}`),
		[]byte("NOT VALID JSON"),
		[]byte(`{"foo":{"bar":"baz2"}}`),
	}
	exp := [][]byte{
		[]byte("eyJiYXIiOiJiYXoifQ=="),
		[]byte("NOT VALID JSON"),
		[]byte("eyJiYXIiOiJiYXoyIn0="),
	}
	msgs, res := proc.ProcessMessage(message.New(parts))
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
	if HasFailed(msgs[0].Get(0)) {
		t.Error("Unexpected part 0 failed flag")
	}
	if !HasFailed(msgs[0].Get(1)) {
		t.Error("Unexpected part 1 failed flag")
	}
	if HasFailed(msgs[0].Get(2)) {
		t.Error("Unexpected part 2 failed flag")
	}
}

func TestTryFilterAll(t *testing.T) {
	cond := condition.NewConfig()
	cond.Type = "text"
	cond.Text.Arg = "foo"
	cond.Text.Operator = "contains"

	filterConf := NewConfig()
	filterConf.Type = "filter"
	filterConf.Filter.Config = cond

	conf := NewConfig()
	conf.Type = TypeTry
	conf.Try = append(conf.Try, filterConf)

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte("bar baz"),
		[]byte("1 2 3 4"),
		[]byte("hello world"),
	}
	msgs, res := proc.ProcessMessage(message.New(parts))
	if res == nil {
		t.Fatal("expected empty response")
	}
	if err = res.Error(); err != nil {
		t.Error(err)
	}
	if len(msgs) != 0 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
}

//------------------------------------------------------------------------------
