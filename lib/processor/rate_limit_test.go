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

package processor

import (
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

type fakeRateLimit struct {
	resFn func() (time.Duration, error)
}

func (f fakeRateLimit) Access() (time.Duration, error) {
	return f.resFn()
}
func (f fakeRateLimit) CloseAsync()                      {}
func (f fakeRateLimit) WaitForClose(time.Duration) error { return nil }

func TestRateLimitBasic(t *testing.T) {
	var hits int32
	rlFn := func() (time.Duration, error) {
		atomic.AddInt32(&hits, 1)
		return 0, nil
	}

	mgr := &fakeMgr{
		ratelimits: map[string]types.RateLimit{
			"foo": fakeRateLimit{resFn: rlFn},
		},
	}

	conf := NewConfig()
	conf.RateLimit.Resource = "foo"
	proc, err := NewRateLimit(conf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	input := message.New([][]byte{
		[]byte(`{"key":"1","value":"foo 1"}`),
		[]byte(`{"key":"2","value":"foo 2"}`),
		[]byte(`{"key":"1","value":"foo 3"}`),
	})

	output, res := proc.ProcessMessage(input)
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(output) != 1 {
		t.Fatalf("Wrong count of result messages: %v", len(output))
	}

	if exp, act := message.GetAllBytes(input), message.GetAllBytes(output[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result messages: %s != %s", act, exp)
	}

	if exp, act := int32(3), atomic.LoadInt32(&hits); exp != act {
		t.Errorf("Wrong count of rate limit hits: %v != %v", act, exp)
	}
}

func TestRateLimitClosed(t *testing.T) {
	var hits int32
	rlFn := func() (time.Duration, error) {
		if i := atomic.AddInt32(&hits, 1); i == 2 {
			return 0, types.ErrTypeClosed
		}
		return 0, nil
	}

	mgr := &fakeMgr{
		ratelimits: map[string]types.RateLimit{
			"foo": fakeRateLimit{resFn: rlFn},
		},
	}

	conf := NewConfig()
	conf.RateLimit.Resource = "foo"
	proc, err := NewRateLimit(conf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	input := message.New([][]byte{
		[]byte(`{"key":"1","value":"foo 1"}`),
		[]byte(`{"key":"2","value":"foo 2"}`),
		[]byte(`{"key":"1","value":"foo 3"}`),
	})

	output, res := proc.ProcessMessage(input)
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(output) != 1 {
		t.Fatalf("Wrong count of result messages: %v", len(output))
	}

	if exp, act := message.GetAllBytes(input), message.GetAllBytes(output[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result messages: %s != %s", act, exp)
	}

	if exp, act := int32(2), atomic.LoadInt32(&hits); exp != act {
		t.Errorf("Wrong count of rate limit hits: %v != %v", act, exp)
	}
}

func TestRateLimitErroredOut(t *testing.T) {
	rlFn := func() (time.Duration, error) {
		return 0, errors.New("omg foo")
	}

	mgr := &fakeMgr{
		ratelimits: map[string]types.RateLimit{
			"foo": fakeRateLimit{resFn: rlFn},
		},
	}

	conf := NewConfig()
	conf.RateLimit.Resource = "foo"
	proc, err := NewRateLimit(conf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	input := message.New([][]byte{
		[]byte(`{"key":"1","value":"foo 1"}`),
		[]byte(`{"key":"2","value":"foo 2"}`),
		[]byte(`{"key":"1","value":"foo 3"}`),
	})

	closedChan := make(chan struct{})
	go func() {
		output, res := proc.ProcessMessage(input)
		if res != nil {
			t.Fatal(res.Error())
		}

		if len(output) != 1 {
			t.Fatalf("Wrong count of result messages: %v", len(output))
		}

		if exp, act := message.GetAllBytes(input), message.GetAllBytes(output[0]); !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong result messages: %s != %s", act, exp)
		}
		close(closedChan)
	}()

	proc.CloseAsync()
	select {
	case <-closedChan:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
}

func TestRateLimitBlocked(t *testing.T) {
	rlFn := func() (time.Duration, error) {
		return time.Second * 10, nil
	}

	mgr := &fakeMgr{
		ratelimits: map[string]types.RateLimit{
			"foo": fakeRateLimit{resFn: rlFn},
		},
	}

	conf := NewConfig()
	conf.RateLimit.Resource = "foo"
	proc, err := NewRateLimit(conf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	input := message.New([][]byte{
		[]byte(`{"key":"1","value":"foo 1"}`),
		[]byte(`{"key":"2","value":"foo 2"}`),
		[]byte(`{"key":"1","value":"foo 3"}`),
	})

	closedChan := make(chan struct{})
	go func() {
		output, res := proc.ProcessMessage(input)
		if res != nil {
			t.Fatal(res.Error())
		}

		if len(output) != 1 {
			t.Fatalf("Wrong count of result messages: %v", len(output))
		}

		if exp, act := message.GetAllBytes(input), message.GetAllBytes(output[0]); !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong result messages: %s != %s", act, exp)
		}
		close(closedChan)
	}()

	proc.CloseAsync()
	select {
	case <-closedChan:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
}
