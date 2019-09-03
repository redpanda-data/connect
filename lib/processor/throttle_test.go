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
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestThrottle(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeThrottle
	conf.Throttle.Period = "1ns"

	throt, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgIn := message.New(nil)
	msgsOut, res := throt.ProcessMessage(msgIn)
	if res != nil {
		t.Fatal(res.Error())
	}

	if exp, act := msgIn, msgsOut[0]; exp != act {
		t.Errorf("Wrong message returned: %v != %v", act, exp)
	}
}

func TestThrottle200Millisecond(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeThrottle
	conf.Throttle.Period = "200ms"

	throt, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	tBefore := time.Now()
	throt.ProcessMessage(message.New(nil))
	tBetween := time.Now()
	throt.ProcessMessage(message.New(nil))
	tAfter := time.Now()

	if dur := tBetween.Sub(tBefore); dur > (time.Millisecond * 50) {
		t.Errorf("First message took too long")
	}
	if dur := tAfter.Sub(tBetween); dur < (time.Millisecond * 200) {
		t.Errorf("First message didn't take long enough")
	}
}

func TestThrottleBadPeriod(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeThrottle
	conf.Throttle.Period = "1gfdfgfdns"

	_, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("Expected error from bad duration")
	}
}
