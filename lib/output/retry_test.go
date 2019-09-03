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

package output

import (
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestRetryConfigErrs(t *testing.T) {
	conf := NewConfig()
	conf.Type = "retry"

	if _, err := New(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Error("Expected error from bad retry output")
	}

	oConf := NewConfig()
	conf.Retry.Output = &oConf
	conf.Retry.Backoff.InitialInterval = "not a time period"

	if _, err := New(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Error("Expected error from bad initial period")
	}
}

func TestRetryBasic(t *testing.T) {
	conf := NewConfig()

	childConf := NewConfig()
	conf.Retry.Output = &childConf

	output, err := NewRetry(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	ret, ok := output.(*Retry)
	if !ok {
		t.Fatal("Failed to cast")
	}

	mOut := &mockOutput{
		ts: make(chan types.Transaction),
	}
	ret.wrapped = mOut

	tChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	if err = ret.Consume(tChan); err != nil {
		t.Fatal(err)
	}

	testMsg := message.New(nil)
	go func() {
		select {
		case tChan <- types.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}()

	var tran types.Transaction
	select {
	case tran = <-mOut.ts:
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	if tran.Payload != testMsg {
		t.Error("Wrong payload returned")
	}

	select {
	case tran.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	select {
	case res := <-resChan:
		if err = res.Error(); err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	output.CloseAsync()
	if err = output.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestRetrySadPath(t *testing.T) {
	conf := NewConfig()

	childConf := NewConfig()
	conf.Retry.Output = &childConf
	conf.Retry.Backoff.InitialInterval = "10us"
	conf.Retry.Backoff.MaxInterval = "10us"

	output, err := NewRetry(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	ret, ok := output.(*Retry)
	if !ok {
		t.Fatal("Failed to cast")
	}

	mOut := &mockOutput{
		ts: make(chan types.Transaction),
	}
	ret.wrapped = mOut

	tChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	if err = ret.Consume(tChan); err != nil {
		t.Fatal(err)
	}

	testMsg := message.New(nil)
	tran := types.NewTransaction(testMsg, resChan)

	go func() {
		select {
		case tChan <- tran:
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}()

	for i := 0; i < 100; i++ {
		select {
		case tran = <-mOut.ts:
		case <-resChan:
			t.Fatal("Received response not retry")
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		if tran.Payload != testMsg {
			t.Error("Wrong payload returned")
		}

		select {
		case tran.ResponseChan <- response.NewNoack():
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}

	select {
	case tran = <-mOut.ts:
	case <-resChan:
		t.Fatal("Received response not retry")
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	if tran.Payload != testMsg {
		t.Error("Wrong payload returned")
	}

	select {
	case tran.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	select {
	case res := <-resChan:
		if err = res.Error(); err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	output.CloseAsync()
	if err = output.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}
