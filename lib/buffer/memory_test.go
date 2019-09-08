// Copyright (c) 2014 Ashley Jeffs
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

package buffer

import (
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestMemoryBuffer(t *testing.T) {
	conf := NewConfig()
	conf.Type = "memory"

	buf, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	tChan, resChan := make(chan types.Transaction), make(chan types.Response)

	if err = buf.Consume(tChan); err != nil {
		t.Error(err)
	}

	msg := message.New([][]byte{
		[]byte(`one`),
		[]byte(`two`),
	})

	select {
	case tChan <- types.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("buffer closed early")
		}
		if res.Error() != nil {
			t.Error(res.Error())
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	var outTr types.Transaction
	var open bool
	select {
	case outTr, open = <-buf.TransactionChan():
		if !open {
			t.Error("buffer closed early")
		}
		if exp, act := 2, outTr.Payload.Len(); exp != act {
			t.Errorf("Wrong message length: %v != %v", exp, act)
		} else {
			if exp, act := `one`, string(outTr.Payload.Get(0).Get()); exp != act {
				t.Errorf("Wrong message length: %s != %s", exp, act)
			}
			if exp, act := `two`, string(outTr.Payload.Get(1).Get()); exp != act {
				t.Errorf("Wrong message length: %s != %s", exp, act)
			}
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	select {
	case outTr.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	buf.CloseAsync()
	if err := buf.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}
