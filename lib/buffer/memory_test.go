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
	"os"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

func TestMemoryBuffer(t *testing.T) {
	conf := NewConfig()
	conf.Type = "memory"

	buf, err := New(conf, log.New(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	tChan, resChan := make(chan types.Transaction), make(chan types.Response)

	if err = buf.Consume(tChan); err != nil {
		t.Error(err)
	}

	msg := types.NewMessage([][]byte{
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
			if exp, act := `one`, string(outTr.Payload.Get(0)); exp != act {
				t.Errorf("Wrong message length: %s != %s", exp, act)
			}
			if exp, act := `two`, string(outTr.Payload.Get(1)); exp != act {
				t.Errorf("Wrong message length: %s != %s", exp, act)
			}
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	select {
	case outTr.ResponseChan <- types.NewSimpleResponse(nil):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	buf.CloseAsync()
	if err := buf.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}
