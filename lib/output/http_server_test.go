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

package output

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestHTTPBasic(t *testing.T) {
	nTestLoops := 10

	conf := NewConfig()
	conf.HTTPServer.Address = "localhost:1237"
	conf.HTTPServer.Path = "/testpost"

	h, err := NewHTTPServer(conf, nil, log.New(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	if err = h.Consume(msgChan); err != nil {
		t.Error(err)
		return
	}
	if err = h.Consume(msgChan); err == nil {
		t.Error("Expected error from double listen")
	}

	<-time.After(time.Millisecond * 100)

	// Test both single and multipart messages.
	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)

		go func() {
			testMsg := message.New([][]byte{[]byte(testStr)})
			select {
			case msgChan <- types.NewTransaction(testMsg, resChan):
			case <-time.After(time.Second):
				t.Error("Timed out waiting for message")
				return
			}
			select {
			case resMsg := <-resChan:
				if resMsg.Error() != nil {
					t.Error(resMsg.Error())
				}
			case <-time.After(time.Second):
				t.Error("Timed out waiting for response")
			}
		}()

		if res, err := http.Get("http://localhost:1237/testpost"); err != nil {
			t.Error(err)
			return
		} else if res.StatusCode != 200 {
			t.Errorf("Wrong error code returned: %v", res.StatusCode)
			return
		}
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestHTTPBadRequests(t *testing.T) {
	conf := NewConfig()
	conf.HTTPServer.Address = "localhost:1236"
	conf.HTTPServer.Path = "/testpost"

	h, err := NewHTTPServer(conf, nil, log.New(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Transaction)

	if err = h.Consume(msgChan); err != nil {
		t.Error(err)
		return
	}

	<-time.After(time.Millisecond * 100)

	h.CloseAsync()
	if err := h.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}

	_, err = http.Get("http://localhost:1236/testpost")
	if err == nil {
		t.Error("request success when service should be closed")
	}
}

func TestHTTPTimeout(t *testing.T) {
	conf := NewConfig()
	conf.HTTPServer.Address = "localhost:1235"
	conf.HTTPServer.Path = "/testpost"
	conf.HTTPServer.Timeout = "1ms"

	h, err := NewHTTPServer(conf, nil, log.New(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Transaction)

	if err = h.Consume(msgChan); err != nil {
		t.Error(err)
		return
	}

	<-time.After(time.Millisecond * 100)

	var res *http.Response
	res, err = http.Get("http://localhost:1235/testpost")
	if err != nil {
		t.Error(err)
		return
	}
	if exp, act := http.StatusRequestTimeout, res.StatusCode; exp != act {
		t.Errorf("Unexpected status code: %v != %v", exp, act)
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}
