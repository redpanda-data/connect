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

package input

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

func TestHTTPBasic(t *testing.T) {
	t.Parallel()

	nTestLoops := 100

	conf := NewConfig()
	conf.HTTPServer.Address = "localhost:1243"
	conf.HTTPServer.Path = "/testpost"

	h, err := NewHTTPServer(conf, nil, log.New(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	<-time.After(time.Millisecond * 1000)

	// Test both single and multipart messages.
	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		// Send it as single part
		go func() {
			if res, err := http.Post(
				"http://localhost:1243/testpost",
				"application/octet-stream",
				bytes.NewBuffer([]byte(testStr)),
			); err != nil {
				t.Error(err)
				return
			} else if res.StatusCode != 200 {
				t.Errorf("Wrong error code returned: %v", res.StatusCode)
				return
			}
		}()

		var ts types.Transaction
		select {
		case ts = <-h.TransactionChan():
			if res := string(ts.Payload.Get(0)); res != testStr {
				t.Errorf("Wrong result, %v != %v", ts.Payload, res)
			}
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		select {
		case ts.ResponseChan <- types.NewSimpleResponse(nil):
		case <-time.After(time.Second):
			t.Error("Timed out waiting for response")
		}
	}

	// Test MIME multipart parsing, as defined in RFC 2046
	for i := 0; i < nTestLoops; i++ {
		partOne := fmt.Sprintf("test%v part one", i)
		partTwo := fmt.Sprintf("test%v part two", i)

		testStr := fmt.Sprintf(
			"--foo\r\n"+
				"Content-Type: application/octet-stream\r\n\r\n"+
				"%v\r\n"+
				"--foo\r\n"+
				"Content-Type: application/octet-stream\r\n\r\n"+
				"%v\r\n"+
				"--foo--\r\n",
			partOne, partTwo)

		// Send it as multi part
		go func() {
			if res, err := http.Post(
				"http://localhost:1243/testpost",
				"multipart/mixed; boundary=foo",
				bytes.NewBuffer([]byte(testStr)),
			); err != nil {
				t.Error(err)
				return
			} else if res.StatusCode != 200 {
				t.Errorf("Wrong error code returned: %v", res.StatusCode)
				return
			}
		}()

		var ts types.Transaction
		select {
		case ts = <-h.TransactionChan():
			if exp, actual := 2, ts.Payload.Len(); exp != actual {
				t.Errorf("Wrong number of parts: %v != %v", actual, exp)
			} else if exp, actual := partOne, string(ts.Payload.Get(0)); exp != actual {
				t.Errorf("Wrong result, %v != %v", actual, exp)
			} else if exp, actual := partTwo, string(ts.Payload.Get(1)); exp != actual {
				t.Errorf("Wrong result, %v != %v", actual, exp)
			}
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		select {
		case ts.ResponseChan <- types.NewSimpleResponse(nil):
		case <-time.After(time.Second):
			t.Error("Timed out waiting for response")
		}
	}

	h.CloseAsync()
	/*
		// TODO: For some reason it seems shutting down a server can block
		// forever.
		if err := h.WaitForClose(time.Second * 5); err != nil {
			t.Error(err)
		}
	*/
}

func TestHTTPBadRequests(t *testing.T) {
	t.Parallel()

	conf := NewConfig()
	conf.HTTPServer.Address = "localhost:1233"
	conf.HTTPServer.Path = "/testpost"

	h, err := NewHTTPServer(conf, nil, log.New(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	<-time.After(time.Millisecond * 1000)

	res, err := http.Get("http://localhost:1233/testpost")
	if err != nil {
		t.Error(err)
		return
	}
	if exp, act := http.StatusMethodNotAllowed, res.StatusCode; exp != act {
		t.Errorf("unexpected HTTP response code: %v != %v", exp, act)
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestHTTPTimeout(t *testing.T) {
	t.Parallel()

	conf := NewConfig()
	conf.HTTPServer.Address = "localhost:1232"
	conf.HTTPServer.Path = "/testpost"
	conf.HTTPServer.TimeoutMS = 1

	h, err := NewHTTPServer(conf, nil, log.New(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	<-time.After(time.Millisecond * 1000)

	var res *http.Response
	res, err = http.Post(
		"http://localhost:1232/testpost",
		"application/octet-stream",
		bytes.NewBuffer([]byte("hello world")),
	)
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
