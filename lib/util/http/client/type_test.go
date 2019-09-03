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

package client

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func TestHTTPClientRetries(t *testing.T) {
	var reqCount uint32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint32(&reqCount, 1)
		http.Error(w, "test error", http.StatusForbidden)
		return
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.URL = ts.URL + "/testpost"
	conf.Retry = "1ms"
	conf.NumRetries = 3

	h, err := New(conf)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := h.Send(message.New([][]byte{[]byte("test")})); err == nil {
		t.Error("Expected error from end of retries")
	}

	if exp, act := uint32(4), atomic.LoadUint32(&reqCount); exp != act {
		t.Errorf("Wrong count of HTTP attempts: %v != %v", exp, act)
	}
}

func TestHTTPClientBadRequest(t *testing.T) {
	conf := NewConfig()
	conf.URL = "htp://notvalid:1111"
	conf.Verb = "notvalid\n"
	conf.NumRetries = 3

	h, err := New(conf)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := h.Send(message.New([][]byte{[]byte("test")})); err == nil {
		t.Error("Expected error from malformed URL")
	}
}

func TestHTTPClientSendBasic(t *testing.T) {
	nTestLoops := 1000

	resultChan := make(chan types.Message, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := message.New(nil)
		defer func() {
			resultChan <- msg
		}()

		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		msg.Append(message.NewPart(b))
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := New(conf)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := message.New([][]byte{[]byte(testStr)})

		if _, err := h.Send(testMsg); err != nil {
			t.Error(err)
		}

		select {
		case resMsg := <-resultChan:
			if resMsg.Len() != 1 {
				t.Errorf("Wrong # parts: %v != %v", resMsg.Len(), 1)
				return
			}
			if exp, actual := testStr, string(resMsg.Get(0).Get()); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}
	}
}

func TestHTTPClientDropOn(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"foo":"bar"}`))
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.URL = ts.URL + "/testpost"
	conf.DropOn = []int{400}

	h, err := New(conf)
	if err != nil {
		t.Fatal(err)
	}

	testMsg := message.New([][]byte{[]byte(`{"bar":"baz"}`)})

	resMsg, err := h.Send(testMsg)
	if err == nil {
		t.Errorf("Expected error, received: %s", message.GetAllBytes(resMsg))
	}
}

func TestHTTPClientSendInterpolate(t *testing.T) {
	nTestLoops := 1000

	resultChan := make(chan types.Message, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if exp, act := "/firstvar", r.URL.Path; exp != act {
			t.Errorf("Wrong path: %v != %v", act, exp)
		}
		if exp, act := "hdr-secondvar", r.Header.Get("dynamic"); exp != act {
			t.Errorf("Wrong path: %v != %v", act, exp)
		}
		if exp, act := "foo", r.Header.Get("static"); exp != act {
			t.Errorf("Wrong header value: %v != %v", act, exp)
		}

		if exp, act := "simpleHost.com", r.Host; exp != act {
			t.Errorf("Wrong host value: %v ! %v", act, exp)
		}

		msg := message.New(nil)
		defer func() {
			resultChan <- msg
		}()

		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		msg.Append(message.NewPart(b))
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.URL = ts.URL + "/${!json_field:foo.bar}"
	conf.Headers["static"] = "foo"
	conf.Headers["dynamic"] = "hdr-${!json_field:foo.baz}"
	conf.Headers["Host"] = "simpleHost.com"

	h, err := New(
		conf,
		OptSetCloseChan(make(chan struct{})),
		OptSetLogger(log.Noop()),
		OptSetStats(metrics.Noop()),
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf(`{"test":%v,"foo":{"bar":"firstvar","baz":"secondvar"}}`, i)
		testMsg := message.New([][]byte{[]byte(testStr)})

		if _, err := h.Send(testMsg); err != nil {
			t.Error(err)
		}

		select {
		case resMsg := <-resultChan:
			if resMsg.Len() != 1 {
				t.Errorf("Wrong # parts: %v != %v", resMsg.Len(), 1)
				return
			}
			if exp, actual := testStr, string(resMsg.Get(0).Get()); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}
	}
}

func TestHTTPClientSendMultipart(t *testing.T) {
	nTestLoops := 1000

	resultChan := make(chan types.Message, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := message.New(nil)
		defer func() {
			resultChan <- msg
		}()

		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil {
			t.Errorf("Bad media type: %v -> %v", r.Header.Get("Content-Type"), err)
			return
		}

		if strings.HasPrefix(mediaType, "multipart/") {
			mr := multipart.NewReader(r.Body, params["boundary"])
			for {
				p, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Error(err)
					return
				}
				msgBytes, err := ioutil.ReadAll(p)
				if err != nil {
					t.Error(err)
					return
				}
				msg.Append(message.NewPart(msgBytes))
			}
		} else {
			b, err := ioutil.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			msg.Append(message.NewPart(b))
		}
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := New(conf)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := message.New([][]byte{
			[]byte(testStr + "PART-A"),
			[]byte(testStr + "PART-B"),
		})

		if _, err := h.Send(testMsg); err != nil {
			t.Error(err)
		}

		select {
		case resMsg := <-resultChan:
			if resMsg.Len() != 2 {
				t.Errorf("Wrong # parts: %v != %v", resMsg.Len(), 2)
				return
			}
			if exp, actual := testStr+"PART-A", string(resMsg.Get(0).Get()); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
			if exp, actual := testStr+"PART-B", string(resMsg.Get(1).Get()); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}
	}
}

func TestHTTPClientReceive(t *testing.T) {
	nTestLoops := 1000

	j := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testStr := fmt.Sprintf("test%v", j)
		j++
		w.Header().Set("foo-bar", "baz-0")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(testStr + "PART-A"))
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := New(conf)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", j)
		resMsg, err := h.Send(nil)
		if err != nil {
			t.Error(err)
		}

		if resMsg.Len() != 1 {
			t.Fatalf("Wrong # parts: %v != %v", resMsg.Len(), 2)
		}
		if exp, actual := testStr+"PART-A", string(resMsg.Get(0).Get()); exp != actual {
			t.Fatalf("Wrong result, %v != %v", exp, actual)
		}
		if exp, act := "", resMsg.Get(0).Metadata().Get("foo-bar"); exp != act {
			t.Fatalf("Wrong metadata value: %v != %v", act, exp)
		}
		if exp, act := "201", resMsg.Get(0).Metadata().Get("http_status_code"); exp != act {
			t.Fatalf("Wrong metadata value: %v != %v", act, exp)
		}
	}
}

func TestHTTPClientReceiveHeaders(t *testing.T) {
	nTestLoops := 1000

	j := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testStr := fmt.Sprintf("test%v", j)
		j++
		w.Header().Set("foo-bar", "baz-0")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(testStr + "PART-A"))
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.URL = ts.URL + "/testpost"
	conf.CopyResponseHeaders = true

	h, err := New(conf)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", j)
		resMsg, err := h.Send(nil)
		if err != nil {
			t.Error(err)
		}

		if resMsg.Len() != 1 {
			t.Fatalf("Wrong # parts: %v != %v", resMsg.Len(), 2)
		}
		if exp, actual := testStr+"PART-A", string(resMsg.Get(0).Get()); exp != actual {
			t.Fatalf("Wrong result, %v != %v", exp, actual)
		}
		if exp, act := "baz-0", resMsg.Get(0).Metadata().Get("foo-bar"); exp != act {
			t.Fatalf("Wrong metadata value: %v != %v", act, exp)
		}
		if exp, act := "201", resMsg.Get(0).Metadata().Get("http_status_code"); exp != act {
			t.Fatalf("Wrong metadata value: %v != %v", act, exp)
		}
	}
}

func TestHTTPClientReceiveMultipart(t *testing.T) {
	nTestLoops := 1000

	j := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testStr := fmt.Sprintf("test%v", j)
		j++
		msg := message.New([][]byte{
			[]byte(testStr + "PART-A"),
			[]byte(testStr + "PART-B"),
		})

		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		for i := 0; i < msg.Len(); i++ {
			var part io.Writer
			var err error
			if part, err = writer.CreatePart(textproto.MIMEHeader{
				"Content-Type": []string{"application/octet-stream"},
				"foo-bar":      []string{"baz-" + strconv.Itoa(i), "ignored"},
			}); err == nil {
				_, err = io.Copy(part, bytes.NewReader(msg.Get(i).Get()))
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		writer.Close()

		w.Header().Add("Content-Type", writer.FormDataContentType())
		w.WriteHeader(http.StatusCreated)
		w.Write(body.Bytes())
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := New(conf)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", j)
		resMsg, err := h.Send(nil)
		if err != nil {
			t.Error(err)
		}

		if resMsg.Len() != 2 {
			t.Fatalf("Wrong # parts: %v != %v", resMsg.Len(), 2)
		}
		if exp, actual := testStr+"PART-A", string(resMsg.Get(0).Get()); exp != actual {
			t.Fatalf("Wrong result, %v != %v", exp, actual)
		}
		if exp, actual := testStr+"PART-B", string(resMsg.Get(1).Get()); exp != actual {
			t.Fatalf("Wrong result, %v != %v", exp, actual)
		}
		if exp, act := "", resMsg.Get(0).Metadata().Get("foo-bar"); exp != act {
			t.Fatalf("Wrong metadata value: %v != %v", act, exp)
		}
		if exp, act := "201", resMsg.Get(0).Metadata().Get("http_status_code"); exp != act {
			t.Fatalf("Wrong metadata value: %v != %v", act, exp)
		}
		if exp, act := "", resMsg.Get(1).Metadata().Get("foo-bar"); exp != act {
			t.Fatalf("Wrong metadata value: %v != %v", act, exp)
		}
		if exp, act := "201", resMsg.Get(1).Metadata().Get("http_status_code"); exp != act {
			t.Fatalf("Wrong metadata value: %v != %v", act, exp)
		}
	}
}

func TestHTTPClientReceiveMultipartWithHeaders(t *testing.T) {
	nTestLoops := 1000

	j := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testStr := fmt.Sprintf("test%v", j)
		j++
		msg := message.New([][]byte{
			[]byte(testStr + "PART-A"),
			[]byte(testStr + "PART-B"),
		})

		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		for i := 0; i < msg.Len(); i++ {
			var part io.Writer
			var err error
			if part, err = writer.CreatePart(textproto.MIMEHeader{
				"Content-Type": []string{"application/octet-stream"},
				"foo-bar":      []string{"baz-" + strconv.Itoa(i), "ignored"},
			}); err == nil {
				_, err = io.Copy(part, bytes.NewReader(msg.Get(i).Get()))
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		writer.Close()

		w.Header().Add("Content-Type", writer.FormDataContentType())
		w.WriteHeader(http.StatusCreated)
		w.Write(body.Bytes())
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.URL = ts.URL + "/testpost"
	conf.CopyResponseHeaders = true

	h, err := New(conf)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", j)
		resMsg, err := h.Send(nil)
		if err != nil {
			t.Error(err)
		}

		if resMsg.Len() != 2 {
			t.Fatalf("Wrong # parts: %v != %v", resMsg.Len(), 2)
		}
		if exp, actual := testStr+"PART-A", string(resMsg.Get(0).Get()); exp != actual {
			t.Fatalf("Wrong result, %v != %v", exp, actual)
		}
		if exp, actual := testStr+"PART-B", string(resMsg.Get(1).Get()); exp != actual {
			t.Fatalf("Wrong result, %v != %v", exp, actual)
		}
		if exp, act := "baz-0", resMsg.Get(0).Metadata().Get("foo-bar"); exp != act {
			t.Fatalf("Wrong metadata value: %v != %v", act, exp)
		}
		if exp, act := "201", resMsg.Get(0).Metadata().Get("http_status_code"); exp != act {
			t.Fatalf("Wrong metadata value: %v != %v", act, exp)
		}
		if exp, act := "baz-1", resMsg.Get(1).Metadata().Get("foo-bar"); exp != act {
			t.Fatalf("Wrong metadata value: %v != %v", act, exp)
		}
		if exp, act := "201", resMsg.Get(1).Metadata().Get("http_status_code"); exp != act {
			t.Fatalf("Wrong metadata value: %v != %v", act, exp)
		}
	}
}

//------------------------------------------------------------------------------
