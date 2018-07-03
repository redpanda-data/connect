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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/types"
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
	conf.RetryMS = 1
	conf.NumRetries = 3

	h := New(conf)
	if _, err := h.Send(types.NewMessage([][]byte{[]byte("test")})); err == nil {
		t.Error("Expected error from end of retries")
	}

	if exp, act := uint32(4), atomic.LoadUint32(&reqCount); exp != act {
		t.Errorf("Wrong count of HTTP attempts: %v != %v", exp, act)
	}
}

func TestHTTPClientSendBasic(t *testing.T) {
	nTestLoops := 1000

	resultChan := make(chan types.Message, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := types.NewMessage(nil)
		defer func() {
			resultChan <- msg
		}()

		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		msg.SetAll([][]byte{b})
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.URL = ts.URL + "/testpost"

	h := New(conf)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := types.NewMessage([][]byte{[]byte(testStr)})

		if _, err := h.Send(testMsg); err != nil {
			t.Error(err)
		}

		select {
		case resMsg := <-resultChan:
			if resMsg.Len() != 1 {
				t.Errorf("Wrong # parts: %v != %v", resMsg.Len(), 1)
				return
			}
			if exp, actual := testStr, string(resMsg.Get(0)); exp != actual {
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
		msg := types.NewMessage(nil)
		defer func() {
			resultChan <- msg
		}()

		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil {
			t.Errorf("Bad media type: %v -> %v", r.Header.Get("Content-Type"), err)
			return
		}

		if strings.HasPrefix(mediaType, "multipart/") {
			msg.SetAll([][]byte{})
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
				msg.Append(msgBytes)
			}
		} else {
			b, err := ioutil.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			msg.SetAll([][]byte{b})
		}
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.URL = ts.URL + "/testpost"

	h := New(conf)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := types.NewMessage([][]byte{
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
			if exp, actual := testStr+"PART-A", string(resMsg.Get(0)); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
			if exp, actual := testStr+"PART-B", string(resMsg.Get(1)); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}
	}
}

func TestHTTPClientReceiveMultipart(t *testing.T) {
	nTestLoops := 1000

	j := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testStr := fmt.Sprintf("test%v", j)
		j++
		msg := types.NewMessage([][]byte{
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
			}); err == nil {
				_, err = io.Copy(part, bytes.NewReader(msg.Get(i)))
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		writer.Close()

		w.Header().Add("Content-Type", writer.FormDataContentType())
		w.Write(body.Bytes())
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.URL = ts.URL + "/testpost"

	h := New(conf)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", j)
		resMsg, err := h.Send(nil)
		if err != nil {
			t.Error(err)
		}

		if resMsg.Len() != 2 {
			t.Errorf("Wrong # parts: %v != %v", resMsg.Len(), 2)
			return
		}
		if exp, actual := testStr+"PART-A", string(resMsg.Get(0)); exp != actual {
			t.Errorf("Wrong result, %v != %v", exp, actual)
			return
		}
		if exp, actual := testStr+"PART-B", string(resMsg.Get(1)); exp != actual {
			t.Errorf("Wrong result, %v != %v", exp, actual)
			return
		}
	}
}

//------------------------------------------------------------------------------
