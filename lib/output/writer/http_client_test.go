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

package writer

import (
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/roundtrip"
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

	conf := NewHTTPClientConfig()
	conf.URL = ts.URL + "/testpost"
	conf.Retry = "1ms"
	conf.NumRetries = 3

	h, err := NewHTTPClient(conf, types.NoopMgr(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if err = h.Write(message.New([][]byte{[]byte("test")})); err == nil {
		t.Error("Expected error from end of retries")
	}

	if exp, act := uint32(4), atomic.LoadUint32(&reqCount); exp != act {
		t.Errorf("Wrong count of HTTP attempts: %v != %v", exp, act)
	}

	h.CloseAsync()
	if err = h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestHTTPClientBasic(t *testing.T) {
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

	conf := NewHTTPClientConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := NewHTTPClient(conf, types.NoopMgr(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := message.New([][]byte{[]byte(testStr)})

		if err = h.Write(testMsg); err != nil {
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

	h.CloseAsync()
	if err = h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestHTTPClientSyncResponse(t *testing.T) {
	nTestLoops := 1000

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		w.Write([]byte("echo: "))
		w.Write(b)
	}))
	defer ts.Close()

	conf := NewHTTPClientConfig()
	conf.URL = ts.URL + "/testpost"
	conf.PropagateResponse = true

	h, err := NewHTTPClient(conf, types.NoopMgr(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)

		resultStore := roundtrip.NewResultStore()
		testMsg := message.New([][]byte{[]byte(testStr)})
		roundtrip.AddResultStore(testMsg, resultStore)

		if err = h.Write(testMsg); err != nil {
			t.Error(err)
		}

		resMsgs := resultStore.Get()
		if len(resMsgs) != 1 {
			t.Fatalf("Wrong count of result msgs: %v != 1", len(resMsgs))
		}
		resMsg := resMsgs[0]
		if resMsg.Len() != 1 {
			t.Fatalf("Wrong # parts: %v != %v", resMsg.Len(), 1)
		}
		if exp, actual := "echo: "+testStr, string(resMsg.Get(0).Get()); exp != actual {
			t.Fatalf("Wrong result, %v != %v", exp, actual)
		}
	}

	h.CloseAsync()
	if err = h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestHTTPClientMultipart(t *testing.T) {
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

	conf := NewHTTPClientConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := NewHTTPClient(conf, types.NoopMgr(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := message.New([][]byte{
			[]byte(testStr + "PART-A"),
			[]byte(testStr + "PART-B"),
		})

		if err = h.Write(testMsg); err != nil {
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

	h.CloseAsync()
	if err = h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------
