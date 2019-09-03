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
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestHTTPClientRetries(t *testing.T) {
	var reqCount uint32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint32(&reqCount, 1)
		http.Error(w, "test error", http.StatusForbidden)
		return
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.HTTP.Client.URL = ts.URL + "/testpost"
	conf.HTTP.Client.Retry = "1ms"
	conf.HTTP.Client.NumRetries = 3

	h, err := NewHTTP(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessMessage(message.New([][]byte{[]byte("test")}))
	if res != nil {
		t.Fatal(res.Error())
	}
	if len(msgs) != 1 {
		t.Fatal("Wrong count of error messages")
	}
	if msgs[0].Len() != 1 {
		t.Fatal("Wrong count of error message parts")
	}
	if exp, act := "test", string(msgs[0].Get(0).Get()); exp != act {
		t.Errorf("Wrong message contents: %v != %v", act, exp)
	}
	if !HasFailed(msgs[0].Get(0)) {
		t.Error("Failed message part not flagged")
	}
	if exp, act := "403", msgs[0].Get(0).Metadata().Get("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	}

	if exp, act := uint32(4), atomic.LoadUint32(&reqCount); exp != act {
		t.Errorf("Wrong count of HTTP attempts: %v != %v", exp, act)
	}
}

func TestHTTPClientBasic(t *testing.T) {
	i := 0
	expPayloads := []string{"foo", "bar", "baz"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if exp, act := expPayloads[i], string(reqBytes); exp != act {
			t.Errorf("Wrong payload value: %v != %v", act, exp)
		}
		i++
		w.Header().Add("foobar", "baz")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("foobar"))
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.HTTP.Client.URL = ts.URL + "/testpost"

	h, err := NewHTTP(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessMessage(message.New([][]byte{[]byte("foo")}))
	if res != nil {
		t.Error(res.Error())
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "201", msgs[0].Get(0).Metadata().Get("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	} else if exp, act := "", msgs[0].Get(0).Metadata().Get("foobar"); exp != act {
		t.Errorf("Wrong metadata value: %v != %v", act, exp)
	}

	msgs, res = h.ProcessMessage(message.New([][]byte{[]byte("bar")}))
	if res != nil {
		t.Error(res.Error())
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "201", msgs[0].Get(0).Metadata().Get("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	} else if exp, act := "", msgs[0].Get(0).Metadata().Get("foobar"); exp != act {
		t.Errorf("Wrong metadata value: %v != %v", act, exp)
	}

	// Check metadata persists.
	msg := message.New([][]byte{[]byte("baz")})
	msg.Get(0).Metadata().Set("foo", "bar")
	msgs, res = h.ProcessMessage(msg)
	if res != nil {
		t.Error(res.Error())
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "bar", msgs[0].Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Metadata not preserved: %v != %v", act, exp)
	} else if exp, act := "201", msgs[0].Get(0).Metadata().Get("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	} else if exp, act := "", msgs[0].Get(0).Metadata().Get("foobar"); exp != act {
		t.Errorf("Wrong metadata value: %v != %v", act, exp)
	}
}

func TestHTTPClientBasicWithMetadata(t *testing.T) {
	i := 0
	expPayloads := []string{"foo", "bar", "baz"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if exp, act := expPayloads[i], string(reqBytes); exp != act {
			t.Errorf("Wrong payload value: %v != %v", act, exp)
		}
		i++
		w.Header().Add("foobar", "baz")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("foobar"))
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.HTTP.Client.URL = ts.URL + "/testpost"
	conf.HTTP.Client.CopyResponseHeaders = true

	h, err := NewHTTP(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessMessage(message.New([][]byte{[]byte("foo")}))
	if res != nil {
		t.Error(res.Error())
	} else if expC, actC := 1, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "201", msgs[0].Get(0).Metadata().Get("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	} else if exp, act := "baz", msgs[0].Get(0).Metadata().Get("foobar"); exp != act {
		t.Errorf("Wrong metadata value: %v != %v", act, exp)
	}
}

func TestHTTPClientParallel(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(5)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wg.Done()
		wg.Wait()
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("foobar"))
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.HTTP.Client.URL = ts.URL + "/testpost"
	conf.HTTP.Parallel = true

	h, err := NewHTTP(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	inputMsg := message.New([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		[]byte("qux"),
		[]byte("quz"),
	})
	inputMsg.Get(0).Metadata().Set("foo", "bar")
	msgs, res := h.ProcessMessage(inputMsg)
	if res != nil {
		t.Error(res.Error())
	} else if expC, actC := 5, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	} else if exp, act := "bar", msgs[0].Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Metadata not preserved: %v != %v", act, exp)
	} else if exp, act := "201", msgs[0].Get(0).Metadata().Get("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	}
}

func TestHTTPClientParallelError(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(5)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wg.Done()
		wg.Wait()
		reqBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(reqBytes) == "baz" {
			http.Error(w, "test error", http.StatusForbidden)
			return
		}
		w.Write([]byte("foobar"))
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.HTTP.Client.URL = ts.URL + "/testpost"
	conf.HTTP.Parallel = true
	conf.HTTP.Client.NumRetries = 0

	h, err := NewHTTP(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessMessage(message.New([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		[]byte("qux"),
		[]byte("quz"),
	}))
	if res != nil {
		t.Error(res.Error())
	}
	if expC, actC := 5, msgs[0].Len(); actC != expC {
		t.Fatalf("Wrong result count: %v != %v", actC, expC)
	}
	if exp, act := "baz", string(msgs[0].Get(2).Get()); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if !HasFailed(msgs[0].Get(2)) {
		t.Error("Expected failed flag")
	}
	if exp, act := "403", msgs[0].Get(2).Metadata().Get("http_status_code"); exp != act {
		t.Errorf("Wrong response code metadata: %v != %v", act, exp)
	}
	for _, i := range []int{0, 1, 3, 4} {
		if exp, act := "foobar", string(msgs[0].Get(i).Get()); act != exp {
			t.Errorf("Wrong result: %v != %v", act, exp)
		}
		if HasFailed(msgs[0].Get(i)) {
			t.Error("Did not expect failed flag")
		}
		if exp, act := "200", msgs[0].Get(i).Metadata().Get("http_status_code"); exp != act {
			t.Errorf("Wrong response code metadata: %v != %v", act, exp)
		}
	}
}

func TestHTTPClientParallelCapped(t *testing.T) {
	var reqs int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if req := atomic.AddInt64(&reqs, 1); req > 5 {
			t.Errorf("Beyond parallelism cap: %v", req)
		}
		<-time.After(time.Millisecond * 10)
		w.Write([]byte("foobar"))
		atomic.AddInt64(&reqs, -1)
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.HTTP.Client.URL = ts.URL + "/testpost"
	conf.HTTP.Parallel = true
	conf.HTTP.MaxParallel = 5

	h, err := NewHTTP(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessMessage(message.New([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		[]byte("qux"),
		[]byte("quz"),
		[]byte("foo2"),
		[]byte("bar2"),
		[]byte("baz2"),
		[]byte("qux2"),
		[]byte("quz2"),
	}))
	if res != nil {
		t.Error(res.Error())
	} else if expC, actC := 10, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}
