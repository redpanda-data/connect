// Copyright (c) 2019 Ashley Jeffs
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

func TestParallelBasic(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(5)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wg.Done()
		wg.Wait()
		w.Write([]byte("foobar"))
	}))
	defer ts.Close()

	httpConf := NewConfig()
	httpConf.Type = TypeHTTP
	httpConf.HTTP.Client.URL = ts.URL + "/testpost"
	conf := NewConfig()
	conf.Parallel.Processors = []Config{httpConf}

	h, err := NewParallel(conf, nil, log.Noop(), metrics.Noop())
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
	} else if expC, actC := 5, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestParallelError(t *testing.T) {
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

	httpConf := NewConfig()
	httpConf.Type = TypeHTTP
	httpConf.HTTP.Client.URL = ts.URL + "/testpost"
	httpConf.HTTP.Client.NumRetries = 0

	conf := NewConfig()
	conf.Parallel.Processors = []Config{httpConf}

	h, err := NewParallel(conf, nil, log.Noop(), metrics.Noop())
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
	for _, i := range []int{0, 1, 3, 4} {
		if exp, act := "foobar", string(msgs[0].Get(i).Get()); act != exp {
			t.Errorf("Wrong result: %v != %v", act, exp)
		}
		if HasFailed(msgs[0].Get(i)) {
			t.Error("Did not expect failed flag")
		}
	}
}

func TestParallelUnack(t *testing.T) {
	batchConf := NewConfig()
	batchConf.Type = TypeBatch
	batchConf.Batch.Count = 1000

	conf := NewConfig()
	conf.Parallel.Processors = []Config{batchConf}

	h, err := NewParallel(conf, nil, log.Noop(), metrics.Noop())
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
	if res == nil {
		t.Error("Expected non-nil response")
	}
	if len(msgs) != 0 {
		t.Error("Expected empty msgs response")
	}
	if !res.SkipAck() {
		t.Errorf("Expected unack response, received: %v", res)
	}
}

func TestParallelCapped(t *testing.T) {
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

	httpConf := NewConfig()
	httpConf.Type = TypeHTTP
	httpConf.HTTP.Client.URL = ts.URL + "/testpost"

	conf := NewConfig()
	conf.Parallel.Processors = []Config{httpConf}
	conf.Parallel.Cap = 5

	h, err := NewParallel(conf, nil, log.Noop(), metrics.Noop())
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
