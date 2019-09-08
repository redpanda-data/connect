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

package serverless

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/output"

	"github.com/Jeffail/benthos/v3/lib/processor"

	"github.com/Jeffail/benthos/v3/lib/config"
)

func TestHandlerAsync(t *testing.T) {
	var results [][]byte
	var resMut sync.Mutex
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resMut.Lock()
		defer resMut.Unlock()

		resBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, resBytes)

		w.Write([]byte("success"))
	}))
	defer ts.Close()

	conf := config.New()
	conf.Output.Type = "http_client"
	conf.Output.HTTPClient.URL = ts.URL

	h, err := NewHandler(conf)
	if err != nil {
		t.Fatal(err)
	}

	var res interface{}
	if res, err = h.Handle(context.Background(), map[string]interface{}{"foo": "bar"}); err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]interface{}{"message": "request successful"}, res; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong sync response: %v != %v", exp, act)
	}
	if exp, act := [][]byte{[]byte(`{"foo":"bar"}`)}, results; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong sync response: %s != %s", exp, act)
	}

	if err = h.Close(time.Second * 10); err != nil {
		t.Error(err)
	}
}

func TestHandlerSync(t *testing.T) {
	conf := config.New()
	conf.Output.Type = ServerlessResponseType

	h, err := NewHandler(conf)
	if err != nil {
		t.Fatal(err)
	}

	var res interface{}
	if res, err = h.Handle(context.Background(), map[string]interface{}{"foo": "bar"}); err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]interface{}{"foo": "bar"}, res; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong sync response: %v != %v", exp, act)
	}

	if err = h.Close(time.Second * 10); err != nil {
		t.Error(err)
	}
}

func TestHandlerSyncBatch(t *testing.T) {
	conf := config.New()
	conf.Output.Type = ServerlessResponseType

	pConf := processor.NewConfig()
	pConf.Type = processor.TypeSelectParts
	pConf.SelectParts.Parts = []int{0, 0, 0}

	conf.Pipeline.Processors = append(conf.Pipeline.Processors, pConf)

	h, err := NewHandler(conf)
	if err != nil {
		t.Fatal(err)
	}

	var res interface{}
	if res, err = h.Handle(context.Background(), map[string]interface{}{"foo": "bar"}); err != nil {
		t.Fatal(err)
	}
	if exp, act := []interface{}{
		map[string]interface{}{"foo": "bar"},
		map[string]interface{}{"foo": "bar"},
		map[string]interface{}{"foo": "bar"},
	}, res; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong sync response: %v != %v", exp, act)
	}

	if err = h.Close(time.Second * 10); err != nil {
		t.Error(err)
	}
}

func TestHandlerSyncBatches(t *testing.T) {
	conf := config.New()
	conf.Output.Type = ServerlessResponseType

	pConf := processor.NewConfig()
	pConf.Type = processor.TypeSelectParts
	pConf.SelectParts.Parts = []int{0, 0, 0}

	conf.Pipeline.Processors = append(conf.Pipeline.Processors, pConf)

	pConf = processor.NewConfig()
	pConf.Type = processor.TypeSplit

	conf.Pipeline.Processors = append(conf.Pipeline.Processors, pConf)

	h, err := NewHandler(conf)
	if err != nil {
		t.Fatal(err)
	}

	var res interface{}
	if res, err = h.Handle(context.Background(), map[string]interface{}{"foo": "bar"}); err != nil {
		t.Fatal(err)
	}
	if exp, act := []interface{}{
		[]interface{}{map[string]interface{}{"foo": "bar"}},
		[]interface{}{map[string]interface{}{"foo": "bar"}},
		[]interface{}{map[string]interface{}{"foo": "bar"}},
	}, res; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong sync response: %v != %v", exp, act)
	}

	if err = h.Close(time.Second * 10); err != nil {
		t.Error(err)
	}
}

func TestHandlerCombined(t *testing.T) {
	var results [][]byte
	var resMut sync.Mutex
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resMut.Lock()
		defer resMut.Unlock()

		resBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, resBytes)

		w.Write([]byte("success"))
	}))
	defer ts.Close()

	conf := config.New()
	conf.Output.Type = "broker"

	cConf := output.NewConfig()
	cConf.Type = ServerlessResponseType

	conf.Output.Broker.Outputs = append(conf.Output.Broker.Outputs, cConf)

	cConf = output.NewConfig()
	cConf.Type = "http_client"
	cConf.HTTPClient.URL = ts.URL

	conf.Output.Broker.Outputs = append(conf.Output.Broker.Outputs, cConf)

	h, err := NewHandler(conf)
	if err != nil {
		t.Fatal(err)
	}

	var res interface{}
	if res, err = h.Handle(context.Background(), map[string]interface{}{"foo": "bar"}); err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]interface{}{"foo": "bar"}, res; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong sync response: %v != %v", exp, act)
	}
	if exp, act := [][]byte{[]byte(`{"foo":"bar"}`)}, results; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong sync response: %s != %s", exp, act)
	}

	if err = h.Close(time.Second * 10); err != nil {
		t.Error(err)
	}
}
