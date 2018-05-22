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

package manager

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/stream"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/gorilla/mux"
	yaml "gopkg.in/yaml.v2"
)

func router(m *Type) *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/streams", m.HandleList)
	router.HandleFunc("/stream/{id}", m.HandleCRUD)
	return router
}

func genRequest(verb, url string, conf *stream.Config) *http.Request {
	var body io.Reader

	if conf != nil {
		sanit, err := conf.Sanitised()
		if err != nil {
			panic(err)
		}
		bodyBytes, err := json.Marshal(sanit)
		if err != nil {
			panic(err)
		}
		body = bytes.NewReader(bodyBytes)
	}

	req, err := http.NewRequest(verb, url, body)
	if err != nil {
		panic(err)
	}

	return req
}

func genYAMLRequest(verb, url string, conf *stream.Config) *http.Request {
	var body io.Reader

	if conf != nil {
		sanit, err := conf.Sanitised()
		if err != nil {
			panic(err)
		}
		bodyBytes, err := yaml.Marshal(sanit)
		if err != nil {
			panic(err)
		}
		body = bytes.NewReader(bodyBytes)
	}

	req, err := http.NewRequest(verb, url, body)
	if err != nil {
		panic(err)
	}

	return req
}

type listItemBody struct {
	Active    bool    `json:"active"`
	Uptime    float64 `json:"uptime"`
	UptimeStr string  `json:"uptime_str"`
}

type listBody map[string]listItemBody

func parseListBody(data *bytes.Buffer) listBody {
	result := listBody{}
	if err := json.Unmarshal(data.Bytes(), &result); err != nil {
		panic(err)
	}
	return result
}

type getBody struct {
	Active    bool          `json:"active"`
	Uptime    float64       `json:"uptime"`
	UptimeStr string        `json:"uptime_str"`
	Config    stream.Config `json:"config"`
}

func parseGetBody(data *bytes.Buffer) getBody {
	result := getBody{
		Config: stream.NewConfig(),
	}
	if err := json.Unmarshal(data.Bytes(), &result); err != nil {
		panic(err)
	}
	return result
}

func TestTypeAPIBasicOperations(t *testing.T) {
	mgr := New(
		OptSetLogger(log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})),
		OptSetStats(metrics.DudType{}),
		OptSetManager(types.DudMgr{}),
		OptSetAPITimeout(time.Millisecond*100),
	)

	r := router(mgr)
	conf := harmlessConf()

	request := genRequest("PUT", "/stream/foo", &conf)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("GET", "/stream/foo", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("POST", "/stream/foo", &conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("POST", "/stream/foo", &conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusBadRequest, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("GET", "/stream/bar", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("GET", "/stream/foo", &conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
	info := parseGetBody(response.Body)
	if !info.Active {
		t.Error("Stream not active")
	} else if act, exp := info.Config, conf; !reflect.DeepEqual(act, exp) {
		t.Errorf("Unexpected config: %v != %v", act, exp)
	}

	newConf := harmlessConf()
	newConf.Buffer.Type = "memory"

	request = genRequest("PUT", "/stream/foo", &newConf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("GET", "/stream/foo", &conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
	info = parseGetBody(response.Body)
	if !info.Active {
		t.Error("Stream not active")
	} else if act, exp := info.Config, newConf; !reflect.DeepEqual(act, exp) {
		t.Errorf("Unexpected config: %v != %v", act, exp)
	}

	request = genRequest("DELETE", "/stream/foo", &conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("DELETE", "/stream/foo", &conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
}

func TestTypeAPIBasicOperationsYAML(t *testing.T) {
	mgr := New(
		OptSetLogger(log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})),
		OptSetStats(metrics.DudType{}),
		OptSetManager(types.DudMgr{}),
		OptSetAPITimeout(time.Millisecond*100),
	)

	r := router(mgr)
	conf := harmlessConf()

	request := genYAMLRequest("PUT", "/stream/foo", &conf)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genYAMLRequest("GET", "/stream/foo", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genYAMLRequest("POST", "/stream/foo", &conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genYAMLRequest("POST", "/stream/foo", &conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusBadRequest, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genYAMLRequest("GET", "/stream/bar", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genYAMLRequest("GET", "/stream/foo", &conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
	info := parseGetBody(response.Body)
	if !info.Active {
		t.Error("Stream not active")
	} else if act, exp := info.Config, conf; !reflect.DeepEqual(act, exp) {
		t.Errorf("Unexpected config: %v != %v", act, exp)
	}

	newConf := harmlessConf()
	newConf.Buffer.Type = "memory"

	request = genYAMLRequest("PUT", "/stream/foo", &newConf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genYAMLRequest("GET", "/stream/foo", &conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
	info = parseGetBody(response.Body)
	if !info.Active {
		t.Error("Stream not active")
	} else if act, exp := info.Config, newConf; !reflect.DeepEqual(act, exp) {
		t.Errorf("Unexpected config: %v != %v", act, exp)
	}

	request = genYAMLRequest("DELETE", "/stream/foo", &conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genYAMLRequest("DELETE", "/stream/foo", &conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
}

func TestTypeAPIList(t *testing.T) {
	mgr := New(
		OptSetLogger(log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})),
		OptSetStats(metrics.DudType{}),
		OptSetManager(types.DudMgr{}),
		OptSetAPITimeout(time.Millisecond*100),
	)

	r := router(mgr)

	request := genRequest("GET", "/streams", nil)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
	info := parseListBody(response.Body)
	if exp, act := (listBody{}), info; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong list response: %v != %v", act, exp)
	}

	if err := mgr.Create("foo", harmlessConf()); err != nil {
		t.Fatal(err)
	}

	request = genRequest("GET", "/streams", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
	info = parseListBody(response.Body)
	if exp, act := true, info["foo"].Active; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong list response: %v != %v", act, exp)
	}
}

func TestTypeAPIDefaultConf(t *testing.T) {
	mgr := New(
		OptSetLogger(log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})),
		OptSetStats(metrics.DudType{}),
		OptSetManager(types.DudMgr{}),
		OptSetAPITimeout(time.Millisecond*100),
	)

	r := router(mgr)

	request, err := http.NewRequest("POST", "/stream/foo", bytes.NewReader([]byte(`{}`)))
	if err != nil {
		panic(err)
	}
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
}
