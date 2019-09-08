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

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/stream"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
	"github.com/gorilla/mux"
	yaml "gopkg.in/yaml.v3"
)

func router(m *Type) *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/streams", m.HandleStreamsCRUD)
	router.HandleFunc("/streams/{id}", m.HandleStreamCRUD)
	router.HandleFunc("/streams/{id}/stats", m.HandleStreamStats)
	return router
}

func genRequest(verb, url string, payload interface{}) *http.Request {
	var body io.Reader

	if payload != nil {
		bodyBytes, err := json.Marshal(payload)
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

func genYAMLRequest(verb, url string, payload interface{}) *http.Request {
	var body io.Reader

	if payload != nil {
		bodyBytes, err := yaml.Marshal(payload)
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

func TestTypeAPIBadMethods(t *testing.T) {
	mgr := New(
		OptSetLogger(log.New(os.Stdout, log.Config{LogLevel: "NONE"})),
		OptSetStats(metrics.DudType{}),
		OptSetManager(types.DudMgr{}),
		OptSetAPITimeout(time.Millisecond*100),
	)

	r := router(mgr)

	request := genRequest("DELETE", "/streams", nil)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusBadRequest, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("DERP", "/streams/foo", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusBadRequest, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
}

func TestTypeAPIBasicOperations(t *testing.T) {
	mgr := New(
		OptSetLogger(log.New(os.Stdout, log.Config{LogLevel: "NONE"})),
		OptSetStats(metrics.DudType{}),
		OptSetManager(types.DudMgr{}),
		OptSetAPITimeout(time.Millisecond*100),
	)

	r := router(mgr)
	conf := harmlessConf()

	request := genRequest("PUT", "/streams/foo", conf)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("GET", "/streams/foo", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("POST", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("POST", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusBadRequest, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("GET", "/streams/bar", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("GET", "/streams/foo", conf)
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

	request = genRequest("PUT", "/streams/foo", newConf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("GET", "/streams/foo", conf)
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

	request = genRequest("DELETE", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("DELETE", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	testVar := "__TEST_INPUT_TYPE"
	originalEnv, orignalSet := os.LookupEnv(testVar)
	defer func() {
		_ = os.Unsetenv(testVar)
		if orignalSet {
			_ = os.Setenv(testVar, originalEnv)
		}
	}()
	_ = os.Setenv(testVar, "http_server")
	newConf = harmlessConf()
	newConf.Input.Type = "${__TEST_INPUT_TYPE}"

	request = genRequest("POST", "/streams/fooEnv", newConf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("GET", "/streams/fooEnv", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
	info = parseGetBody(response.Body)
	// replace the env var with the expected value in the struct
	// because we will be comparing it to the rendered version.
	newConf.Input.Type = "http_server"
	if !info.Active {
		t.Error("Stream not active")
	} else if act, exp := info.Config, newConf; !reflect.DeepEqual(act, exp) {
		t.Errorf("Unexpected config: %v != %v", act, exp)
	}
	request = genRequest("DELETE", "/streams/fooEnv", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
}

func TestTypeAPIPatch(t *testing.T) {
	mgr := New(
		OptSetLogger(log.Noop()),
		OptSetStats(metrics.DudType{}),
		OptSetManager(types.DudMgr{}),
		OptSetAPITimeout(time.Millisecond*100),
	)

	r := router(mgr)
	conf := harmlessConf()

	request := genRequest("PATCH", "/streams/foo", conf)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("POST", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	patchConf := map[string]interface{}{
		"input": map[string]interface{}{
			"http_server": map[string]interface{}{
				"path": "/foobarbaz",
			},
		},
	}
	request = genRequest("PATCH", "/streams/foo", patchConf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	conf.Input.HTTPServer.Path = "/foobarbaz"
	request = genRequest("GET", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
	info := parseGetBody(response.Body)
	if !info.Active {
		t.Fatal("Stream not active")
	}
	if act, exp := info.Config.Input.HTTPServer.Path, conf.Input.HTTPServer.Path; exp != act {
		t.Errorf("Unexpected config: %v != %v", act, exp)
	}
	if act, exp := info.Config.Input.Type, conf.Input.Type; exp != act {
		t.Errorf("Unexpected config: %v != %v", act, exp)
	}
}

func TestTypeAPIBasicOperationsYAML(t *testing.T) {
	mgr := New(
		OptSetLogger(log.New(os.Stdout, log.Config{LogLevel: "NONE"})),
		OptSetStats(metrics.DudType{}),
		OptSetManager(types.DudMgr{}),
		OptSetAPITimeout(time.Millisecond*100),
	)

	r := router(mgr)
	conf := harmlessConf()

	request := genYAMLRequest("PUT", "/streams/foo", conf)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genYAMLRequest("GET", "/streams/foo", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genYAMLRequest("POST", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genYAMLRequest("POST", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusBadRequest, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genYAMLRequest("GET", "/streams/bar", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genYAMLRequest("GET", "/streams/foo", conf)
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

	request = genYAMLRequest("PUT", "/streams/foo", newConf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genYAMLRequest("GET", "/streams/foo", conf)
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

	request = genYAMLRequest("DELETE", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genYAMLRequest("DELETE", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
}

func TestTypeAPIList(t *testing.T) {
	mgr := New(
		OptSetLogger(log.New(os.Stdout, log.Config{LogLevel: "NONE"})),
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

func TestTypeAPISetStreams(t *testing.T) {
	mgr := New(
		OptSetLogger(log.New(os.Stdout, log.Config{LogLevel: "NONE"})),
		OptSetStats(metrics.DudType{}),
		OptSetManager(types.DudMgr{}),
		OptSetAPITimeout(time.Millisecond*100),
	)

	r := router(mgr)

	if err := mgr.Create("foo", harmlessConf()); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Create("bar", harmlessConf()); err != nil {
		t.Fatal(err)
	}

	request := genRequest("GET", "/streams", nil)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
	info := parseListBody(response.Body)
	if exp, act := true, info["foo"].Active; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong list response: %v != %v", act, exp)
	}
	if exp, act := true, info["bar"].Active; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong list response: %v != %v", act, exp)
	}

	barConf := harmlessConf()
	barConf.Input.File.Path = "BAR_ONE"
	bar2Conf := harmlessConf()
	bar2Conf.Input.File.Path = "BAR_TWO"
	bazConf := harmlessConf()
	bazConf.Input.File.Path = "BAZ_ONE"
	streamsBody := map[string]stream.Config{
		"bar":  barConf,
		"bar2": bar2Conf,
		"baz":  bazConf,
	}

	request = genRequest("POST", "/streams", streamsBody)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
		t.Logf("Message: %v", response.Body.String())
	}

	request = genRequest("GET", "/streams", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
		t.Logf("Message: %v", response.Body.String())
	}
	info = parseListBody(response.Body)
	if _, exists := info["foo"]; exists {
		t.Error("Expected foo to be deleted")
	}
	if exp, act := true, info["bar"].Active; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong list response: %v != %v", act, exp)
	}
	if exp, act := true, info["baz"].Active; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong list response: %v != %v", act, exp)
	}

	var barVal, bar2Val, bazVal string

	mgr.lock.Lock()
	if val, exists := mgr.streams["bar"]; exists {
		barVal = val.Config().Input.File.Path
	}
	if val, exists := mgr.streams["bar2"]; exists {
		bar2Val = val.Config().Input.File.Path
	}
	if val, exists := mgr.streams["baz"]; exists {
		bazVal = val.Config().Input.File.Path
	}
	mgr.lock.Unlock()

	if act, exp := barVal, "BAR_ONE"; act != exp {
		t.Errorf("Bar was not updated: %v != %v", act, exp)
	}
	if act, exp := bar2Val, "BAR_TWO"; act != exp {
		t.Errorf("Bar2 was not created: %v != %v", act, exp)
	}
	if act, exp := bazVal, "BAZ_ONE"; act != exp {
		t.Errorf("Baz was not created: %v != %v", act, exp)
	}
}

func TestTypeAPIDefaultConf(t *testing.T) {
	mgr := New(
		OptSetLogger(log.New(os.Stdout, log.Config{LogLevel: "NONE"})),
		OptSetStats(metrics.DudType{}),
		OptSetManager(types.DudMgr{}),
		OptSetAPITimeout(time.Millisecond*100),
	)

	r := router(mgr)

	body := []byte(`{
	"input": {
		"type": "nanomsg"
	},
	"output": {
		"type": "nanomsg"
	}
}`)

	request, err := http.NewRequest("POST", "/streams/foo", bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
}

func TestTypeAPIGetStats(t *testing.T) {
	mgr := New(
		OptSetLogger(log.Noop()),
		OptSetStats(metrics.Noop()),
		OptSetManager(types.DudMgr{}),
		OptSetAPITimeout(time.Millisecond*100),
	)

	r := router(mgr)

	if err := mgr.Create("foo", harmlessConf()); err != nil {
		t.Fatal(err)
	}

	<-time.After(time.Millisecond * 100)

	request := genRequest("GET", "/streams/not_exist/stats", nil)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("POST", "/streams/foo/stats", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusBadRequest, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("GET", "/streams/foo/stats", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	stats, err := gabs.ParseJSON(response.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	if exp, act := float64(1), stats.S("input", "running").Data().(float64); exp != act {
		t.Errorf("Wrong stat value: %v != %v", act, exp)
		t.Logf("Metrics: %v", stats)
	}
}
