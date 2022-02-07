package manager_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	bmanager "github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/stream"
	"github.com/Jeffail/benthos/v3/lib/stream/manager"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func router(m *manager.Type) *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/streams", m.HandleStreamsCRUD)
	router.HandleFunc("/streams/{id}", m.HandleStreamCRUD)
	router.HandleFunc("/streams/{id}/stats", m.HandleStreamStats)
	router.HandleFunc("/resources/{type}/{id}", m.HandleResourceCRUD)
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
		if s, ok := payload.(string); ok {
			body = bytes.NewReader([]byte(s))
		} else {
			bodyBytes, err := yaml.Marshal(payload)
			if err != nil {
				panic(err)
			}
			body = bytes.NewReader(bodyBytes)
		}
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
	Active    bool        `json:"active"`
	Uptime    float64     `json:"uptime"`
	UptimeStr string      `json:"uptime_str"`
	Config    interface{} `json:"config"`
}

func parseGetBody(t *testing.T, data *bytes.Buffer) getBody {
	t.Helper()
	result := getBody{}
	if err := yaml.Unmarshal(data.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

type endpointReg struct {
	endpoints map[string]http.HandlerFunc
}

func (f *endpointReg) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
	f.endpoints[path] = h
}

func TestTypeAPIDisabled(t *testing.T) {
	r := &endpointReg{endpoints: map[string]http.HandlerFunc{}}
	rMgr, err := bmanager.NewV2(bmanager.NewResourceConfig(), r, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	_ = manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(rMgr),
		manager.OptSetAPITimeout(time.Millisecond*100),
		manager.OptAPIEnabled(true),
	)
	assert.NotEmpty(t, r.endpoints)

	r = &endpointReg{endpoints: map[string]http.HandlerFunc{}}
	_ = manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(rMgr),
		manager.OptSetAPITimeout(time.Millisecond*100),
		manager.OptAPIEnabled(false),
	)
	assert.Len(t, r.endpoints, 1)
	assert.Contains(t, r.endpoints, "/ready")
}

func TestTypeAPIBadMethods(t *testing.T) {
	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(types.NoopMgr()),
		manager.OptSetAPITimeout(time.Millisecond*100),
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

func harmlessConf() stream.Config {
	c := stream.NewConfig()
	c.Input.Type = "http_server"
	c.Output.Type = "http_server"
	return c
}

func TestTypeAPIBasicOperations(t *testing.T) {
	res, err := bmanager.NewV2(bmanager.NewResourceConfig(), types.NoopMgr(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(res),
		manager.OptSetAPITimeout(time.Second*10),
	)

	r := router(mgr)
	conf, err := harmlessConf().Sanitised()
	require.NoError(t, err)

	request := genRequest("PUT", "/streams/foo", conf)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	require.Equal(t, http.StatusNotFound, response.Code, response.Body.String())

	request = genRequest("GET", "/streams/foo", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusNotFound, response.Code)

	request = genRequest("POST", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)

	request = genRequest("POST", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusBadRequest, response.Code)

	request = genRequest("GET", "/streams/bar", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusNotFound, response.Code)

	request = genRequest("GET", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)

	info := parseGetBody(t, response.Body)
	assert.True(t, info.Active)

	assert.Equal(t, conf, info.Config)

	newConf := harmlessConf()
	newConf.Buffer.Type = "memory"
	newConfSanit, err := newConf.Sanitised()
	require.NoError(t, err)

	request = genRequest("PUT", "/streams/foo", newConfSanit)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	request = genRequest("GET", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	info = parseGetBody(t, response.Body)
	assert.True(t, info.Active)

	assert.Equal(t, newConfSanit, info.Config)

	request = genRequest("DELETE", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	request = genRequest("DELETE", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusNotFound, response.Code, response.Body.String())

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

	request = genRequest("POST", "/streams/fooEnv?chilled=true", newConf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	request = genRequest("GET", "/streams/fooEnv", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	info = parseGetBody(t, response.Body)
	// replace the env var with the expected value in the struct
	// because we will be comparing it to the rendered version.
	newConf.Input.Type = "http_server"
	sanitNewConf, err := newConf.Sanitised()
	require.NoError(t, err)

	assert.True(t, info.Active)
	assert.Equal(t, sanitNewConf, info.Config)

	request = genRequest("DELETE", "/streams/fooEnv", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())
}

func TestTypeAPIPatch(t *testing.T) {
	res, err := bmanager.NewV2(bmanager.NewResourceConfig(), types.NoopMgr(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(res),
		manager.OptSetAPITimeout(time.Millisecond*100),
	)

	r := router(mgr)
	conf := harmlessConf()

	request := genRequest("PATCH", "/streams/foo", conf)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	request = genRequest("POST", "/streams/foo?chilled=true", conf)
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
	info := parseGetBody(t, response.Body)
	if !info.Active {
		t.Fatal("Stream not active")
	}

	assert.Equal(t, conf.Input.HTTPServer.Path, gabs.Wrap(info.Config).S("input", "http_server", "path").Data())
}

func TestTypeAPIBasicOperationsYAML(t *testing.T) {
	res, err := bmanager.NewV2(bmanager.NewResourceConfig(), types.NoopMgr(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(res),
		manager.OptSetAPITimeout(time.Second*10),
	)

	r := router(mgr)
	conf := harmlessConf()

	request := genYAMLRequest("PUT", "/streams/foo?chilled=true", conf)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusNotFound, response.Code)

	request = genYAMLRequest("GET", "/streams/foo", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusNotFound, response.Code)

	request = genYAMLRequest("POST", "/streams/foo?chilled=true", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)

	request = genYAMLRequest("POST", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusBadRequest, response.Code)

	request = genYAMLRequest("GET", "/streams/bar", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusNotFound, response.Code)

	request = genYAMLRequest("GET", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)

	sanitConf, err := conf.Sanitised()
	require.NoError(t, err)

	info := parseGetBody(t, response.Body)
	require.True(t, info.Active)
	assert.Equal(t, sanitConf, info.Config)

	newConf := harmlessConf()
	newConf.Buffer.Type = "memory"

	request = genYAMLRequest("PUT", "/streams/foo?chilled=true", newConf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)

	request = genYAMLRequest("GET", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)

	sanitNewConf, err := newConf.Sanitised()
	require.NoError(t, err)

	info = parseGetBody(t, response.Body)
	require.True(t, info.Active)
	assert.Equal(t, sanitNewConf, info.Config)

	request = genYAMLRequest("DELETE", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)

	request = genYAMLRequest("DELETE", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusNotFound, response.Code)
}

func TestTypeAPIList(t *testing.T) {
	res, err := bmanager.NewV2(bmanager.NewResourceConfig(), types.NoopMgr(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(res),
		manager.OptSetAPITimeout(time.Millisecond*100),
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
	res, err := bmanager.NewV2(bmanager.NewResourceConfig(), types.NoopMgr(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(res),
		manager.OptSetAPITimeout(time.Millisecond*100),
	)

	r := router(mgr)

	require.NoError(t, mgr.Create("foo", harmlessConf()))
	require.NoError(t, mgr.Create("bar", harmlessConf()))

	request := genRequest("GET", "/streams", nil)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)

	info := parseListBody(response.Body)
	assert.True(t, info["foo"].Active)
	assert.True(t, info["bar"].Active)

	barConf := harmlessConf()
	barConf.Input.HTTPServer.Path = "BAR_ONE"
	bar2Conf := harmlessConf()
	bar2Conf.Input.HTTPServer.Path = "BAR_TWO"
	bazConf := harmlessConf()
	bazConf.Input.HTTPServer.Path = "BAZ_ONE"

	streamsBody := map[string]interface{}{}
	streamsBody["bar"], _ = barConf.Sanitised()
	streamsBody["bar2"], _ = bar2Conf.Sanitised()
	streamsBody["baz"], _ = bazConf.Sanitised()

	request = genRequest("POST", "/streams", streamsBody)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	request = genRequest("GET", "/streams", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	info = parseListBody(response.Body)
	assert.NotContains(t, info, "foo")
	assert.Contains(t, info, "bar")
	assert.Contains(t, info, "baz")

	request = genRequest("GET", "/streams/bar", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	conf := parseGetBody(t, response.Body)
	assert.Equal(t, "BAR_ONE", gabs.Wrap(conf.Config).S("input", "http_server", "path").Data())

	request = genRequest("GET", "/streams/bar2", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	conf = parseGetBody(t, response.Body)
	assert.Equal(t, "BAR_TWO", gabs.Wrap(conf.Config).S("input", "http_server", "path").Data())

	request = genRequest("GET", "/streams/baz", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	conf = parseGetBody(t, response.Body)
	assert.Equal(t, "BAZ_ONE", gabs.Wrap(conf.Config).S("input", "http_server", "path").Data())
}

func TestTypeAPIStreamsDefaultConf(t *testing.T) {
	res, err := bmanager.NewV2(bmanager.NewResourceConfig(), types.NoopMgr(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(res),
		manager.OptSetAPITimeout(time.Millisecond*100),
	)

	r := router(mgr)

	body := []byte(`{
	"foo": {
		"input": {
			"nanomsg": {}
		},
		"output": {
			"nanomsg": {}
		}
	}
}`)

	request, err := http.NewRequest("POST", "/streams", bytes.NewReader(body))
	require.NoError(t, err)

	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)

	status, err := mgr.Read("foo")
	require.NoError(t, err)

	assert.Equal(t, status.Config().Input.Nanomsg.PollTimeout, "5s")
}

func TestTypeAPIStreamsLinting(t *testing.T) {
	res, err := bmanager.NewV2(bmanager.NewResourceConfig(), types.NoopMgr(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(res),
		manager.OptSetAPITimeout(time.Millisecond*100),
	)

	r := router(mgr)

	body := []byte(`{
	"foo": {
		"input": {
			"nanomsg": {}
		},
		"output": {
			"type":"nanomsg",
			"file": {}
		}
	},
	"bar": {
		"input": {
			"type":"nanomsg",
			"file": {}
		},
		"output": {
			"nanomsg": {}
		}
	}
}`)

	request, err := http.NewRequest("POST", "/streams", bytes.NewReader(body))
	require.NoError(t, err)

	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusBadRequest, response.Code)

	expLints := `{"lint_errors":["stream 'bar': line 14: field file is invalid when the component type is nanomsg (input)","stream 'foo': line 8: field file is invalid when the component type is nanomsg (output)"]}`
	assert.Equal(t, expLints, response.Body.String())

	request, err = http.NewRequest("POST", "/streams?chilled=true", bytes.NewReader(body))
	require.NoError(t, err)

	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)
}

func TestTypeAPIDefaultConf(t *testing.T) {
	res, err := bmanager.NewV2(bmanager.NewResourceConfig(), types.NoopMgr(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(res),
		manager.OptSetAPITimeout(time.Millisecond*100),
	)

	r := router(mgr)

	body := []byte(`{
	"input": {
		"nanomsg": {}
	},
	"output": {
		"nanomsg": {}
	}
}`)

	request, err := http.NewRequest("POST", "/streams/foo", bytes.NewReader(body))
	require.NoError(t, err)

	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)

	status, err := mgr.Read("foo")
	require.NoError(t, err)

	assert.Equal(t, status.Config().Input.Nanomsg.PollTimeout, "5s")
}

func TestTypeAPILinting(t *testing.T) {
	res, err := bmanager.NewV2(bmanager.NewResourceConfig(), types.NoopMgr(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(res),
		manager.OptSetAPITimeout(time.Millisecond*100),
	)

	r := router(mgr)

	body := []byte(`{
	"input": {
		"type":"nanomsg",
		"file": {}
	},
	"output": {
		"nanomsg": {}
	},
	"cache_resources": [
		{"label":"not_interested","memory":{}}
	]
}`)

	request, err := http.NewRequest("POST", "/streams/foo", bytes.NewReader(body))
	require.NoError(t, err)

	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusBadRequest, response.Code)

	expLints := `{"lint_errors":["line 4: field file is invalid when the component type is nanomsg (input)","line 9: field cache_resources not recognised"]}`
	assert.Equal(t, expLints, response.Body.String())

	request, err = http.NewRequest("POST", "/streams/foo?chilled=true", bytes.NewReader(body))
	require.NoError(t, err)

	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)
}

func TestResourceAPILinting(t *testing.T) {
	tests := []struct {
		name   string
		ctype  string
		config string
		lints  []string
	}{
		{
			name:  "cache bad",
			ctype: "cache",
			config: `memory:
  default_ttl: 123s
  nope: nah
  compaction_interval: 1s`,
			lints: []string{
				"line 3: field nope not recognised",
			},
		},
		{
			name:  "input bad",
			ctype: "input",
			config: `http_server:
  path: /foo/bar
  nope: nah`,
			lints: []string{
				"line 3: field nope not recognised",
			},
		},
		{
			name:  "output bad",
			ctype: "output",
			config: `http_server:
  path: /foo/bar
  nope: nah`,
			lints: []string{
				"line 3: field nope not recognised",
			},
		},
		{
			name:  "processor bad",
			ctype: "processor",
			config: `split:
  size: 10
  nope: nah`,
			lints: []string{
				"line 3: field nope not recognised",
			},
		},
		{
			name:  "rate limit bad",
			ctype: "rate_limit",
			config: `local:
  count: 10
  nope: nah`,
			lints: []string{
				"line 3: field nope not recognised",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bmgr, err := bmanager.NewV2(bmanager.NewResourceConfig(), types.NoopMgr(), log.Noop(), metrics.Noop())
			require.NoError(t, err)

			mgr := manager.New(
				manager.OptSetLogger(log.Noop()),
				manager.OptSetStats(metrics.Noop()),
				manager.OptSetManager(bmgr),
				manager.OptSetAPITimeout(time.Millisecond*100),
			)

			r := router(mgr)

			url := fmt.Sprintf("/resources/%v/foo", test.ctype)
			body := []byte(test.config)

			request, err := http.NewRequest("POST", url, bytes.NewReader(body))
			require.NoError(t, err)

			response := httptest.NewRecorder()
			r.ServeHTTP(response, request)
			assert.Equal(t, http.StatusBadRequest, response.Code)

			expLints, err := json.Marshal(struct {
				LintErrors []string `json:"lint_errors"`
			}{
				LintErrors: test.lints,
			})
			require.NoError(t, err)

			assert.Equal(t, string(expLints), response.Body.String())

			request, err = http.NewRequest("POST", url+"?chilled=true", bytes.NewReader(body))
			require.NoError(t, err)

			response = httptest.NewRecorder()
			r.ServeHTTP(response, request)
			assert.Equal(t, http.StatusOK, response.Code, response.Body.String())
		})
	}
}

func TestTypeAPIGetStats(t *testing.T) {
	mgr, err := bmanager.NewV2(bmanager.NewResourceConfig(), types.NoopMgr(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	smgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(mgr),
		manager.OptSetAPITimeout(time.Millisecond*100),
	)

	r := router(smgr)

	err = smgr.Create("foo", harmlessConf())
	require.NoError(t, err)

	<-time.After(time.Millisecond * 100)

	request := genRequest("GET", "/streams/not_exist/stats", nil)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusNotFound, response.Code)

	request = genRequest("POST", "/streams/foo/stats", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusBadRequest, response.Code)

	request = genRequest("GET", "/streams/foo/stats", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)

	stats, err := gabs.ParseJSON(response.Body.Bytes())
	require.NoError(t, err)

	assert.Equal(t, 1.0, stats.S("input", "running").Data(), response.Body.String())
}

func TestTypeAPISetResources(t *testing.T) {
	bmgr, err := bmanager.NewV2(bmanager.NewResourceConfig(), types.NoopMgr(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	tChan := make(chan types.Transaction)
	bmgr.SetPipe("feed_in", tChan)

	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(bmgr),
		manager.OptSetAPITimeout(time.Millisecond*100),
	)

	tmpDir := t.TempDir()

	dir1 := filepath.Join(tmpDir, "dir1")
	require.NoError(t, os.MkdirAll(dir1, 0o750))

	dir2 := filepath.Join(tmpDir, "dir2")
	require.NoError(t, os.MkdirAll(dir2, 0o750))

	r := router(mgr)

	request := genYAMLRequest("POST", "/resources/cache/foocache?chilled=true", fmt.Sprintf(`
file:
  directory: %v
`, dir1))
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	streamConf := stream.NewConfig()
	streamConf.Input.Type = input.TypeInproc
	streamConf.Input.Inproc = "feed_in"
	streamConf.Output.Type = output.TypeCache
	streamConf.Output.Cache.Key = `${! json("id") }`
	streamConf.Output.Cache.Target = "foocache"

	request = genYAMLRequest("POST", "/streams/foo?chilled=true", streamConf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	resChan := make(chan types.Response)
	select {
	case tChan <- types.NewTransaction(message.QuickBatch([][]byte{[]byte(`{"id":"first","content":"hello world"}`)}), resChan):
	case <-time.After(time.Second * 5):
		t.Fatal("timed out")
	}
	select {
	case <-resChan:
	case <-time.After(time.Second * 5):
		t.Fatal("timed out")
	}

	request = genYAMLRequest("POST", "/resources/cache/foocache?chilled=true", fmt.Sprintf(`
file:
  directory: %v
`, dir2))
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	select {
	case tChan <- types.NewTransaction(message.QuickBatch([][]byte{[]byte(`{"id":"second","content":"hello world 2"}`)}), resChan):
	case <-time.After(time.Second * 5):
		t.Fatal("timed out")
	}
	select {
	case <-resChan:
	case <-time.After(time.Second * 5):
		t.Fatal("timed out")
	}

	files, err := os.ReadDir(dir1)
	require.NoError(t, err)
	assert.Len(t, files, 1)

	file1Bytes, err := os.ReadFile(filepath.Join(dir1, "first"))
	require.NoError(t, err)
	assert.Equal(t, `{"id":"first","content":"hello world"}`, string(file1Bytes))

	files, err = os.ReadDir(dir2)
	require.NoError(t, err)
	assert.Len(t, files, 1)

	file2Bytes, err := os.ReadFile(filepath.Join(dir2, "second"))
	require.NoError(t, err)
	assert.Equal(t, `{"id":"second","content":"hello world 2"}`, string(file2Bytes))
}
