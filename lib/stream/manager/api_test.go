package manager_test

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

func parseGetBody(t *testing.T, data *bytes.Buffer) getBody {
	t.Helper()
	result := getBody{
		Config: stream.NewConfig(),
	}
	if err := json.Unmarshal(data.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestTypeAPIBadMethods(t *testing.T) {
	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(types.DudMgr{}),
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
	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(types.NoopMgr()),
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

	actSanit, err := info.Config.Sanitised()
	require.NoError(t, err)
	assert.Equal(t, conf, actSanit)

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

	actSanit, err = info.Config.Sanitised()
	require.NoError(t, err)
	assert.Equal(t, newConfSanit, actSanit)

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

	request = genRequest("POST", "/streams/fooEnv", newConf)
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
	assert.True(t, info.Active)
	assert.Equal(t, newConf, info.Config)

	request = genRequest("DELETE", "/streams/fooEnv", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())
}

func TestTypeAPIPatch(t *testing.T) {
	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(types.DudMgr{}),
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
	info := parseGetBody(t, response.Body)
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
	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(types.NoopMgr()),
		manager.OptSetAPITimeout(time.Second*10),
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
	info := parseGetBody(t, response.Body)
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
	info = parseGetBody(t, response.Body)
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
	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(types.DudMgr{}),
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
	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(types.DudMgr{}),
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
	streamsBody := map[string]stream.Config{
		"bar":  barConf,
		"bar2": bar2Conf,
		"baz":  bazConf,
	}

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
	assert.Equal(t, "BAR_ONE", conf.Config.Input.HTTPServer.Path)

	request = genRequest("GET", "/streams/bar2", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	conf = parseGetBody(t, response.Body)
	assert.Equal(t, "BAR_TWO", conf.Config.Input.HTTPServer.Path)

	request = genRequest("GET", "/streams/baz", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	conf = parseGetBody(t, response.Body)
	assert.Equal(t, "BAZ_ONE", conf.Config.Input.HTTPServer.Path)
}

func TestTypeAPIDefaultConf(t *testing.T) {
	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(types.DudMgr{}),
		manager.OptSetAPITimeout(time.Millisecond*100),
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
	mgr := manager.New(
		manager.OptSetLogger(log.Noop()),
		manager.OptSetStats(metrics.Noop()),
		manager.OptSetManager(types.DudMgr{}),
		manager.OptSetAPITimeout(time.Millisecond*100),
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
