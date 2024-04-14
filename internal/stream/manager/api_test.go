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
	"strings"
	"testing"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/docs"
	bmanager "github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/stream/manager"

	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func router(m *manager.Type) *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/ready", m.HandleStreamReady)
	router.HandleFunc("/streams", m.HandleStreamsCRUD)
	router.HandleFunc("/streams/{id}", m.HandleStreamCRUD)
	router.HandleFunc("/streams/{id}/stats", m.HandleStreamStats)
	router.HandleFunc("/resources/{type}/{id}", m.HandleResourceCRUD)
	return router
}

func genRequest(verb, url string, payload any) *http.Request {
	var body io.Reader

	if payload != nil {
		if s, ok := payload.(string); ok {
			body = strings.NewReader(s)
		} else {
			bodyBytes, err := json.Marshal(payload)
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

func genYAMLRequest(verb, url string, payload any) *http.Request {
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
	Active    bool    `json:"active"`
	Uptime    float64 `json:"uptime"`
	UptimeStr string  `json:"uptime_str"`
	Config    any     `json:"config"`
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
	rMgr, err := bmanager.New(bmanager.NewResourceConfig(), bmanager.OptSetAPIReg(r))
	require.NoError(t, err)

	_ = manager.New(rMgr,
		manager.OptAPIEnabled(true),
	)
	assert.Greater(t, len(r.endpoints), 1)

	r = &endpointReg{endpoints: map[string]http.HandlerFunc{}}
	rMgr, err = bmanager.New(bmanager.NewResourceConfig(), bmanager.OptSetAPIReg(r))
	require.NoError(t, err)

	_ = manager.New(rMgr,
		manager.OptAPIEnabled(false),
	)
	assert.Len(t, r.endpoints, 1)
	assert.Contains(t, r.endpoints, "/ready")
}

func TestTypeAPIBadMethods(t *testing.T) {
	mgr := manager.New(mock.NewManager())

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

func harmlessConf() any {
	return map[string]any{
		"input": map[string]any{
			"generate": map[string]any{
				"mapping": "root = deleted()",
			},
		},
		"output": map[string]any{
			"drop": map[string]any{},
		},
	}
}

func TestTypeAPIBasicOperations(t *testing.T) {
	res, err := bmanager.New(bmanager.NewResourceConfig())
	require.NoError(t, err)

	mgr := manager.New(res)

	r := router(mgr)
	conf := harmlessConf()

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

	assert.Eventually(t, func() bool {
		request = genRequest("GET", "/ready", nil)
		response = httptest.NewRecorder()
		r.ServeHTTP(response, request)
		return response.Code == http.StatusOK
	}, time.Second*10, time.Millisecond*50)

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

	assert.Equal(t, "root = deleted()", gabs.Wrap(info.Config).S("input", "generate", "mapping").Data())

	newConf := harmlessConf()
	_, _ = gabs.Wrap(newConf).Set("memory", "buffer", "type")

	request = genRequest("PUT", "/streams/foo", newConf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	request = genRequest("GET", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	info = parseGetBody(t, response.Body)
	assert.True(t, info.Active)

	assert.Equal(t, "memory", gabs.Wrap(info.Config).S("buffer", "type").Data())

	request = genRequest("DELETE", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	request = genRequest("DELETE", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusNotFound, response.Code, response.Body.String())

	testVar := "__TEST_INPUT_MAPPING"

	t.Setenv(testVar, `root.meow = 5`)

	request = genRequest("POST", "/streams/fooEnv?chilled=true", map[string]any{
		"input": map[string]any{
			"generate": map[string]any{
				"mapping": "${__TEST_INPUT_MAPPING}",
			},
		},
		"output": map[string]any{
			"type": "drop",
		},
	})
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	request = genRequest("GET", "/streams/fooEnv", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	info = parseGetBody(t, response.Body)

	assert.True(t, info.Active)
	assert.Equal(t, `root.meow = 5`, gabs.Wrap(info.Config).S("input", "generate", "mapping").Data())

	request = genRequest("DELETE", "/streams/fooEnv", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())
}

func TestTypeAPIPatch(t *testing.T) {
	res, err := bmanager.New(bmanager.NewResourceConfig())
	require.NoError(t, err)

	mgr := manager.New(res)

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
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	patchConf := map[string]any{
		"input": map[string]any{
			"generate": map[string]any{
				"interval": "2s",
			},
		},
	}
	request = genRequest("PATCH", "/streams/foo", patchConf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v: %v", act, exp, response.Body.String())
	}

	request = genRequest("GET", "/streams/foo", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected result: %v != %v: %v", act, exp, response.Body.String())
	}
	info := parseGetBody(t, response.Body)
	if !info.Active {
		t.Fatal("Stream not active")
	}

	assert.Equal(t, "2s", gabs.Wrap(info.Config).S("input", "generate", "interval").Data())
}

func TestTypeAPIBasicOperationsYAML(t *testing.T) {
	res, err := bmanager.New(bmanager.NewResourceConfig())
	require.NoError(t, err)

	mgr := manager.New(res)

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

	info := parseGetBody(t, response.Body)
	require.True(t, info.Active)
	assert.Equal(t, "root = deleted()", gabs.Wrap(info.Config).S("input", "generate", "mapping").Data())

	newConf := harmlessConf()
	_, _ = gabs.Wrap(newConf).Set("memory", "buffer", "type")

	request = genYAMLRequest("PUT", "/streams/foo?chilled=true", newConf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)

	request = genYAMLRequest("GET", "/streams/foo", conf)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)

	info = parseGetBody(t, response.Body)
	require.True(t, info.Active)
	assert.Equal(t, "memory", gabs.Wrap(info.Config).S("buffer", "type").Data())

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
	res, err := bmanager.New(bmanager.NewResourceConfig())
	require.NoError(t, err)

	mgr := manager.New(res)

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

	conf, err := testutil.StreamFromYAML(`
input:
  generate:
    mapping: 'root = deleted()'
output:
  drop: {}
`)
	require.NoError(t, err)

	if err := mgr.Create("foo", conf); err != nil {
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
	res, err := bmanager.New(bmanager.NewResourceConfig())
	require.NoError(t, err)

	mgr := manager.New(res)

	r := router(mgr)

	origConf, err := testutil.StreamFromYAML(`
input:
  generate:
    mapping: 'root = deleted()'
output:
  drop: {}
`)
	require.NoError(t, err)

	require.NoError(t, mgr.Create("foo", origConf))
	require.NoError(t, mgr.Create("bar", origConf))

	request := genRequest("GET", "/streams", nil)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)

	info := parseListBody(response.Body)
	assert.True(t, info["foo"].Active)
	assert.True(t, info["bar"].Active)

	barConf := harmlessConf()
	_, _ = gabs.Wrap(barConf).Set("root = this.BAR_ONE", "input", "generate", "mapping")
	bar2Conf := harmlessConf()
	_, _ = gabs.Wrap(bar2Conf).Set("root = this.BAR_TWO", "input", "generate", "mapping")
	bazConf := harmlessConf()
	_, _ = gabs.Wrap(bazConf).Set("root = this.BAZ_ONE", "input", "generate", "mapping")

	streamsBody := map[string]any{}
	streamsBody["bar"] = barConf
	streamsBody["bar2"] = bar2Conf
	streamsBody["baz"] = bazConf

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
	assert.Equal(t, "root = this.BAR_ONE", gabs.Wrap(conf.Config).S("input", "generate", "mapping").Data())

	request = genRequest("GET", "/streams/bar2", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	conf = parseGetBody(t, response.Body)
	assert.Equal(t, "root = this.BAR_TWO", gabs.Wrap(conf.Config).S("input", "generate", "mapping").Data())

	request = genRequest("GET", "/streams/baz", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code, response.Body.String())

	conf = parseGetBody(t, response.Body)
	assert.Equal(t, "root = this.BAZ_ONE", gabs.Wrap(conf.Config).S("input", "generate", "mapping").Data())
}

func testConfToAny(t testing.TB, conf any) any {
	var node yaml.Node
	err := node.Encode(conf)
	require.NoError(t, err)

	sanitConf := docs.NewSanitiseConfig(bundle.GlobalEnvironment)
	sanitConf.RemoveTypeField = true
	sanitConf.ScrubSecrets = true
	err = config.Spec().SanitiseYAML(&node, sanitConf)
	require.NoError(t, err)

	var v any
	require.NoError(t, node.Decode(&v))
	return v
}

func TestTypeAPIStreamsDefaultConf(t *testing.T) {
	res, err := bmanager.New(bmanager.NewResourceConfig())
	require.NoError(t, err)

	mgr := manager.New(res)

	r := router(mgr)

	body := []byte(`{
	"foo": {
		"input": {
			"generate": {
				"mapping": "root = deleted()"
			}
		},
		"output": {
			"drop": {}
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

	v := testConfToAny(t, status.Config())

	assert.Nil(t, gabs.Wrap(v).S("input", "generate", "interval").Data())
}

func TestTypeAPIStreamsLinting(t *testing.T) {
	res, err := bmanager.New(bmanager.NewResourceConfig())
	require.NoError(t, err)

	mgr := manager.New(res)

	r := router(mgr)

	body := []byte(`{
	"foo": {
		"input": {
			"generate": {
				"mapping": "root = deleted()"
			}
		},
		"output": {
			"type":"drop",
			"inproc": "meow"
		}
	},
	"bar": {
		"input": {
			"generate": {
				"mapping": "root = deleted()"
			},
			"type": "inproc"
		},
		"output": {
			"drop": {}
		}
	}
}`)

	request, err := http.NewRequest("POST", "/streams", bytes.NewReader(body))
	require.NoError(t, err)

	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusBadRequest, response.Code)
	assert.Equal(t, "application/json", response.Result().Header.Get("Content-Type"))

	expLints := []string{
		"stream 'foo': (10,1) field inproc is invalid when the component type is drop (output)",
		"stream 'bar': (15,1) field generate is invalid when the component type is inproc (input)",
	}
	var actLints struct {
		LintErrors []string `json:"lint_errors"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &actLints))
	assert.ElementsMatch(t, expLints, actLints.LintErrors)

	request, err = http.NewRequest("POST", "/streams?chilled=true", bytes.NewReader(body))
	require.NoError(t, err)

	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)
}

func TestTypeAPIDefaultConf(t *testing.T) {
	res, err := bmanager.New(bmanager.NewResourceConfig())
	require.NoError(t, err)

	mgr := manager.New(res)

	r := router(mgr)

	body := []byte(`{
	"input": {
		"generate": {
			"mapping": "root = deleted()"
		}
	},
	"output": {
		"drop": {}
	}
}`)

	request, err := http.NewRequest("POST", "/streams/foo", bytes.NewReader(body))
	require.NoError(t, err)

	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)

	status, err := mgr.Read("foo")
	require.NoError(t, err)

	v := testConfToAny(t, status.Config())
	assert.Nil(t, gabs.Wrap(v).S("input", "generate", "interval").Data())
}

func TestTypeAPILinting(t *testing.T) {
	res, err := bmanager.New(bmanager.NewResourceConfig())
	require.NoError(t, err)

	mgr := manager.New(res)

	r := router(mgr)

	body := []byte(`{
	"input": {
		"generate": {
			"mapping": "root = deleted()"
		}
	},
	"output": {
		"type":"drop",
		"inproc": "meow"
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
	assert.Equal(t, "application/json", response.Result().Header.Get("Content-Type"))

	expLints := `{"lint_errors":["(9,1) field inproc is invalid when the component type is drop (output)","(11,1) field cache_resources not recognised"]}`
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
				"(3,1) field nope not recognised",
			},
		},
		{
			name:  "input bad",
			ctype: "input",
			config: `generate:
  mapping: root = deleted()
  nope: nah`,
			lints: []string{
				"(3,1) field nope not recognised",
			},
		},
		{
			name:  "output bad",
			ctype: "output",
			config: `retry:
  output:
    drop: {}
  nope: nah`,
			lints: []string{
				"(4,1) field nope not recognised",
			},
		},
		{
			name:  "processor bad",
			ctype: "processor",
			config: `split:
  size: 10
  nope: nah`,
			lints: []string{
				"(3,1) field nope not recognised",
			},
		},
		{
			name:  "rate limit bad",
			ctype: "rate_limit",
			config: `local:
  count: 10
  nope: nah`,
			lints: []string{
				"(3,1) field nope not recognised",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bmgr, err := bmanager.New(bmanager.NewResourceConfig())
			require.NoError(t, err)

			mgr := manager.New(bmgr)

			r := router(mgr)

			url := fmt.Sprintf("/resources/%v/foo", test.ctype)
			body := []byte(test.config)

			request, err := http.NewRequest("POST", url, bytes.NewReader(body))
			require.NoError(t, err)

			response := httptest.NewRecorder()
			r.ServeHTTP(response, request)
			assert.Equal(t, http.StatusBadRequest, response.Code)
			assert.Equal(t, "application/json", response.Result().Header.Get("Content-Type"))

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
	mgr, err := bmanager.New(bmanager.NewResourceConfig())
	require.NoError(t, err)

	smgr := manager.New(mgr)

	r := router(smgr)

	origConf, err := testutil.StreamFromYAML(`
input:
  generate:
    mapping: 'root = deleted()'
output:
  drop: {}
`)
	require.NoError(t, err)

	err = smgr.Create("foo", origConf)
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

	assert.NotEmpty(t, stats.ChildrenMap(), response.Body.String())
}

func TestTypeAPISetResources(t *testing.T) {
	bmgr, err := bmanager.New(bmanager.NewResourceConfig())
	require.NoError(t, err)

	tChan := make(chan message.Transaction)
	bmgr.SetPipe("feed_in", tChan)

	mgr := manager.New(bmgr)

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
	hResponse := httptest.NewRecorder()
	r.ServeHTTP(hResponse, request)
	assert.Equal(t, http.StatusOK, hResponse.Code, hResponse.Body.String())

	streamConf, err := testutil.StreamFromYAML(`
input:
  inproc: feed_in
output:
  cache:
    key: '${! json("id") }'
    target: foocache
`)
	require.NoError(t, err)

	request = genYAMLRequest("POST", "/streams/foo?chilled=true", streamConf)
	hResponse = httptest.NewRecorder()
	r.ServeHTTP(hResponse, request)
	assert.Equal(t, http.StatusOK, hResponse.Code, hResponse.Body.String())

	resChan := make(chan error)
	select {
	case tChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte(`{"id":"first","content":"hello world"}`)}), resChan):
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
	hResponse = httptest.NewRecorder()
	r.ServeHTTP(hResponse, request)
	assert.Equal(t, http.StatusOK, hResponse.Code, hResponse.Body.String())

	select {
	case tChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte(`{"id":"second","content":"hello world 2"}`)}), resChan):
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

func TestAPIReady(t *testing.T) {
	res, err := bmanager.New(bmanager.NewResourceConfig())
	require.NoError(t, err)

	mgr := manager.New(res)

	r := router(mgr)

	request := genRequest("POST", "/streams/foo", `
input:
  generate:
    count: 1
    mapping: 'root = {}'
    interval: ""

output:
  drop: {}
`)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)

	assert.Eventually(t, func() bool {
		request = genRequest("GET", "/ready", nil)
		response = httptest.NewRecorder()
		r.ServeHTTP(response, request)
		return response.Code == http.StatusOK
	}, time.Second*10, time.Millisecond*50)

	request = genRequest("POST", "/streams/bar", `
input:
  generate:
    count: 1
    mapping: 'root = {}'
    interval: ""

output:
  websocket:
    url: not**a**valid**url
`)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusOK, response.Code)

	assert.Eventually(t, func() bool {
		request = genRequest("GET", "/ready", nil)
		response = httptest.NewRecorder()
		r.ServeHTTP(response, request)
		return response.Code == http.StatusServiceUnavailable
	}, time.Second*10, time.Millisecond*50)
}
