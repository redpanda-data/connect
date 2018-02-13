package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync"

	"github.com/Jeffail/benthos/lib/util/service"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	yaml "gopkg.in/yaml.v2"
)

//------------------------------------------------------------------------------

var endpoints = map[string]string{}
var endpointsMut sync.Mutex

// RegisterEndpoint will register a global endpoint and keep the path and
// description cached for returning on `/endpoints` calls.
func RegisterEndpoint(path, description string, handler http.HandlerFunc) {
	endpointsMut.Lock()
	defer endpointsMut.Unlock()

	endpoints[path] = description

	http.HandleFunc(path, handler)
	http.HandleFunc("/benthos"+path, handler)
}

//------------------------------------------------------------------------------

// Implements types.Manager.
type httpManager struct {
}

func (h httpManager) RegisterEndpoint(path, desc string, handler http.HandlerFunc) {
	RegisterEndpoint(path, desc, handler)
}

//------------------------------------------------------------------------------

func handleVersion(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(fmt.Sprintf("{\"version\":\"%v\", \"built\":\"%v\"}", service.Version, service.DateBuilt)))
}

func handleEndpoints(w http.ResponseWriter, r *http.Request) {
	endpointsMut.Lock()
	defer endpointsMut.Unlock()

	resBytes, err := json.Marshal(endpoints)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
	} else {
		w.Write(resBytes)
	}
}

func registerHTTPEndpoints(
	conf Config,
	log log.Modular,
	stats metrics.Type,
) {
	handlePing := func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pong"))
	}

	handleStackTrace := func(w http.ResponseWriter, r *http.Request) {
		stackSlice := make([]byte, 1024*100)
		s := runtime.Stack(stackSlice, true)
		w.Write(stackSlice[:s])
	}

	handlePrintJSONConfig := func(w http.ResponseWriter, r *http.Request) {
		resBytes, err := json.Marshal(conf)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.Write(resBytes)
	}

	handlePrintYAMLConfig := func(w http.ResponseWriter, r *http.Request) {
		resBytes, err := yaml.Marshal(conf)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.Write(resBytes)
	}

	RegisterEndpoint(
		"/config/json", "Returns the loaded config as JSON.",
		handlePrintJSONConfig,
	)
	RegisterEndpoint(
		"/config/yaml", "Returns the loaded config as YAML.",
		handlePrintYAMLConfig,
	)
	RegisterEndpoint(
		"/stack", "Returns a snapshot of the current Benthos stack trace.",
		handleStackTrace,
	)
	RegisterEndpoint("/ping", "Ping Benthos.", handlePing)
	RegisterEndpoint("/version", "Returns the Benthos version.", handleVersion)
	RegisterEndpoint("/endpoints", "Returns this map of endpoints.", handleEndpoints)

	// If we want to expose a JSON stats endpoint we register the endpoints.
	if wHandlerFunc, ok := stats.(metrics.WithHandlerFunc); ok {
		RegisterEndpoint(
			"/stats", "Returns a JSON object of Benthos metrics.",
			wHandlerFunc.HandlerFunc(),
		)
		RegisterEndpoint(
			"/metrics", "Returns a JSON object of Benthos metrics.",
			wHandlerFunc.HandlerFunc(),
		)
	}
}

//------------------------------------------------------------------------------
