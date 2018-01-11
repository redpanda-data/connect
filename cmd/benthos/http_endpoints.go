package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"

	"github.com/Jeffail/benthos/lib/util/service"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	yaml "gopkg.in/yaml.v2"
)

//------------------------------------------------------------------------------

var endpoints []string

func registerEndpoint(path string, handler http.HandlerFunc) {
	endpoints = append(endpoints, path)

	http.HandleFunc(path, handler)
	http.HandleFunc("/benthos"+path, handler)
}

func handleVersion(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(fmt.Sprintf("{\"version\":\"%v\", \"built\":\"%v\"}", service.Version, service.DateBuilt)))
}

func handleEndpoints(w http.ResponseWriter, r *http.Request) {
	res := struct {
		Endpoints []string `json:"endpoints"`
	}{
		Endpoints: endpoints,
	}
	resBytes, err := json.Marshal(res)
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

	registerEndpoint("/config/json", handlePrintJSONConfig)
	registerEndpoint("/config/yaml", handlePrintYAMLConfig)
	registerEndpoint("/stack", handleStackTrace)
	registerEndpoint("/ping", handlePing)
	registerEndpoint("/version", handleVersion)
	registerEndpoint("/endpoints", handleEndpoints)

	// If we want to expose a JSON stats endpoint we register the endpoints.
	if conf.Metrics.Type == "http_server" {
		if httpStats, ok := stats.(*metrics.HTTP); ok {
			registerEndpoint("/stats", httpStats.JSONHandler())
		}
	}
}

//------------------------------------------------------------------------------
