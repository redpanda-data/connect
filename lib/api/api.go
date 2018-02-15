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

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	"github.com/gorilla/mux"
	yaml "gopkg.in/yaml.v2"
)

//------------------------------------------------------------------------------

// Config contains the configuration fields for the Benthos API.
type Config struct {
	Address       string `json:"address" yaml:"address"`
	ReadTimeoutMS int    `json:"read_timeout_ms" yaml:"read_timeout_ms"`
}

// NewConfig creates a new API config with default values.
func NewConfig() Config {
	return Config{
		Address:       "0.0.0.0:4195",
		ReadTimeoutMS: 5000,
	}
}

//------------------------------------------------------------------------------

// Type implements the Benthos HTTP API.
type Type struct {
	conf         Config
	endpoints    map[string]string
	endpointsMut sync.Mutex

	mux    *mux.Router
	server *http.Server
}

// New creates a new Benthos HTTP API.
func New(
	version string,
	dateBuilt string,
	conf Config,
	wholeConf interface{},
	log log.Modular,
	stats metrics.Type,
) *Type {
	handler := mux.NewRouter()
	server := &http.Server{
		Addr:        conf.Address,
		Handler:     handler,
		ReadTimeout: time.Millisecond * time.Duration(conf.ReadTimeoutMS),
	}

	t := &Type{
		conf:      conf,
		endpoints: map[string]string{},
		mux:       handler,
		server:    server,
	}

	handlePing := func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pong"))
	}

	handleStackTrace := func(w http.ResponseWriter, r *http.Request) {
		stackSlice := make([]byte, 1024*100)
		s := runtime.Stack(stackSlice, true)
		w.Write(stackSlice[:s])
	}

	handlePrintJSONConfig := func(w http.ResponseWriter, r *http.Request) {
		resBytes, err := json.Marshal(wholeConf)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.Write(resBytes)
	}

	handlePrintYAMLConfig := func(w http.ResponseWriter, r *http.Request) {
		resBytes, err := yaml.Marshal(wholeConf)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.Write(resBytes)
	}

	handleVersion := func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprintf("{\"version\":\"%v\", \"built\":\"%v\"}", version, dateBuilt)))
	}

	handleEndpoints := func(w http.ResponseWriter, r *http.Request) {
		t.endpointsMut.Lock()
		defer t.endpointsMut.Unlock()

		resBytes, err := json.Marshal(t.endpoints)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
		} else {
			w.Write(resBytes)
		}
	}

	t.RegisterEndpoint(
		"/config/json", "Returns the loaded config as JSON.",
		handlePrintJSONConfig,
	)
	t.RegisterEndpoint(
		"/config/yaml", "Returns the loaded config as YAML.",
		handlePrintYAMLConfig,
	)
	t.RegisterEndpoint(
		"/stack", "Returns a snapshot of the current Benthos stack trace.",
		handleStackTrace,
	)
	t.RegisterEndpoint("/ping", "Ping Benthos.", handlePing)
	t.RegisterEndpoint("/version", "Returns the Benthos version.", handleVersion)
	t.RegisterEndpoint("/endpoints", "Returns this map of endpoints.", handleEndpoints)

	// If we want to expose a JSON stats endpoint we register the endpoints.
	if wHandlerFunc, ok := stats.(metrics.WithHandlerFunc); ok {
		t.RegisterEndpoint(
			"/stats", "Returns a JSON object of Benthos metrics.",
			wHandlerFunc.HandlerFunc(),
		)
		t.RegisterEndpoint(
			"/metrics", "Returns a JSON object of Benthos metrics.",
			wHandlerFunc.HandlerFunc(),
		)
	}

	return t
}

// RegisterEndpoint registers a http.HandlerFunc under a path with a
// description that will be displayed under the /endpoints path.
func (t *Type) RegisterEndpoint(path, desc string, handler http.HandlerFunc) {
	t.endpointsMut.Lock()
	defer t.endpointsMut.Unlock()

	t.endpoints[path] = desc

	t.mux.HandleFunc(path, handler)
	t.mux.HandleFunc("/benthos"+path, handler)
}

// ListenAndServe launches the API and blocks until the server closes or fails.
func (t *Type) ListenAndServe() error {
	return t.server.ListenAndServe()
}

// Shutdown attempts to close the http server.
func (t *Type) Shutdown(ctx context.Context) error {
	return t.server.Shutdown(ctx)
}

//------------------------------------------------------------------------------
