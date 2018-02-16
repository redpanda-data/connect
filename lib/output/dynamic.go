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

package output

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"path"
	"sync"
	"time"

	"github.com/Jeffail/benthos/lib/broker"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	"github.com/gorilla/mux"
)

//------------------------------------------------------------------------------

func init() {
	constructors["dynamic"] = typeSpec{
		constructor: NewDynamic,
		description: `
The dynamic type is similar to the 'fan_out' type except the outputs can be
changed during runtime via a REST HTTP interface.

To GET a JSON map of output identifiers with their current uptimes use the
'/outputs' endpoint.

To perform CRUD actions on the outputs themselves use POST, DELETE, and GET
methods on the '/output/{output_id}' endpoint. When using POST the body of the
request should be a JSON configuration for the output, if the output already
exists it will be changed.`,
	}
}

//------------------------------------------------------------------------------

// DynamicConfig is configuration for the Dynamic input type.
type DynamicConfig struct {
	Outputs   map[string]Config `json:"outputs" yaml:"outputs"`
	Prefix    string            `json:"prefix" yaml:"prefix"`
	TimeoutMS int               `json:"timeout_ms" yaml:"timeout_ms"`
}

// NewDynamicConfig creates a new DynamicConfig with default values.
func NewDynamicConfig() DynamicConfig {
	return DynamicConfig{
		Outputs:   map[string]Config{},
		Prefix:    "",
		TimeoutMS: 5000,
	}
}

//------------------------------------------------------------------------------

// NewDynamic creates a new Dynamic output type.
func NewDynamic(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	outputMap := map[string]time.Time{}
	outputMapMut := sync.Mutex{}

	outputs := map[string]broker.DynamicOutput{}
	for k, v := range conf.Dynamic.Outputs {
		newOutput, err := New(v, mgr, log, stats)
		if err != nil {
			return nil, err
		}
		outputs[k] = newOutput
	}

	fanOut, err := broker.NewDynamicFanOut(
		outputs, log, stats,
		broker.OptDynamicFanOutSetOnAdd(func(l string) {
			outputMapMut.Lock()
			outputMap[l] = time.Now()
			outputMapMut.Unlock()
		}),
		broker.OptDynamicFanOutSetOnRemove(func(l string) {
			outputMapMut.Lock()
			delete(outputMap, l)
			outputMapMut.Unlock()
		}),
	)
	if err != nil {
		return nil, err
	}
	outputs = nil

	reqTimeout := time.Millisecond * time.Duration(conf.Dynamic.TimeoutMS)

	outputConfigs := conf.Dynamic.Outputs
	outputConfigsMut := sync.RWMutex{}

	mgr.RegisterEndpoint(
		path.Join(conf.Dynamic.Prefix, "/output/{output_id}"),
		"Perform CRUD operations on the configuration of dynamic outputs. For"+
			" more information read the `dynamic` output type documentation.",
		func(w http.ResponseWriter, r *http.Request) {
			var httpErr error
			defer func() {
				r.Body.Close()
				if httpErr != nil {
					log.Warnf("Request error: %v\n", httpErr)
					http.Error(w, "Internal server error", http.StatusBadGateway)
				}
			}()

			outputConfigsMut.Lock()
			defer outputConfigsMut.Unlock()

			outputID := mux.Vars(r)["output_id"]
			if len(outputID) == 0 {
				http.Error(w, "Var `output_id` must be set", http.StatusBadRequest)
				return
			}

			switch r.Method {
			case "POST":
				newConf := NewConfig()
				var reqBytes []byte
				if reqBytes, httpErr = ioutil.ReadAll(r.Body); httpErr != nil {
					return
				}
				if httpErr = json.Unmarshal(reqBytes, &newConf); httpErr != nil {
					return
				}
				var newOutput Type
				if newOutput, httpErr = New(newConf, mgr, log, stats); httpErr != nil {
					return
				}
				if httpErr = fanOut.SetOutput(outputID, newOutput, reqTimeout); httpErr == nil {
					outputConfigs[outputID] = newConf
				}
			case "GET":
				getConf, exists := outputConfigs[outputID]
				if !exists {
					http.Error(w, "Output does not exist", http.StatusBadRequest)
					return
				}
				var cBytes []byte
				cBytes, httpErr = json.Marshal(getConf)
				if httpErr != nil {
					return
				}

				hashMap := map[string]interface{}{}
				if httpErr = json.Unmarshal(cBytes, &hashMap); httpErr != nil {
					return
				}

				outputMap := map[string]interface{}{}
				outputMap["type"] = hashMap["type"]
				outputMap[getConf.Type] = hashMap[getConf.Type]
				outputMap["processors"] = hashMap["processors"]

				cBytes, httpErr = json.Marshal(outputMap)
				if httpErr != nil {
					return
				}
				w.Write(cBytes)
			case "DELETE":
				if _, exists := outputConfigs[outputID]; !exists {
					http.Error(w, "Output does not exist", http.StatusBadRequest)
					return
				}
				if httpErr = fanOut.SetOutput(outputID, nil, reqTimeout); httpErr == nil {
					delete(outputConfigs, outputID)
				}
			}
		},
	)
	mgr.RegisterEndpoint(
		path.Join(conf.Dynamic.Prefix, "/outputs"),
		"Get a map of running output identifiers with their current uptimes.",
		func(w http.ResponseWriter, r *http.Request) {
			var httpErr error
			defer func() {
				r.Body.Close()
				if httpErr != nil {
					log.Warnf("Request error: %v\n", httpErr)
					http.Error(w, "Internal server error", http.StatusBadGateway)
				}
			}()

			outputMapMut.Lock()
			defer outputMapMut.Unlock()

			uptimes := map[string]string{}
			for k, v := range outputMap {
				uptimes[k] = time.Since(v).String()
			}

			var resBytes []byte
			if resBytes, httpErr = json.Marshal(uptimes); httpErr == nil {
				w.Write(resBytes)
			}
		},
	)
	return fanOut, nil
}

//------------------------------------------------------------------------------
