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

package input

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/Jeffail/benthos/lib/broker"
	"github.com/Jeffail/benthos/lib/pipeline"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	"github.com/gorilla/mux"
)

//------------------------------------------------------------------------------

func init() {
	constructors["dynamic_fan_in"] = typeSpec{
		brokerConstructor: NewDynamicFanIn,
		description: `
The dynamic fan in type is similar to the regular fan in type except the inputs
can be changed during runtime via a REST HTTP interface.

To GET the full list of input identifiers use the '/inputs' endpoint.

To perform CRUD actions on the inputs themselves use POST, DELETE, and GET
methods on the '/input/{input_id}' endpoint. When using POST the body of the
request should be a JSON configuration for the input, if the input already
exists it will be changed.`,
	}
}

//------------------------------------------------------------------------------

// DynamicFanInConfig is configuration for the DynamicFanIn input type.
type DynamicFanInConfig struct {
	Inputs    map[string]Config `json:"inputs" yaml:"inputs"`
	Prefix    string            `json:"prefix" yaml:"prefix"`
	TimeoutMS int               `json:"timeout_ms" yaml:"timeout_ms"`
}

// NewDynamicFanInConfig creates a new DynamicFanInConfig with default values.
func NewDynamicFanInConfig() DynamicFanInConfig {
	return DynamicFanInConfig{
		Inputs:    map[string]Config{},
		Prefix:    "",
		TimeoutMS: 5000,
	}
}

//------------------------------------------------------------------------------

// NewDynamicFanIn creates a new DynamicFanIn input type.
func NewDynamicFanIn(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...pipeline.ConstructorFunc,
) (Type, error) {
	inputs := map[string]broker.DynamicInput{}
	for k, v := range conf.DynamicFanIn.Inputs {
		newInput, err := New(v, mgr, log, stats, pipelines...)
		if err != nil {
			return nil, err
		}
		inputs[k] = newInput
	}

	fanIn, err := broker.NewDynamicFanIn(inputs, log, stats)
	if err != nil {
		return nil, err
	}

	reqTimeout := time.Millisecond * time.Duration(conf.DynamicFanIn.TimeoutMS)

	inputConfigs := conf.DynamicFanIn.Inputs
	inputConfigsMut := sync.RWMutex{}
	mgr.RegisterEndpoint(
		path.Join(conf.DynamicFanIn.Prefix, "/input/{input_id}"),
		"Perform CRUD operations on the configuration of dynamic inputs. For"+
			" more information read the `dynamic_fan_in` documentation.",
		func(w http.ResponseWriter, r *http.Request) {
			var httpErr error
			defer func() {
				r.Body.Close()
				if httpErr != nil {
					log.Warnf("Request error: %v\n", httpErr)
					http.Error(w, "Internal server error", http.StatusBadGateway)
				}
			}()

			inputConfigsMut.Lock()
			defer inputConfigsMut.Unlock()

			inputID := mux.Vars(r)["input_id"]
			if len(inputID) == 0 {
				http.Error(w, "Var `input_id` must be set", http.StatusBadRequest)
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
				var newInput Type
				if newInput, httpErr = New(newConf, mgr, log, stats, pipelines...); httpErr != nil {
					return
				}
				if httpErr = fanIn.SetInput(inputID, newInput, reqTimeout); httpErr == nil {
					inputConfigs[inputID] = newConf
				}
			case "GET":
				if _, exists := inputConfigs[inputID]; !exists {
					http.Error(w, "Input does not exist", http.StatusBadRequest)
					return
				}
				var cBytes []byte
				cBytes, httpErr = json.Marshal(inputConfigs[inputID])
				if httpErr != nil {
					return
				}
				w.Write(cBytes)
			case "DELETE":
				if _, exists := inputConfigs[inputID]; !exists {
					http.Error(w, "Input does not exist", http.StatusBadRequest)
					return
				}
				if httpErr = fanIn.SetInput(inputID, nil, reqTimeout); httpErr == nil {
					delete(inputConfigs, inputID)
				}
			}
		},
	)
	mgr.RegisterEndpoint(
		path.Join(conf.DynamicFanIn.Prefix, "/inputs"),
		"Get a full list of all input identifiers.",
		func(w http.ResponseWriter, r *http.Request) {
			var httpErr error
			defer func() {
				r.Body.Close()
				if httpErr != nil {
					log.Warnf("Request error: %v\n", httpErr)
					http.Error(w, "Internal server error", http.StatusBadGateway)
				}
			}()

			inputConfigsMut.Lock()
			defer inputConfigsMut.Unlock()

			labels := []string{}
			for k := range inputConfigs {
				labels = append(labels, k)
			}
			sort.Strings(labels)

			var resBytes []byte
			if resBytes, httpErr = json.Marshal(labels); httpErr == nil {
				w.Write(resBytes)
			}
		},
	)
	return fanIn, nil
}

//------------------------------------------------------------------------------
