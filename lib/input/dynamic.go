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
	"path"
	"sync"
	"time"

	"github.com/Jeffail/benthos/lib/api"
	"github.com/Jeffail/benthos/lib/broker"
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/pipeline"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["dynamic"] = TypeSpec{
		brokerConstructor: NewDynamic,
		description: `
The dynamic type is a special broker type where the inputs are identified by
unique labels and can be created, changed and removed during runtime via a REST
HTTP interface.

To GET a JSON map of input identifiers with their current uptimes use the
'/inputs' endpoint.

To perform CRUD actions on the inputs themselves use POST, DELETE, and GET
methods on the '/input/{input_id}' endpoint. When using POST the body of the
request should be a JSON configuration for the input, if the input already
exists it will be changed.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			nestedInputs := conf.Dynamic.Inputs
			inMap := map[string]interface{}{}
			for k, input := range nestedInputs {
				sanInput, err := SanitiseConfig(input)
				if err != nil {
					return nil, err
				}
				inMap[k] = sanInput
			}
			return map[string]interface{}{
				"inputs":     inMap,
				"prefix":     conf.Dynamic.Prefix,
				"timeout_ms": conf.Dynamic.TimeoutMS,
			}, nil
		},
	}
}

//------------------------------------------------------------------------------

// DynamicConfig is configuration for the Dynamic input type.
type DynamicConfig struct {
	Inputs    map[string]Config `json:"inputs" yaml:"inputs"`
	Prefix    string            `json:"prefix" yaml:"prefix"`
	TimeoutMS int               `json:"timeout_ms" yaml:"timeout_ms"`
}

// NewDynamicConfig creates a new DynamicConfig with default values.
func NewDynamicConfig() DynamicConfig {
	return DynamicConfig{
		Inputs:    map[string]Config{},
		Prefix:    "",
		TimeoutMS: 5000,
	}
}

//------------------------------------------------------------------------------

// NewDynamic creates a new Dynamic input type.
func NewDynamic(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...pipeline.ConstructorFunc,
) (Type, error) {
	dynAPI := api.NewDynamic()

	inputs := map[string]broker.DynamicInput{}
	for k, v := range conf.Dynamic.Inputs {
		newInput, err := New(v, mgr, log, stats, pipelines...)
		if err != nil {
			return nil, err
		}
		inputs[k] = newInput
	}

	reqTimeout := time.Millisecond * time.Duration(conf.Dynamic.TimeoutMS)

	inputConfigs := conf.Dynamic.Inputs
	inputConfigsMut := sync.RWMutex{}

	fanIn, err := broker.NewDynamicFanIn(
		inputs, log, stats,
		broker.OptDynamicFanInSetOnAdd(func(l string) {
			inputConfigsMut.Lock()
			defer inputConfigsMut.Unlock()

			uConf, exists := inputConfigs[l]
			if !exists {
				return
			}
			sConf, bErr := SanitiseConfig(uConf)
			if bErr != nil {
				log.Errorf("Failed to sanitise config: %v\n", bErr)
			}

			confBytes, _ := json.Marshal(sConf)
			dynAPI.Started(l, confBytes)
			delete(inputConfigs, l)
		}),
		broker.OptDynamicFanInSetOnRemove(func(l string) {
			dynAPI.Stopped(l)
		}),
	)
	if err != nil {
		return nil, err
	}

	dynAPI.OnUpdate(func(id string, c []byte) error {
		type confAlias Config
		newConf := confAlias(NewConfig())
		newConf.Processors = nil // Remove default processors
		if err := json.Unmarshal(c, &newConf); err != nil {
			return err
		}
		newInput, err := New(Config(newConf), mgr, log, stats, pipelines...)
		if err != nil {
			return err
		}
		inputConfigsMut.Lock()
		inputConfigs[id] = Config(newConf)
		inputConfigsMut.Unlock()
		if err = fanIn.SetInput(id, newInput, reqTimeout); err != nil {
			inputConfigsMut.Lock()
			delete(inputConfigs, id)
			inputConfigsMut.Unlock()
		}
		return err
	})
	dynAPI.OnDelete(func(id string) error {
		return fanIn.SetInput(id, nil, reqTimeout)
	})

	mgr.RegisterEndpoint(
		path.Join(conf.Dynamic.Prefix, "/input/{id}"),
		"Perform CRUD operations on the configuration of dynamic inputs. For"+
			" more information read the `dynamic` input type documentation.",
		dynAPI.HandleCRUD,
	)
	mgr.RegisterEndpoint(
		path.Join(conf.Dynamic.Prefix, "/inputs"),
		"Get a map of running input identifiers with their current uptimes.",
		dynAPI.HandleList,
	)

	return fanIn, nil
}

//------------------------------------------------------------------------------
