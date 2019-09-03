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
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/api"
	"github.com/Jeffail/benthos/v3/lib/broker"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeDynamic] = TypeSpec{
		constructor: NewDynamic,
		description: `
The dynamic type is a special broker type where the outputs are identified by
unique labels and can be created, changed and removed during runtime via a REST
HTTP interface. The broker pattern used is always ` + "`fan_out`" + `, meaning
each message will be delivered to each dynamic output.

To GET a JSON map of output identifiers with their current uptimes use the
'/outputs' endpoint.

To perform CRUD actions on the outputs themselves use POST, DELETE, and GET
methods on the ` + "`/outputs/{output_id}`" + ` endpoint. When using POST the
body of the request should be a JSON configuration for the output, if the output
already exists it will be changed.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			nestedOutputs := conf.Dynamic.Outputs
			outMap := map[string]interface{}{}
			for k, output := range nestedOutputs {
				sanOutput, err := SanitiseConfig(output)
				if err != nil {
					return nil, err
				}
				outMap[k] = sanOutput
			}
			return map[string]interface{}{
				"outputs": outMap,
				"prefix":  conf.Dynamic.Prefix,
				"timeout": conf.Dynamic.Timeout,
			}, nil
		},
	}
}

//------------------------------------------------------------------------------

// DynamicConfig contains configuration fields for the Dynamic output type.
type DynamicConfig struct {
	Outputs map[string]Config `json:"outputs" yaml:"outputs"`
	Prefix  string            `json:"prefix" yaml:"prefix"`
	Timeout string            `json:"timeout" yaml:"timeout"`
}

// NewDynamicConfig creates a new DynamicConfig with default values.
func NewDynamicConfig() DynamicConfig {
	return DynamicConfig{
		Outputs: map[string]Config{},
		Prefix:  "",
		Timeout: "5s",
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
	dynAPI := api.NewDynamic()

	outputs := map[string]broker.DynamicOutput{}
	for k, v := range conf.Dynamic.Outputs {
		newOutput, err := New(v, mgr, log, stats)
		if err != nil {
			return nil, err
		}
		outputs[k] = newOutput
	}

	var reqTimeout time.Duration
	if tout := conf.Dynamic.Timeout; len(tout) > 0 {
		var err error
		if reqTimeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}

	outputConfigs := conf.Dynamic.Outputs
	outputConfigsMut := sync.RWMutex{}

	fanOut, err := broker.NewDynamicFanOut(
		outputs, log, stats,
		broker.OptDynamicFanOutSetOnAdd(func(l string) {
			outputConfigsMut.Lock()
			defer outputConfigsMut.Unlock()

			uConf, exists := outputConfigs[l]
			if !exists {
				return
			}
			sConf, bErr := SanitiseConfig(uConf)
			if bErr != nil {
				log.Errorf("Failed to sanitise config: %v\n", bErr)
			}

			confBytes, _ := json.Marshal(sConf)
			dynAPI.Started(l, confBytes)
			delete(outputConfigs, l)
		}),
		broker.OptDynamicFanOutSetOnRemove(func(l string) {
			dynAPI.Stopped(l)
		}),
	)
	if err != nil {
		return nil, err
	}

	dynAPI.OnUpdate(func(id string, c []byte) error {
		newConf := NewConfig()
		if err := yaml.Unmarshal(c, &newConf); err != nil {
			return err
		}
		ns := fmt.Sprintf("dynamic.outputs.%v", id)
		newOutput, err := New(
			newConf, mgr,
			log.NewModule("."+ns),
			metrics.Combine(stats, metrics.Namespaced(stats, ns)),
		)
		if err != nil {
			return err
		}
		outputConfigsMut.Lock()
		outputConfigs[id] = newConf
		outputConfigsMut.Unlock()
		if err = fanOut.SetOutput(id, newOutput, reqTimeout); err != nil {
			outputConfigsMut.Lock()
			delete(outputConfigs, id)
			outputConfigsMut.Unlock()
		}
		return err
	})
	dynAPI.OnDelete(func(id string) error {
		return fanOut.SetOutput(id, nil, reqTimeout)
	})

	mgr.RegisterEndpoint(
		path.Join(conf.Dynamic.Prefix, "/outputs/{id}"),
		"Perform CRUD operations on the configuration of dynamic outputs. For"+
			" more information read the `dynamic` output type documentation.",
		dynAPI.HandleCRUD,
	)
	mgr.RegisterEndpoint(
		path.Join(conf.Dynamic.Prefix, "/outputs"),
		"Get a map of running output identifiers with their current uptimes.",
		dynAPI.HandleList,
	)

	return fanOut, nil
}

//------------------------------------------------------------------------------
