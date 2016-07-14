/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package input

import (
	"errors"

	"gopkg.in/yaml.v2"

	"github.com/jeffail/benthos/lib/broker"
	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

var (
	// ErrFanInNoInputs - Returned when creating a FanIn type with zero inputs.
	ErrFanInNoInputs = errors.New("attempting to create fan_in input type with no inputs")
)

//--------------------------------------------------------------------------------------------------

func init() {
	constructors["fan_in"] = NewFanIn
}

//--------------------------------------------------------------------------------------------------

// FanInConfig - Configuration for the FanIn input type.
type FanInConfig struct {
	Inputs []interface{} `json:"inputs" yaml:"inputs"`
}

// NewFanInConfig - Creates a new FanInConfig with default values.
func NewFanInConfig() FanInConfig {
	return FanInConfig{
		Inputs: []interface{}{},
	}
}

//--------------------------------------------------------------------------------------------------

/*
parseInputConfsWithDefaults - Takes a fan in config and returns an array of input configs with
default values in place of omitted values. This is necessary because when unmarshalling config files
using structs you can pre-populate non-reference type struct fields with default values, but array
objects will lose those defaults.

In order to ensure that omitted values are set to default we initially parse they array as
interface{} types and then individually apply the defaults by marshalling and unmarshalling. The
correct way to do this would be to use json.RawMessage, but our config files can contain a range of
different formats that we do not know at this stage, therefore we use the more hacky method as
performance is not an issue at this stage.
*/
func parseInputConfsWithDefaults(conf FanInConfig) ([]Config, error) {
	inputConfs := make([]Config, len(conf.Inputs))

	// NOTE: Use yaml here as it supports more types than JSON (map[interface{}]interface{}).
	for i, boxedConfig := range conf.Inputs {
		rawBytes, err := yaml.Marshal(boxedConfig)
		if err != nil {
			return nil, err
		}
		inputConfs[i] = NewConfig()
		if err = yaml.Unmarshal(rawBytes, &inputConfs[i]); err != nil {
			return nil, err
		}
	}

	return inputConfs, nil
}

//--------------------------------------------------------------------------------------------------

// NewFanIn - Create a new FanIn input type.
func NewFanIn(conf Config, log log.Modular, stats metrics.Aggregator) (Type, error) {
	if len(conf.FanIn.Inputs) == 0 {
		return nil, ErrFanInNoInputs
	}

	inputConfs, err := parseInputConfsWithDefaults(conf.FanIn)
	if err != nil {
		return nil, err
	}

	inputs := make([]types.Producer, len(inputConfs))

	for i, iConf := range inputConfs {
		inputs[i], err = Construct(iConf, log, stats)
		if err != nil {
			return nil, err
		}
	}

	return broker.NewFanIn(inputs, stats)
}

//--------------------------------------------------------------------------------------------------
