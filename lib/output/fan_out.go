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

package output

import (
	"encoding/json"
	"errors"

	"github.com/jeffail/benthos/lib/broker"
	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

var (
	// ErrFanOutNoOutputs - Returned when creating a FanOut type with zero outputs.
	ErrFanOutNoOutputs = errors.New("attempting to create fan_out output type with no outputs")
)

//--------------------------------------------------------------------------------------------------

func init() {
	constructors["fan_out"] = NewFanOut
}

//--------------------------------------------------------------------------------------------------

// FanOutConfig - Configuration for the FanOut output type.
type FanOutConfig struct {
	Outputs []interface{} `json:"outputs" yaml:"outputs"`
}

// NewFanOutConfig - Creates a new FanOutConfig with default values.
func NewFanOutConfig() FanOutConfig {
	return FanOutConfig{
		Outputs: []interface{}{},
	}
}

//--------------------------------------------------------------------------------------------------

/*
parseOutputConfsWithDefaults - Takes a fan out config and returns an array of output configs with
default values in place of omitted values. This is necessary because when unmarshalling config files
using structs you can pre-populate non-reference type struct fields with default values, but array
objects will lose those defaults.

In order to ensure that omitted values are set to default we initially parse they array as
interface{} types and then individually apply the defaults by marshalling and unmarshalling. The
correct way to do this would be to use json.RawMessage, but our config files can contain a range of
different formats that we do not know at this stage, therefore we use the more hacky method as
performance is not an issue at this stage.
*/
func parseOutputConfsWithDefaults(conf FanOutConfig) ([]Config, error) {
	outputConfs := make([]Config, len(conf.Outputs))

	for i, boxedConfig := range conf.Outputs {
		rawBytes, err := json.Marshal(boxedConfig)
		if err != nil {
			return nil, err
		}
		outputConfs[i] = NewConfig()
		if err = json.Unmarshal(rawBytes, &outputConfs[i]); err != nil {
			return nil, err
		}
	}

	return outputConfs, nil
}

//--------------------------------------------------------------------------------------------------

/*
NewFanOut - Create a new FanOut output type. Messages will be sent out to ALL outputs, outputs which
block will apply backpressure upstream, meaning other outputs will also stop receiving messages.
*/
func NewFanOut(conf Config, log log.Modular, stats metrics.Aggregator) (Type, error) {
	if len(conf.FanOut.Outputs) == 0 {
		return nil, ErrFanOutNoOutputs
	}

	outputConfs, err := parseOutputConfsWithDefaults(conf.FanOut)
	if err != nil {
		return nil, err
	}

	outputs := make([]types.Consumer, len(outputConfs))

	for i, oConf := range outputConfs {
		outputs[i], err = Construct(oConf, log, stats)
		if err != nil {
			return nil, err
		}
	}

	return broker.NewFanOut(outputs, stats)
}

//--------------------------------------------------------------------------------------------------
