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

package processor

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	"github.com/Jeffail/benthos/lib/util/text"
	"github.com/Jeffail/gabs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["set_json"] = TypeSpec{
		constructor: NewSetJSON,
		description: `
Parses a message part as a JSON blob, sets a path to a value, and writes the
modified JSON back to the message part.

Values can be any value type, including objects and arrays. When using YAML
configuration files a YAML object will be converted into a JSON object, i.e.
with the config:

` + "``` yaml" + `
set_json:
  part: 0
  path: some.path
  value:
    foo:
      bar: 5
` + "```" + `

The value will be converted into '{"foo":{"bar":5}}'. If the YAML object
contains keys that aren't strings those fields will be ignored.

The part index can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if part = -1 then the selected part
will be the last part of the message, if part = -2 then the part before the
last element with be selected, and so on.

This processor will interpolate functions within the 'value' field, you can find
a list of functions [here](../config_interpolation.md#functions).`,
	}
}

//------------------------------------------------------------------------------

// Errors for the SetJSON type.
var (
	ErrEmptyTargetPath = errors.New("target path is empty")
)

//------------------------------------------------------------------------------

type rawJSONValue []byte

func (r *rawJSONValue) UnmarshalJSON(bytes []byte) error {
	*r = append((*r)[0:0], bytes...)
	return nil
}

func (r rawJSONValue) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	return r, nil
}

func (r *rawJSONValue) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var yamlObj interface{}
	if err := unmarshal(&yamlObj); err != nil {
		return err
	}

	var convertMap func(m map[interface{}]interface{}) map[string]interface{}
	convertMap = func(m map[interface{}]interface{}) map[string]interface{} {
		newMap := map[string]interface{}{}
		for k, v := range m {
			keyStr, ok := k.(string)
			if !ok {
				continue
			}
			newVal := v
			if iMap, isIMap := v.(map[interface{}]interface{}); isIMap {
				newVal = convertMap(iMap)
			}
			newMap[keyStr] = newVal
		}
		return newMap
	}

	if iMap, isIMap := yamlObj.(map[interface{}]interface{}); isIMap {
		yamlObj = convertMap(iMap)
	}

	rawJSON, err := json.Marshal(yamlObj)
	if err != nil {
		return err
	}

	*r = append((*r)[0:0], rawJSON...)
	return nil
}

func (r rawJSONValue) MarshalYAML() (interface{}, error) {
	var val interface{}
	if err := json.Unmarshal(r, &val); err != nil {
		return nil, err
	}
	return val, nil
}

//------------------------------------------------------------------------------

// SetJSONConfig contains any configuration for the SetJSON processor.
type SetJSONConfig struct {
	Part  int          `json:"part" yaml:"part"`
	Path  string       `json:"path" yaml:"path"`
	Value rawJSONValue `json:"value" yaml:"value"`
}

// NewSetJSONConfig returns a SetJSONConfig with default values.
func NewSetJSONConfig() SetJSONConfig {
	return SetJSONConfig{
		Part:  0,
		Path:  "",
		Value: rawJSONValue(`""`),
	}
}

//------------------------------------------------------------------------------

// SetJSON is a processor that inserts a new message part at a specific
// index.
type SetJSON struct {
	target      []string
	interpolate bool
	valueBytes  rawJSONValue

	conf  Config
	log   log.Modular
	stats metrics.Type
}

// NewSetJSON returns a SetJSON processor.
func NewSetJSON(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	j := &SetJSON{
		target:     strings.Split(conf.SetJSON.Path, "."),
		valueBytes: conf.SetJSON.Value,
		conf:       conf,
		log:        log.NewModule(".processor.set_json"),
		stats:      stats,
	}
	if len(j.target) == 0 || len(conf.SetJSON.Path) == 0 {
		return nil, ErrEmptyTargetPath
	}
	j.interpolate = text.ContainsFunctionVariables(j.valueBytes)
	return j, nil
}

//------------------------------------------------------------------------------

// ProcessMessage prepends a new message part to the message.
func (p *SetJSON) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.stats.Incr("processor.set_json.count", 1)

	msgs := [1]types.Message{msg}

	valueBytes := p.valueBytes
	if p.interpolate {
		valueBytes = text.ReplaceFunctionVariables(valueBytes)
	}

	index := p.conf.SetJSON.Part
	if index < 0 {
		index = msg.Len() + index
	}

	if index < 0 || index >= msg.Len() {
		p.stats.Incr("processor.set_json.skipped", 1)
		p.stats.Incr("processor.set_json.dropped", 1)
		return msgs[:], nil
	}

	jsonPart, err := msg.GetJSON(index)
	if err != nil {
		p.stats.Incr("processor.set_json.error.json_parse", 1)
		p.stats.Incr("processor.set_json.dropped", 1)
		p.log.Errorf("Failed to parse part into json: %v\n", err)
		return msgs[:], nil
	}

	var gPart *gabs.Container
	if gPart, err = gabs.Consume(jsonPart); err != nil {
		p.stats.Incr("processor.set_json.error.json_parse", 1)
		p.stats.Incr("processor.set_json.dropped", 1)
		p.log.Errorf("Failed to parse part into json: %v\n", err)
		return msgs[:], nil
	}

	gPart.Set(valueBytes, p.target...)

	if err = msg.SetJSON(index, gPart.Data()); err != nil {
		p.stats.Incr("processor.set_json.error.json_set", 1)
		p.log.Errorf("Failed to convert json into part: %v\n", err)
	}

	p.stats.Incr("processor.set_json.sent", 1)
	return msgs[:], nil
}

//------------------------------------------------------------------------------
