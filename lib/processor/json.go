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
	"fmt"
	"strings"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/text"
	"github.com/Jeffail/gabs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["json"] = TypeSpec{
		constructor: NewJSON,
		description: `
Parses a message part as a JSON blob, performs a mutation on the data, and then
overwrites the previous contents with the new value.

If the path is empty or "." the root of the data will be targeted.

This processor will interpolate functions within the 'value' field, you can find
a list of functions [here](../config_interpolation.md#functions).

### Operations

#### ` + "`append`" + `

Appends a value to an array at a target dot path. If the path does not exist all
objects in the path are created (unless there is a collision).

If a non-array value already exists in the target path it will be replaced by an
array containing the original value as well as the new value.

If the value is an array the elements of the array are expanded into the new
array. E.g. if the target is an array ` + "`[0,1]`" + ` and the value is also an
array ` + "`[2,3]`" + `, the result will be ` + "`[0,1,2,3]`" + ` as opposed to
` + "`[0,1,[2,3]]`" + `.

#### ` + "`clean`" + `

Walks the JSON structure and deletes any fields where the value is:

- An empty array
- An empty object
- An empty string
- null

#### ` + "`copy`" + `

Copies the value of a target dot path (if it exists) to a location. The
destination path is specified in the ` + "`value`" + ` field. If the destination
path does not exist all objects in the path are created (unless there is a
collision).

#### ` + "`delete`" + `

Removes a key identified by the dot path. If the path does not exist this is a
no-op.

#### ` + "`move`" + `

Moves the value of a target dot path (if it exists) to a new location. The
destination path is specified in the ` + "`value`" + ` field. If the destination
path does not exist all objects in the path are created (unless there is a
collision).

#### ` + "`select`" + `

Reads the value found at a dot path and replaced the original contents entirely
by the new value.

#### ` + "`set`" + `

Sets the value of a field at a dot path. If the path does not exist all objects
in the path are created (unless there is a collision).

The value can be any type, including objects and arrays. When using YAML
configuration files a YAML object will be converted into a JSON object, i.e.
with the config:

` + "``` yaml" + `
json:
  operator: set
  parts: [0]
  path: some.path
  value:
    foo:
      bar: 5
` + "```" + `

The value will be converted into '{"foo":{"bar":5}}'. If the YAML object
contains keys that aren't strings those fields will be ignored.`,
	}
}

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

// JSONConfig contains any configuration for the JSON processor.
type JSONConfig struct {
	Parts    []int        `json:"parts" yaml:"parts"`
	Operator string       `json:"operator" yaml:"operator"`
	Path     string       `json:"path" yaml:"path"`
	Value    rawJSONValue `json:"value" yaml:"value"`
}

// NewJSONConfig returns a JSONConfig with default values.
func NewJSONConfig() JSONConfig {
	return JSONConfig{
		Parts:    []int{},
		Operator: "get",
		Path:     "",
		Value:    rawJSONValue(`""`),
	}
}

//------------------------------------------------------------------------------

type jsonOperator func(body interface{}, value rawJSONValue) (interface{}, error)

func newSetOperator(path []string) jsonOperator {
	return func(body interface{}, value rawJSONValue) (interface{}, error) {
		if len(path) == 0 {
			return value, nil
		}

		gPart, err := gabs.Consume(body)
		if err != nil {
			return nil, err
		}

		gPart.Set(value, path...)
		return gPart.Data(), nil
	}
}

func newMoveOperator(srcPath, destPath []string) (jsonOperator, error) {
	if len(srcPath) == 0 {
		return nil, errors.New("an empty source path is not valid for the move operator")
	}
	if len(destPath) == 0 {
		return nil, errors.New("an empty destination path is not valid for the move operator")
	}
	return func(body interface{}, value rawJSONValue) (interface{}, error) {
		gPart, err := gabs.Consume(body)
		if err != nil {
			return nil, err
		}

		gSrc := gPart.S(srcPath...).Data()
		if gSrc == nil {
			return nil, fmt.Errorf("item not found at path '%v'", strings.Join(srcPath, "."))
		}

		if _, err = gPart.Set(gSrc, destPath...); err != nil {
			return nil, fmt.Errorf("failed to set destination path '%v': %v", strings.Join(destPath, "."), err)
		}
		gPart.Delete(srcPath...)
		return gPart.Data(), nil
	}, nil
}

func newCopyOperator(srcPath, destPath []string) (jsonOperator, error) {
	if len(srcPath) == 0 {
		return nil, errors.New("an empty source path is not valid for the copy operator")
	}
	if len(destPath) == 0 {
		return nil, errors.New("an empty destination path is not valid for the copy operator")
	}
	return func(body interface{}, value rawJSONValue) (interface{}, error) {
		gPart, err := gabs.Consume(body)
		if err != nil {
			return nil, err
		}

		gSrc := gPart.S(srcPath...).Data()
		if gSrc == nil {
			return nil, fmt.Errorf("item not found at path '%v'", strings.Join(srcPath, "."))
		}

		if _, err = gPart.Set(gSrc, destPath...); err != nil {
			return nil, fmt.Errorf("failed to set destination path '%v': %v", strings.Join(destPath, "."), err)
		}
		return gPart.Data(), nil
	}, nil
}

func newSelectOperator(path []string) jsonOperator {
	return func(body interface{}, value rawJSONValue) (interface{}, error) {
		gPart, err := gabs.Consume(body)
		if err != nil {
			return nil, err
		}

		target := gPart
		if len(path) > 0 {
			target = gPart.Search(path...)
		}

		switch t := target.Data().(type) {
		case string:
			return rawJSONValue(t), nil
		case json.Number:
			return rawJSONValue(t.String()), nil
		}

		return target.Data(), nil
	}
}

func newDeleteOperator(path []string) jsonOperator {
	return func(body interface{}, value rawJSONValue) (interface{}, error) {
		if len(path) == 0 {
			return nil, nil
		}

		gPart, err := gabs.Consume(body)
		if err != nil {
			return nil, err
		}

		if err = gPart.Delete(path...); err != nil {
			return nil, err
		}
		return gPart.Data(), nil
	}
}

func newCleanOperator(path []string) jsonOperator {
	return func(body interface{}, value rawJSONValue) (interface{}, error) {
		gRoot, err := gabs.Consume(body)
		if err != nil {
			return nil, err
		}

		var cleanValueFn func(g interface{}) interface{}
		var cleanArrayFn func(g []interface{}) []interface{}
		var cleanObjectFn func(g map[string]interface{}) map[string]interface{}
		cleanValueFn = func(g interface{}) interface{} {
			if g == nil {
				return nil
			}
			switch t := g.(type) {
			case map[string]interface{}:
				if nv := cleanObjectFn(t); len(nv) > 0 {
					return nv
				}
				return nil
			case []interface{}:
				if na := cleanArrayFn(t); len(na) > 0 {
					return na
				}
				return nil
			case string:
				if len(t) > 0 {
					return t
				}
				return nil
			}
			return g
		}
		cleanArrayFn = func(g []interface{}) []interface{} {
			newArray := []interface{}{}
			for _, v := range g {
				if nv := cleanValueFn(v); nv != nil {
					newArray = append(newArray, nv)
				}
			}
			return newArray
		}
		cleanObjectFn = func(g map[string]interface{}) map[string]interface{} {
			newObject := map[string]interface{}{}
			for k, v := range g {
				if nv := cleanValueFn(v); nv != nil {
					newObject[k] = nv
				}
			}
			return newObject
		}
		if val := cleanValueFn(gRoot.S(path...).Data()); val == nil {
			if len(path) == 0 {
				switch gRoot.Data().(type) {
				case []interface{}:
					return []interface{}{}, nil
				case map[string]interface{}:
					return map[string]interface{}{}, nil
				}
				return nil, nil
			}
			gRoot.Delete(path...)
		} else {
			gRoot.Set(val, path...)
		}

		return gRoot.Data(), nil
	}
}

func newAppendOperator(path []string) jsonOperator {
	return func(body interface{}, value rawJSONValue) (interface{}, error) {
		gPart, err := gabs.Consume(body)
		if err != nil {
			return nil, err
		}

		var array []interface{}

		var valueParsed interface{}
		if err = json.Unmarshal(value, &valueParsed); err != nil {
			return nil, err
		}
		switch t := valueParsed.(type) {
		case []interface{}:
			array = t
		default:
			array = append(array, t)
		}

		if gTarget := gPart.S(path...); gTarget != nil {
			switch t := gTarget.Data().(type) {
			case []interface{}:
				t = append(t, array...)
				array = t
			case nil:
				array = append([]interface{}{t}, array...)
			default:
				array = append([]interface{}{t}, array...)
			}
		}
		gPart.Set(array, path...)

		return gPart.Data(), nil
	}
}

func getOperator(opStr string, path []string, value rawJSONValue) (jsonOperator, error) {
	var destPath []string
	if opStr == "move" || opStr == "copy" {
		var destDotPath string
		if err := json.Unmarshal(value, &destDotPath); err != nil {
			return nil, fmt.Errorf("failed to parse destination path from value: %v", err)
		}
		if len(destDotPath) > 0 {
			destPath = strings.Split(destDotPath, ".")
		}
	}
	switch opStr {
	case "set":
		return newSetOperator(path), nil
	case "select":
		return newSelectOperator(path), nil
	case "copy":
		return newCopyOperator(path, destPath)
	case "move":
		return newMoveOperator(path, destPath)
	case "delete":
		return newDeleteOperator(path), nil
	case "append":
		return newAppendOperator(path), nil
	case "clean":
		return newCleanOperator(path), nil
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

//------------------------------------------------------------------------------

// JSON is a processor that performs an operation on a JSON payload.
type JSON struct {
	parts       []int
	interpolate bool
	valueBytes  rawJSONValue
	operator    jsonOperator

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErrJSONP  metrics.StatCounter
	mErrJSONS  metrics.StatCounter
	mErr       metrics.StatCounter
	mSucc      metrics.StatCounter
	mSent      metrics.StatCounter
	mSentParts metrics.StatCounter
}

// NewJSON returns a JSON processor.
func NewJSON(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	j := &JSON{
		conf:  conf,
		log:   log.NewModule(".processor.json"),
		stats: stats,

		valueBytes: conf.JSON.Value,

		mCount:     stats.GetCounter("processor.json.count"),
		mErrJSONP:  stats.GetCounter("processor.json.error.json_parse"),
		mErrJSONS:  stats.GetCounter("processor.json.error.json_set"),
		mErr:       stats.GetCounter("processor.json.error"),
		mSucc:      stats.GetCounter("processor.json.success"),
		mSent:      stats.GetCounter("processor.json.sent"),
		mSentParts: stats.GetCounter("processor.json.parts.sent"),
	}

	j.interpolate = text.ContainsFunctionVariables(j.valueBytes)

	splitPath := strings.Split(conf.JSON.Path, ".")
	if len(conf.JSON.Path) == 0 || conf.JSON.Path == "." {
		splitPath = []string{}
	}

	var err error
	if j.operator, err = getOperator(conf.JSON.Operator, splitPath, j.valueBytes); err != nil {
		return nil, err
	}
	return j, nil
}

//------------------------------------------------------------------------------

// ProcessMessage prepends a new message part to the message.
func (p *JSON) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)

	newMsg := msg.ShallowCopy()

	valueBytes := p.valueBytes
	if p.interpolate {
		valueBytes = text.ReplaceFunctionVariables(valueBytes)
	}

	targetParts := p.parts
	if len(targetParts) == 0 {
		targetParts = make([]int, newMsg.Len())
		for i := range targetParts {
			targetParts[i] = i
		}
	}

	for _, index := range targetParts {
		var data interface{} = valueBytes

		jsonPart, err := msg.GetJSON(index)
		if err != nil {
			p.mErrJSONP.Incr(1)
			p.log.Debugf("Failed to parse part into json: %v\n", err)
			continue
		}

		if data, err = p.operator(jsonPart, valueBytes); err != nil {
			p.mErr.Incr(1)
			p.log.Debugf("Failed to apply operator: %v\n", err)
			continue
		}

		switch t := data.(type) {
		case rawJSONValue:
			newMsg.Set(index, []byte(t))
		case []byte:
			newMsg.Set(index, t)
		default:
			if err = newMsg.SetJSON(index, data); err != nil {
				p.mErrJSONS.Incr(1)
				p.log.Debugf("Failed to convert json into part: %v\n", err)
			}
		}

		if err == nil {
			p.mSucc.Incr(1)
		}
	}

	msgs := [1]types.Message{newMsg}

	p.mSent.Incr(1)
	p.mSentParts.Incr(int64(newMsg.Len()))
	return msgs[:], nil
}

//------------------------------------------------------------------------------
