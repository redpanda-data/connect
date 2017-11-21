// Copyright (c) 2014 Ashley Jeffs
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
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v2"
)

//------------------------------------------------------------------------------

// parseOutputConfsWithDefaults takes an array of output configs as
// []interface{} and returns an array of output configs with default values in
// place of omitted values. This is necessary because when unmarshalling config
// files using structs you can pre-populate non-reference type struct fields
// with default values, but array objects will lose those defaults.
//
// In order to ensure that omitted values are set to default we initially parse
// the array as interface{} types and then individually apply the defaults by
// marshalling and unmarshalling. The correct way to do this would be to use
// json.RawMessage, but our config files can contain a range of different
// formats that we do not know at this stage (JSON, YAML, etc), therefore we use
// the more hacky method as performance is not an issue at this stage.
func parseOutputConfsWithDefaults(outConfs []interface{}) ([]Config, error) {
	outputConfs := []Config{}

	for i, boxedConfig := range outConfs {
		newConfs := []Config{NewConfig()}
		if i > 0 {
			// If the type of this output is 'ditto' we want to start with a
			// duplicate of the previous config.
			newConfsFromDitto := func(label string) error {
				newConfs[0] = outputConfs[i-1]
				if len(label) > 5 && label[5] == '_' {
					n, err := strconv.Atoi(label[6:])
					if err != nil {
						return fmt.Errorf("failed to parse ditto multiplier: %v", err)
					}
					for j := 1; j < n; j++ {
						newConfs = append(newConfs, outputConfs[i-1])
					}
				}
				return nil
			}
			switch unboxed := boxedConfig.(type) {
			case map[string]interface{}:
				if t, ok := unboxed["type"].(string); ok && strings.Index(t, "ditto") == 0 {
					if err := newConfsFromDitto(t); err != nil {
						return nil, err
					}
					unboxed["type"] = newConfs[0].Type
				}
			case map[interface{}]interface{}:
				if t, ok := unboxed["type"].(string); ok && strings.Index(t, "ditto") == 0 {
					if err := newConfsFromDitto(t); err != nil {
						return nil, err
					}
					unboxed["type"] = newConfs[0].Type
				}
			}
		}
		for _, conf := range newConfs {
			rawBytes, err := yaml.Marshal(boxedConfig)
			if err != nil {
				return nil, err
			}
			if err = yaml.Unmarshal(rawBytes, &conf); err != nil {
				return nil, err
			}
			outputConfs = append(outputConfs, conf)
		}
	}

	return outputConfs, nil
}

//------------------------------------------------------------------------------
