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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/broker"

	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

type brokerOutputList []Config

// UnmarshalJSON ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (b *brokerOutputList) UnmarshalJSON(bytes []byte) error {
	genericOutputs := []interface{}{}
	if err := json.Unmarshal(bytes, &genericOutputs); err != nil {
		return err
	}

	outputConfs, err := parseOutputConfsWithDefaults(genericOutputs)
	if err != nil {
		return err
	}

	*b = outputConfs
	return nil
}

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (b *brokerOutputList) UnmarshalYAML(unmarshal func(interface{}) error) error {
	genericOutputs := []interface{}{}
	if err := unmarshal(&genericOutputs); err != nil {
		return err
	}

	outputConfs, err := parseOutputConfsWithDefaults(genericOutputs)
	if err != nil {
		return err
	}

	*b = outputConfs
	return nil
}

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
		newConfs := make([]Config, 1)
		label := broker.GetGenericType(boxedConfig)

		if i > 0 && strings.Index(label, "ditto") == 0 {
			broker.RemoveGenericType(boxedConfig)

			// Check if there is a ditto multiplier.
			if len(label) > 5 && label[5] == '_' {
				if label[6:] == "0" {
					// This is a special case where we are expressing that
					// we want to end up with zero duplicates.
					newConfs = nil
				} else {
					n, err := strconv.Atoi(label[6:])
					if err != nil {
						return nil, fmt.Errorf("failed to parse ditto multiplier: %v", err)
					}
					newConfs = make([]Config, n)
				}
			} else {
				newConfs = make([]Config, 1)
			}

			broker.ComplementGenericConfig(boxedConfig, outConfs[i-1])
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
