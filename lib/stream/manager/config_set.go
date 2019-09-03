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

package manager

import (
	"github.com/Jeffail/benthos/v3/lib/stream"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

// ConfigSet is a map of stream configurations mapped by ID, which can be YAML
// parsed without losing default values inside the stream configs.
type ConfigSet map[string]stream.Config

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (c ConfigSet) UnmarshalYAML(unmarshal func(interface{}) error) error {
	tmpSet := map[string]interface{}{}
	if err := unmarshal(&tmpSet); err != nil {
		return err
	}

	for k, v := range tmpSet {
		conf := stream.NewConfig()
		confBytes, err := yaml.Marshal(v)
		if err != nil {
			return err
		}
		if err = yaml.Unmarshal(confBytes, &conf); err != nil {
			return err
		}
		c[k] = conf
	}

	return nil
}

//------------------------------------------------------------------------------
