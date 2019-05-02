// Copyright (c) 2019 Ashley Jeffs
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

package config

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"path/filepath"

	"github.com/Jeffail/benthos/lib/util/text"
	"gopkg.in/yaml.v2"
)

//------------------------------------------------------------------------------

// readWithJSONRefs takes a config file path, reads the contents, performs a
// generic parse, replaces any JSON reference fields, marshals the result back
// into bytes and returns it so that it can be unmarshalled into a typed
// structure.
//
// CAVEATS:
// - Currently only supports paths ("$ref": "path/to/file.yaml")
// - Ignores fragments
func readWithJSONRefs(path string, replaceEnvs bool) ([]byte, error) {
	configBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if replaceEnvs {
		configBytes = text.ReplaceEnvVariables(configBytes)
	}

	var gen interface{}
	if err = yaml.Unmarshal(configBytes, &gen); err != nil {
		return nil, err
	}

	refFound, err := refWalk(path, gen)
	if err != nil {
		return nil, err
	}
	if !refFound {
		return configBytes, nil
	}
	if configBytes, err = yaml.Marshal(gen); err != nil {
		return nil, fmt.Errorf("failed to marshal ref evaluated structure: %v", err)
	}

	return configBytes, nil
}

//------------------------------------------------------------------------------

func getRefVal(obj interface{}) interface{} {
	switch x := obj.(type) {
	case map[interface{}]interface{}:
		for k, v := range x {
			if k == "$ref" {
				return v
			}
		}
	}
	return nil
}

func expandRefVal(path string, v interface{}) (interface{}, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("config '%v' contained non-string $ref value '%v' (%T)", path, v, v)
	}
	u, err := url.Parse(s)
	if err != nil {
		return nil, fmt.Errorf("failed to parse $ref value '%v' in config '%v' as URI: %v", s, path, err)
	}
	if u.Scheme != "" && u.Scheme != "file" {
		return nil, fmt.Errorf("config '%v' contained non-path $ref value '%v'", path, v)
	}
	rPath := u.Path
	if !filepath.IsAbs(rPath) {
		rPath = filepath.Join(filepath.Dir(path), rPath)
	}

	configBytes, err := ioutil.ReadFile(rPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read relative $ref path '%v' in config '%v': %v", rPath, path, err)
	}
	configBytes = text.ReplaceEnvVariables(configBytes)

	var gen interface{}
	if err = yaml.Unmarshal(configBytes, &gen); err != nil {
		return nil, err
	}

	if _, err = refWalk(rPath, gen); err != nil {
		return nil, err
	}
	return gen, nil
}

func refWalk(path string, root interface{}) (refFound bool, err error) {
	switch x := root.(type) {
	case map[interface{}]interface{}:
		for k, v := range x {
			if rv := getRefVal(v); rv != nil {
				if x[k], err = expandRefVal(path, rv); err != nil {
					return
				}
				refFound = true
			} else {
				var rFound bool
				if rFound, err = refWalk(path, v); err != nil {
					return
				} else if rFound {
					refFound = true
				}
			}
		}
	case []interface{}:
		for i, v := range x {
			if rv := getRefVal(v); rv != nil {
				if x[i], err = expandRefVal(path, rv); err != nil {
					return
				}
				refFound = true
			} else {
				var rFound bool
				if rFound, err = refWalk(path, v); err != nil {
					return
				} else if rFound {
					refFound = true
				}
			}
		}
	}
	return
}

//------------------------------------------------------------------------------
