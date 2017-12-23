// Copyright (c) 2017 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, sub to the following conditions:
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

package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"

	"gopkg.in/yaml.v2"
)

//------------------------------------------------------------------------------

var envRegex *regexp.Regexp

func init() {
	var err error
	envRegex, err = regexp.Compile(`\${[0-9A-Za-z_]+(:[^}]*)?}`)
	if err != nil {
		panic(err)
	}
}

func replaceEnvVariables(configBytes []byte) []byte {
	return envRegex.ReplaceAllFunc(configBytes, func(content []byte) []byte {
		var value string
		if len(content) > 3 {
			if colonIndex := bytes.IndexByte(content, ':'); colonIndex == -1 {
				value = os.Getenv(string(content[2 : len(content)-1]))
			} else {
				targetVar := content[2:colonIndex]
				defaultVal := content[colonIndex+1 : len(content)-1]

				value = os.Getenv(string(targetVar))
				if len(value) == 0 {
					value = string(defaultVal)
				}
			}
		}
		return []byte(value)
	})
}

func readConfig(path string, replaceEnvs bool, config interface{}) error {
	configBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	if replaceEnvs {
		configBytes = replaceEnvVariables(configBytes)
	}

	ext := filepath.Ext(path)
	if ".js" == ext || ".json" == ext {
		if err = json.Unmarshal(configBytes, config); err != nil {
			return err
		}
	} else if ".yml" == ext || ".yaml" == ext {
		if err = yaml.Unmarshal(configBytes, config); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("config file extension not recognised: %v", path)
	}
	return nil
}

//------------------------------------------------------------------------------
