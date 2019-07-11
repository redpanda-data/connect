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

package test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

func isBenthosConfig(path string) (bool, error) {
	cbytes, err := ioutil.ReadFile(path)
	if err != nil {
		return false, err
	}
	fields := map[string]*yaml.Node{}
	if err = yaml.Unmarshal(cbytes, &fields); err == nil {
		if _, exists := fields["pipeline"]; exists {
			return true, nil
		}
	}
	return false, nil
}

func generateDefinitions(targetPath string, recurse bool) error {
	defaultDefBytes, err := yaml.Marshal(ExampleDefinition())
	if err != nil {
		return fmt.Errorf("failed to generate default test definition: %v", err)
	}

	targetPath = filepath.Clean(targetPath)
	info, err := os.Stat(targetPath)
	if err != nil {
		return fmt.Errorf("failed to inspect target file '%v': %v", targetPath, err)
	}
	if !info.IsDir() {
		_, definitionPath := getBothPaths(targetPath)
		if _, err = os.Stat(definitionPath); err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("unable to access existing test definition file '%v': %v", definitionPath, err)
			}
		} else {
			return fmt.Errorf("test definition file '%v' already exists", definitionPath)
		}
		if err = ioutil.WriteFile(definitionPath, defaultDefBytes, 0666); err != nil {
			return fmt.Errorf("failed to write test definition '%v': %v", definitionPath, err)
		}
	}

	seenConfigs := map[string]struct{}{}
	return filepath.Walk(targetPath, func(path string, info os.FileInfo, werr error) error {
		if werr != nil {
			return werr
		}
		if info.IsDir() {
			if recurse || path == targetPath {
				return nil
			}
			return filepath.SkipDir
		}

		configPath, definitionPath := getBothPaths(path)
		if _, exists := seenConfigs[configPath]; exists {
			return nil
		}

		seenConfigs[configPath] = struct{}{}

		if _, err = os.Stat(definitionPath); err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("unable to check existing test definition file '%v': %v", definitionPath, err)
			}
		} else {
			return nil
		}

		if isBenthos, _ := isBenthosConfig(configPath); !isBenthos {
			return nil
		}

		if err = ioutil.WriteFile(definitionPath, defaultDefBytes, 0666); err != nil {
			return fmt.Errorf("failed to write test definition '%v': %v", definitionPath, err)
		}
		return nil
	})
}

//------------------------------------------------------------------------------

// Generate executes the generate-tests command for a specified path. The path
// can either be a config file, a directory, or the special pattern './...'.
func Generate(path string) error {
	var recurse bool
	path, recurse = resolveTestPath(path)
	return generateDefinitions(path, recurse)
}

//------------------------------------------------------------------------------
