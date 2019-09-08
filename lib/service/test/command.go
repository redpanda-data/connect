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
	"sort"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/config"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

func getBothPaths(fullPath, testSuffix string) (configPath string, definitionPath string) {
	path, file := filepath.Split(fullPath)
	ext := filepath.Ext(file)
	filename := strings.TrimSuffix(file, ext)
	if strings.HasSuffix(filename, testSuffix) {
		definitionPath = filepath.Clean(fullPath)
		configPath = filepath.Join(path, strings.TrimSuffix(filename, testSuffix)+ext)
	} else {
		configPath = filepath.Clean(fullPath)
		definitionPath = filepath.Join(path, filename+testSuffix+ext)
	}
	return
}

// Searches for test definition targets.
func getTestTargets(targetPath, testSuffix string, recurse bool) ([]string, error) {
	targetPath = filepath.Clean(targetPath)
	info, err := os.Stat(targetPath)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		configPath, definitionPath := getBothPaths(targetPath, testSuffix)
		if _, err = os.Stat(configPath); err != nil {
			return nil, fmt.Errorf("unable to access target config file '%v': %v", configPath, err)
		}
		if _, err = os.Stat(definitionPath); err != nil {
			return nil, fmt.Errorf("unable to access test definition file '%v': %v", definitionPath, err)
		}
		return []string{definitionPath}, nil
	}

	pathMap := map[string]struct{}{}
	err = filepath.Walk(targetPath, func(path string, info os.FileInfo, werr error) error {
		if werr != nil {
			return werr
		}
		if info.IsDir() {
			if recurse || path == targetPath {
				return nil
			}
			return filepath.SkipDir
		}

		configPath, definitionPath := getBothPaths(path, testSuffix)
		if _, exists := pathMap[definitionPath]; exists {
			return nil
		}

		if _, err = os.Stat(configPath); err != nil {
			return fmt.Errorf("unable to access target config file '%v': %v", configPath, err)
		}
		if _, err = os.Stat(definitionPath); err != nil {
			if os.IsNotExist(err) {
				// We simply skip files where there is no test definition.
				return nil
			}
			return fmt.Errorf("unable to access test definition file '%v': %v", definitionPath, err)
		}
		pathMap[definitionPath] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, err
	}

	list := []string{}
	for k := range pathMap {
		list = append(list, k)
	}
	sort.Strings(list)
	return list, nil
}

// Executes a test definition and either returns fails or returns an error.
func testTarget(path, testSuffix string) ([]CaseFailure, error) {
	confPath, _ := getBothPaths(path, testSuffix)
	var definition Definition
	defBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read test definition '%v': %v", path, err)
	}
	if err := yaml.Unmarshal(defBytes, &definition); err != nil {
		return nil, fmt.Errorf("failed to parse test definition '%v': %v", path, err)
	}
	return definition.Execute(confPath)
}

// Lints the config target of a test definition and either returns linting
// errors (false for failed) or returns an error.
func lintTarget(path, testSuffix string) ([]string, error) {
	confPath, _ := getBothPaths(path, testSuffix)
	dummyConf := config.New()
	lints, err := config.Read(confPath, true, &dummyConf)
	if err != nil {
		return nil, err
	}
	return lints, nil
}

//------------------------------------------------------------------------------

func resolveTestPath(path string) (string, bool) {
	recurse := false
	if path == "./..." || path == "..." {
		recurse = true
		path = "."
	}
	if strings.HasSuffix(path, "/...") {
		recurse = true
		path = strings.TrimSuffix(path, "/...")
	}
	return path, recurse
}

// Run executes the test command for a specified path. The path can either be a
// config file, a config files test definition file, a directory, or the
// wildcard pattern './...'.
func Run(path, testSuffix string, lint bool) bool {
	var recurse bool
	path, recurse = resolveTestPath(path)
	targets, err := getTestTargets(path, testSuffix, recurse)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to obtain test targets: %v\n", err)
		return false
	}

	failed := false
	for _, target := range targets {
		var lints []string
		var fails []CaseFailure
		if lint {
			if lints, err = lintTarget(target, testSuffix); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to execute test target '%v': %v\n", target, err)
				return false
			}
		}
		if fails, err = testTarget(target, testSuffix); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to execute test target '%v': %v\n", target, err)
			return false
		}
		if len(lints) > 0 || len(fails) > 0 {
			failed = true
			fmt.Printf("Test '%v' failed\n", target)
			for _, lint := range lints {
				fmt.Printf("  Lint: %v\n", lint)
			}
			if len(fails) > 0 {
				fmt.Printf("  %v [line %v]:\n", fails[0].Name, fails[0].TestLine)
				for _, fail := range fails {
					fmt.Printf("    %v\n", fail.Reason)
				}
			}
		} else {
			fmt.Printf("Test '%v' succeeded\n", target)
		}
	}

	if failed {
		return false
	}
	return true
}

//------------------------------------------------------------------------------
