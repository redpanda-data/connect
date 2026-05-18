// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const packagesJSONPath = "cmd/tools/integration/packages.json"

type testPackage struct {
	Path string `json:"path"`
	Skip bool   `json:"skip,omitempty"`
}

func main() {
	registered, err := loadRegistered()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading %s: %v\n", packagesJSONPath, err)
		os.Exit(1)
	}

	found, err := findIntegrationPackages(".")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error walking repo: %v\n", err)
		os.Exit(1)
	}

	var missing []string
	for _, pkg := range found {
		if _, ok := registered[pkg]; !ok {
			missing = append(missing, pkg)
		}
	}

	if len(missing) == 0 {
		fmt.Println("All integration test packages are registered.")
		return
	}

	fmt.Fprintln(os.Stderr, "The following packages contain integration tests but are not registered in "+packagesJSONPath+":")
	for _, pkg := range missing {
		fmt.Fprintf(os.Stderr, "  %s\n", pkg)
	}
	fmt.Fprintln(os.Stderr, "\nAdd each missing entry to "+packagesJSONPath+` as {"path": "<package>"}.`)
	os.Exit(1)
}

func loadRegistered() (map[string]struct{}, error) {
	data, err := os.ReadFile(packagesJSONPath)
	if err != nil {
		return nil, err
	}
	var pkgs []testPackage
	if err := json.Unmarshal(data, &pkgs); err != nil {
		return nil, err
	}
	m := make(map[string]struct{}, len(pkgs))
	for _, p := range pkgs {
		m[p.Path] = struct{}{}
	}
	return m, nil
}

var skipDirs = map[string]bool{
	"vendor":                  true,
	".git":                    true,
	"cmd/tools/integration":   true,
}

func findIntegrationPackages(root string) ([]string, error) {
	seen := map[string]struct{}{}

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			rel := strings.TrimPrefix(filepath.ToSlash(path), "./")
			if skipDirs[rel] || (rel != "." && strings.HasPrefix(d.Name(), ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if containsCheckSkip(path) {
			dir := filepath.ToSlash(filepath.Dir(path))
			if dir == "." {
				dir = "./"
			} else {
				dir = "./" + strings.TrimPrefix(dir, "./")
			}
			seen[dir] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(seen))
	for pkg := range seen {
		result = append(result, pkg)
	}
	return result, nil
}

func containsCheckSkip(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "integration.CheckSkip(") {
			return true
		}
	}
	return false
}
