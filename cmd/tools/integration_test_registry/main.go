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

// Command integration_test_registry manages the list of integration test packages
// in cmd/tools/integration/packages.json.
//
// Usage:
//
//	go run ./cmd/tools/integration_test_registry generate  -- scan the repo and update packages.json, adding new packages
//	go run ./cmd/tools/integration_test_registry verify    -- exit non-zero if any integration test package is not registered
//
// A package is considered an integration test package if it contains a test
// function matching the pattern Test.*Integration.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const packagesJSONPath = "cmd/tools/integration/packages.json"

type testPackage struct {
	Path    string `json:"path"`
	Timeout string `json:"timeout,omitempty"`
	Skip    bool   `json:"skip,omitempty"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: integration_test_registry <generate|verify>")
		os.Exit(1)
	}
	switch os.Args[1] {
	case "generate":
		cmdGenerate()
	case "verify":
		cmdVerify()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q — expected generate or verify\n", os.Args[1])
		os.Exit(1)
	}
}

func cmdGenerate() {
	existing, err := loadExisting()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading %s: %v\n", packagesJSONPath, err)
		os.Exit(1)
	}

	found, err := findIntegrationPackages(".")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error walking repo: %v\n", err)
		os.Exit(1)
	}

	merged := merge(existing, found)

	if err := write(merged); err != nil {
		fmt.Fprintf(os.Stderr, "error writing %s: %v\n", packagesJSONPath, err)
		os.Exit(1)
	}

	fmt.Printf("Updated %s (%d packages)\n", packagesJSONPath, len(merged))
}

func cmdVerify() {
	existing, err := loadExisting()
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
		if _, ok := existing[pkg]; !ok {
			missing = append(missing, pkg)
		}
	}
	sort.Strings(missing)

	if len(missing) == 0 {
		fmt.Println("All integration test packages are registered.")
		return
	}

	fmt.Fprintln(os.Stderr, "The following packages contain integration tests but are not registered in "+packagesJSONPath+":")
	for _, pkg := range missing {
		fmt.Fprintf(os.Stderr, "  %s\n", pkg)
	}
	fmt.Fprintln(os.Stderr, "\nRun the generate command to update "+packagesJSONPath+".")
	os.Exit(1)
}

func loadExisting() (map[string]testPackage, error) {
	data, err := os.ReadFile(packagesJSONPath)
	if err != nil {
		return nil, err
	}
	var pkgs []testPackage
	if err := json.Unmarshal(data, &pkgs); err != nil {
		return nil, err
	}
	m := make(map[string]testPackage, len(pkgs))
	for _, p := range pkgs {
		m[p.Path] = p
	}
	return m, nil
}

// merge preserves existing entries unchanged and adds newly discovered paths as enabled (skip=false).
func merge(existing map[string]testPackage, found []string) []testPackage {
	for _, path := range found {
		if _, ok := existing[path]; !ok {
			existing[path] = testPackage{Path: path}
		}
	}
	result := make([]testPackage, 0, len(existing))
	for _, p := range existing {
		result = append(result, p)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Path < result[j].Path
	})
	return result
}

func write(pkgs []testPackage) error {
	var buf bytes.Buffer
	buf.WriteString("[\n")
	for i, p := range pkgs {
		b, err := json.Marshal(p)
		if err != nil {
			return err
		}
		buf.WriteString("  ")
		buf.Write(b)
		if i < len(pkgs)-1 {
			buf.WriteByte(',')
		}
		buf.WriteByte('\n')
	}
	buf.WriteString("]\n")
	return os.WriteFile(packagesJSONPath, buf.Bytes(), 0o644)
}

var skipDirs = map[string]bool{
	"vendor":                true,
	".git":                  true,
	"cmd/tools/integration": true,
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
		if hasIntegrationTest(path) {
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

var integrationTestFunc = regexp.MustCompile(`^func Test\w*Integration`)

func hasIntegrationTest(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if integrationTestFunc.MatchString(scanner.Text()) {
			return true
		}
	}
	return false
}
