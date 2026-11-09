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
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

// cmdAffected reads changed file paths (one per line, relative to the module
// root) from stdin and prints the integration test packages whose tests
// depend on them, as GITHUB_OUTPUT lines:
//
//	affected=<package paths>  every registered package whose tests depend on a change
//	filters=<package paths>   the same list, or empty when it exceeds the budget
//	budget_exceeded=true|false
//
// The budget is a number of packages, counted the way the run command
// selects them. The run command runs packages one after another, and in the
// nightly run on 2026-11-05 the 20 slowest packages took 24 minutes, so 20
// packages fit the 30-minute timeout of the filtered run.
//
// A change to a non-test Go package triggers every registered package whose
// test binary (go list -test -deps) includes it. A _test.go file or a file
// under testdata/ only triggers the package it belongs to. A file that is not
// in a Go package (workflows, docs) triggers nothing.
func cmdAffected(args []string) error {
	fset := flag.NewFlagSet("affected", flag.ContinueOnError)
	maxPackages := fset.Int("max-packages", 20, "maximum number of packages a filtered run may select")
	if err := fset.Parse(args); err != nil {
		return err
	}

	changed, err := readChangedFiles(os.Stdin)
	if err != nil {
		return err
	}

	pkgs := filterPackages(allPackages, nil)
	modPath, err := goModulePath()
	if err != nil {
		return err
	}
	deps, err := goListTestDeps(modPath, pkgs)
	if err != nil {
		return err
	}

	res := affectedPackages(pkgs, deps, modPath, changed, isGoPackageDir)
	return writeAffected(os.Stdout, os.Stderr, res, *maxPackages)
}

// affectedResult is the outcome of mapping changed files to packages.
type affectedResult struct {
	// Packages are the registered package paths (./internal/impl/...) to run.
	Packages []string
	// Reasons maps each package path to the changed import paths that
	// triggered it.
	Reasons map[string][]string
}

// affectedPackages maps changed files to the registered packages whose tests
// depend on them. deps maps a registered package path (as in packages.json)
// to the import paths its test binary depends on, including itself. isPkgDir
// reports whether a directory (relative to the module root) holds Go files.
func affectedPackages(pkgs []TestPackage, deps map[string]map[string]bool, modPath string, changed []string, isPkgDir func(string) bool) affectedResult {
	// Import paths changed in non-test code, which every dependent's test
	// binary compiles in.
	shared := map[string]bool{}
	// Import paths whose test-only files changed; only that package's own
	// tests see them.
	testOnly := map[string]bool{}

	for _, f := range changed {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		dir := path.Dir(f)
		isTest := strings.HasSuffix(f, "_test.go")
		if segs := strings.Split(dir, "/"); slices.Contains(segs, "testdata") {
			// Files under testdata/ belong to the package that owns the
			// testdata directory and are only read by its tests.
			dir = path.Join(append([]string{"."}, segs[:slices.Index(segs, "testdata")]...)...)
			isTest = true
		}
		// Walk up to the nearest directory that is a Go package, so that
		// embedded resources and other non-Go files count for their package.
		for dir != "." && !isPkgDir(dir) {
			dir = path.Dir(dir)
		}
		if dir == "." {
			continue
		}
		ip := modPath + "/" + dir
		if isTest {
			testOnly[ip] = true
		} else {
			shared[ip] = true
		}
	}

	res := affectedResult{Reasons: map[string][]string{}}
	for _, p := range pkgs {
		own := modPath + "/" + strings.TrimPrefix(p.Path, "./")
		var why []string
		for ip := range shared {
			if deps[p.Path][ip] {
				why = append(why, ip)
			}
		}
		if testOnly[own] {
			why = append(why, own+" (tests)")
		}
		if len(why) == 0 {
			continue
		}
		sort.Strings(why)
		res.Packages = append(res.Packages, p.Path)
		res.Reasons[p.Path] = why
	}
	sort.Strings(res.Packages)
	return res
}

func writeAffected(out, log io.Writer, res affectedResult, maxPackages int) error {
	for _, p := range res.Packages {
		fmt.Fprintf(log, "%s <- %s\n", p, strings.Join(res.Reasons[p], ", "))
	}
	// The run command matches filters as substrings, so a filter can select
	// more packages than it names: ./internal/impl/kafka also selects
	// ./internal/impl/kafka/enterprise. Count what the run would select.
	var selected []TestPackage
	if len(res.Packages) > 0 {
		selected = filterPackages(allPackages, res.Packages)
	}
	exceeded := len(selected) > maxPackages
	fmt.Fprintf(log, "%d packages selected, budget %d\n", len(selected), maxPackages)

	list := strings.Join(res.Packages, " ")
	filters := list
	if exceeded {
		filters = ""
	}
	fmt.Fprintf(out, "affected=%s\n", list)
	fmt.Fprintf(out, "filters=%s\n", filters)
	fmt.Fprintf(out, "budget_exceeded=%t\n", exceeded)
	return nil
}

// goListTestDeps returns, for each package path, the set of import paths its
// test binary depends on, including the package itself.
func goListTestDeps(modPath string, pkgs []TestPackage) (map[string]map[string]bool, error) {
	byImport := map[string]string{}
	args := []string{"list", "-test", "-f", "{{.ImportPath}}|{{join .Deps \",\"}}"}
	for _, p := range pkgs {
		args = append(args, p.Path)
	}
	cmd := exec.Command("go", args...)
	cmd.Stderr = os.Stderr
	outb, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go list: %w", err)
	}
	for _, p := range pkgs {
		byImport[modPath+"/"+strings.TrimPrefix(p.Path, "./")+".test"] = p.Path
	}
	return parseGoListTestDeps(outb, byImport), nil
}

// parseGoListTestDeps parses "ImportPath|dep,dep,..." lines from go list
// -test and keeps the test binary entries (ImportPath ending in .test) named
// in byImport. Test variants such as "x [y.test]" are reduced to "x".
func parseGoListTestDeps(b []byte, byImport map[string]string) map[string]map[string]bool {
	deps := map[string]map[string]bool{}
	sc := bufio.NewScanner(bytes.NewReader(b))
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		ip, list, ok := strings.Cut(sc.Text(), "|")
		if !ok {
			continue
		}
		p, ok := byImport[ip]
		if !ok {
			continue
		}
		set := map[string]bool{}
		for d := range strings.SplitSeq(list, ",") {
			if i := strings.Index(d, " ["); i >= 0 {
				d = d[:i]
			}
			if d != "" {
				set[d] = true
			}
		}
		deps[p] = set
	}
	return deps
}

func goModulePath() (string, error) {
	out, err := exec.Command("go", "list", "-m").Output()
	if err != nil {
		return "", fmt.Errorf("go list -m: %w", err)
	}
	mod := strings.TrimSpace(string(out))
	if mod == "" || strings.Contains(mod, "\n") {
		return "", errors.New("go list -m: expected a single module")
	}
	return mod, nil
}

func isGoPackageDir(dir string) bool {
	matches, _ := filepath.Glob(filepath.Join(filepath.FromSlash(dir), "*.go"))
	return len(matches) > 0
}

func readChangedFiles(r io.Reader) ([]string, error) {
	var lines []string
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	return lines, sc.Err()
}
