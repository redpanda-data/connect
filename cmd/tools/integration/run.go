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
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"golang.org/x/sync/errgroup"
)

var runArgs struct {
	outputDir    string
	clean        bool
	debug        bool
	race         bool
	unit         bool
	coverProfile bool
	parallel     int
}

func debugf(format string, args ...any) {
	if runArgs.debug {
		log.Printf("[debug] "+format, args...)
	}
}

func cmdRun(args []string) error {
	fset := flag.NewFlagSet("run", flag.ExitOnError)
	fset.StringVar(&runArgs.outputDir, "output-dir", "", "directory for test output (default: .integration/<timestamp>)")
	fset.BoolVar(&runArgs.clean, "clean", false, "ignore cache, start a fresh run")
	fset.BoolVar(&runArgs.debug, "debug", false, "enable debug logging to stderr")
	fset.BoolVar(&runArgs.race, "race", false, "enable race detector (sets CGO_ENABLED=1)")
	fset.BoolVar(&runArgs.unit, "unit", false, "run all tests, not just integration tests")
	fset.BoolVar(&runArgs.coverProfile, "cover-profile", false, "generate coverage profile per package")
	fset.IntVar(&runArgs.parallel, "parallel", 1, "number of packages to test at the same time")

	if err := fset.Parse(args); err != nil {
		return err
	}

	if runArgs.outputDir != "" && runArgs.clean {
		return errors.New("--output-dir and --clean are mutually exclusive")
	}
	if runArgs.parallel < 1 {
		return errors.New("--parallel must be 1 or more")
	}

	if runArgs.debug {
		log.SetOutput(os.Stderr)
	}

	filters := fset.Args()

	packages := filterPackages(allPackages, filters)
	if len(packages) == 0 {
		// No registered, non-skipped packages match the filter. This is common
		// in CI: a pull request's filter is derived from its changed paths, so a
		// PR that only touches packages without integration tests (or only
		// skipped ones) matches nothing. Treat that as a no-op success rather
		// than an error so those PRs don't fail spuriously.
		fmt.Printf("No integration test packages match filter %v; nothing to run.\n", filters)
		return nil
	}

	// Determine output directory.
	outputDir := ""
	if runArgs.outputDir != "" {
		outputDir = runArgs.outputDir
		debugf("using provided output dir: %s", outputDir)
	} else {
		if !runArgs.clean {
			outputDir = findLatestRunDir(".integration")
			debugf("resuming run dir: %s", outputDir)
		}
		if outputDir == "" {
			outputDir = resolveRunDir()
			debugf("new run dir: %s", outputDir)
		}
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("creating run directory: %w", err)
	}

	debugf("resolved run dir: %s", outputDir)

	// Packages that run at the same time would mix their lines on the terminal,
	// so each one writes its lines as one block when it is done. A single
	// package streams its lines as they happen.
	out := NewOutput(os.Stdout, filepath.Join(outputDir, "index.md"), runArgs.parallel > 1)
	total := len(packages)

	headerLabel := "Integration Tests"
	if runArgs.unit {
		headerLabel = "All Tests"
	}
	out.Header(fmt.Sprintf("%s (%d packages)", headerLabel, total))
	out.Info("Output: " + outputDir)
	// The runner does not remove containers itself. Without Ryuk, the
	// containers of tests that do not terminate them stay. ReadConfig reads
	// the same env var and properties file as the test binaries.
	if testcontainers.ReadConfig().Config.RyukDisabled {
		out.Error("Warning: Ryuk is disabled. Containers will not be cleaned up and will accumulate.")
	}
	out.Blank()

	if runArgs.parallel > 1 && total > 1 {
		start := time.Now()
		if err := prebuildDeps(packages); err != nil {
			// The package workers build again and report the real error.
			out.Info(fmt.Sprintf("Build of shared dependencies failed: %v", err))
		} else {
			out.Info(fmt.Sprintf("Built shared dependencies in %s", time.Since(start).Round(time.Second)))
		}
		out.Blank()
	}

	// Go blocks while runArgs.parallel packages run, so the packages start in
	// the registry order of packages.json. Each package writes only its own
	// slot of results, so the count below needs no lock.
	results := make([]TestResult, total)
	var g errgroup.Group
	g.SetLimit(runArgs.parallel)
	for i, pkg := range packages {
		g.Go(func() error {
			results[i] = runPackage(pkg, i+1, total, outputDir, out)
			return nil
		})
	}
	_ = g.Wait()

	var passed, failed, skipped int
	for _, r := range results {
		switch r {
		case ResultPass:
			passed++
		case ResultFail:
			failed++
		case ResultSkip:
			skipped++
		}
	}

	// Final report.
	sep := strings.Repeat("=", 40)
	out.Info(sep)
	out.Summary(passed, failed, skipped)
	out.Info(sep)

	if failed > 0 {
		return errors.New("integration tests failed")
	}
	return nil
}

// prebuildDeps compiles the dependencies of the test binaries of pkgs into the
// build cache, before the package workers start. Each worker runs its own
// `go test` process, and processes do not share builds that are in progress.
// Without this step, on a cold cache, all workers compile the same
// dependencies at the same time. That wastes CPU that the Docker VM needs.
//
// It uses `go build` and not `go test -run '^$'`, because `go test` runs the
// test binary, and a TestMain can start containers. The build flags must match
// the flags of runPackageTest that change the compiled code (-tags and
// -race), or the cache entries do not match.
func prebuildDeps(pkgs []TestPackage) error {
	paths := make([]string, len(pkgs))
	for i, pkg := range pkgs {
		paths[i] = pkg.Path
	}
	buildFlags := []string{"-tags", "integration"}
	if runArgs.race {
		buildFlags = append(buildFlags, "-race")
	}

	listArgs := append([]string{"list", "-deps", "-test", "-f", "{{if not .ForTest}}{{.ImportPath}}{{end}}"}, buildFlags...)
	listArgs = append(listArgs, paths...)
	var stderr bytes.Buffer
	list := goCommand(listArgs...)
	list.Stderr = &stderr
	listOut, err := list.Output()
	if err != nil {
		return fmt.Errorf("go list: %w: %s", err, strings.TrimSpace(stderr.String()))
	}

	buildArgs := append(append([]string{"build"}, buildFlags...), buildablePackages(string(listOut))...)
	debugf("prebuilding %d packages", len(buildArgs)-len(buildFlags)-1)
	stderr.Reset()
	build := goCommand(buildArgs...)
	build.Stderr = &stderr
	if err := build.Run(); err != nil {
		return fmt.Errorf("go build: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// buildablePackages returns the import paths in the output of
// `go list -deps -test -f '{{if not .ForTest}}{{.ImportPath}}{{end}}'` that
// `go build` can compile. The template prints an empty line for the test
// variants of packages, and the list also has the generated "pkg.test" main
// packages, which only `go test` can build.
func buildablePackages(listOut string) []string {
	var pkgs []string
	for line := range strings.Lines(listOut) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasSuffix(line, ".test") {
			continue
		}
		pkgs = append(pkgs, line)
	}
	return pkgs
}

// goCommand returns a go command with the environment of runPackageTest.
func goCommand(args ...string) *exec.Cmd {
	cmd := exec.Command("go", args...)
	cmd.Env = os.Environ()
	if runArgs.race {
		cmd.Env = append(cmd.Env, "CGO_ENABLED=1")
	}
	return cmd
}

// runPackage tests one package, or prints its cached result, and returns its
// result. num is the 1-based position of the package in the run.
func runPackage(pkg TestPackage, num, total int, outputDir string, out *Output) TestResult {
	short := pkgShort(pkg.Path)
	fname := pkgFilename(pkg.Path)
	pkgOutFile := filepath.Join(outputDir, fname)

	tag := strings.TrimSuffix(fname, ".txt")
	pout := out.Package(tag, num, total, short)

	// Check cache from current run directory.
	var cached PackageCache
	if !runArgs.clean {
		debugf("checking cache for %s at %s", short, pkgOutFile)
		cached = checkCache(pkgOutFile)
	}

	// Print cached items (subtests compacted into parent summaries).
	for _, t := range compactCacheEntries(cached.Tests) {
		switch t.Result {
		case ResultPass:
			pout.TestPass(t.TestName, "", true)
		case ResultSkip:
			pout.TestSkip(t.TestName, "", t.SkipReason, true)
		case ResultFail:
			pout.TestFail(t.TestName, "", pkgOutFile, t.FailLine, true)
		}
	}

	// If complete and all passed, close and move on.
	if cached.Complete && cached.Overall() == ResultPass {
		debugf("found cached result for %s: complete", short)
		pout.Cached()
		return ResultPass
	}

	res := runPackageTest(pkg, short, pkgOutFile, cached, pout)
	pout.Result(res.status, res.duration)
	return res.status
}

type packageResult struct {
	status   TestResult
	duration time.Duration
	outFile  string
	tests    []CacheEntry
}

// runPackageTest executes `go test -json` for a single package, parses JSON
// events, streams filtered output, and returns the result.
func runPackageTest(pkg TestPackage, short, outFile string, cached PackageCache, out *PackageOutput) packageResult {
	result := packageResult{outFile: outFile}

	// Count existing lines without loading the file into memory (fix #4).
	lineNum, endsNewline, err := countFileLines(outFile)
	if err != nil {
		out.Error(fmt.Sprintf("  failed to read existing output file: %v", err))
		result.status = ResultFail
		return result
	}

	f, err := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		out.Error(fmt.Sprintf("  failed to open output file: %v", err))
		result.status = ResultFail
		return result
	}
	defer f.Close()

	if lineNum > 0 && !endsNewline {
		fmt.Fprintln(f)
		lineNum++
	}

	// Write start-run marker.
	writeEvent(f, TestEvent{Action: ActionStartRun, Time: time.Now(), Package: pkg.Path})
	lineNum++

	// Derive test flags from config (fix #3).
	skipRegex := completedTestSkipRegex(cached.Tests)
	runRegex := "^Test.*Integration"
	if runArgs.unit {
		runRegex = ""
	}
	timeout := pkg.TimeoutStr()

	var coverFile string
	if runArgs.coverProfile {
		coverFile = strings.TrimSuffix(outFile, ".txt") + ".cov"
	}

	debugf("starting go test for %s with -run %q -skip %q", short, runRegex, skipRegex)

	goArgs := []string{"test", "-json", "-count=1", "-shuffle=on", "-tags", "integration", "-timeout", timeout}
	if runRegex != "" {
		goArgs = append(goArgs, "-run", runRegex)
	}
	if runArgs.race {
		goArgs = append(goArgs, "-race")
	}
	if skipRegex != "" {
		goArgs = append(goArgs, "-skip", skipRegex)
	}
	if coverFile != "" {
		goArgs = append(goArgs, "-coverprofile="+coverFile)
	}
	goArgs = append(goArgs, pkg.Path)
	cmd := goCommand(goArgs...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		out.Error(fmt.Sprintf("  failed to create stdout pipe: %v", err))
		result.status = ResultFail
		return result
	}
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		out.Error(fmt.Sprintf("  failed to start: %v", err))
		result.status = ResultFail
		return result
	}

	startedTests := make(map[string]struct{})

	var (
		sawRun     bool
		sawPass    bool
		sawSkip    bool
		lastFailLn int
	)

	parseEvents(stdout, lineNum, EventCallbacks{
		OnRawLine: func(line []byte, ln int) {
			lineNum = ln
			fmt.Fprintf(f, "%s\n", line)
		},
		OnEvent: func(pe ParsedEvent) {
			switch pe.Action {
			case ActionRun:
				sawRun = true
				if !pe.IsSubtest {
					startedTests[pe.Test] = struct{}{}
					out.TestRun(pe.Test)
				}
			case ActionPass:
				sawPass = true
				delete(startedTests, pe.Test)
				elapsed := fmtElapsed(pe.Elapsed)
				if pe.ParentHasSubs {
					out.TestPass(pe.Test, elapsed, false)
					return
				}
				result.tests = append(result.tests, CacheEntry{
					TestName: pe.Test,
					Result:   ResultPass,
					Duration: time.Duration(pe.Elapsed * float64(time.Second)),
				})
				if !pe.IsSubtest {
					out.TestPass(pe.Test, elapsed, false)
				}
			case ActionFail:
				delete(startedTests, pe.Test)
				if err := f.Sync(); err != nil {
					debugf("failed to sync output file %s: %v", outFile, err)
				}
				lastFailLn = pe.LineNum
				elapsed := fmtElapsed(pe.Elapsed)
				if pe.ParentHasSubs {
					out.TestFail(pe.Test, elapsed, outFile, pe.LineNum, false)
					return
				}
				result.tests = append(result.tests, CacheEntry{
					TestName: pe.Test,
					Result:   ResultFail,
					FailLine: pe.LineNum,
					Duration: time.Duration(pe.Elapsed * float64(time.Second)),
				})
				out.TestFail(pe.Test, elapsed, outFile, pe.LineNum, false)
			case ActionSkip:
				sawSkip = true
				delete(startedTests, pe.Test)
				elapsed := fmtElapsed(pe.Elapsed)
				if pe.IsSubtest {
					result.tests = append(result.tests, CacheEntry{
						TestName:   pe.Test,
						Result:     ResultSkip,
						SkipReason: pe.SkipReason,
					})
				} else {
					out.TestSkip(pe.Test, elapsed, pe.SkipReason, false)
				}
			}
		},
		OnPackageDone: func(elapsed float64) {
			result.duration = time.Duration(elapsed * float64(time.Second))
		},
	})

	// Write end-run marker.
	writeEvent(f, TestEvent{Action: ActionEndRun})

	exitErr := cmd.Wait()

	if stderrBuf.Len() > 0 {
		debugf("go test stderr: %s", stderrBuf.String())
	}

	// Tests that started but never finished are assumed to have timed out.
	if exitErr != nil {
		for testName := range startedTests {
			out.TestFail(testName, "timeout", outFile, lineNum, false)
			result.tests = append(result.tests, CacheEntry{
				TestName: testName,
				Result:   ResultFail,
				FailLine: lineNum,
			})
		}
	}

	// Determine status from state tracked in the event callbacks.
	if exitErr != nil {
		result.status = ResultFail
	} else if !sawRun || (!sawPass && sawSkip) {
		result.status = ResultSkip
	} else {
		result.status = ResultPass
	}

	if result.status == ResultFail {
		hasFail := false
		for _, t := range result.tests {
			if t.Result == ResultFail {
				hasFail = true
				break
			}
		}
		if !hasFail {
			failLine := lastFailLn
			if failLine == 0 {
				failLine = 1
			}
			result.tests = append(result.tests, CacheEntry{
				TestName: short,
				Result:   ResultFail,
				FailLine: failLine,
			})
		}
	}

	return result
}
