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
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

var runArgs struct {
	clean  bool
	debug  bool
	race   bool
	filter []string
}

func debugf(format string, args ...any) {
	if runArgs.debug {
		log.Printf("[debug] "+format, args...)
	}
}

func cmdRun(args []string) error {
	fset := flag.NewFlagSet("run", flag.ExitOnError)
	fset.BoolVar(&runArgs.clean, "clean", false, "ignore cache, start a fresh run")
	fset.BoolVar(&runArgs.debug, "debug", false, "enable debug logging to stderr")
	fset.BoolVar(&runArgs.race, "race", false, "enable race detector (sets CGO_ENABLED=1)")

	if err := fset.Parse(args); err != nil {
		return err
	}

	if runArgs.debug {
		log.SetOutput(os.Stderr)
	}

	filters := fset.Args()

	packages := filterPackages(allPackages, filters)
	if len(packages) == 0 {
		return fmt.Errorf("no packages match filter: %v", filters)
	}

	// Resume from the latest previous run directory, or create a new one.
	runDir := ""
	if !runArgs.clean {
		runDir = findLatestRunDir(".integration")
		debugf("resuming run dir: %s", runDir)
	}
	if runDir == "" {
		runDir = resolveRunDir()
		debugf("new run dir: %s", runDir)
	}
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return fmt.Errorf("creating run directory: %w", err)
	}

	ts := filepath.Base(runDir)
	debugf("resolved run dir: %s", runDir)

	indexPath := filepath.Join(runDir, "index-"+ts+".md")
	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("creating index file: %w", err)
	}
	defer indexFile.Close()

	out := NewOutput(os.Stdout, indexFile)
	total := len(packages)

	out.Header(fmt.Sprintf("Integration Tests (%d packages)", total))
	out.Dim("Output: " + runDir)
	out.Blank()

	var (
		passed  int
		failed  int
		skipped int
	)

	for i, pkg := range packages {
		short := pkgShort(pkg.Path)
		fname := pkgFilename(pkg.Path)
		pkgOutFile := filepath.Join(runDir, fname)

		tag := strings.TrimSuffix(fname, ".txt")
		out.OpenPackage(tag, i+1, total, short)

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
				out.Dim(fmt.Sprintf("  PASS %s (cached)", t.TestName))
			case ResultSkip:
				if t.SkipReason != "" {
					out.Dim(fmt.Sprintf("  SKIP %s (cached): %s", t.TestName, t.SkipReason))
				} else {
					out.Dim(fmt.Sprintf("  SKIP %s (cached)", t.TestName))
				}
			case ResultFail:
				out.Red(fmt.Sprintf("  FAIL %s (cached) -> %s:%d", t.TestName, pkgOutFile, t.FailLine))
			}
		}

		// If complete and all passed, close and move on.
		if cached.Complete && cached.Overall() == ResultPass {
			debugf("found cached result for %s: complete", short)
			out.Dim(fmt.Sprintf("● %s (cached)", short))
			out.Blank()
			passed++
			out.ClosePackage(tag)
			continue
		}

		// Incomplete — run remaining tests, skipping already completed ones.
		skipRegex := completedTestSkipRegex(cached.Tests)

		res := runPackageTest(pkg.Path, short, pkgOutFile, ts,
			"^Test.*Integration", skipRegex, pkg.TimeoutStr(), out)

		switch res.status {
		case ResultPass:
			passed++
			out.Green(fmt.Sprintf("✓ %s%s", short, fmtDuration(res.duration)))
		case ResultFail:
			failed++
			out.Red(fmt.Sprintf("✗ %s%s", short, fmtDuration(res.duration)))
		case ResultSkip:
			skipped++
			out.Yellow(fmt.Sprintf("⊘ %s (skipped)", short))
		}

		out.Blank()
		out.ClosePackage(tag)
		dockerCleanup()
	}

	// Final report.
	sep := strings.Repeat("=", 40)
	out.Header(sep)
	out.Summary(passed, failed, skipped)
	out.Header(sep)

	if failed > 0 {
		return errors.New("integration tests failed")
	}
	return nil
}

type packageResult struct {
	status   TestResult
	duration time.Duration
	outFile  string
	tests    []CacheEntry
}

// pendingSkip holds buffered SKIP output waiting for the reason line.
type pendingSkip struct {
	testName string
	elapsed  string
}

type eventKind int

const (
	eventOther      eventKind = iota
	eventRun                  // === RUN
	eventPass                 // --- PASS
	eventFail                 // --- FAIL
	eventSkip                 // --- SKIP
	eventSkipReason           // indented line after SKIP with reason
)

// lineEvent represents a parsed event from a go test -v output line.
type lineEvent struct {
	kind     eventKind
	testName string
	elapsed  string
	reason   string // skip reason (only for eventSkipReason)
	lineNum  int
	subtest  bool // true if testName contains /
}

// parseLine parses a single go test -v output line and returns a lineEvent.
// pending is the current pending SKIP waiting for a reason line; it may be nil.
func parseLine(line string, lineNum int, pending *pendingSkip) lineEvent {
	if pending != nil {
		reason := extractSkipReason(line)
		return lineEvent{
			kind:     eventSkipReason,
			testName: pending.testName,
			elapsed:  pending.elapsed,
			reason:   reason,
			lineNum:  lineNum,
		}
	}

	if m := reRun.FindStringSubmatch(line); m != nil {
		return lineEvent{kind: eventRun, testName: m[1], lineNum: lineNum, subtest: isSubtest(m[1])}
	}
	if m := rePass.FindStringSubmatch(line); m != nil {
		return lineEvent{kind: eventPass, testName: m[1], elapsed: m[2], lineNum: lineNum, subtest: isSubtest(m[1])}
	}
	if m := reFail.FindStringSubmatch(line); m != nil {
		return lineEvent{kind: eventFail, testName: m[1], elapsed: m[2], lineNum: lineNum, subtest: isSubtest(m[1])}
	}
	if m := reSkip.FindStringSubmatch(line); m != nil {
		return lineEvent{kind: eventSkip, testName: m[1], elapsed: m[2], lineNum: lineNum, subtest: isSubtest(m[1])}
	}

	return lineEvent{kind: eventOther, lineNum: lineNum}
}

// runPackageTest executes go test for a single package, streams filtered output,
// and returns the result.
func runPackageTest(pkg, short, outFile, timestamp, runRegex, skipRegex, timeout string, out *Output) packageResult {
	result := packageResult{
		outFile: outFile,
	}

	// Preserve existing content so cached line numbers stay valid.
	var lineNum int
	existingData, err := os.ReadFile(outFile)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		out.Red(fmt.Sprintf("  failed to read existing output file: %v", err))
		result.status = ResultFail
		return result
	}
	if len(existingData) > 0 {
		lineNum = bytes.Count(existingData, []byte{'\n'})
	}

	f, err := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		out.Red(fmt.Sprintf("  failed to open output file: %v", err))
		result.status = ResultFail
		return result
	}
	defer f.Close()

	if len(existingData) > 0 && !strings.HasSuffix(string(existingData), "\n") {
		fmt.Fprintln(f)
		lineNum++
	}

	fmt.Fprintf(f, "<results timestamp=%q package=%q>\n", timestamp, pkg)
	lineNum++

	debugf("starting go test for %s with -run %q -skip %q", short, runRegex, skipRegex)

	goArgs := []string{"test", "-v", "-count=1", "-shuffle=on", "-tags", "integration", "-timeout", timeout, "-run", runRegex}
	if runArgs.race {
		goArgs = append(goArgs, "-race")
	}
	if skipRegex != "" {
		goArgs = append(goArgs, "-skip", skipRegex)
	}
	goArgs = append(goArgs, pkg)
	cmd := exec.Command("go", goArgs...)
	cmd.Env = os.Environ()
	if runArgs.race {
		cmd.Env = append(cmd.Env, "CGO_ENABLED=1")
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		out.Red(fmt.Sprintf("  failed to create stdout pipe: %v", err))
		result.status = ResultFail
		return result
	}
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		out.Red(fmt.Sprintf("  failed to start: %v", err))
		result.status = ResultFail
		return result
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
	var pending *pendingSkip
	hasSubtests := make(map[string]struct{})
	startedTests := make(map[string]struct{}) // top-level tests that started but haven't finished

	// Track state incrementally to avoid re-reading the output file.
	var (
		sawRun     bool
		sawPass    bool
		sawSkip    bool
		lastFailLn int
		lastTiming time.Duration
	)

	for scanner.Scan() {
		line := scanner.Text()
		lineNum++
		fmt.Fprintln(f, line)

		ev := parseLine(line, lineNum, pending)
		pending = nil

		if ev.subtest {
			hasSubtests[parentTest(ev.testName)] = struct{}{}
		}

		// Track timing from ok/FAIL summary lines.
		if m := reOKTime.FindStringSubmatch(line); m != nil {
			if d, err := time.ParseDuration(m[1]); err == nil {
				lastTiming = d
			}
		} else if m := reFAILTime.FindStringSubmatch(line); m != nil {
			if d, err := time.ParseDuration(m[1]); err == nil {
				lastTiming = d
			}
		}

		switch ev.kind {
		case eventSkipReason:
			if !isSubtest(ev.testName) {
				delete(startedTests, ev.testName)
				if ev.reason != "" {
					out.Yellow(fmt.Sprintf("  SKIP %s (%s): %s", ev.testName, ev.elapsed, ev.reason))
				} else {
					out.Yellow(fmt.Sprintf("  SKIP %s (%s)", ev.testName, ev.elapsed))
				}
			}
		case eventRun:
			sawRun = true
			if !ev.subtest {
				startedTests[ev.testName] = struct{}{}
				out.Dim("  RUN  " + ev.testName)
			}
		case eventPass:
			sawPass = true
			delete(startedTests, ev.testName)
			// Skip parent summary when subtests exist.
			if _, ok := hasSubtests[ev.testName]; !ev.subtest && ok {
				out.Green(fmt.Sprintf("  PASS %s (%s)", ev.testName, ev.elapsed))
				break
			}
			result.tests = append(result.tests, CacheEntry{
				TestName: ev.testName,
				Result:   ResultPass,
			})
			if !ev.subtest {
				out.Green(fmt.Sprintf("  PASS %s (%s)", ev.testName, ev.elapsed))
			}
		case eventFail:
			delete(startedTests, ev.testName)
			if err := f.Sync(); err != nil {
				debugf("failed to sync output file %s: %v", outFile, err)
			}
			lastFailLn = lineNum
			// Skip parent summary when subtests exist.
			if _, ok := hasSubtests[ev.testName]; !ev.subtest && ok {
				out.Red(fmt.Sprintf("  FAIL %s (%s)", ev.testName, ev.elapsed))
				break
			}
			result.tests = append(result.tests, CacheEntry{
				TestName: ev.testName,
				Result:   ResultFail,
				FailLine: lineNum,
			})
			out.Red(fmt.Sprintf("  FAIL %s (%s) -> %s:%d", ev.testName, ev.elapsed, outFile, lineNum))
		case eventSkip:
			sawSkip = true
			delete(startedTests, ev.testName)
			if ev.subtest {
				result.tests = append(result.tests, CacheEntry{
					TestName: ev.testName,
					Result:   ResultSkip,
				})
			} else {
				pending = &pendingSkip{testName: ev.testName, elapsed: ev.elapsed}
			}
		}
	}

	if pending != nil {
		out.Yellow(fmt.Sprintf("  SKIP %s (%s)", pending.testName, pending.elapsed))
	}

	fmt.Fprintln(f, "</results>")

	exitErr := cmd.Wait()

	// Tests that started but never finished are assumed to have timed out.
	if exitErr != nil {
		for testName := range startedTests {
			out.Red(fmt.Sprintf("  FAIL %s (timeout) -> %s:%d", testName, outFile, lineNum))
			result.tests = append(result.tests, CacheEntry{
				TestName: testName,
				Result:   ResultFail,
				FailLine: lineNum,
			})
		}
	}

	// Determine status from state tracked in the scanner loop.
	result.duration = lastTiming
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
