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

	"github.com/redpanda-data/connect/v4/cmd/tools/integration/llmfix"
)

const fixPollInterval = 15 * time.Second

var runArgs struct {
	outputDir      string
	clean          bool
	debug          bool
	race           bool
	unit           bool
	coverProfile   bool
	fix            bool
	fixMaxParallel int
	fixTimeout     time.Duration
	loop           int
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
	fset.BoolVar(&runArgs.fix, "fix", false, "triage and fix failed packages using Claude agents")
	fset.IntVar(&runArgs.fixMaxParallel, "fix-max-parallel", 4, "max parallel fix agents (requires --fix)")
	fset.DurationVar(&runArgs.fixTimeout, "fix-timeout", 30*time.Minute, "timeout per fix agent run (requires --fix)")
	fset.IntVar(&runArgs.loop, "loop", 0, "number of successful clean iterations required (0 = single run)")

	// Go's flag package stops parsing at the first non-flag argument.
	// Separate flags from positional filters so interspersed usage works:
	//   run --fix --race amqp1 --debug
	flags, filters := splitFlagsAndArgs(fset, args)
	if err := fset.Parse(flags); err != nil {
		return err
	}
	filters = append(filters, fset.Args()...)

	if runArgs.outputDir != "" && runArgs.clean {
		return errors.New("--output-dir and --clean are mutually exclusive")
	}

	if runArgs.debug {
		log.SetOutput(os.Stderr)
	}

	packages := filterPackages(allPackages, filters)
	if len(packages) == 0 {
		return fmt.Errorf("no packages match filter: %v", filters)
	}

	loopCount := runArgs.loop
	if loopCount <= 0 {
		loopCount = 1
	}

	outputDir := resolveOutputDir(runArgs.outputDir, runArgs.clean)
	clean := runArgs.clean
	for success := 0; success < loopCount; {
		if runArgs.loop > 0 {
			log.Printf("loop: starting iteration (success %d/%d)", success, runArgs.loop)
		}

		failed, err := runIteration(packages, outputDir, clean)
		if err != nil {
			return err
		}

		if failed > 0 {
			if runArgs.loop <= 0 {
				return errors.New("integration tests failed")
			}
			// Retry in same dir — agents may have applied fixes.
			clean = false
			continue
		}

		success++
		if runArgs.loop > 0 {
			log.Printf("loop: iteration succeeded (success %d/%d)", success, runArgs.loop)
		}

		if runArgs.outputDir != "" {
			// Fixed output dir — can't iterate further, cache would make it a no-op.
			break
		}
		// Fresh dir for next iteration.
		outputDir = resolveRunDir()
		clean = false
	}
	return nil
}

func resolveOutputDir(explicit string, clean bool) string {
	if explicit != "" {
		debugf("using provided output dir: %s", explicit)
		return explicit
	}
	if !clean {
		if dir := findLatestRunDir(".integration"); dir != "" {
			debugf("resuming run dir: %s", dir)
			return dir
		}
	}
	dir := resolveRunDir()
	debugf("new run dir: %s", dir)
	return dir
}

// runIteration executes a single test run in the given output directory.
// Returns the number of failed packages and any non-recoverable error.
// When --fix is enabled, it always waits for agents to finish before returning.
func runIteration(packages []TestPackage, outputDir string, clean bool) (int, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return 0, fmt.Errorf("creating run directory: %w", err)
	}

	debugf("resolved run dir: %s", outputDir)

	out := NewOutput(os.Stdout, filepath.Join(outputDir, "index.md"))

	var mgr *llmfix.Manager
	if runArgs.fix {
		baseSHA, err := resolveHEAD()
		if err != nil {
			return 0, fmt.Errorf("initializing fix manager: %w", err)
		}
		var mgrErr error
		mgr, mgrErr = llmfix.NewManager(outputDir, baseSHA, runArgs.fixMaxParallel)
		if mgrErr != nil {
			return 0, fmt.Errorf("initializing fix manager: %w", mgrErr)
		}
		defer mgr.Close()
		for _, r := range mgr.RecoverWorktrees() {
			if r.Err != nil {
				out.Error(fmt.Sprintf("  recover %s: %v", r.Name, r.Err))
			}
			for _, c := range r.Commits {
				out.Info("  recovered: " + c)
			}
		}

		// Retry fix agents that failed in the previous run.
		if !clean {
			pending := mgr.PendingSlugs()
			if len(pending) > 0 {
				out.Info(fmt.Sprintf("Retrying %d failed fix agent(s)...", len(pending)))
				for slug, pkgPath := range pending {
					outFile := filepath.Join(outputDir, slug+".txt")
					cached := checkCache(outFile)
					if cached.Overall() != ResultFail {
						continue
					}
					testOutput := dumpTestOutput(outFile, cached.Tests)
					if testOutput == "" {
						continue
					}
					mgr.Dispatch(slug, llmfix.FixRequest{
						PkgPath:    pkgPath,
						TestOutput: testOutput,
						Timeout:    runArgs.fixTimeout,
					})
					out.Info("  " + pkgShort(pkgPath))
				}
				out.Blank()
			}
		}
	}

	total := len(packages)

	headerLabel := "Integration Tests"
	if runArgs.unit {
		headerLabel = "All Tests"
	}
	out.Header(fmt.Sprintf("%s (%d packages)", headerLabel, total))
	out.Info("Output: " + outputDir)
	out.Blank()

	var (
		passed  int
		failed  int
		skipped int
		run     int
	)

	for len(packages) > 0 {
		var pending []TestPackage
		for _, pkg := range packages {
			slug := pkgSlug(pkg.Path)
			short := pkgShort(pkg.Path)
			fname := pkgFilename(pkg.Path)
			pkgOutFile := filepath.Join(outputDir, fname)
			tag := strings.TrimSuffix(fname, ".txt")

			if mgr != nil && mgr.IsFixing(slug) {
				pending = append(pending, pkg)
				continue
			}

			run++
			out.PackageHeader(tag, run, total, short)

			var cached PackageCache
			if !clean {
				debugf("checking cache for %s at %s", short, pkgOutFile)
				cached = checkCache(pkgOutFile)
			}

			for _, t := range compactCacheEntries(cached.Tests) {
				switch t.Result {
				case ResultPass:
					out.TestPass(t.TestName, "", true)
				case ResultSkip:
					out.TestSkip(t.TestName, "", t.SkipReason, true)
				case ResultFail:
					out.TestFail(t.TestName, "", pkgOutFile, t.FailLine, true)
				}
			}

			if cached.Complete && cached.Overall() == ResultPass {
				debugf("found cached result for %s: complete", short)
				out.Info("● " + short + " (cached)")
				out.PackageDone()
				passed++
				continue
			}

			res := runPackageTest(pkg, short, pkgOutFile, cached, out)

			switch res.status {
			case ResultPass:
				passed++
			case ResultFail:
				failed++
			case ResultSkip:
				skipped++
			}
			out.PackageResult(short, res.status, res.duration)

			if runArgs.fix && res.status == ResultFail {
				mgr.Dispatch(slug, llmfix.FixRequest{
					PkgPath:    pkg.Path,
					TestOutput: dumpTestOutput(res.outFile, res.tests),
					Timeout:    runArgs.fixTimeout,
				})
				out.Info(fmt.Sprintf("  fixing %s → %s", short, filepath.Join(outputDir, "fix", "agents.log")))
			}

			dockerCleanup()
		}
		if len(pending) == 0 {
			break
		}
		out.Info(fmt.Sprintf("Waiting for %d package(s) being fixed...", len(pending)))
		time.Sleep(fixPollInterval)
		packages = pending
	}

	// Final report.
	sep := strings.Repeat("=", 40)
	out.Info(sep)
	out.Summary(passed, failed, skipped)
	out.Info(sep)

	if mgr != nil {
		mgr.Wait()
	}
	return failed, nil
}

type packageResult struct {
	status   TestResult
	duration time.Duration
	outFile  string
	tests    []CacheEntry
}

// runPackageTest executes `go test -json` for a single package, parses JSON
// events, streams filtered output, and returns the result.
func runPackageTest(pkg TestPackage, short, outFile string, cached PackageCache, out *Output) packageResult {
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
		runRegex = "^Test"
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
	cmd := exec.Command("go", goArgs...)
	cmd.Env = os.Environ()
	if runArgs.race {
		cmd.Env = append(cmd.Env, "CGO_ENABLED=1")
	}

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
