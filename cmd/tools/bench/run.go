// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"
)

// resultSchemaVersion is bumped on a change that breaks a reader of Result.
const resultSchemaVersion = 1

// Run statuses.
const (
	// statusOK: the scenario ran and messages were measured.
	statusOK = "ok"
	// statusNoSamples: the scenario ran but no message was measured.
	statusNoSamples = "no_samples"
	// statusFailed: a prerequisite was missing or a task failed.
	statusFailed = "failed"
)

// Reasons the run phase ended.
const (
	stopExited      = "exited"
	stopIdle        = "idle"
	stopMaxDuration = "max_duration"
	stopInterrupted = "interrupted"
)

// stopGrace is how long a suite gets to shut down once interrupted. The
// benchmark processor logs its total stats during that time.
const stopGrace = 30 * time.Second

// Result is the document a run writes to result.json. It is the contract with
// whatever reads benchmark results: add fields freely, but bump
// resultSchemaVersion before renaming or removing one.
type Result struct {
	SchemaVersion int    `json:"schema_version"`
	Suite         string `json:"suite"`
	Component     string `json:"component"`
	Scenario      string `json:"scenario"`
	Status        string `json:"status"`
	// Error says what went wrong when the status is not ok.
	Error string `json:"error,omitempty"`

	GitSHA   string `json:"git_sha"`
	GitDirty bool   `json:"git_dirty"`

	// StartedAt and FinishedAt bound the run phase, without setup or teardown.
	StartedAt       time.Time `json:"started_at"`
	FinishedAt      time.Time `json:"finished_at"`
	DurationSeconds float64   `json:"duration_seconds"`
	// StopReason is why the run phase ended: exited, idle, max_duration or
	// interrupted.
	StopReason string `json:"stop_reason,omitempty"`

	Host ResultHost `json:"host"`
	// Vars are the Taskfile variables the run passed to the suite.
	Vars map[string]string `json:"vars,omitempty"`

	Summary Summary `json:"summary"`
	// Total is the lifetime average the benchmark processor logs at shutdown.
	// Unlike Summary it includes warm-up and idle time.
	Total   *Total   `json:"total,omitempty"`
	Samples []Sample `json:"samples,omitempty"`

	// ResultPath and LogPath are relative to the repository root.
	ResultPath string `json:"result_path,omitempty"`
	LogPath    string `json:"log_path,omitempty"`
}

// ResultHost describes the machine the benchmark ran on.
type ResultHost struct {
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	CPUs      int    `json:"cpus"`
	GoVersion string `json:"go_version"`
}

type runOptions struct {
	scenario    string
	outDir      string
	maxDuration time.Duration
	idleSamples int
	skipSetup   bool
	keep        bool
	vars        map[string]string
}

type varFlags map[string]string

func (varFlags) String() string { return "" }

func (v varFlags) Set(s string) error {
	k, val, ok := strings.Cut(s, "=")
	if !ok || !varRe.MatchString(k) {
		return fmt.Errorf("expected NAME=value with an upper-case name, got %q", s)
	}
	v[k] = val
	return nil
}

func cmdRun(args []string, stdout, stderr io.Writer) error {
	opts := runOptions{vars: map[string]string{}}
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	fs.StringVar(&opts.scenario, "scenario", "", "Scenario to run (default: the first one of the suite)")
	fs.StringVar(&opts.outDir, "out", "", "Directory for result.json and run.log (default: target/bench/<suite>/<scenario>/<timestamp>)")
	fs.DurationVar(&opts.maxDuration, "max-duration", 0, "Stop the run phase after this long (default: the scenario's max_duration, or 10m)")
	fs.IntVar(&opts.idleSamples, "idle-samples", 5, "Stop the run phase after this many samples in a row without a message, once messages have flowed (0 disables)")
	fs.BoolVar(&opts.skipSetup, "skip-setup", false, "Skip bench:up, bench:load and bench:down: measure again on a suite that is already up")
	fs.BoolVar(&opts.keep, "keep", false, "Skip bench:down so the suite stays up for another run or for inspection")
	fs.Var(varFlags(opts.vars), "var", "Taskfile variable NAME=value passed to every task, overrides the scenario's vars (repeatable)")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: bench run [flags] <suite>")
		fs.PrintDefaults()
	}
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New("expected exactly one suite: see 'bench list'")
	}

	root, err := repoRoot()
	if err != nil {
		return err
	}
	suites, err := discover(root)
	if err != nil {
		return err
	}
	suite, err := findSuite(suites, fs.Arg(0))
	if err != nil {
		return err
	}
	if !suite.Conformant {
		return fmt.Errorf("suite %s cannot be run by this tool yet: %s", suite.Dir, strings.Join(suite.Problems, "; "))
	}
	scenario, err := suite.scenario(opts.scenario)
	if err != nil {
		return err
	}

	// The first interrupt stops the measurement and still tears the suite
	// down; a second one falls back to the default and kills the tool.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		stop()
	}()

	res, err := runSuite(ctx, root, suite, scenario, opts, stderr)
	if err != nil {
		return err
	}
	summary := *res
	summary.Samples = nil
	if err := writeJSON(stdout, summary); err != nil {
		return err
	}
	if res.Status != statusOK {
		return fmt.Errorf("benchmark %s: %s", res.Status, res.Error)
	}
	return nil
}

// runSuite takes a scenario through the task interface and returns its
// result, which it has also written to the output directory. The error is
// only non-nil when the result could not be written at all.
func runSuite(ctx context.Context, root string, suite *Suite, scenario Scenario, opts runOptions, stderr io.Writer) (*Result, error) {
	vars := maps.Clone(scenario.Vars)
	if vars == nil {
		vars = map[string]string{}
	}
	maps.Copy(vars, opts.vars)

	maxDuration := opts.maxDuration
	if maxDuration <= 0 {
		maxDuration = scenario.maxDuration()
	}

	outDir := opts.outDir
	if outDir == "" {
		outDir = filepath.Join(root, "target", "bench", suite.Name, scenario.Name, time.Now().UTC().Format("20060102T150405Z"))
	}
	outDir, err := filepath.Abs(outDir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, err
	}
	resultPath := filepath.Join(outDir, "result.json")
	logPath := filepath.Join(outDir, "run.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	defer logFile.Close()

	res := &Result{
		SchemaVersion: resultSchemaVersion,
		Suite:         suite.Name,
		Component:     suite.Component,
		Scenario:      scenario.Name,
		Status:        statusFailed,
		Host: ResultHost{
			OS:        runtime.GOOS,
			Arch:      runtime.GOARCH,
			CPUs:      runtime.NumCPU(),
			GoVersion: runtime.Version(),
		},
		Vars:       vars,
		ResultPath: relPath(root, resultPath),
		LogPath:    relPath(root, logPath),
	}
	res.GitSHA, res.GitDirty = gitState(root)

	r := &runner{
		dir:  filepath.Join(root, filepath.FromSlash(suite.Dir)),
		vars: vars,
		out:  io.MultiWriter(stderr, logFile),
	}

	res.StartedAt = time.Now().UTC()
	res.FinishedAt = res.StartedAt
	if err := r.execute(ctx, suite, scenario, opts, maxDuration, res); err != nil {
		res.Status = statusFailed
		res.Error = err.Error()
	}

	raw, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(resultPath, append(raw, '\n'), 0o644); err != nil {
		return nil, err
	}
	return res, nil
}

type runner struct {
	dir  string
	vars map[string]string
	out  io.Writer
}

func (r *runner) execute(ctx context.Context, suite *Suite, scenario Scenario, opts runOptions, maxDuration time.Duration, res *Result) (err error) {
	if err := preflight(ctx, suite.Requires); err != nil {
		return err
	}

	if !opts.skipSetup {
		if !opts.keep {
			// Teardown runs whatever happened, also after an interrupt, so
			// it does not use the run's context.
			defer func() {
				if downErr := r.task(context.WithoutCancel(ctx), taskDown); downErr != nil && err == nil {
					err = downErr
				}
			}()
		}
		for _, name := range []string{taskUp, taskLoad} {
			if err := r.task(ctx, name); err != nil {
				return err
			}
		}
	}
	if err := r.task(ctx, taskReset); err != nil {
		return err
	}

	stats, reason, runErr := r.measure(ctx, scenario.runTask(), maxDuration, opts.idleSamples, res)
	res.StopReason = reason
	res.Samples = stats.samples
	res.Total = stats.total
	res.Summary = summarise(stats.samples)
	if runErr != nil {
		return runErr
	}
	if res.Summary.Samples == 0 {
		res.Status = statusNoSamples
		res.Error = "the run logged no rolling stats with messages: check that the suite's config has a benchmark processor and that data was loaded"
		return nil
	}
	res.Status = statusOK
	return nil
}

func (r *runner) taskCmd(name string) *exec.Cmd {
	args := []string{"--dir", r.dir, name}
	for _, k := range slices.Sorted(maps.Keys(r.vars)) {
		args = append(args, k+"="+r.vars[k])
	}
	cmd := exec.Command("task", args...)
	setProcessGroup(cmd)
	return cmd
}

// task runs a setup or teardown task to completion.
func (r *runner) task(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("interrupted before task %s", name)
	}
	fmt.Fprintf(r.out, "==> task %s\n", name)
	cmd := r.taskCmd(name)
	cmd.Stdout = r.out
	cmd.Stderr = r.out
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("task %s: %w", name, err)
	}
	var waitErr error
	done := make(chan struct{})
	go func() {
		waitErr = cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
		if waitErr != nil {
			return fmt.Errorf("task %s: %w", name, waitErr)
		}
		return nil
	case <-ctx.Done():
		killProcess(cmd)
		<-done
		return fmt.Errorf("interrupted during task %s", name)
	}
}

// measure runs the scenario's task and collects the rolling stats it logs.
// Most suites read a source that never ends, so the run stops by itself only
// rarely: it is interrupted once the source is drained (idle) or after the
// maximum duration, and the suite then shuts down and logs its total stats.
func (r *runner) measure(ctx context.Context, name string, maxDuration time.Duration, idleSamples int, res *Result) (*statsCollector, string, error) {
	stats := &statsCollector{}
	fmt.Fprintf(r.out, "==> task %s (measuring for up to %s)\n", name, maxDuration)

	pr, pw, err := os.Pipe()
	if err != nil {
		return stats, "", err
	}
	cmd := r.taskCmd(name)
	cmd.Stdout = pw
	cmd.Stderr = pw
	res.StartedAt = time.Now().UTC()
	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		return stats, "", fmt.Errorf("task %s: %w", name, err)
	}
	pw.Close()

	lines := make(chan string, 256)
	go func() {
		defer close(lines)
		br := bufio.NewReader(pr)
		for {
			line, err := br.ReadString('\n')
			if line != "" {
				lines <- line
			}
			if err != nil {
				return
			}
		}
	}()
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()

	deadline := time.NewTimer(maxDuration)
	defer deadline.Stop()
	var grace <-chan time.Time
	interrupted := ctx.Done()

	reason := ""
	stop := func(why string) {
		if reason != "" {
			return
		}
		reason = why
		fmt.Fprintf(r.out, "==> stopping task %s: %s\n", name, why)
		interruptProcess(cmd)
		grace = time.After(stopGrace)
	}

	var waitErr error
	for lines != nil || exited != nil {
		select {
		case line, ok := <-lines:
			if !ok {
				lines = nil
				continue
			}
			_, _ = io.WriteString(r.out, line)
			if stats.add(line) && idleSamples > 0 && stats.trailingIdle() >= idleSamples {
				stop(stopIdle)
			}
		case waitErr = <-exited:
			exited = nil
			res.FinishedAt = time.Now().UTC()
			// Whatever still holds the pipe open is a stray child: the
			// output that matters is in by now.
			time.AfterFunc(2*time.Second, func() { pr.Close() })
		case <-deadline.C:
			stop(stopMaxDuration)
		case <-interrupted:
			interrupted = nil
			stop(stopInterrupted)
		case <-grace:
			grace = nil
			fmt.Fprintf(r.out, "==> task %s did not stop within %s, killing it\n", name, stopGrace)
			killProcess(cmd)
		}
	}
	pr.Close()
	res.DurationSeconds = res.FinishedAt.Sub(res.StartedAt).Seconds()

	switch reason {
	case "":
		reason = stopExited
		if waitErr != nil {
			return stats, reason, fmt.Errorf("task %s: %w", name, waitErr)
		}
	case stopInterrupted:
		return stats, reason, fmt.Errorf("interrupted during task %s", name)
	}
	return stats, reason, nil
}

// preflight reports every missing prerequisite at once, before anything is
// started.
func preflight(ctx context.Context, req Requires) error {
	var missing []string
	tools := req.Tools
	if !slices.Contains(tools, "task") {
		tools = append([]string{"task"}, tools...)
	}
	for _, tool := range tools {
		if _, err := exec.LookPath(tool); err != nil {
			missing = append(missing, "tool "+tool+" is not on PATH")
			continue
		}
		if tool == "docker" {
			ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
			err := exec.CommandContext(ctx, "docker", "info").Run()
			cancel()
			if err != nil {
				missing = append(missing, "the Docker daemon is not reachable")
			}
		}
	}
	for _, name := range req.Env {
		if os.Getenv(name) == "" {
			missing = append(missing, "environment variable "+name+" is not set")
		}
	}
	if len(missing) > 0 {
		return errors.New("missing prerequisites: " + strings.Join(missing, "; "))
	}
	return nil
}

func gitState(root string) (sha string, dirty bool) {
	out, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		return "", false
	}
	status, _ := exec.Command("git", "-C", root, "status", "--porcelain").Output()
	return strings.TrimSpace(string(out)), len(status) > 0
}

func relPath(root, path string) string {
	if rel, err := filepath.Rel(root, path); err == nil && !strings.HasPrefix(rel, "..") {
		return filepath.ToSlash(rel)
	}
	return path
}

// cmdParse summarises a Connect log that was captured some other way, such as
// a run of a suite that has no manifest yet.
func cmdParse(args []string, stdin io.Reader, stdout io.Writer) error {
	fs := flag.NewFlagSet("parse", flag.ExitOnError)
	withSamples := fs.Bool("samples", false, "Include every sample, not only the summary")
	_ = fs.Parse(args)

	stats, err := parseStats(stdin)
	if err != nil {
		return err
	}
	out := struct {
		Summary Summary  `json:"summary"`
		Total   *Total   `json:"total,omitempty"`
		Samples []Sample `json:"samples,omitempty"`
	}{Summary: summarise(stats.samples), Total: stats.total}
	if *withSamples {
		out.Samples = stats.samples
	}
	return writeJSON(stdout, out)
}
