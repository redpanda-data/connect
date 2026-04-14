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

package llmfix

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// FixRequest describes a single package fix job dispatched to a triage+fix agent pair.
type FixRequest struct {
	// Tag is a unique identifier for this fix attempt, used as branch name and log file prefix.
	Tag string
	// PkgPath is the Go package path being fixed.
	PkgPath string
	// TestOutput is pre-rendered test failure output passed to the triage agent.
	TestOutput string
	// OutputDir is the directory for triage JSON and agent logs.
	OutputDir string
	// WorktreeDir is the git worktree where the fix agent runs.
	WorktreeDir string
	// Timeout is the maximum duration for the fix agent run. Zero means no timeout.
	Timeout time.Duration
}

// Operator runs the triage+fix pipeline for a single package.
// Worktree lifecycle and cherry-picking are the caller's responsibility.
type Operator struct {
	req FixRequest
	log *log.Logger
}

// NewOperator creates an Operator for the given request.
func NewOperator(req FixRequest, l *log.Logger) *Operator {
	return &Operator{
		req: req,
		log: l,
	}
}

// Run executes the triage agent followed by the fix agent.
func (op *Operator) Run() error {
	if op.req.TestOutput == "" {
		return errors.New("no failure data")
	}

	if err := os.MkdirAll(filepath.Join(op.req.OutputDir, "fix"), 0o755); err != nil {
		return fmt.Errorf("creating fix dir: %w", err)
	}

	ctx := context.Background()
	if op.req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, op.req.Timeout)
		defer cancel()
	}

	op.log.Printf("triaging %s", op.req.PkgPath)

	triageStart := time.Now()
	triage, err := op.runTriageAgent(ctx, op.req.TestOutput)
	if err != nil {
		return fmt.Errorf("triage: %w", err)
	}
	op.log.Printf("triage completed in %s, found %d issue(s)", time.Since(triageStart).Truncate(time.Second), len(triage.Issues))

	for _, failure := range triage.Issues {
		label := failure.Type
		if failure.JiraKey != "" {
			label += " " + failure.JiraKey
		}
		op.log.Printf("  [%s] %s: %s", label, failure.Test, failure.Description)
	}

	if len(triage.Issues) == 0 {
		return nil
	}

	op.log.Printf("running fix agent for %d issue(s) in %s", len(triage.Issues), op.req.WorktreeDir)

	fixStart := time.Now()
	if err := op.runFixAgent(ctx, buildFixPrompt(op.req.PkgPath, triage.Issues, op.req.TestOutput)); err != nil {
		return fmt.Errorf("fix: %w", err)
	}
	op.log.Printf("fix agent completed in %s", time.Since(fixStart).Truncate(time.Second))

	return nil
}

func (op *Operator) runTriageAgent(ctx context.Context, testOutputDump string) (triageResult, error) {
	args := []string{
		"-p",
		"--model", "sonnet",
		"--output-format", "json",
		"--json-schema", triageResultSchema,
	}
	cmd := exec.CommandContext(ctx, "claude", args...)
	cmd.Stdin = strings.NewReader(triagePrompt + "\n\n" + testOutputDump)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	var res triageResult

	if err := cmd.Run(); err != nil {
		return res, fmt.Errorf("claude triage: %w\nstderr: %s", err, stderr.String())
	}

	var env claudeEnvelope
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		return res, fmt.Errorf("parsing claude response envelope: %w\nraw: %s", err, stdout.String())
	}
	if env.IsError {
		return res, fmt.Errorf("claude returned error: %s", env.Result)
	}
	if len(env.StructuredOutput) == 0 || bytes.Equal(env.StructuredOutput, []byte("null")) {
		return res, fmt.Errorf("claude returned no structured output (result: %q, stop_reason: %s, terminal_reason: %s)",
			env.Result, env.StopReason, env.TerminalReason)
	}
	op.log.Printf("triage agent: cost=$%.4f, turns=%d, duration=%dms, session=%s",
		env.TotalCostUSD, env.NumTurns, env.DurationMS, env.SessionID)

	if err := json.Unmarshal(env.StructuredOutput, &res); err != nil {
		return res, fmt.Errorf("parsing triage output: %w\nraw: %s", err, string(env.StructuredOutput))
	}

	triageFile, err := os.Create(op.outPath("-triage.json"))
	if err != nil {
		return res, fmt.Errorf("creating triage output file: %w", err)
	}
	defer triageFile.Close()
	enc := json.NewEncoder(triageFile)
	enc.SetIndent("", "  ")
	if err := enc.Encode(res); err != nil {
		return res, fmt.Errorf("writing triage output: %w", err)
	}
	op.log.Printf("triage: %s", op.outPath("-triage.json"))

	return res, nil
}

func (op *Operator) runFixAgent(ctx context.Context, prompt string) error {
	args := []string{
		"-p",
		"--model", "opus",
		"--output-format", "stream-json",
		"--effort", "high",
		"--verbose",
		"--dangerously-skip-permissions",
	}
	cmd := exec.CommandContext(ctx, "claude", args...)
	cmd.Stdin = strings.NewReader(fixPrompt + "\n\n" + prompt)
	cmd.Dir = op.req.WorktreeDir

	stdoutFile, err := os.Create(op.outPath(".jsonl"))
	if err != nil {
		return fmt.Errorf("creating stdout log: %w", err)
	}
	defer stdoutFile.Close()

	stderrFile, err := os.Create(op.outPath(".stderr"))
	if err != nil {
		return fmt.Errorf("creating stderr log: %w", err)
	}
	defer stderrFile.Close()

	cmd.Stdout = stdoutFile
	cmd.Stderr = stderrFile

	if err := cmd.Run(); err != nil {
		stderrContent, _ := os.ReadFile(stderrFile.Name())
		return fmt.Errorf("claude fix: %w\nstderr: %s", err, string(stderrContent))
	}

	return nil
}

func (op *Operator) outPath(suffix string) string {
	p, _ := filepath.Abs(filepath.Join(op.req.OutputDir, "fix", op.req.Tag+suffix))
	return p
}

func buildFixPrompt(pkgPath string, issues []issue, testOutputDump string) string {
	var buf strings.Builder
	buf.WriteString("# Package ")
	buf.WriteString(pkgPath)
	buf.WriteString("\n\n")

	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	_ = enc.Encode(issues)
	buf.WriteString("\n\n")

	buf.WriteString("## Test Failure Output\n\n")
	buf.WriteString(testOutputDump)
	return buf.String()
}
