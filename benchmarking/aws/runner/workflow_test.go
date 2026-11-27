// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// The workflows live at the repo root; this package is benchmarking/aws/runner.
var workflowDir = filepath.Join("..", "..", "..", ".github", "workflows")

type wfStep struct {
	ID   string            `yaml:"id"`
	Name string            `yaml:"name"`
	If   string            `yaml:"if"`
	Uses string            `yaml:"uses"`
	Run  string            `yaml:"run"`
	Env  map[string]string `yaml:"env"`
	With map[string]any    `yaml:"with"`
}

type wfJob struct {
	TimeoutMinutes int            `yaml:"timeout-minutes"`
	Concurrency    any            `yaml:"concurrency"`
	Env            map[string]any `yaml:"env"`
	Steps          []wfStep       `yaml:"steps"`
}

type workflow struct {
	raw         map[string]any
	On          map[string]any    `yaml:"on"`
	Concurrency any               `yaml:"concurrency"`
	Permissions map[string]string `yaml:"permissions"`
	Jobs        map[string]wfJob  `yaml:"jobs"`
}

func loadWorkflow(t *testing.T, name string) workflow {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(workflowDir, name))
	require.NoError(t, err)
	var w workflow
	require.NoError(t, yaml.Unmarshal(raw, &w), name)
	require.NoError(t, yaml.Unmarshal(raw, &w.raw), name)
	return w
}

// stepText is everything in a step an expression can appear in.
func stepText(t *testing.T, s wfStep) string {
	t.Helper()
	b, err := yaml.Marshal(s)
	require.NoError(t, err)
	return string(b)
}

// stepIndex returns the index of the step with id, failing if absent.
func stepIndex(t *testing.T, steps []wfStep, id string) int {
	t.Helper()
	for i, s := range steps {
		if s.ID == id {
			return i
		}
	}
	require.Failf(t, "step missing", "no step with id %q", id)
	return -1
}

// firstRunIndex returns the first step whose run script contains sub.
func firstRunIndex(steps []wfStep, sub string) int {
	for i, s := range steps {
		if strings.Contains(s.Run, sub) {
			return i
		}
	}
	return -1
}

func requireAlways(t *testing.T, s wfStep) {
	t.Helper()
	require.True(t, strings.HasPrefix(strings.TrimSpace(s.If), "always()"), "step %q must run if: always(), has %q", s.ID, s.If)
}

func benchBotJob(t *testing.T) (workflow, wfJob) {
	t.Helper()
	w := loadWorkflow(t, "bench_bot.yml")
	require.Len(t, w.Jobs, 1, "bench_bot.yml is one job")
	for _, j := range w.Jobs {
		return w, j
	}
	return w, wfJob{}
}

func TestWorkflowBenchBotShape(t *testing.T) {
	w, job := benchBotJob(t)

	// The shared-stack lock, not a concurrency group, serialises runs: a
	// group would cancel queued dispatches and silently drop approvals.
	require.NotContains(t, w.raw, "concurrency")
	require.Nil(t, job.Concurrency)

	dispatch, ok := w.On["workflow_dispatch"].(map[string]any)
	require.True(t, ok, "workflow_dispatch trigger")
	in := dispatch["inputs"].(map[string]any)["plan_id"].(map[string]any)
	require.Equal(t, true, in["required"])
	require.Equal(t, "string", in["type"])
	require.Len(t, w.On, 1, "dispatch is the only trigger")
	require.Equal(t, "bench-bot ${{ inputs.plan_id }}", w.raw["run-name"])

	require.Equal(t, map[string]string{"id-token": "write", "contents": "read"}, w.Permissions)
	require.Equal(t, 330, job.TimeoutMinutes)
}

func TestWorkflowBenchBotValidatesPlanIDFirst(t *testing.T) {
	_, job := benchBotJob(t)
	steps := job.Steps
	v := firstRunIndex(steps, `^[0-9a-f]{12}$`)
	require.Equal(t, 0, v, "plan_id validation is the first step")
	require.Equal(t, "${{ inputs.plan_id }}", steps[v].Env["PLAN_ID"])
	require.Contains(t, steps[v].Run, `[[ "$PLAN_ID" =~ $re ]]`, "full-string bash match, not grep (a multi-line value passes grep)")
	require.Empty(t, steps[v].Uses, "validation runs no action")

	for name, env := range job.Env {
		require.NotContains(t, env, "inputs.", "job env %s would use the input before validation", name)
	}
	for i, s := range steps {
		if i != v && strings.Contains(stepText(t, s), "inputs.plan_id") {
			require.Greater(t, i, v, "step %q uses plan_id before validation", s.Name)
		}
	}
}

func TestWorkflowBenchBotOrder(t *testing.T) {
	_, job := benchBotJob(t)
	steps := job.Steps
	gate := stepIndex(t, steps, "gate")
	record := stepIndex(t, steps, "record")
	lock := stepIndex(t, steps, "lock")
	reauth := stepIndex(t, steps, "reauth")
	bench := stepIndex(t, steps, "bench")
	release := stepIndex(t, steps, "release")
	outcome := stepIndex(t, steps, "outcome")
	notify := stepIndex(t, steps, "notify")
	done := stepIndex(t, steps, "done")

	require.Contains(t, steps[gate].Run, " status ")
	require.Contains(t, steps[gate].Run, `.claimed`)
	require.Contains(t, steps[record].Run, "record-run")
	require.Contains(t, steps[record].Run, "--run-id")
	require.Equal(t, "${{ github.run_id }}", steps[record].Env["RUN_ID"])

	require.Contains(t, steps[lock].Run, "lock acquire")
	for _, f := range []string{`--holder "$RUN_ID"`, "--ttl 4h", "--wait 2h", "--poll 2m"} {
		require.Contains(t, steps[lock].Run, f)
	}
	require.Equal(t, "${{ github.run_id }}", steps[lock].Env["RUN_ID"])
	require.Contains(t, steps[lock].Run, "stack_busy")

	require.Equal(t, bench, firstRunIndex(steps, "task aws:bench"))
	require.Contains(t, steps[bench].Run, `scenario_file="$SCENARIO_FILE"`)
	require.Equal(t, "steps.lock.outputs.acquired == 'true'", steps[bench].If)

	require.True(t, strings.HasPrefix(steps[reauth].Uses, "aws-actions/configure-aws-credentials@"),
		"fresh 4h credentials after the up-to-2h lock wait")

	require.Less(t, gate, record)
	require.Less(t, record, lock)
	require.Less(t, lock, reauth)
	require.Less(t, reauth, bench)
	require.Less(t, bench, release)
	require.Less(t, release, outcome)
	require.Less(t, outcome, notify)
	require.Less(t, notify, done)

	require.Contains(t, steps[release].Run, "lock release")
	require.Contains(t, steps[notify].Run, " notify ")
	require.Contains(t, steps[done].Run, " done ")
	for _, i := range []int{release, outcome, notify, done} {
		requireAlways(t, steps[i])
	}
	// A refused or duplicate dispatch must send nothing and mark nothing.
	for _, i := range []int{outcome, notify, done} {
		require.Contains(t, steps[i].If, "steps.record.outcome == 'success'", steps[i].ID)
	}
	// The token is minted inside benchplan; the step never sees it.
	require.Equal(t, "${{ vars.BENCH_BOT_A2A_URL }}", steps[notify].Env["BENCH_BOT_A2A_URL"])
	require.Equal(t, "${{ secrets.BENCH_BOT_ADP_CLIENT_ID }}", steps[notify].Env["BENCH_BOT_ADP_CLIENT_ID"])
	require.Equal(t, "${{ secrets.BENCH_BOT_ADP_CLIENT_SECRET }}", steps[notify].Env["BENCH_BOT_ADP_CLIENT_SECRET"])
	require.NotContains(t, steps[notify].Run, "set -x")
}

func TestWorkflowBenchBotEntryIDComesFromRunnerMarker(t *testing.T) {
	_, job := benchBotJob(t)
	s := job.Steps[stepIndex(t, job.Steps, "outcome")]
	require.Contains(t, s.Run, "^"+catalogueEntryMarker, "anchored at column 0, where only the runner prints")
}

// Every expression substitution into a run script is evaluated before bash
// parses it, so a crafted value becomes shell syntax. Inputs go through env.
func TestWorkflowNoInputInterpolatedIntoRun(t *testing.T) {
	expr := regexp.MustCompile(`\$\{\{[^}]*\b(inputs|github\.event\.inputs|matrix|steps\.[a-z]+\.outputs\.(plan|scenario|entry|outcome|run_url))\b`)
	for _, name := range []string{"bench_bot.yml", "bench_bot_watchdog.yml"} {
		w := loadWorkflow(t, name)
		for jn, j := range w.Jobs {
			for _, s := range j.Steps {
				require.NotRegexp(t, expr, s.Run, "%s job %s step %q interpolates a value into run:", name, jn, s.Name)
			}
		}
	}
}

func TestWorkflowSoaksTakeTheSharedLock(t *testing.T) {
	for _, name := range []string{"soak_nightly.yml", "soak_pr.yml"} {
		t.Run(name, func(t *testing.T) {
			w := loadWorkflow(t, name)
			require.Equal(t, "bench-soak", w.Concurrency.(map[string]any)["group"], "keeps its concurrency group")
			var job wfJob
			for _, j := range w.Jobs {
				if firstRunIndex(j.Steps, "task aws:bench") >= 0 {
					job = j
				}
			}
			steps := job.Steps
			bench := firstRunIndex(steps, "task aws:bench")
			require.GreaterOrEqual(t, bench, 0)
			lock := stepIndex(t, steps, "lock")
			reauth := stepIndex(t, steps, "reauth")
			release := stepIndex(t, steps, "release")
			require.Less(t, lock, reauth)
			require.Less(t, reauth, bench)
			require.Less(t, bench, release)
			require.Contains(t, steps[lock].Run, "lock acquire")
			for _, f := range []string{`--holder "soak-$RUN_ID"`, "--ttl 4h", "--wait 2h"} {
				require.Contains(t, steps[lock].Run, f)
			}
			require.Equal(t, "${{ github.run_id }}", steps[lock].Env["RUN_ID"])
			require.Contains(t, steps[release].Run, "lock release")
			require.Contains(t, steps[release].Run, `--holder "soak-$RUN_ID"`)
			requireAlways(t, steps[release])
			// The wait is added to the job budget: 2h wait + the soak.
			require.GreaterOrEqual(t, job.TimeoutMinutes, 120+200)
		})
	}
}

func TestWorkflowWatchdog(t *testing.T) {
	w := loadWorkflow(t, "bench_bot_watchdog.yml")
	sched, ok := w.On["schedule"].([]any)
	require.True(t, ok, "schedule trigger")
	require.Equal(t, "*/30 * * * *", sched[0].(map[string]any)["cron"])
	require.Contains(t, w.On, "workflow_dispatch")
	require.Equal(t, map[string]string{"id-token": "write", "contents": "read"}, w.Permissions)

	require.Len(t, w.Jobs, 1)
	for _, j := range w.Jobs {
		all := ""
		for _, s := range j.Steps {
			all += s.Run + "\n"
		}
		require.Contains(t, all, "stale")
		require.Contains(t, all, "--older-than 6h")
		require.Contains(t, all, "--unstarted-older-than 30m")
		require.Contains(t, all, " notify ")
		require.Contains(t, all, " done ")
	}
}

// actions.lock maps each version-tagged action to the commit it resolved to;
// CONTRIBUTING.md 3.3.4 forbids SHA refs in uses: and requires the lockfile.
var lockedCommit = regexp.MustCompile(`^sha1-[0-9a-f]{40}$`)

func TestWorkflowActionsArePinnedByTheLockfile(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(workflowDir, "actions.lock"))
	require.NoError(t, err)
	var lock struct {
		Dependencies map[string]struct {
			Commit string `yaml:"commit"`
		} `yaml:"dependencies"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &lock))
	tag := regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@v[0-9]+\.[0-9]+\.[0-9]+$`)
	for _, name := range []string{"bench_bot.yml", "bench_bot_watchdog.yml", "soak_nightly.yml", "soak_pr.yml"} {
		w := loadWorkflow(t, name)
		for _, j := range w.Jobs {
			for _, s := range j.Steps {
				switch {
				case s.Uses == "":
				case strings.HasPrefix(s.Uses, "$/.github/actions/"):
					// Same-repo action, locked to the workflow's own commit.
				default:
					require.Regexp(t, tag, s.Uses, "%s: %q must be an exact version tag", name, s.Uses)
					dep, ok := lock.Dependencies[s.Uses]
					require.True(t, ok, "%s: %q has no actions.lock entry", name, s.Uses)
					require.Regexp(t, lockedCommit, dep.Commit, "%s: %q", name, s.Uses)
				}
				require.False(t, strings.HasPrefix(s.Uses, "./"), "%s: ./ actions run workspace code", name)
			}
		}
	}
}

// The repo is public, so its CI logs are: the plan's pipeline, the rendered
// scenario or Connect config, lint findings and Connect's log must never be
// printed or uploaded.
func TestWorkflowBenchBotNeverPrintsThePipeline(t *testing.T) {
	_, job := benchBotJob(t)
	show := regexp.MustCompile(`benchplan"?\s+show\b`)
	cat := regexp.MustCompile(`\b(cat|less|more|head|tail|tee)\b[^\n|]*(SCENARIO_FILE|CONNECT_FILE|scenario_file|connect_file|bench-bot/|connect-|bot-|lint\.out|sweep-|prom-)`)
	for _, s := range job.Steps {
		require.NotRegexp(t, show, s.Run, "step %q prints the whole plan", s.Name)
		require.NotRegexp(t, cat, s.Run, "step %q prints a rendered file", s.Name)
		require.NotContains(t, s.Run, "set -x", s.Name)
		if strings.HasPrefix(s.Uses, "actions/upload-artifact@") {
			require.Equal(t, "${{ runner.temp }}/bench-run.log", s.With["path"], "only the runner's own log is uploaded")
		}
	}
	render := job.Steps[stepIndex(t, job.Steps, "render")]
	require.Contains(t, render.Run, " summary --plan ")
}

// Lint runs before the lock: a config Connect rejects is invalid_config,
// with no lock and no spend.
func TestWorkflowBenchBotLintsBeforeTheLock(t *testing.T) {
	_, job := benchBotJob(t)
	steps := job.Steps
	render := stepIndex(t, steps, "render")
	lint := stepIndex(t, steps, "lint")
	lock := stepIndex(t, steps, "lock")
	outcome := stepIndex(t, steps, "outcome")
	require.Less(t, render, lint)
	require.Less(t, lint, lock)
	require.Contains(t, steps[lint].Run, `redpanda-connect" lint "$CONNECT_FILE"`)
	require.Equal(t, "${{ steps.render.outputs.connect_file }}", steps[lint].Env["CONNECT_FILE"])
	require.Contains(t, steps[lint].Run, "invalid=true")
	build := firstRunIndex(steps, "go build -o \"$RUNNER_TEMP/redpanda-connect\" ./cmd/redpanda-connect")
	require.GreaterOrEqual(t, build, 0)
	require.Less(t, build, lint)
	require.Equal(t, "steps.lint.outputs.invalid != 'true'", steps[lock].If)
	require.Equal(t, "${{ steps.lint.outputs.invalid }}", steps[outcome].Env["LINT_INVALID"])
	require.Contains(t, steps[outcome].Run, "OUTCOME=invalid_config")
	// Every step that spends (terraform, the bench) sits behind the lock.
	require.Less(t, lock, stepIndex(t, steps, "bench"))
}

// The plan is marked done only once the completion message went out, or was
// skipped for unset config: a failed send is retried by the watchdog.
func TestWorkflowBenchBotDoneGatedOnNotify(t *testing.T) {
	_, job := benchBotJob(t)
	notify := job.Steps[stepIndex(t, job.Steps, "notify")]
	done := job.Steps[stepIndex(t, job.Steps, "done")]
	require.Contains(t, notify.Run, `echo "rc=$rc" >> "$GITHUB_OUTPUT"`)
	require.Contains(t, notify.Run, `[ "$rc" -ne 5 ]`)
	require.Equal(t, "always() && steps.record.outcome == 'success' && (steps.notify.outputs.rc == '0' || steps.notify.outputs.rc == '5')", done.If)

	w := loadWorkflow(t, "bench_bot_watchdog.yml")
	for _, j := range w.Jobs {
		all := ""
		for _, s := range j.Steps {
			all += s.Run
		}
		require.Contains(t, all, `[ "$rc" -ne 0 ] && [ "$rc" -ne 5 ]`, "the watchdog marks done on 0 or 5 only")
	}
}
