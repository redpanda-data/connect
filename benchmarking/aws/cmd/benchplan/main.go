// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
// Command benchplan builds, stores and renders Bench Bot plans, and takes the
// shared bench-stack lock.
// Exit codes: 0 ok, 1 runtime error, 2 bad flags or usage, 3 lock wait timeout.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/plan"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

const usageText = "usage: benchplan <render|show|build|lock> ..."

var errUsage = errors.New("usage")

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, usageText)
		return 2
	}
	var err error
	switch args[0] {
	case "render":
		err = runRender(args[1:], stdout)
	case "show":
		err = runShow(args[1:], stdout)
	case "build":
		err = runBuild(args[1:], stdout)
	case "lock":
		err = runLock(args[1:], stderr)
	default:
		fmt.Fprintf(stderr, "unknown subcommand %q\n%s\n", args[0], usageText)
		return 2
	}
	switch {
	case err == nil:
		return 0
	case errors.Is(err, errTimeout):
		fmt.Fprintln(stderr, err)
		return 3
	case errors.Is(err, errUsage):
		fmt.Fprintln(stderr, err)
		return 2
	default:
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
}

func parseFlags(name string, args []string, needOut bool) (planPath, outDir string, err error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&planPath, "plan", "", "plan JSON file, or s3://<bucket>/plans/<id>.json")
	if needOut {
		fs.StringVar(&outDir, "out", "", "directory to write the scenario into")
	}
	if err := fs.Parse(args); err != nil {
		return "", "", fmt.Errorf("%w: %v", errUsage, err)
	}
	if planPath == "" {
		return "", "", fmt.Errorf("%w: --plan is required", errUsage)
	}
	if needOut && outDir == "" {
		return "", "", fmt.Errorf("%w: --out is required", errUsage)
	}
	return planPath, outDir, nil
}

var s3PlanRef = regexp.MustCompile(`^s3://([^/]+)/plans/([^/]+)\.json$`)

func loadPlan(path string) (plan.Plan, error) {
	if strings.HasPrefix(path, "s3://") {
		m := s3PlanRef.FindStringSubmatch(path)
		if m == nil {
			return plan.Plan{}, fmt.Errorf("plan location %q: want s3://<bucket>/plans/<id>.json", path)
		}
		// Validate the id before anything touches S3 (plan.Store checks too).
		if !plan.ValidID(m[2]) {
			return plan.Plan{}, fmt.Errorf("plan location %q: invalid plan id %q", path, m[2])
		}
		ctx := context.Background()
		objs, err := deps.s3Objects(ctx, m[1])
		if err != nil {
			return plan.Plan{}, err
		}
		return plan.NewStore(objs).Get(ctx, m[2])
	}
	if strings.Contains(path, "://") {
		return plan.Plan{}, fmt.Errorf("plan location %q: only local files and s3:// are supported", path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return plan.Plan{}, fmt.Errorf("reading plan: %w", err)
	}
	var p plan.Plan
	if err := json.Unmarshal(raw, &p); err != nil {
		return plan.Plan{}, fmt.Errorf("parsing plan %s: %w", path, err)
	}
	return p, nil
}

func runRender(args []string, stdout io.Writer) error {
	planPath, outDir, err := parseFlags("render", args, true)
	if err != nil {
		return err
	}
	p, err := loadPlan(planPath)
	if err != nil {
		return err
	}
	out, err := plan.Render(p)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", outDir, err)
	}
	dst := filepath.Join(outDir, "bot-"+p.ID+".yaml")
	if err := os.WriteFile(dst, out, 0o644); err != nil {
		return fmt.Errorf("writing scenario: %w", err)
	}
	fmt.Fprintln(stdout, dst)
	return nil
}

func runShow(args []string, stdout io.Writer) error {
	planPath, _, err := parseFlags("show", args, false)
	if err != nil {
		return err
	}
	p, err := loadPlan(planPath)
	if err != nil {
		return err
	}
	return printPlan(stdout, p)
}

func printPlan(stdout io.Writer, p plan.Plan) error {
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, string(b))
	return nil
}
