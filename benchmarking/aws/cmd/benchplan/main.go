// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
// Command benchplan renders a stored Bench Bot plan into a runner scenario.
// Exit codes: 0 ok, 1 runtime error, 2 bad flags or usage.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/plan"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

var errUsage = errors.New("usage")

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: benchplan <render|show> --plan <file> [--out <dir>]")
		return 2
	}
	var err error
	switch args[0] {
	case "render":
		err = runRender(args[1:], stdout)
	case "show":
		err = runShow(args[1:], stdout)
	default:
		fmt.Fprintf(stderr, "unknown subcommand %q\nusage: benchplan <render|show> --plan <file> [--out <dir>]\n", args[0])
		return 2
	}
	switch {
	case err == nil:
		return 0
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
	fs.StringVar(&planPath, "plan", "", "path to the plan JSON file")
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

func loadPlan(path string) (plan.Plan, error) {
	if strings.Contains(path, "://") {
		return plan.Plan{}, fmt.Errorf("plan location %q: only local files are supported", path)
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
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, string(b))
	return nil
}
