// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/plan"
)

// errDuplicate means another workflow run is already recorded for the plan.
var errDuplicate = errors.New("duplicate dispatch")

// planRefFlags registers --plan and --store. --plan is either an
// s3://<bucket>/plans/<id>.json location, or a bare id with --store.
func planRefFlags(fs *flag.FlagSet) (ref, store *string) {
	return fs.String("plan", "", "plan id (with --store), or s3://<bucket>/plans/<id>.json"),
		fs.String("store", "", "s3://<bucket> or dir:<path>")
}

// openPlan resolves --plan/--store to a Store and a validated plan id.
func openPlan(ctx context.Context, ref, store string) (plan.Store, string, error) {
	if ref == "" {
		return nil, "", fmt.Errorf("%w: --plan is required", errUsage)
	}
	if strings.HasPrefix(ref, "s3://") {
		if store != "" {
			return nil, "", fmt.Errorf("%w: --plan is a location; drop --store or pass a bare id", errUsage)
		}
		m := s3PlanRef.FindStringSubmatch(ref)
		if m == nil || !plan.ValidID(m[2]) {
			return nil, "", fmt.Errorf("plan location %q: want s3://<bucket>/plans/<12 hex>.json", ref)
		}
		objs, err := deps.s3Objects(ctx, m[1])
		if err != nil {
			return nil, "", err
		}
		return plan.NewStore(objs), m[2], nil
	}
	if store == "" {
		return nil, "", fmt.Errorf("%w: a bare plan id needs --store", errUsage)
	}
	if !plan.ValidID(ref) {
		return nil, "", fmt.Errorf("invalid plan id %q: want 12 lower-case hex characters", ref)
	}
	objs, err := openObjects(ctx, store)
	if err != nil {
		return nil, "", err
	}
	return plan.NewStore(objs, plan.WithClock(deps.now)), ref, nil
}

// planStatus is what status prints. Every field is always present so a
// workflow can read it with jq without null checks.
type planStatus struct {
	PlanID          string `json:"plan_id"`
	Claimed         bool   `json:"claimed"`
	Approver        string `json:"approver"`
	DispatchedRunID int64  `json:"dispatched_run_id"`
	Done            bool   `json:"done"`
	Outcome         string `json:"outcome"`
}

func runStatus(args []string, stdout io.Writer) error {
	fs := newFlags("status")
	ref, store := planRefFlags(fs)
	if err := parse(fs, args); err != nil {
		return err
	}
	ctx := context.Background()
	s, id, err := openPlan(ctx, *ref, *store)
	if err != nil {
		return err
	}
	p, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	return printJSON(stdout, planStatus{
		PlanID: p.ID, Claimed: p.Claimed, Approver: p.Approver,
		DispatchedRunID: p.DispatchedRunID, Done: p.Done, Outcome: p.Outcome,
	})
}

// claimResult is what claim prints. Approver is the plan's approver after
// the call: the caller's name when it won, the earlier winner's when not.
type claimResult struct {
	PlanID   string `json:"plan_id"`
	Won      bool   `json:"won"`
	Approver string `json:"approver"`
}

// runClaim claims a plan for dispatch (Store.Claim), as the agent's tool
// server does on approval; an operator runs it by hand before
// `gh workflow run`. Exit 0 either way: won says whether this call claimed
// it, and only the winner may dispatch.
func runClaim(args []string, stdout io.Writer) error {
	fs := newFlags("claim")
	ref, store := planRefFlags(fs)
	approver := fs.String("approver", "", "who approved the run")
	if err := parse(fs, args); err != nil {
		return err
	}
	if strings.TrimSpace(*approver) == "" {
		return fmt.Errorf("%w: --approver is required", errUsage)
	}
	ctx := context.Background()
	s, id, err := openPlan(ctx, *ref, *store)
	if err != nil {
		return err
	}
	won, err := s.Claim(ctx, id, *approver)
	if err != nil {
		return err
	}
	p, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	return printJSON(stdout, claimResult{PlanID: id, Won: won, Approver: p.Approver})
}

func runRecordRun(args []string, stderr io.Writer) error {
	fs := newFlags("record-run")
	ref, store := planRefFlags(fs)
	runID := fs.Int64("run-id", 0, "GitHub Actions run id")
	if err := parse(fs, args); err != nil {
		return err
	}
	if *runID <= 0 {
		return fmt.Errorf("%w: --run-id must be a positive run id", errUsage)
	}
	ctx := context.Background()
	s, id, err := openPlan(ctx, *ref, *store)
	if err != nil {
		return err
	}
	// The gate runs in Go too, so no caller can record a run against an
	// unapproved or already reported plan.
	p, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	if err := plan.RequireClaimed(p); err != nil {
		return err
	}
	if err := s.RecordRun(ctx, id, *runID); err != nil {
		if errors.Is(err, plan.ErrConflict) {
			return fmt.Errorf("%w: %v", errDuplicate, err)
		}
		return err
	}
	// Re-check after recording: the watchdog may have reported the plan
	// never_started between the read above and the write.
	if p, err = s.Get(ctx, id); err != nil {
		return err
	}
	if p.Done {
		return fmt.Errorf("plan %s is already done (outcome %s)", id, p.Outcome)
	}
	fmt.Fprintf(stderr, "recorded run %d for plan %s\n", *runID, id)
	return nil
}

func runDone(args []string, stderr io.Writer) error {
	fs := newFlags("done")
	ref, store := planRefFlags(fs)
	outcome := fs.String("outcome", "", "the run outcome")
	if err := parse(fs, args); err != nil {
		return err
	}
	if !plan.ValidOutcome(*outcome) {
		return fmt.Errorf("%w: --outcome %q is not a known outcome", errUsage, *outcome)
	}
	ctx := context.Background()
	s, id, err := openPlan(ctx, *ref, *store)
	if err != nil {
		return err
	}
	if _, err := s.Get(ctx, id); err != nil {
		return err
	}
	if err := s.MarkDone(ctx, id, *outcome); err != nil {
		return err
	}
	fmt.Fprintf(stderr, "plan %s done: %s (no-op if it was already done)\n", id, *outcome)
	return nil
}

func runStale(args []string, stdout io.Writer) error {
	fs := newFlags("stale")
	store := fs.String("store", "", "s3://<bucket> or dir:<path>")
	var notReported, notStarted time.Duration
	fs.DurationVar(&notReported, "older-than", 0, "report claimed plans not done this long after the claim")
	fs.DurationVar(&notStarted, "unstarted-older-than", 0, "report claimed plans with no run this long after the claim")
	if err := parse(fs, args); err != nil {
		return err
	}
	switch {
	case *store == "":
		return fmt.Errorf("%w: --store is required", errUsage)
	case notReported <= 0 || notStarted <= 0:
		return fmt.Errorf("%w: --older-than and --unstarted-older-than must be positive", errUsage)
	}
	ctx := context.Background()
	objs, err := openObjects(ctx, *store)
	if err != nil {
		return err
	}
	stale, err := plan.NewStore(objs).Stale(ctx, deps.now().UTC(), notReported, notStarted)
	if err != nil {
		return err
	}
	b, err := json.Marshal(stale)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, string(b))
	return nil
}

func printJSON(stdout io.Writer, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, string(b))
	return nil
}
