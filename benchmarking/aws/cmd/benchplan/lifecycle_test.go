// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/benchmarking/aws/plan"
)

var fixedNow = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

// seedStore puts a plan with id into a dir store under root and, when
// claimedAgo >= 0, claims it that long before fixedNow.
func seedStore(t *testing.T, root, id string, claimedAgo time.Duration) plan.Store {
	t.Helper()
	at := fixedNow.Add(-claimedAgo)
	s := plan.NewStore(plan.NewDirObjects(root), plan.WithClock(func() time.Time { return at }))
	p := runnablePlan(t)
	p.ID = id
	require.NoError(t, s.Put(context.Background(), p))
	if claimedAgo >= 0 {
		first, err := s.Claim(context.Background(), id, "U123")
		require.NoError(t, err)
		require.True(t, first)
	}
	return s
}

func useClock(t *testing.T) {
	t.Helper()
	old := deps.now
	deps.now = func() time.Time { return fixedNow }
	t.Cleanup(func() { deps.now = old })
}

type statusOut struct {
	PlanID          string `json:"plan_id"`
	Claimed         bool   `json:"claimed"`
	Approver        string `json:"approver"`
	DispatchedRunID int64  `json:"dispatched_run_id"`
	Done            bool   `json:"done"`
	Outcome         string `json:"outcome"`
}

func status(t *testing.T, args ...string) (statusOut, int, string) {
	t.Helper()
	var so, se bytes.Buffer
	code := run(append([]string{"status"}, args...), &so, &se)
	var st statusOut
	if code == 0 {
		require.NoError(t, json.Unmarshal(so.Bytes(), &st), so.String())
	}
	return st, code, se.String()
}

func TestStatusUnclaimed(t *testing.T) {
	root := t.TempDir()
	seedStore(t, root, "0123456789ab", -1)
	st, code, se := status(t, "--store", "dir:"+root, "--plan", "0123456789ab")
	require.Equal(t, 0, code, se)
	require.Equal(t, statusOut{PlanID: "0123456789ab"}, st)
}

func TestStatusClaimedRunDone(t *testing.T) {
	root := t.TempDir()
	s := seedStore(t, root, "0123456789ab", time.Minute)
	require.NoError(t, s.RecordRun(context.Background(), "0123456789ab", 77))
	require.NoError(t, s.MarkDone(context.Background(), "0123456789ab", "failure"))
	st, code, se := status(t, "--store", "dir:"+root, "--plan", "0123456789ab")
	require.Equal(t, 0, code, se)
	require.Equal(t, statusOut{PlanID: "0123456789ab", Claimed: true, Approver: "U123", DispatchedRunID: 77, Done: true, Outcome: "failure"}, st)
}

func TestStatusPrintsAllFieldsEvenWhenFalse(t *testing.T) {
	root := t.TempDir()
	seedStore(t, root, "0123456789ab", -1)
	var so, se bytes.Buffer
	require.Equal(t, 0, run([]string{"status", "--store", "dir:" + root, "--plan", "0123456789ab"}, &so, &se), se.String())
	var raw map[string]any
	require.NoError(t, json.Unmarshal(so.Bytes(), &raw))
	for _, k := range []string{"plan_id", "claimed", "approver", "dispatched_run_id", "done", "outcome"} {
		require.Contains(t, raw, k, "a workflow reads %s with jq; it must never be absent", k)
	}
}

func TestStatusAcceptsS3PlanLocation(t *testing.T) {
	root := t.TempDir()
	useFakes(t, root, &fakeEC2{})
	seedStore(t, root, "0123456789ab", time.Minute)
	st, code, se := status(t, "--plan", "s3://b/plans/0123456789ab.json")
	require.Equal(t, 0, code, se)
	require.True(t, st.Claimed)
}

func TestStatusErrors(t *testing.T) {
	root := t.TempDir()
	_, code, _ := status(t, "--store", "dir:"+root)
	require.Equal(t, 2, code, "--plan required")
	_, code, _ = status(t, "--store", "dir:"+root, "--plan", "0123456789ab")
	require.Equal(t, 1, code, "missing plan")
	_, code, _ = status(t, "--store", "dir:"+root, "--plan", "../etc/passwd")
	require.Equal(t, 1, code, "bad id")
	_, code, _ = status(t, "--plan", "0123456789ab")
	require.Equal(t, 2, code, "a bare id needs --store")
	_, code, _ = status(t, "--store", "dir:"+root, "--plan", "s3://b/plans/0123456789ab.json")
	require.Equal(t, 2, code, "a location and --store are exclusive")
}

func TestRecordRun(t *testing.T) {
	root := t.TempDir()
	seedStore(t, root, "0123456789ab", time.Minute)
	args := func(id string) []string {
		return []string{"record-run", "--store", "dir:" + root, "--plan", "0123456789ab", "--run-id", id}
	}
	var so, se bytes.Buffer
	require.Equal(t, 0, run(args("1001"), &so, &se), se.String())
	require.Equal(t, 0, run(args("1001"), &so, &se), "same run again is a no-op (a re-run of the same job)")
	se.Reset()
	require.Equal(t, 4, run(args("1002"), &so, &se), "another run already recorded: duplicate dispatch")
	require.Contains(t, se.String(), "duplicate dispatch")

	st, code, _ := status(t, "--store", "dir:"+root, "--plan", "0123456789ab")
	require.Equal(t, 0, code)
	require.EqualValues(t, 1001, st.DispatchedRunID)
}

func TestRecordRunErrors(t *testing.T) {
	root := t.TempDir()
	seedStore(t, root, "0123456789ab", time.Minute)
	var so, se bytes.Buffer
	base := []string{"record-run", "--store", "dir:" + root, "--plan", "0123456789ab"}
	require.Equal(t, 2, run(base, &so, &se), "--run-id required")
	require.Equal(t, 2, run(append(base, "--run-id", "0"), &so, &se))
	require.Equal(t, 2, run(append(base, "--run-id", "-3"), &so, &se))
	require.Equal(t, 2, run(append(base, "--run-id", "abc"), &so, &se))
	require.Equal(t, 1, run([]string{"record-run", "--store", "dir:" + root, "--plan", "ZZ", "--run-id", "1"}, &so, &se))
}

func TestRecordRunRefusesUnclaimedPlan(t *testing.T) {
	root := t.TempDir()
	seedStore(t, root, "0123456789ab", -1)
	var so, se bytes.Buffer
	require.Equal(t, 1, run([]string{"record-run", "--store", "dir:" + root, "--plan", "0123456789ab", "--run-id", "5"}, &so, &se))
	require.Contains(t, se.String(), "not claimed")
	st, _, _ := status(t, "--store", "dir:"+root, "--plan", "0123456789ab")
	require.Zero(t, st.DispatchedRunID, "no run is recorded against an unclaimed plan")
}

func TestDone(t *testing.T) {
	root := t.TempDir()
	seedStore(t, root, "0123456789ab", time.Minute)
	var so, se bytes.Buffer
	require.Equal(t, 0, run([]string{"done", "--store", "dir:" + root, "--plan", "0123456789ab", "--outcome", "stack_busy"}, &so, &se), se.String())
	require.Equal(t, 0, run([]string{"done", "--store", "dir:" + root, "--plan", "0123456789ab", "--outcome", "success"}, &so, &se), "second is a no-op")
	st, _, _ := status(t, "--store", "dir:"+root, "--plan", "0123456789ab")
	require.True(t, st.Done)
	require.Equal(t, "stack_busy", st.Outcome, "first outcome wins")
}

func TestDoneErrors(t *testing.T) {
	root := t.TempDir()
	seedStore(t, root, "0123456789ab", time.Minute)
	var so, se bytes.Buffer
	base := []string{"done", "--store", "dir:" + root, "--plan", "0123456789ab"}
	require.Equal(t, 2, run(base, &so, &se), "--outcome required")
	require.Equal(t, 2, run(append(base, "--outcome", "ok"), &so, &se), "unknown outcome")
	require.Equal(t, 1, run([]string{"done", "--store", "dir:" + root, "--plan", "aaaaaaaaaaaa", "--outcome", "success"}, &so, &se), "no such plan")
}

func TestStale(t *testing.T) {
	root := t.TempDir()
	useClock(t)
	seedStore(t, root, "000000000001", -1)
	seedStore(t, root, "000000000002", 10*time.Minute)
	seedStore(t, root, "000000000003", 45*time.Minute)
	s := seedStore(t, root, "000000000004", 7*time.Hour)
	require.NoError(t, s.RecordRun(context.Background(), "000000000004", 99))
	s = seedStore(t, root, "000000000005", 7*time.Hour)
	require.NoError(t, s.RecordRun(context.Background(), "000000000005", 98))
	require.NoError(t, s.MarkDone(context.Background(), "000000000005", "success"))

	var so, se bytes.Buffer
	require.Equal(t, 0, run([]string{"stale", "--store", "dir:" + root, "--older-than", "6h", "--unstarted-older-than", "30m"}, &so, &se), se.String())
	var got []plan.StalePlan
	require.NoError(t, json.Unmarshal(so.Bytes(), &got))
	require.Equal(t, []plan.StalePlan{
		{ID: "000000000003", Outcome: "never_started", ClaimedAt: fixedNow.Add(-45 * time.Minute)},
		{ID: "000000000004", Outcome: "never_reported", RunID: 99, ClaimedAt: fixedNow.Add(-7 * time.Hour)},
	}, got)
}

func TestStaleEmptyIsArray(t *testing.T) {
	root := t.TempDir()
	useClock(t)
	var so, se bytes.Buffer
	require.Equal(t, 0, run([]string{"stale", "--store", "dir:" + root, "--older-than", "6h", "--unstarted-older-than", "30m"}, &so, &se), se.String())
	require.Equal(t, "[]\n", so.String(), "jq '.[]' must see an array, never null")
}

// The watchdog runs before the catalogue bucket exists: no bucket, no
// stale plans, and stdout is still the array jq reads.
func TestStaleToleratesMissingBucket(t *testing.T) {
	useErroringS3(t, &s3types.NoSuchBucket{}, &fakeEC2{})
	var so, se bytes.Buffer
	args := []string{"stale", "--store", "s3://redpanda-connect-bench-catalog", "--older-than", "6h", "--unstarted-older-than", "30m"}
	require.Equal(t, 0, run(args, &so, &se), se.String())
	require.Equal(t, "[]\n", so.String())
	require.Contains(t, se.String(), "does not exist; no stale plans")
}

func TestStaleOtherS3ErrorsFail(t *testing.T) {
	useErroringS3(t, errors.New("AccessDenied"), &fakeEC2{})
	var so, se bytes.Buffer
	args := []string{"stale", "--store", "s3://redpanda-connect-bench-catalog", "--older-than", "6h", "--unstarted-older-than", "30m"}
	require.Equal(t, 1, run(args, &so, &se))
	require.Empty(t, so.String())
}

func TestStaleErrors(t *testing.T) {
	var so, se bytes.Buffer
	require.Equal(t, 2, run([]string{"stale", "--older-than", "6h", "--unstarted-older-than", "30m"}, &so, &se), "--store required")
	require.Equal(t, 2, run([]string{"stale", "--store", "dir:x", "--unstarted-older-than", "30m"}, &so, &se), "--older-than required")
	require.Equal(t, 2, run([]string{"stale", "--store", "dir:x", "--older-than", "6h"}, &so, &se), "--unstarted-older-than required")
	require.Equal(t, 2, run([]string{"stale", "--store", "dir:x", "--older-than", "-1h", "--unstarted-older-than", "30m"}, &so, &se))
}

func TestRecordRunRefusesDonePlan(t *testing.T) {
	root := t.TempDir()
	s := seedStore(t, root, "0123456789ab", time.Hour)
	require.NoError(t, s.MarkDone(context.Background(), "0123456789ab", "never_started"))
	var so, se bytes.Buffer
	require.Equal(t, 1, run([]string{"record-run", "--store", "dir:" + root, "--plan", "0123456789ab", "--run-id", "5"}, &so, &se))
	require.Contains(t, se.String(), "already done")
}
