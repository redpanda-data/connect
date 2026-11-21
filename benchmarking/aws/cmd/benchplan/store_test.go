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
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/plan"
)

// countingObjects counts calls reaching the object store.
type countingObjects struct {
	plan.Objects
	calls atomic.Int32
}

func (c *countingObjects) Create(ctx context.Context, k string, b []byte) error {
	c.calls.Add(1)
	return c.Objects.Create(ctx, k, b)
}

func (c *countingObjects) Get(ctx context.Context, k string) ([]byte, string, error) {
	c.calls.Add(1)
	return c.Objects.Get(ctx, k)
}

// fakeEC2 reports a number of running bench instances per call, then none.
type fakeEC2 struct {
	running []int // instances reported on successive calls; empty after
	calls   int
	errs    int // the first errs calls fail
	inputs  []*ec2.DescribeInstancesInput
}

func (f *fakeEC2) DescribeInstances(_ context.Context, in *ec2.DescribeInstancesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	f.inputs = append(f.inputs, in)
	if f.errs > 0 {
		f.errs--
		return nil, errors.New("throttled")
	}
	n := 0
	if f.calls < len(f.running) {
		n = f.running[f.calls]
	}
	f.calls++
	out := &ec2.DescribeInstancesOutput{}
	if n > 0 {
		var insts []ec2types.Instance
		for i := 0; i < n; i++ {
			insts = append(insts, ec2types.Instance{InstanceId: aws.String("i-1")})
		}
		out.Reservations = []ec2types.Reservation{{Instances: insts}}
	}
	return out, nil
}

func useFakes(t *testing.T, root string, e *fakeEC2) *countingObjects {
	t.Helper()
	co := &countingObjects{Objects: plan.NewDirObjects(root)}
	old := deps
	deps = dependencies{
		s3Objects: func(context.Context, string) (plan.Objects, error) { return co, nil },
		ec2:       func(context.Context) (EC2API, error) { return e, nil },
		now:       time.Now,
	}
	t.Cleanup(func() { deps = old })
	return co
}

func writeConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cfg.yaml")
	require.NoError(t, os.WriteFile(path, []byte("input:\n  http_server:\n    path: /post\noutput:\n  drop: {}\n"), 0o600))
	return path
}

func TestBuildRoundTripsThroughDirStore(t *testing.T) {
	root := t.TempDir()
	var so, se bytes.Buffer
	code := run([]string{
		"build", "--config", writeConfig(t), "--message-size", "500", "--target-mb-s", "10",
		"--slack-channel", "C1", "--slack-thread", "1.2", "--store", "dir:" + root,
	}, &so, &se)
	require.Equal(t, 0, code, se.String())
	var printed plan.Plan
	require.NoError(t, json.Unmarshal(so.Bytes(), &printed))
	require.Regexp(t, "^[0-9a-f]{12}$", printed.ID)
	require.Equal(t, "C1", printed.Slack.Channel)
	require.Equal(t, 500, printed.Workload.MessageSizeBytes)
	require.Equal(t, 10.0, printed.Workload.TargetMBps)

	got, err := plan.NewDirStore(root).Get(context.Background(), printed.ID)
	require.NoError(t, err)
	require.Equal(t, printed.ID, got.ID)
	require.Equal(t, printed.Fingerprint, got.Fingerprint)
}

func TestBuildUsageErrors(t *testing.T) {
	var so, se bytes.Buffer
	cfg := writeConfig(t)
	require.Equal(t, 2, run([]string{"build", "--message-size", "5", "--target-mb-s", "1", "--store", "dir:x"}, &so, &se), "no config")
	require.Equal(t, 2, run([]string{"build", "--config", cfg, "--target-mb-s", "1", "--store", "dir:x"}, &so, &se), "no message size")
	require.Equal(t, 2, run([]string{"build", "--config", cfg, "--message-size", "5", "--target-mb-s", "1"}, &so, &se), "no store")
	require.Equal(t, 2, run([]string{"build", "--config", cfg, "--message-size", "5", "--target-mb-s", "1", "--store", "gcs://x"}, &so, &se), "bad store")
	require.Equal(t, 2, run([]string{"build", "--config", cfg, "--message-size", "5", "--target-mb-s", "1", "--mode", "zzz", "--store", "dir:x"}, &so, &se), "bad mode")
}

func TestShowReadsFromS3Location(t *testing.T) {
	root := t.TempDir()
	co := useFakes(t, root, &fakeEC2{})
	var so, se bytes.Buffer
	require.Equal(t, 0, run([]string{"build", "--config", writeConfig(t), "--message-size", "500", "--target-mb-s", "10", "--store", "s3://b"}, &so, &se), se.String())
	var p plan.Plan
	require.NoError(t, json.Unmarshal(so.Bytes(), &p))

	so.Reset()
	require.Equal(t, 0, run([]string{"show", "--plan", "s3://b/plans/" + p.ID + ".json"}, &so, &se), se.String())
	var got plan.Plan
	require.NoError(t, json.Unmarshal(so.Bytes(), &got))
	require.Equal(t, p.ID, got.ID)

	out := t.TempDir()
	require.Equal(t, 0, run([]string{"render", "--plan", "s3://b/plans/" + p.ID + ".json", "--out", out}, &so, &se), se.String())
	require.FileExists(t, filepath.Join(out, "bot-"+p.ID+".yaml"))
	require.Positive(t, co.calls.Load())
}

func TestBadPlanIDRejectedBeforeAnyS3Call(t *testing.T) {
	co := useFakes(t, t.TempDir(), &fakeEC2{})
	var so, se bytes.Buffer
	for _, loc := range []string{
		"s3://b/plans/../x.json", "s3://b/plans/ZZZ.json", "s3://b/other/0123456789ab.json",
		"s3://b/plans/0123456789ab.dispatched", "s3://b/plans/a/b.json", "s3://b",
	} {
		require.Equal(t, 1, run([]string{"show", "--plan", loc}, &so, &se), loc)
	}
	require.Zero(t, co.calls.Load())
}

func lockArgs(sub string, root string, extra ...string) []string {
	return append([]string{"lock", sub, "--store", "dir:" + root}, extra...)
}

func TestLockAcquireReleaseExitCodes(t *testing.T) {
	root := t.TempDir()
	e := &fakeEC2{}
	useFakes(t, root, e)
	var so, se bytes.Buffer
	require.Equal(t, 0, run(lockArgs("acquire", root, "--holder", "a", "--wait", "1s", "--poll", "5ms"), &so, &se), se.String())
	require.Equal(t, 3, run(lockArgs("acquire", root, "--holder", "b", "--wait", "50ms", "--poll", "5ms"), &so, &se), "held lock: wait timeout")
	require.Equal(t, 0, run(lockArgs("release", root, "--holder", "b"), &so, &se), "non-holder release is fine")
	require.Equal(t, 3, run(lockArgs("acquire", root, "--holder", "b", "--wait", "30ms", "--poll", "5ms"), &so, &se))
	require.Equal(t, 0, run(lockArgs("release", root, "--holder", "a"), &so, &se))
	require.Equal(t, 0, run(lockArgs("release", root, "--holder", "a"), &so, &se), "not held is fine")
	require.Equal(t, 0, run(lockArgs("acquire", root, "--holder", "b", "--wait", "1s", "--poll", "5ms"), &so, &se))
	require.Equal(t, 2, run(lockArgs("acquire", root), &so, &se), "holder required")
	require.Equal(t, 2, run(lockArgs("release", root), &so, &se), "holder required")
	require.Equal(t, 2, run(lockArgs("bogus", root, "--holder", "a"), &so, &se))
}

func TestLockAcquireWaitsForRunningInstances(t *testing.T) {
	root := t.TempDir()
	e := &fakeEC2{running: []int{2, 1, 1}}
	useFakes(t, root, e)
	var so, se bytes.Buffer
	code := run(lockArgs("acquire", root, "--holder", "a", "--wait", "5s", "--poll", "5ms"), &so, &se)
	require.Equal(t, 0, code, se.String())
	require.Equal(t, 4, e.calls, "polled until the stack was empty")

	// Filters: bench tag and live states.
	in := e.inputs[0]
	got := map[string][]string{}
	for _, f := range in.Filters {
		got[aws.ToString(f.Name)] = f.Values
	}
	require.Equal(t, []string{"redpanda-connect-bench"}, got["tag:Project"])
	require.Contains(t, got["instance-state-name"], "running")

	// The lock was taken only after the stack was empty: nothing in the lock dir until the end.
	_, err := os.Stat(filepath.Join(root, "locks", "shared-stack"))
	require.NoError(t, err)
}

func TestLockAcquireTimesOutWhileInstancesRunning(t *testing.T) {
	root := t.TempDir()
	e := &fakeEC2{running: make([]int, 1000)}
	for i := range e.running {
		e.running[i] = 1
	}
	useFakes(t, root, e)
	var so, se bytes.Buffer
	require.Equal(t, 3, run(lockArgs("acquire", root, "--holder", "a", "--wait", "40ms", "--poll", "5ms"), &so, &se))
	_, err := os.Stat(filepath.Join(root, "locks", "shared-stack"))
	require.True(t, os.IsNotExist(err), "lock must not be taken while instances run")
}

func TestLockAcquireRetriesEC2Errors(t *testing.T) {
	root := t.TempDir()
	e := &fakeEC2{errs: 2}
	useFakes(t, root, e)
	var so, se bytes.Buffer
	require.Equal(t, 0, run(lockArgs("acquire", root, "--holder", "a", "--wait", "5s", "--poll", "5ms"), &so, &se), se.String())
	require.Contains(t, se.String(), "will retry")
}

func TestLockAcquireFailsWhenEC2ErrorsPersist(t *testing.T) {
	root := t.TempDir()
	useFakes(t, root, &fakeEC2{errs: 1 << 20})
	var so, se bytes.Buffer
	require.Equal(t, 1, run(lockArgs("acquire", root, "--holder", "a", "--wait", "40ms", "--poll", "5ms"), &so, &se))
}

func TestLockAcquireTimeoutAfterEC2ErrorIsExit3(t *testing.T) {
	root := t.TempDir()
	e := &fakeEC2{}
	useFakes(t, root, e)
	var so, se bytes.Buffer
	require.Equal(t, 0, run(lockArgs("acquire", root, "--holder", "a", "--wait", "1s", "--poll", "5ms"), &so, &se), se.String())
	e.errs = 1
	require.Equal(t, 3, run(lockArgs("acquire", root, "--holder", "b", "--wait", "40ms", "--poll", "5ms"), &so, &se),
		"one EC2 error, then a held lock until the deadline")
}
