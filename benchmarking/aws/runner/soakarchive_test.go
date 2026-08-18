// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"errors"
	"fmt"
	"testing"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

// TestBuildSoakArchivePlan_AgreesWithFetchKeys pins buildSoakArchivePlan's
// three RawKeys entries to the EXACT source keys MatrixRunner.fetchLog,
// fetchProm, and fetchBrokerSeriesForEngine read (see matrix.go) — those
// three functions and this one independently format
// "runs/<sessionID>/sweep-<key>.log" etc., and nothing else asserts they
// stay in sync. A drift here means the archive silently copies the wrong
// object (or none), which looks like "the soak archived fine" rather than
// a missing-evidence bug.
func TestBuildSoakArchivePlan_AgreesWithFetchKeys(t *testing.T) {
	const sessionID = "bench-20260101-000000"
	const key = "4"
	const engine = "connect"
	brokerArtifact := sourceTopology{}.MetricArtifact(engine, key)

	plan := buildSoakArchivePlan(sessionID, "postgres_cdc-soak", key, brokerArtifact)

	require.Equal(t, fmt.Sprintf("runs/%s/result.json", sessionID), plan.ResultKey)
	require.Equal(t, fmt.Sprintf("soak-index/postgres_cdc-soak/%s.json", sessionID), plan.IndexKey)
	require.Equal(t, []string{
		fmt.Sprintf("runs/%s/sweep-%s.log", sessionID, key),
		fmt.Sprintf("runs/%s/prom-%s.txt", sessionID, key),
		fmt.Sprintf("runs/%s/%s", sessionID, brokerArtifact),
	}, plan.RawKeys)

	// Pinned against the literal formats fetchLog/fetchProm/
	// fetchBrokerSeriesForEngine use, so a future edit to either side that
	// silently diverges the key format fails this test instead of shipping.
	require.Contains(t, plan.RawKeys, "runs/bench-20260101-000000/sweep-4.log")
	require.Contains(t, plan.RawKeys, "runs/bench-20260101-000000/prom-4.txt")
	require.Contains(t, plan.RawKeys, "runs/bench-20260101-000000/redpanda-4-connect.txt")
}

// TestBuildSoakArchivePlan_ArmKeyFormat exercises the "<vcpu>-<armID>" key
// shape (see sweepPoint.Key), which is how a future arms-enabled soak
// scenario would key its artifacts.
func TestBuildSoakArchivePlan_ArmKeyFormat(t *testing.T) {
	key := sweepPoint{VCPU: 4, ArmID: "a0"}.Key()
	require.Equal(t, "4-a0", key)

	plan := buildSoakArchivePlan("bench-x", "scenario-y", key, "")
	require.Equal(t, []string{
		"runs/bench-x/sweep-4-a0.log",
		"runs/bench-x/prom-4-a0.txt",
	}, plan.RawKeys)
}

// TestBuildSoakArchivePlan_NoKeyMeansNoRawArtifacts covers the degrade-to-
// zero-values path uploadSoakResult takes when result.Points is empty
// (documented as "impossible in the success path" but not enforced by the
// type system) — the result.json/soak-index keys must still be well-formed.
func TestBuildSoakArchivePlan_NoKeyMeansNoRawArtifacts(t *testing.T) {
	plan := buildSoakArchivePlan("bench-x", "scenario-y", "", "should-be-ignored")
	require.Equal(t, "runs/bench-x/result.json", plan.ResultKey)
	require.Equal(t, "soak-index/scenario-y/bench-x.json", plan.IndexKey)
	require.Empty(t, plan.RawKeys)
}

// TestBuildSoakArchivePlan_EmptyBrokerArtifactSkipsThirdRawKey covers a
// Topology-less caller (topo == nil in uploadSoakResult): only the two
// engine-agnostic raw artifacts are archived.
func TestBuildSoakArchivePlan_EmptyBrokerArtifactSkipsThirdRawKey(t *testing.T) {
	plan := buildSoakArchivePlan("bench-x", "scenario-y", "4", "")
	require.Equal(t, []string{
		"runs/bench-x/sweep-4.log",
		"runs/bench-x/prom-4.txt",
	}, plan.RawKeys)
}

// TestBuildSoakArchivePlanForPoints_ArchivesEveryPoint is the CON-179 R6
// increment 5 regression this function exists to prevent: a binary-arm
// soak measures TWO points (one per arm) in one session, and
// buildSoakArchivePlan alone (built for a single-point soak) would silently
// archive only the first. Every point's raw keys must appear, using each
// point's OWN arm-keyed sweepPoint.Key(), not just point 0's.
func TestBuildSoakArchivePlanForPoints_ArchivesEveryPoint(t *testing.T) {
	points := []PointResult{
		{VCPU: 2, Arm: "base", Engine: "connect"},
		{VCPU: 2, Arm: "pr", Engine: "connect"},
	}
	plan := buildSoakArchivePlanForPoints("bench-x", "scenario-y", points, nil)
	require.Equal(t, "runs/bench-x/result.json", plan.ResultKey)
	require.Equal(t, "soak-index/scenario-y/bench-x.json", plan.IndexKey)
	require.Equal(t, []string{
		"runs/bench-x/sweep-2-base.log", "runs/bench-x/prom-2-base.txt",
		"runs/bench-x/sweep-2-pr.log", "runs/bench-x/prom-2-pr.txt",
	}, plan.RawKeys)
}

// TestBuildSoakArchivePlanForPoints_SinglePointMatchesLegacyPlan is the
// parity guard: a single-point (non-binary-arm) soak's plan must be
// identical to what buildSoakArchivePlan itself would have produced, byte
// for byte.
func TestBuildSoakArchivePlanForPoints_SinglePointMatchesLegacyPlan(t *testing.T) {
	points := []PointResult{{VCPU: 4, Engine: "connect"}}
	got := buildSoakArchivePlanForPoints("bench-x", "postgres_cdc-soak", points, nil)
	want := buildSoakArchivePlan("bench-x", "postgres_cdc-soak", sweepPoint{VCPU: 4}.Key(), "")
	require.Equal(t, want, got)
}

// TestBuildSoakArchivePlanForPoints_NoPointsMeansNoRawArtifacts covers the
// empty-result degrade path.
func TestBuildSoakArchivePlanForPoints_NoPointsMeansNoRawArtifacts(t *testing.T) {
	plan := buildSoakArchivePlanForPoints("bench-x", "scenario-y", nil, nil)
	require.Equal(t, "runs/bench-x/result.json", plan.ResultKey)
	require.Empty(t, plan.RawKeys)
}

// TestWrapSoakArchiveUploadErr_NoSuchBucket pins the actionable hint a
// missing archive bucket must carry: the bucket is created by the
// persistent terraform stack, not by any bench session's own apply, so the
// operator needs to be told what command fixes it rather than just seeing
// "NoSuchBucket".
func TestWrapSoakArchiveUploadErr_NoSuchBucket(t *testing.T) {
	err := wrapSoakArchiveUploadErr("runs/x/result.json", "missing-bucket", &s3types.NoSuchBucket{})
	require.ErrorContains(t, err, "task aws:persistent")
	require.ErrorContains(t, err, "missing-bucket")
	require.ErrorContains(t, err, "runs/x/result.json")
}

// TestWrapSoakArchiveUploadErr_OtherErrorPassesThrough covers the non-
// NoSuchBucket path: no false-positive "run task aws:persistent" hint for
// e.g. a permissions error, which that command would not fix.
func TestWrapSoakArchiveUploadErr_OtherErrorPassesThrough(t *testing.T) {
	underlying := errors.New("access denied")
	err := wrapSoakArchiveUploadErr("runs/x/result.json", "some-bucket", underlying)
	require.ErrorContains(t, err, "access denied")
	require.NotContains(t, err.Error(), "task aws:persistent")
}
