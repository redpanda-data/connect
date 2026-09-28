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
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
)

// preflightProjectTag is the default_tags value every bench session's own
// terraform stack (shared + per-connector) applies — see
// terraform/shared/main.tf. The persistent stack tags its own resources
// "redpanda-connect-bench-persistent" instead, so this filter deliberately
// never matches the always-on persistent infrastructure (soak archive
// bucket, orphan reaper) that a bench session is not in conflict with.
const preflightProjectTag = "redpanda-connect-bench"

// preflightSessionTagKey is the tag every bench session's terraform stamps
// onto its own resources with its runner-generated session ID — see
// newSessionID and sharedVars["bench_session_id"] in runBench.
const preflightSessionTagKey = "bench-session-id"

// preflightActiveStates are the EC2 instance-state-name values a
// concurrent session could be in that would race this one's own apply and
// destroy. "stopped"/"terminated"/"shutting-down" instances are not a
// conflict: nothing is actively holding the shared Terraform stack.
var preflightActiveStates = []string{"pending", "running"}

// EC2Client is the narrow slice of EC2 the preflight guard needs. Tests
// fake this; production wires an instance backed by the real SDK — see
// NewEC2Client.
type EC2Client interface {
	DescribeInstances(ctx context.Context, in *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error)
	// DescribeRegions lists this account's ENABLED regions (the default —
	// see preflightCheck, which deliberately never passes AllRegions=true).
	// A disabled-but-not-opted-out region is not somewhere this account's
	// terraform could ever have applied a stack, so scanning it would only
	// slow the guard down for no safety benefit.
	DescribeRegions(ctx context.Context, in *ec2.DescribeRegionsInput) (*ec2.DescribeRegionsOutput, error)
}

// EC2ClientFactory builds a region-scoped EC2Client on demand. preflightCheck
// takes one of these (rather than a fixed list of clients) so it can scan
// however many regions DescribeRegions reports without the caller having to
// pre-construct a client per region — and so tests can fake per-region
// client construction without a real EC2 API in the loop.
type EC2ClientFactory func(ctx context.Context, region string) (EC2Client, error)

type awsEC2 struct {
	client *ec2.Client
}

// NewEC2Client builds an EC2Client backed by the AWS SDK in the given
// region. Also usable directly as an EC2ClientFactory.
func NewEC2Client(ctx context.Context, region string) (EC2Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return &awsEC2{client: ec2.NewFromConfig(cfg)}, nil
}

func (a *awsEC2) DescribeInstances(ctx context.Context, in *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
	return a.client.DescribeInstances(ctx, in)
}

func (a *awsEC2) DescribeRegions(ctx context.Context, in *ec2.DescribeRegionsInput) (*ec2.DescribeRegionsOutput, error) {
	return a.client.DescribeRegions(ctx, in)
}

// fakeDefaultRegion is the single region FakeEC2Client.DescribeRegions
// reports when Regions is left nil — every pre-multi-region preflight test
// passes this same string as its homeRegion, so those tests keep working
// unmodified beyond the signature change to preflightCheck itself.
const fakeDefaultRegion = "us-east-1"

// FakeEC2Client returns canned DescribeInstances/DescribeRegions responses
// or errors — for tests.
type FakeEC2Client struct {
	Output *ec2.DescribeInstancesOutput
	Err    error
	// Requests records every DescribeInstancesInput this fake was called
	// with, in call order — so a test can assert on the exact filters
	// preflightCheck sent without a real EC2 API in the loop.
	Requests []*ec2.DescribeInstancesInput

	// Regions is the canned DescribeRegions response. Nil defaults to a
	// single region named fakeDefaultRegion.
	Regions    *ec2.DescribeRegionsOutput
	RegionsErr error
}

func (f *FakeEC2Client) DescribeInstances(_ context.Context, in *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
	f.Requests = append(f.Requests, in)
	if f.Err != nil {
		return nil, f.Err
	}
	if f.Output == nil {
		return &ec2.DescribeInstancesOutput{}, nil
	}
	return f.Output, nil
}

func (f *FakeEC2Client) DescribeRegions(_ context.Context, _ *ec2.DescribeRegionsInput) (*ec2.DescribeRegionsOutput, error) {
	if f.RegionsErr != nil {
		return nil, f.RegionsErr
	}
	if f.Regions != nil {
		return f.Regions, nil
	}
	return &ec2.DescribeRegionsOutput{
		Regions: []ec2types.Region{{RegionName: aws.String(fakeDefaultRegion)}},
	}, nil
}

// runningBenchSession is one already-launching-or-running EC2 instance
// preflightCheck found tagged as bench infrastructure — enough detail for
// an operator to go identify (and, if truly abandoned, tear down) the
// conflicting session.
type runningBenchSession struct {
	InstanceID string
	SessionID  string
	Region     string
	LaunchTime time.Time
}

// preflightCheck fails loudly when another bench session's EC2 instances
// (runner and/or load-gen, tagged Project=redpanda-connect-bench by every
// session's own terraform apply) are already pending or running IN ANY OF
// this account's enabled regions. Every bench session applies and later
// destroys the SAME shared Terraform stack, and that stack's own state key
// is region-free (see terraform.go) — a session in one region does not
// merely risk colliding with another session in the SAME region, it
// collides with one in ANY region, and the shared stack also owns
// globally-named IAM roles. Observed live on 2026-08-17: a laptop bench in
// one region and the scheduled soak in another mutually destroyed each
// other's infrastructure. Called from runBench before any terraform apply,
// so the conflict is caught before either session's state is touched.
//
// homeClient is reused for homeRegion (the region this run itself targets)
// rather than round-tripping it through newClient; newClient builds a
// region-scoped client for every OTHER enabled region DescribeRegions
// reports (see EC2ClientFactory — NewEC2Client satisfies this directly in
// production).
//
// A DescribeRegions failure, or a DescribeInstances failure in any single
// region, fails the whole check loudly rather than skipping that region:
// a silently skipped region is a silent hole in the guard, defeating the
// reason this check exists. The operator can still disable the guard
// entirely via --preflight=off if that tradeoff is ever wrong for them.
func preflightCheck(ctx context.Context, homeClient EC2Client, homeRegion string, newClient EC2ClientFactory) error {
	regionsOut, err := homeClient.DescribeRegions(ctx, &ec2.DescribeRegionsInput{})
	if err != nil {
		return fmt.Errorf("preflight: describe regions: %w", err)
	}

	var sessions []runningBenchSession
	for _, r := range regionsOut.Regions {
		region := aws.ToString(r.RegionName)
		client := homeClient
		if region != homeRegion {
			client, err = newClient(ctx, region)
			if err != nil {
				return fmt.Errorf("preflight: build EC2 client for region %s: %w", region, err)
			}
		}
		regionSessions, err := preflightCheckRegion(ctx, client, region)
		if err != nil {
			// An enabled region can still be blocked by an org SCP (this
			// account denies EC2 outside an allowlist). A policy that
			// denies DescribeInstances in a region also denies
			// RunInstances there, so no bench session can exist in it —
			// skipping is safe, and failing instead would brick every run
			// (live-hit 2026-08-19). The home region never gets the skip:
			// if we cannot see instances where we are about to apply,
			// something is genuinely wrong.
			if region != homeRegion && isEC2AccessDenied(err) {
				fmt.Printf("preflight: region %s: EC2 access denied by policy — skipping (no bench session can run there)\n", region)
				continue
			}
			return fmt.Errorf("preflight: region %s: %w", region, err)
		}
		sessions = append(sessions, regionSessions...)
	}
	if len(sessions) == 0 {
		return nil
	}

	sort.Slice(sessions, func(i, j int) bool {
		if sessions[i].Region != sessions[j].Region {
			return sessions[i].Region < sessions[j].Region
		}
		return sessions[i].InstanceID < sessions[j].InstanceID
	})
	return fmt.Errorf(
		"preflight: %d concurrent bench session(s) already running — concurrent sessions destroy each other's infrastructure, refusing to proceed (use --preflight=off to override, at your own risk):\n%s",
		len(sessions), formatRunningBenchSessions(sessions))
}

// isEC2AccessDenied reports whether err is EC2's authorization failure
// (api error code "UnauthorizedOperation") — the shape an SCP explicit
// deny produces. Narrow on the code so throttling, expired credentials,
// and transport errors still fail preflight loudly.
func isEC2AccessDenied(err error) bool {
	var ae smithy.APIError
	return errors.As(err, &ae) && ae.ErrorCode() == "UnauthorizedOperation"
}

// preflightCheckRegion is preflightCheck's single-region core: it queries
// one EC2Client for pending/running bench-tagged instances and returns
// them as runningBenchSessions, tagged with region so a multi-region
// caller can name where each conflict actually lives.
func preflightCheckRegion(ctx context.Context, client EC2Client, region string) ([]runningBenchSession, error) {
	out, err := client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Project"), Values: []string{preflightProjectTag}},
			{Name: aws.String("instance-state-name"), Values: preflightActiveStates},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("describe EC2 instances: %w", err)
	}

	var sessions []runningBenchSession
	for _, r := range out.Reservations {
		for _, inst := range r.Instances {
			s := runningBenchSession{InstanceID: aws.ToString(inst.InstanceId), Region: region}
			for _, t := range inst.Tags {
				if aws.ToString(t.Key) == preflightSessionTagKey {
					s.SessionID = aws.ToString(t.Value)
				}
			}
			if inst.LaunchTime != nil {
				s.LaunchTime = *inst.LaunchTime
			}
			sessions = append(sessions, s)
		}
	}
	return sessions, nil
}

// formatRunningBenchSessions renders one line per session for
// preflightCheck's error, naming the session ID and region an operator
// would look for in CI/scheduling logs rather than only the opaque
// instance ID.
func formatRunningBenchSessions(sessions []runningBenchSession) string {
	lines := make([]string, 0, len(sessions))
	for _, s := range sessions {
		sessionID := s.SessionID
		if sessionID == "" {
			sessionID = "<untagged>"
		}
		launch := "<unknown launch time>"
		if !s.LaunchTime.IsZero() {
			launch = s.LaunchTime.UTC().Format(time.RFC3339)
		}
		lines = append(lines, fmt.Sprintf("  - instance %s (region %s): bench-session-id=%s, launched %s", s.InstanceID, s.Region, sessionID, launch))
	}
	return strings.Join(lines, "\n")
}
