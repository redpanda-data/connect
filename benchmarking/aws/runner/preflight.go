// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
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
}

type awsEC2 struct {
	client *ec2.Client
}

// NewEC2Client builds an EC2Client backed by the AWS SDK in the given
// region.
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

// FakeEC2Client returns a canned DescribeInstances response or error — for
// tests.
type FakeEC2Client struct {
	Output *ec2.DescribeInstancesOutput
	Err    error
	// Requests records every DescribeInstancesInput this fake was called
	// with, in call order — so a test can assert on the exact filters
	// preflightCheck sent without a real EC2 API in the loop.
	Requests []*ec2.DescribeInstancesInput
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

// runningBenchSession is one already-launching-or-running EC2 instance
// preflightCheck found tagged as bench infrastructure — enough detail for
// an operator to go identify (and, if truly abandoned, tear down) the
// conflicting session.
type runningBenchSession struct {
	InstanceID string
	SessionID  string
	LaunchTime time.Time
}

// preflightCheck fails loudly when another bench session's EC2 instances
// (runner and/or load-gen, tagged Project=redpanda-connect-bench by every
// session's own terraform apply) are already pending or running. Every
// bench session applies and later destroys the SAME shared Terraform
// stack, so two concurrent sessions destroy each other's infrastructure
// mid-run — observed live twice on 2026-08-17 (a laptop bench and the
// scheduled soak collided). Called from runBench before any terraform
// apply, so the conflict is caught before either session's state is
// touched.
func preflightCheck(ctx context.Context, client EC2Client) error {
	out, err := client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Project"), Values: []string{preflightProjectTag}},
			{Name: aws.String("instance-state-name"), Values: preflightActiveStates},
		},
	})
	if err != nil {
		return fmt.Errorf("preflight: describe EC2 instances: %w", err)
	}

	var sessions []runningBenchSession
	for _, r := range out.Reservations {
		for _, inst := range r.Instances {
			s := runningBenchSession{InstanceID: aws.ToString(inst.InstanceId)}
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
	if len(sessions) == 0 {
		return nil
	}

	sort.Slice(sessions, func(i, j int) bool { return sessions[i].InstanceID < sessions[j].InstanceID })
	return fmt.Errorf(
		"preflight: %d concurrent bench session(s) already running — concurrent sessions destroy each other's infrastructure, refusing to proceed (use --preflight=off to override, at your own risk):\n%s",
		len(sessions), formatRunningBenchSessions(sessions))
}

// formatRunningBenchSessions renders one line per session for
// preflightCheck's error, naming the session ID an operator would look
// for in CI/scheduling logs rather than only the opaque instance ID.
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
		lines = append(lines, fmt.Sprintf("  - instance %s: bench-session-id=%s, launched %s", s.InstanceID, sessionID, launch))
	}
	return strings.Join(lines, "\n")
}
