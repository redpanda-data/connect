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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/require"
)

// noFactory fails the test if preflightCheck ever calls it — every
// single-region test below relies on FakeEC2Client.DescribeRegions'
// default (one region, named fakeDefaultRegion) matching the homeRegion
// argument exactly, so the home client should always be reused and the
// factory should never be invoked.
func noFactory(t *testing.T) EC2ClientFactory {
	return func(context.Context, string) (EC2Client, error) {
		t.Fatal("factory must not be called for the home region")
		return nil, nil
	}
}

func TestPreflightCheck_NoInstancesPasses(t *testing.T) {
	client := &FakeEC2Client{Output: &ec2.DescribeInstancesOutput{}}
	require.NoError(t, preflightCheck(context.Background(), client, fakeDefaultRegion, noFactory(t)))
}

func TestPreflightCheck_SendsExpectedFilters(t *testing.T) {
	client := &FakeEC2Client{Output: &ec2.DescribeInstancesOutput{}}
	require.NoError(t, preflightCheck(context.Background(), client, fakeDefaultRegion, noFactory(t)))
	require.Len(t, client.Requests, 1)

	req := client.Requests[0]
	require.Len(t, req.Filters, 2)
	require.Equal(t, "tag:Project", aws.ToString(req.Filters[0].Name))
	require.Equal(t, []string{"redpanda-connect-bench"}, req.Filters[0].Values)
	require.Equal(t, "instance-state-name", aws.ToString(req.Filters[1].Name))
	require.Equal(t, []string{"pending", "running"}, req.Filters[1].Values)
}

func TestPreflightCheck_RunningInstanceFailsAndNamesIt(t *testing.T) {
	launch := time.Date(2026, 8, 17, 10, 30, 0, 0, time.UTC)
	client := &FakeEC2Client{
		Output: &ec2.DescribeInstancesOutput{
			Reservations: []ec2types.Reservation{
				{
					Instances: []ec2types.Instance{
						{
							InstanceId: aws.String("i-abc123"),
							LaunchTime: &launch,
							Tags: []ec2types.Tag{
								{Key: aws.String("bench-session-id"), Value: aws.String("bench-20260817-103000")},
							},
						},
					},
				},
			},
		},
	}
	err := preflightCheck(context.Background(), client, fakeDefaultRegion, noFactory(t))
	require.Error(t, err)
	require.Contains(t, err.Error(), "i-abc123")
	require.Contains(t, err.Error(), "bench-20260817-103000")
	require.Contains(t, err.Error(), "2026-08-17T10:30:00Z")
	require.Contains(t, err.Error(), "concurrent sessions destroy each other's infrastructure")
	require.Contains(t, err.Error(), "--preflight=off")
	require.Contains(t, err.Error(), fakeDefaultRegion, "the conflicting session's region must be named")
}

func TestPreflightCheck_MultipleInstancesListsAll(t *testing.T) {
	launch1 := time.Date(2026, 8, 17, 9, 0, 0, 0, time.UTC)
	launch2 := time.Date(2026, 8, 17, 9, 5, 0, 0, time.UTC)
	client := &FakeEC2Client{
		Output: &ec2.DescribeInstancesOutput{
			Reservations: []ec2types.Reservation{
				{
					Instances: []ec2types.Instance{
						{
							InstanceId: aws.String("i-runner"),
							LaunchTime: &launch1,
							Tags:       []ec2types.Tag{{Key: aws.String("bench-session-id"), Value: aws.String("bench-a")}},
						},
						{
							InstanceId: aws.String("i-loadgen"),
							LaunchTime: &launch2,
							Tags:       []ec2types.Tag{{Key: aws.String("bench-session-id"), Value: aws.String("bench-a")}},
						},
					},
				},
			},
		},
	}
	err := preflightCheck(context.Background(), client, fakeDefaultRegion, noFactory(t))
	require.Error(t, err)
	require.Contains(t, err.Error(), "2 concurrent bench session(s)")
	require.Contains(t, err.Error(), "i-runner")
	require.Contains(t, err.Error(), "i-loadgen")
}

func TestPreflightCheck_UntaggedInstanceStillReported(t *testing.T) {
	client := &FakeEC2Client{
		Output: &ec2.DescribeInstancesOutput{
			Reservations: []ec2types.Reservation{
				{Instances: []ec2types.Instance{{InstanceId: aws.String("i-mystery")}}},
			},
		},
	}
	err := preflightCheck(context.Background(), client, fakeDefaultRegion, noFactory(t))
	require.Error(t, err)
	require.Contains(t, err.Error(), "i-mystery")
	require.Contains(t, err.Error(), "<untagged>")
	require.Contains(t, err.Error(), "<unknown launch time>")
}

func TestPreflightCheck_DescribeInstancesErrorPropagates(t *testing.T) {
	client := &FakeEC2Client{Err: errors.New("access denied")}
	err := preflightCheck(context.Background(), client, fakeDefaultRegion, noFactory(t))
	require.Error(t, err)
	require.Contains(t, err.Error(), "access denied")
}

// TestPreflightCheck_DescribeRegionsErrorFailsLoudly is the regression
// test for the "loudly, never silently skip a region" half of Finding #H:
// if the guard can't even enumerate the account's enabled regions, it
// must fail outright rather than falling back to checking only the home
// region (which would silently narrow the guard back to its old,
// insufficient single-region behavior with no indication anything was
// skipped).
func TestPreflightCheck_DescribeRegionsErrorFailsLoudly(t *testing.T) {
	client := &FakeEC2Client{RegionsErr: errors.New("access denied to ec2:DescribeRegions")}
	err := preflightCheck(context.Background(), client, fakeDefaultRegion, noFactory(t))
	require.Error(t, err)
	require.Contains(t, err.Error(), "access denied to ec2:DescribeRegions")
}

// TestPreflightCheck_ConflictInNonHomeRegionFailsAndNamesRegion is the
// regression test for the core of Finding #H: the concurrent-session guard
// used to scan only opts.region, but the shared Terraform state key it
// protects is region-free — so a session running in one region passed
// preflight while colliding with a session already running in ANOTHER
// region (the exact 2026-08-17 mutual-destroy incident the guard exists to
// prevent). A conflicting instance that exists ONLY in a non-home enabled
// region must still fail preflight, and the error must name that region.
func TestPreflightCheck_ConflictInNonHomeRegionFailsAndNamesRegion(t *testing.T) {
	const homeRegion = "us-east-1"
	const otherRegion = "us-west-2"

	homeClient := &FakeEC2Client{
		Output: &ec2.DescribeInstancesOutput{}, // nothing running at home
		Regions: &ec2.DescribeRegionsOutput{
			Regions: []ec2types.Region{
				{RegionName: aws.String(homeRegion)},
				{RegionName: aws.String(otherRegion)},
			},
		},
	}
	otherClient := &FakeEC2Client{
		Output: &ec2.DescribeInstancesOutput{
			Reservations: []ec2types.Reservation{
				{
					Instances: []ec2types.Instance{
						{
							InstanceId: aws.String("i-other-region"),
							Tags:       []ec2types.Tag{{Key: aws.String("bench-session-id"), Value: aws.String("bench-conflict")}},
						},
					},
				},
			},
		},
	}
	factory := func(_ context.Context, region string) (EC2Client, error) {
		require.Equal(t, otherRegion, region, "the home region must be served by the home client, never routed through the factory")
		return otherClient, nil
	}

	err := preflightCheck(context.Background(), homeClient, homeRegion, factory)
	require.Error(t, err)
	require.Contains(t, err.Error(), "i-other-region")
	require.Contains(t, err.Error(), "bench-conflict")
	require.Contains(t, err.Error(), otherRegion, "the error must name the region the conflict actually lives in")
}

// TestPreflightCheck_FactoryErrorFailsLoudly pins the same "no silent
// region skip" contract for the per-region client construction step, not
// just DescribeRegions/DescribeInstances.
func TestPreflightCheck_FactoryErrorFailsLoudly(t *testing.T) {
	const homeRegion = "us-east-1"
	const otherRegion = "eu-west-1"
	homeClient := &FakeEC2Client{
		Output: &ec2.DescribeInstancesOutput{},
		Regions: &ec2.DescribeRegionsOutput{
			Regions: []ec2types.Region{
				{RegionName: aws.String(homeRegion)},
				{RegionName: aws.String(otherRegion)},
			},
		},
	}
	factory := func(_ context.Context, region string) (EC2Client, error) {
		return nil, errors.New("could not load AWS config for region " + region)
	}
	err := preflightCheck(context.Background(), homeClient, homeRegion, factory)
	require.Error(t, err)
	require.Contains(t, err.Error(), otherRegion)
}
