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

func TestPreflightCheck_NoInstancesPasses(t *testing.T) {
	client := &FakeEC2Client{Output: &ec2.DescribeInstancesOutput{}}
	require.NoError(t, preflightCheck(context.Background(), client))
}

func TestPreflightCheck_SendsExpectedFilters(t *testing.T) {
	client := &FakeEC2Client{Output: &ec2.DescribeInstancesOutput{}}
	require.NoError(t, preflightCheck(context.Background(), client))
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
	err := preflightCheck(context.Background(), client)
	require.Error(t, err)
	require.Contains(t, err.Error(), "i-abc123")
	require.Contains(t, err.Error(), "bench-20260817-103000")
	require.Contains(t, err.Error(), "2026-08-17T10:30:00Z")
	require.Contains(t, err.Error(), "concurrent sessions destroy each other's infrastructure")
	require.Contains(t, err.Error(), "--preflight=off")
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
	err := preflightCheck(context.Background(), client)
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
	err := preflightCheck(context.Background(), client)
	require.Error(t, err)
	require.Contains(t, err.Error(), "i-mystery")
	require.Contains(t, err.Error(), "<untagged>")
	require.Contains(t, err.Error(), "<unknown launch time>")
}

func TestPreflightCheck_DescribeInstancesErrorPropagates(t *testing.T) {
	client := &FakeEC2Client{Err: errors.New("access denied")}
	err := preflightCheck(context.Background(), client)
	require.Error(t, err)
	require.Contains(t, err.Error(), "access denied")
}
