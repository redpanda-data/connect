// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/require"
)

// TestClassifyPollError is the regression test for Finding #E: every
// GetCommandInvocation error used to be swallowed with an unconditional
// `continue` ("not yet propagated; keep polling"), so a persistent error
// (e.g. STS credentials expiring mid-24h soak) looped forever with no
// output and no failure while paid EC2/RDS sat idle. classifyPollError is
// the pure decision this now routes through, table-tested here because
// Run itself has no test seam for the real AWS SDK client.
func TestClassifyPollError(t *testing.T) {
	notFoundErr := &types.InvocationDoesNotExist{Message: aws.String("no such invocation")}
	otherErr := fmt.Errorf("ExpiredTokenException: the security token included in the request is expired")

	tests := []struct {
		name                    string
		err                     error
		notYetPropagatedIn      int
		otherErrIn              int
		wantKeepPolling         bool
		wantNotYetPropagatedOut int
		wantOtherErrOut         int
	}{
		{
			name:                    "success resets both counters",
			err:                     nil,
			notYetPropagatedIn:      5,
			otherErrIn:              10,
			wantKeepPolling:         true,
			wantNotYetPropagatedOut: 0,
			wantOtherErrOut:         0,
		},
		{
			name:                    "InvocationDoesNotExist well under its cap keeps polling",
			err:                     notFoundErr,
			notYetPropagatedIn:      3,
			otherErrIn:              0,
			wantKeepPolling:         true,
			wantNotYetPropagatedOut: 4,
			wantOtherErrOut:         0,
		},
		{
			name:                    "InvocationDoesNotExist at exactly the cap still keeps polling",
			err:                     notFoundErr,
			notYetPropagatedIn:      notYetPropagatedMaxPolls - 1,
			otherErrIn:              0,
			wantKeepPolling:         true,
			wantNotYetPropagatedOut: notYetPropagatedMaxPolls,
			wantOtherErrOut:         0,
		},
		{
			name:                    "InvocationDoesNotExist over its cap gives up",
			err:                     notFoundErr,
			notYetPropagatedIn:      notYetPropagatedMaxPolls,
			otherErrIn:              0,
			wantKeepPolling:         false,
			wantNotYetPropagatedOut: notYetPropagatedMaxPolls + 1,
			wantOtherErrOut:         0,
		},
		{
			name:                    "other error well under its cap keeps polling",
			err:                     otherErr,
			notYetPropagatedIn:      0,
			otherErrIn:              2,
			wantKeepPolling:         true,
			wantNotYetPropagatedOut: 0,
			wantOtherErrOut:         3,
		},
		{
			name:                    "other error at exactly the cap still keeps polling",
			err:                     otherErr,
			notYetPropagatedIn:      0,
			otherErrIn:              otherPollErrMaxPolls - 1,
			wantKeepPolling:         true,
			wantNotYetPropagatedOut: 0,
			wantOtherErrOut:         otherPollErrMaxPolls,
		},
		{
			name:                    "other error over its cap gives up — the ExpiredTokenException case",
			err:                     otherErr,
			notYetPropagatedIn:      0,
			otherErrIn:              otherPollErrMaxPolls,
			wantKeepPolling:         false,
			wantNotYetPropagatedOut: 0,
			wantOtherErrOut:         otherPollErrMaxPolls + 1,
		},
		{
			name:                    "an unrelated error kind does not touch the notYetPropagated counter",
			err:                     otherErr,
			notYetPropagatedIn:      42,
			otherErrIn:              0,
			wantKeepPolling:         true,
			wantNotYetPropagatedOut: 42,
			wantOtherErrOut:         1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keepPolling, gotNotYetPropagated, gotOtherErr := classifyPollError(tt.err, tt.notYetPropagatedIn, tt.otherErrIn)
			require.Equal(t, tt.wantKeepPolling, keepPolling)
			require.Equal(t, tt.wantNotYetPropagatedOut, gotNotYetPropagated)
			require.Equal(t, tt.wantOtherErrOut, gotOtherErr)
		})
	}
}
