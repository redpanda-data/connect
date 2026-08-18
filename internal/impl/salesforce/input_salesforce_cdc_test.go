// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package salesforce

import (
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/salesforce/salesforcegrpc"
)

// TestHandleStreamErr locks in the stale-replay heuristic's scope: a bare
// gRPC InvalidArgument from the stream means Salesforce rejected the replay
// ID (recoverable: clear the checkpoint, resubscribe via the preset), but a
// terminal verdict from the subscription (undecodable payload, unfetchable
// schema) must NEVER clear the checkpoint - even when it wraps an
// InvalidArgument, that status describes the schema, not the replay ID.
func TestHandleStreamErr(t *testing.T) {
	newExecutor := func() *salesforceCDCInputExecutor {
		return &salesforceCDCInputExecutor{
			salesforceCDCInput: &salesforceCDCInput{
				logger: service.NewLoggerFromSlog(slog.Default()),
				mgr:    service.MockResources(),
			},
			state: executorState{Topics: TopicReplays{"/data/T__e": []byte{0x01}}},
		}
	}

	for _, tc := range []struct {
		name           string
		err            error
		wantReset      bool
		wantCheckpoint bool
	}{
		{
			name:           "bare InvalidArgument is a stale replay ID: clear and resubscribe",
			err:            status.Error(codes.InvalidArgument, "replay id rejected"),
			wantReset:      true,
			wantCheckpoint: false,
		},
		{
			name:           "wrapped InvalidArgument from the stream still resets",
			err:            fmt.Errorf("topic stream: %w", status.Error(codes.InvalidArgument, "replay id rejected")),
			wantReset:      true,
			wantCheckpoint: false,
		},
		{
			name:           "terminal verdict wrapping InvalidArgument must not clear the checkpoint",
			err:            &salesforcegrpc.TerminalStreamError{Err: fmt.Errorf("fetching schema: %w", status.Error(codes.InvalidArgument, "malformed schema id"))},
			wantReset:      false,
			wantCheckpoint: true,
		},
		{
			name:           "terminal decode verdict is fatal, checkpoint intact",
			err:            &salesforcegrpc.TerminalStreamError{Err: errors.New("payload permanently undecodable")},
			wantReset:      false,
			wantCheckpoint: true,
		},
		{
			name:           "unrelated stream error is fatal without a reset",
			err:            status.Error(codes.Unavailable, "gone"),
			wantReset:      false,
			wantCheckpoint: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newExecutor()
			got := e.handleStreamErr(t.Context(), "/data/T__e", tc.err)
			require.Equal(t, tc.wantReset, got)
			_, ok := e.state.Topics["/data/T__e"]
			require.Equal(t, tc.wantCheckpoint, ok, "checkpoint presence mismatch")
		})
	}
}
