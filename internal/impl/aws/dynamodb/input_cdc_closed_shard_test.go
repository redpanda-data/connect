// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	incsnapshot "github.com/redpanda-data/connect/v4/internal/impl/aws/dynamodb/incrementalsnapshot"
)

// TestShardReaderClosedShardFinalPageTouchesWindow: the last GetRecords response
// of a closed shard (nil NextShardIterator) can still carry records. Both
// reader paths must touch the incremental window with them and deliver them
// before marking the shard exhausted, and must not mark it exhausted while
// the send is still pending (cleanup would cancel the reader and lose the
// page).
func TestShardReaderClosedShardFinalPageTouchesWindow(t *testing.T) {
	mk := func() aws.HTTPClient { return &closedShardStubTransport{records: timedRecordsPage} }
	for name, mkHarness := range readerHarnessesWithTransport(mk, 100, 0) { // unbuffered: the send blocks until received
		t.Run(name, func(t *testing.T) {
			h := mkHarness()
			inc := incsnapshot.NewState(0, time.Minute)
			h.d.incremental = inc
			ts := h.tableStream()
			if ts != nil {
				ts.incremental = inc
			}
			inc.Register("shard-001")
			inc.RefreshDone([]string{"shard-001"})
			inc.Window().Begin(0)
			inc.Window().Hold(0, DynamoItems{{"pk": &dynamodbtypes.AttributeValueMemberS{Value: "a"}}}, []string{mustStreamKey(t, "a")}, time.Unix(1790000000, 0), nil)

			d := h.d
			exhausted := func() bool {
				if ts != nil {
					ts.mu.RLock()
					defer ts.mu.RUnlock()
				} else {
					d.mu.RLock()
					defer d.mu.RUnlock()
				}
				return h.shardReaders["shard-001"].exhausted
			}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan struct{})
			go func() {
				h.start(ctx)
				close(done)
			}()

			require.Eventually(t, func() bool { return inc.Window().HeldItems() == 0 },
				5*time.Second, 5*time.Millisecond, "the final page's records must touch the window")
			assert.False(t, exhausted(), "the shard must not be exhausted while its final page is unsent")

			select {
			case am := <-h.d.msgChan:
				require.Len(t, am.msg, 1, "the closed shard's final page must be delivered")
			case <-time.After(5 * time.Second):
				t.Fatal("the closed shard's final page was dropped")
			}

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("the reader should return once the closed shard is drained")
			}
			assert.True(t, exhausted(), "the shard is exhausted after its final page is sent")
		})
	}
}

// TestShardReaderClosedShardRequestsRefresh: a shard that closes normally
// must ask its coordinator for an immediate shard refresh. Waiting for the
// next periodic refresh delays its children, and in incremental mode keeps
// the shard progress tracker unsettled, holding every snapshot page, until
// that refresh registers them.
func TestShardReaderClosedShardRequestsRefresh(t *testing.T) {
	mk := func() aws.HTTPClient { return &closedShardStubTransport{records: `[]`} }
	for name, mkHarness := range readerHarnessesWithTransport(mk, 100, 1) {
		t.Run(name, func(t *testing.T) {
			h := mkHarness()
			refresh := make(chan struct{}, 1)
			if ts := h.tableStream(); ts != nil {
				ts.shardRefreshCh = refresh
			} else {
				h.d.shardRefreshCh = refresh
			}

			done := make(chan struct{})
			go func() {
				h.start(t.Context())
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("the reader should return once the closed shard is drained")
			}
			select {
			case <-refresh:
			default:
				t.Fatal("a closed shard must request an immediate shard refresh")
			}
		})
	}
}
