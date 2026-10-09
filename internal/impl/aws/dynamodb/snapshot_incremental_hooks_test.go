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

const timedRecordsPage = `[{"eventID":"1","eventName":"MODIFY","dynamodb":{"ApproximateCreationDateTime":1790000000,"SequenceNumber":"00001","Keys":{"pk":{"S":"a"}},"NewImage":{"pk":{"S":"a"}}}}]`

// A held snapshot item for key a must be dropped by the reader before the
// reader's batch for a reaches the message channel, on both reader paths.
func TestIncrementalHooksReaderTouchesBeforeSend(t *testing.T) {
	// msgChanCap 0: the reader blocks on the unbuffered send until this test
	// receives, so the held item's fate can be observed before the batch is
	// drained rather than racing the receive against TouchRecords.
	for name, mk := range readerHarnesses(timedRecordsPage, 100, 0) {
		t.Run(name, func(t *testing.T) {
			h := mk()
			inc := incsnapshot.NewState(0, time.Minute)
			h.d.incremental = inc
			if ts := h.tableStream(); ts != nil {
				ts.incremental = inc
			}
			inc.Register("shard-001")
			inc.RefreshDone([]string{"shard-001"})
			inc.Window().Begin(0)
			inc.Window().Hold(0, DynamoItems{{"pk": &dynamodbtypes.AttributeValueMemberS{Value: "a"}}}, []string{mustStreamKey(t, "a")}, time.Unix(1790000000, 0), nil)
			require.Equal(t, 1, inc.Window().HeldItems(), "the held item must be in place before the reader starts")

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			go h.start(ctx)

			// The reader is now blocked on the unbuffered send (or about to
			// be): if TouchRecords ran after the send instead of before, the
			// item would still be held here, and the reader would stay
			// blocked forever since nothing else drains msgChan yet.
			assert.Eventually(t, func() bool {
				return inc.Window().HeldItems() == 0
			}, 5*time.Second, 10*time.Millisecond, "the reader must have touched a before sending")

			select {
			case <-h.d.msgChan:
			case <-time.After(5 * time.Second):
				t.Fatal("no batch from reader")
			}
			assert.Eventually(t, func() bool {
				return inc.Progress().CaughtUpPast(time.Unix(1790000000, 0))
			}, 5*time.Second, 10*time.Millisecond, "record time observed after send")
		})
	}
}

func mustStreamKey(t *testing.T, pk string) string {
	t.Helper()
	k, ok := incsnapshot.WindowKeyFromItem(map[string]dynamodbtypes.AttributeValue{"pk": &dynamodbtypes.AttributeValueMemberS{Value: pk}},
		[]dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("pk")}})
	require.True(t, ok)
	return k
}
