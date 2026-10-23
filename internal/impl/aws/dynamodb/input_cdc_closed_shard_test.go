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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	incsnapshot "github.com/redpanda-data/connect/v4/internal/impl/aws/dynamodb/incrementalsnapshot"
)

// closedShardStubTransport serves a single GetRecords page for a shard that
// has just closed: the page carries records but no NextShardIterator.
type closedShardStubTransport struct {
	records string
	pages   atomic.Int64
}

func (s *closedShardStubTransport) Do(req *http.Request) (*http.Response, error) {
	target := req.Header.Get("X-Amz-Target")
	if !strings.HasSuffix(target, ".GetRecords") {
		return nil, fmt.Errorf("closedShardStubTransport: unexpected operation %q", target)
	}
	s.pages.Add(1)
	return jsonResponse(req, 200, fmt.Sprintf(`{"Records":%s}`, s.records)), nil
}

// TestShardReader_ClosedShardFinalPageDelivered: the last GetRecords response
// of a closed shard (nil NextShardIterator) can still carry records. Both
// reader paths must touch the incremental window with them and deliver them
// before marking the shard exhausted, and must not mark it exhausted while
// the send is still pending (cleanup would cancel the reader and lose the
// page).
func TestShardReader_ClosedShardFinalPageDelivered(t *testing.T) {
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

// trimmedCheckpointStubTransport fakes GetShardIterator for a shard whose
// checkpoint has aged out of the stream: AFTER_SEQUENCE_NUMBER fails with
// TrimmedDataAccessException, TRIM_HORIZON succeeds.
type trimmedCheckpointStubTransport struct {
	mu    sync.Mutex
	types []string
}

func (s *trimmedCheckpointStubTransport) Do(req *http.Request) (*http.Response, error) {
	target := req.Header.Get("X-Amz-Target")
	if !strings.HasSuffix(target, ".GetShardIterator") {
		return nil, fmt.Errorf("trimmedCheckpointStubTransport: unexpected operation %q", target)
	}
	payload, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	var in struct {
		ShardIteratorType string
		SequenceNumber    string
	}
	if err := json.Unmarshal(payload, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.types = append(s.types, in.ShardIteratorType)
	s.mu.Unlock()
	if in.ShardIteratorType == "AFTER_SEQUENCE_NUMBER" {
		return jsonResponse(req, 400, `{"__type":"TrimmedDataAccessException","message":"stubbed for test"}`), nil
	}
	return jsonResponse(req, 200, `{"ShardIterator":"iter-trim-horizon"}`), nil
}

// TestPrepareShardReader_TrimmedCheckpointFallsBackToTrimHorizon: a listed
// shard whose checkpointed sequence is past the stream's retention can never
// get an AFTER_SEQUENCE_NUMBER iterator. Failing the prepare on every refresh
// left the shard unread forever and, in incremental mode, kept the shard
// progress tracker unsettled so the backfill never released a page.
func TestPrepareShardReader_TrimmedCheckpointFallsBackToTrimHorizon(t *testing.T) {
	transport := &trimmedCheckpointStubTransport{}
	d := &dynamoDBCDCInput{
		log:           service.MockResources().Logger(),
		streamsClient: newStubStreamsClient(transport),
	}
	cp := newTestCheckpointer(t)
	cp.svc = &fakeCheckpointAPI{
		getItem: func(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: map[string]dynamodbtypes.AttributeValue{
				"SequenceNumber": &dynamodbtypes.AttributeValueMemberS{Value: "00001"},
			}}, nil
		},
	}

	p, err := d.prepareShardReader(t.Context(), cp, aws.String(testStreamArn), "shard-001", "", false)
	require.NoError(t, err)
	assert.Equal(t, "iter-trim-horizon", aws.ToString(p.iterator))
	transport.mu.Lock()
	defer transport.mu.Unlock()
	assert.Equal(t, []string{"AFTER_SEQUENCE_NUMBER", "TRIM_HORIZON"}, transport.types)
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
