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
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// newIncSingleInput builds a single-table incremental input for table t on
// stream testStreamArn, with its checkpoints in api.
func newIncSingleInput(t *testing.T, scan *scanStubTransport, streams *incStreamsStub, api *memCheckpointAPI) *dynamoDBCDCInput {
	t.Helper()
	d := newIncConnectInput(t, scan, streams)
	d.resolvedTable = "t"
	d.streamArn = aws.String(testStreamArn)
	d.keySchema = []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash}}
	d.checkpointer = memCheckpointer(t, api, "t", testStreamArn)
	d.recordBatcher = newInputRecordBatcher(d.conf, d.log)
	d.shardReaders = map[string]*dynamoDBShardReader{}
	d.shardRefreshCh = make(chan struct{}, 1)
	d.incremental = newIncrementalState(0, time.Minute)
	return d
}

// waitStopped soft-stops a single-table input and waits for its coordinator.
func waitStopped(t *testing.T, d *dynamoDBCDCInput) {
	t.Helper()
	d.shutSig.TriggerSoftStop()
	select {
	case <-d.shutSig.HasStoppedChan():
	case <-time.After(10 * time.Second):
		t.Fatal("input did not stop")
	}
}

const oneStreamRecord = `[{"eventID":"1","eventName":"INSERT","dynamodb":{"SequenceNumber":"00001","Keys":{"pk":{"S":"k"}},"NewImage":{"pk":{"S":"k"}}}}]`

// TestConnectIncrementalSingleFailedBackfillKeepsStreaming: a fatal backfill
// error is logged and reflected in the snapshot state, but ReadBatch keeps
// delivering stream batches; surfacing the error there would stop CDC for
// good, since the input never reconnects on it.
func TestConnectIncrementalSingleFailedBackfillKeepsStreaming(t *testing.T) {
	fastReleaseTick(t)
	streams := &incStreamsStub{
		describe: func(string) []string { return []string{"s1"} },
		records:  map[string]string{"s1": oneStreamRecord},
	}
	// A keyless item fails the backfill.
	scan := &scanStubTransport{items: `[{"other":{"S":"x"}}]`}
	d := newIncSingleInput(t, scan, streams, newMemCheckpointAPI(nil))
	defer waitStopped(t, d)

	require.NoError(t, d.connectIncrementalSingle(t.Context(), "t"))
	require.Eventually(t, func() bool { return d.snapshot.state.Load() == snapshotStateFailed },
		5*time.Second, 5*time.Millisecond, "the backfill fails")

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	batch, ack, err := d.ReadBatch(ctx)
	require.NoError(t, err, "a failed backfill must not stop streaming")
	require.Len(t, batch, 1)
	s, err := batch[0].AsStructured()
	require.NoError(t, err)
	assert.Equal(t, "INSERT", s.(map[string]any)["eventName"])
	require.NoError(t, ack(t.Context(), nil))
}

// TestConnectIncrementalSingleBackfillCancelStopsWorker: the cancel the
// shard coordinator calls on exit stops a backfill whose page is held, so
// the coordinator's wait on msgSenders cannot hang.
func TestConnectIncrementalSingleBackfillCancelStopsWorker(t *testing.T) {
	fastReleaseTick(t)
	streams := &incStreamsStub{describe: func(string) []string { return []string{"s1"} }}
	scan := &scanStubTransport{items: `[{"pk":{"S":"a"}}]`}
	d := newIncSingleInput(t, scan, streams, newMemCheckpointAPI(nil))
	defer waitStopped(t, d)

	require.NoError(t, d.connectIncrementalSingle(t.Context(), "t"))
	// s1 never returns a record, so within the idle grace the page is held.
	require.Eventually(t, func() bool { return d.incremental.window.HeldItems() == 1 },
		5*time.Second, 5*time.Millisecond, "page held")

	require.NotNil(t, d.backfillCancel)
	d.backfillCancel()
	done := make(chan struct{})
	go func() {
		d.msgSenders.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("backfill worker did not stop on cancel")
	}
	assert.Equal(t, 0, d.incremental.window.HeldItems(), "held page discarded")
}

func TestShardCoordinatorCancelsBackfillBeforeWaiting(t *testing.T) {
	d := newBackfillTestInput(t, &scanStubTransport{})
	d.shardReaders = map[string]*dynamoDBShardReader{}
	d.shardRefreshCh = make(chan struct{}, 1)
	backfillCtx, backfillCancel := context.WithCancel(t.Context())
	d.backfillCancel = backfillCancel
	// A backfill that only returns once cancelled, like one whose held page
	// can no longer release.
	d.msgSenders.Go(func() { <-backfillCtx.Done() })

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	done := make(chan struct{})
	go func() {
		d.startShardCoordinator(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("coordinator hung waiting on the backfill")
	}
}

// TestIncrementalBackfillResumesFromAckedPosition: after a crash the
// backfill resumes each segment from its persisted LastKey, so pages acked
// before the crash are not re-read or re-emitted.
func TestIncrementalBackfillResumesFromAckedPosition(t *testing.T) {
	fastReleaseTick(t)
	api := newMemCheckpointAPI(nil)
	cp := memCheckpointer(t, api, "t", testStreamArn)
	api.put(testStreamArn, snapshotRow(testStreamArn, "snapshot#segment#0", false,
		map[string]dynamodbtypes.AttributeValue{"pk": &dynamodbtypes.AttributeValueMemberS{Value: "a"}}))

	tr := &scanStubTransport{scanItems: func(startKey string) string {
		if startKey == "" {
			return `[{"pk":{"S":"a"}},{"pk":{"S":"b"}}]`
		}
		return `[{"pk":{"S":"b"}}]`
	}}
	d := newBackfillTestInput(t, tr)
	inc := settledIncrementalState(time.Now().Add(time.Hour))

	var got []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		for m := range d.msgChan {
			got = append(got, snapshotPKs(t, m.msg)...)
			assert.NoError(t, m.ackFn(t.Context(), nil))
		}
	}()
	require.NoError(t, d.runIncrementalBackfill(t.Context(), backfillTargetFor(cp, inc)))
	close(d.msgChan)
	<-done

	tr.mu.Lock()
	startKeys := append([]string(nil), tr.scanStartKeys...)
	tr.mu.Unlock()
	require.Len(t, startKeys, 1)
	var startKey map[string]map[string]string
	require.NoError(t, json.Unmarshal([]byte(startKeys[0]), &startKey))
	assert.Equal(t, map[string]map[string]string{"pk": {"S": "a"}}, startKey, "the Scan resumes after the acked key")
	assert.Equal(t, []string{"b"}, got, "the acked item a is not emitted again")
	assert.True(t, api.has(testStreamArn, "snapshot#complete"))
}

// describeTableStub serves DescribeTable with a fixed stream view.
type describeTableStub struct {
	mu   sync.Mutex
	view dynamodbtypes.StreamViewType
}

func (s *describeTableStub) Do(req *http.Request) (*http.Response, error) {
	target := req.Header.Get("X-Amz-Target")
	if !strings.HasSuffix(target, ".DescribeTable") {
		return nil, fmt.Errorf("describeTableStub: unexpected %q", target)
	}
	s.mu.Lock()
	view := s.view
	s.mu.Unlock()
	return jsonResponse(req, 200, fmt.Sprintf(`{"Table":{"TableName":"t","LatestStreamArn":%q,"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"StreamSpecification":{"StreamEnabled":true,"StreamViewType":%q}}}`, testStreamArn, view)), nil
}

func TestConnectIncrementalSingleRejectsKeysOnly(t *testing.T) {
	pConf, err := dynamoDBCDCInputConfig().ParseYAML(`
tables: [t]
checkpoint_table: ckpt
snapshot_mode: incremental
region: us-east-1
`, service.NewEnvironment())
	require.NoError(t, err)
	d, err := newDynamoDBCDCInputFromConfig(pConf, service.MockResources())
	require.NoError(t, err)
	d.awsConf = aws.Config{
		Region:      "us-east-1",
		Credentials: aws.AnonymousCredentials{},
		HTTPClient:  &describeTableStub{view: dynamodbtypes.StreamViewTypeKeysOnly},
		Retryer:     func() aws.Retryer { return aws.NopRetryer{} },
	}

	err = d.Connect(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KEYS_ONLY")
	assert.Contains(t, err.Error(), "NEW_IMAGE or NEW_AND_OLD_IMAGES")
}
