// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// signalConnectStub fakes DynamoDB and DynamoDB Streams for a whole Connect.
// DescribeTable serves the tables in views (and the checkpoint table ckpt);
// any other table does not exist. ListTables lists listed, and every table
// carries the tag cdc=on. The checkpoint table is empty and every stream has
// no shards. DescribeTable and Scan table names are recorded.
type signalConnectStub struct {
	mu        sync.Mutex
	views     map[string]dynamodbtypes.StreamViewType
	listed    []string
	described []string
	scanned   []string
}

func (s *signalConnectStub) Do(req *http.Request) (*http.Response, error) {
	target := req.Header.Get("X-Amz-Target")
	body, _ := io.ReadAll(req.Body)
	var in struct {
		TableName string
		StreamArn string
	}
	_ = json.Unmarshal(body, &in)
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case strings.HasSuffix(target, ".DescribeTable"):
		s.described = append(s.described, in.TableName)
		if in.TableName == "ckpt" {
			return jsonResponse(req, 200, `{"Table":{"TableName":"ckpt","TableStatus":"ACTIVE"}}`), nil
		}
		view, exists := s.views[in.TableName]
		if !exists {
			resp := jsonResponse(req, 400, `{"__type":"ResourceNotFoundException","message":"stubbed missing"}`)
			resp.Header.Set("X-Amzn-ErrorType", "ResourceNotFoundException")
			return resp, nil
		}
		return jsonResponse(req, 200, fmt.Sprintf(
			`{"Table":{"TableName":%q,"TableArn":%q,"LatestStreamArn":%q,"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"StreamSpecification":{"StreamEnabled":true,"StreamViewType":%q}}}`,
			in.TableName, "arn:aws:dynamodb:us-east-1:123456789012:table/"+in.TableName, testArn(in.TableName), view)), nil
	case strings.HasSuffix(target, ".ListTables"):
		names, err := json.Marshal(s.listed)
		if err != nil {
			return nil, err
		}
		return jsonResponse(req, 200, fmt.Sprintf(`{"TableNames":%s}`, names)), nil
	case strings.HasSuffix(target, ".ListTagsOfResource"):
		return jsonResponse(req, 200, `{"Tags":[{"Key":"cdc","Value":"on"}]}`), nil
	case strings.HasSuffix(target, ".Query"):
		return jsonResponse(req, 200, `{"Items":[]}`), nil
	case strings.HasSuffix(target, ".GetItem"), strings.HasSuffix(target, ".PutItem"), strings.HasSuffix(target, ".DeleteItem"):
		return jsonResponse(req, 200, `{}`), nil
	case strings.HasSuffix(target, ".Scan"):
		s.scanned = append(s.scanned, in.TableName)
		return jsonResponse(req, 200, `{"Items":[]}`), nil
	case strings.HasSuffix(target, ".DescribeStream"):
		return jsonResponse(req, 200, fmt.Sprintf(`{"StreamDescription":{"StreamArn":%q,"StreamStatus":"ENABLED","Shards":[]}}`, in.StreamArn)), nil
	default:
		return nil, fmt.Errorf("signalConnectStub: unexpected operation %q", target)
	}
}

func (s *signalConnectStub) describeCount(table string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, d := range s.described {
		if d == table {
			n++
		}
	}
	return n
}

func (s *signalConnectStub) scannedTables() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.scanned...)
}

// newSignalConnectInput builds an input from yaml whose AWS calls go to stub.
func newSignalConnectInput(t *testing.T, yaml string, stub *signalConnectStub) *dynamoDBCDCInput {
	t.Helper()
	pConf, err := dynamoDBCDCInputConfig().ParseYAML(yaml, service.NewEnvironment())
	require.NoError(t, err)
	d, err := newDynamoDBCDCInputFromConfig(pConf, service.MockResources())
	require.NoError(t, err)
	d.awsConf = aws.Config{
		Region:      "us-east-1",
		Credentials: aws.AnonymousCredentials{},
		HTTPClient:  stub,
		Retryer:     func() aws.Retryer { return aws.NopRetryer{} },
	}
	return d
}

// streamedTables returns the names of d's table streams, sorted.
func streamedTables(d *dynamoDBCDCInput) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	names := make([]string, 0, len(d.tableStreams))
	for name := range d.tableStreams {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func TestSignalTableConfigRejectedInTables(t *testing.T) {
	_, err := parseCDCConfig(t, `
tables: [orders, rpcn_signals]
table_discovery_mode: includelist
signal_table_name: rpcn_signals
`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `signal_table_name "rpcn_signals" must not also appear in tables`)
}

func TestSignalTableConfigDefaultEmpty(t *testing.T) {
	conf, err := parseCDCConfig(t, `
tables: [orders]
`)
	require.NoError(t, err)
	assert.Empty(t, conf.signalTable)

	conf, err = parseCDCConfig(t, `
tables: [orders]
signal_table_name: rpcn_signals
`)
	require.NoError(t, err)
	assert.Equal(t, "rpcn_signals", conf.signalTable)
}

// TestSignalTableConfigRejectsScanSnapshotModes: a signal table always takes
// the multi-table path, which does not run snapshot_only or snapshot_and_cdc,
// so those modes are rejected rather than silently skipping their snapshot.
func TestSignalTableConfigRejectsScanSnapshotModes(t *testing.T) {
	for _, mode := range []string{snapshotModeOnly, snapshotModeAndCDC} {
		t.Run(mode, func(t *testing.T) {
			_, err := parseCDCConfig(t, fmt.Sprintf(`
tables: [orders]
signal_table_name: rpcn_signals
snapshot_mode: %s
`, mode))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cannot be used with signal_table_name")
		})
	}
	for _, mode := range []string{snapshotModeNone, snapshotModeIncremental} {
		_, err := parseCDCConfig(t, fmt.Sprintf(`
tables: [orders]
signal_table_name: rpcn_signals
snapshot_mode: %s
`, mode))
		require.NoError(t, err, mode)
	}
}

// TestConnectRoutesSignalTableToMultiTable: one source table plus a signal
// table takes the multi-table path, building a stream for each. Only the
// source table gets incremental state, and a fresh source table waits for a
// signal instead of being backfilled.
func TestConnectRoutesSignalTableToMultiTable(t *testing.T) {
	stub := &signalConnectStub{views: map[string]dynamodbtypes.StreamViewType{
		"orders":       dynamodbtypes.StreamViewTypeNewImage,
		"rpcn_signals": dynamodbtypes.StreamViewTypeNewAndOldImages,
	}}
	d := newSignalConnectInput(t, `
tables: [orders]
checkpoint_table: ckpt
snapshot_mode: incremental
signal_table_name: rpcn_signals
region: us-east-1
`, stub)

	require.NoError(t, d.Connect(t.Context()))
	defer stopInput(t, d)

	assert.Empty(t, d.resolvedTable, "the single-table path is not taken")
	require.Equal(t, []string{"orders", "rpcn_signals"}, streamedTables(d))
	d.mu.RLock()
	orders, signals := d.tableStreams["orders"], d.tableStreams["rpcn_signals"]
	d.mu.RUnlock()
	assert.False(t, orders.isSignalTable)
	assert.NotNil(t, orders.incremental)
	assert.True(t, signals.isSignalTable)
	assert.Nil(t, signals.incremental, "the signal table is never backfilled")
	require.NotNil(t, d.backfills)
	assert.Zero(t, d.backfills.Len(), "a fresh table waits for a signal")
	assert.Empty(t, stub.scannedTables())
}

func TestConnectFailsWhenSignalTableKeysOnly(t *testing.T) {
	stub := &signalConnectStub{views: map[string]dynamodbtypes.StreamViewType{
		"orders":       dynamodbtypes.StreamViewTypeNewImage,
		"rpcn_signals": dynamodbtypes.StreamViewTypeKeysOnly,
	}}
	d := newSignalConnectInput(t, `
tables: [orders]
checkpoint_table: ckpt
signal_table_name: rpcn_signals
region: us-east-1
`, stub)

	err := d.Connect(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "signal_table_name")
	assert.Contains(t, err.Error(), "KEYS_ONLY")
}

func TestConnectFailsWhenSignalTableMissing(t *testing.T) {
	stub := &signalConnectStub{views: map[string]dynamodbtypes.StreamViewType{
		"orders": dynamodbtypes.StreamViewTypeNewImage,
	}}
	d := newSignalConnectInput(t, `
tables: [orders]
checkpoint_table: ckpt
signal_table_name: rpcn_signals
region: us-east-1
`, stub)

	err := d.Connect(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rpcn_signals")
}

// TestTagDiscoveryExcludesSignalTable: tag discovery matches the signal
// table too, but it is left out of the discovered set and streamed once, as
// the signal table.
func TestTagDiscoveryExcludesSignalTable(t *testing.T) {
	stub := &signalConnectStub{
		views: map[string]dynamodbtypes.StreamViewType{
			"orders":       dynamodbtypes.StreamViewTypeNewImage,
			"rpcn_signals": dynamodbtypes.StreamViewTypeNewImage,
		},
		listed: []string{"orders", "rpcn_signals"},
	}
	d := newSignalConnectInput(t, `
table_discovery_mode: tag
table_tag_filter: "cdc:on"
checkpoint_table: ckpt
snapshot_mode: incremental
signal_table_name: rpcn_signals
region: us-east-1
`, stub)

	require.NoError(t, d.Connect(t.Context()))
	defer stopInput(t, d)

	require.Equal(t, []string{"orders", "rpcn_signals"}, streamedTables(d))
	d.mu.RLock()
	assert.True(t, d.tableStreams["rpcn_signals"].isSignalTable)
	d.mu.RUnlock()
	assert.Equal(t, 1, stub.describeCount("rpcn_signals"), "the signal table is skipped by discovery and initialized once")

	tables, err := d.discoverTablesByTag(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []string{"orders"}, tables)
}

func TestTagDiscoveryOnlySignalTableIsNoTables(t *testing.T) {
	stub := &signalConnectStub{
		views:  map[string]dynamodbtypes.StreamViewType{"rpcn_signals": dynamodbtypes.StreamViewTypeNewImage},
		listed: []string{"rpcn_signals"},
	}
	d := newSignalConnectInput(t, `
table_discovery_mode: tag
table_tag_filter: "cdc:on"
checkpoint_table: ckpt
signal_table_name: rpcn_signals
region: us-east-1
`, stub)

	err := d.Connect(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no tables found to stream from")
}

func TestPrepareTableBackfillSignalOnlyMode(t *testing.T) {
	const table = "t1"
	arn := testArn(table)
	tests := []struct {
		name        string
		signalTable string
		rows        func(api *memCheckpointAPI)
		wantQueued  bool
		wantReset   bool
	}{
		{
			name:        "fresh table waits for a signal",
			signalTable: "rpcn_signals",
		},
		{
			name:        "requested table is queued",
			signalTable: "rpcn_signals",
			rows: func(api *memCheckpointAPI) {
				api.put(arn, snapshotRow(arn, "snapshot#requested", false, nil))
			},
			wantQueued: true,
		},
		{
			name:        "mid-backfill table is queued",
			signalTable: "rpcn_signals",
			rows: func(api *memCheckpointAPI) {
				api.put(arn, snapshotRow(arn, "snapshot#segment#0", false,
					map[string]dynamodbtypes.AttributeValue{"pk": &dynamodbtypes.AttributeValueMemberS{Value: "a"}}))
			},
			wantQueued: true,
		},
		{
			name:        "complete and stale table is reset and queued",
			signalTable: "rpcn_signals",
			rows: func(api *memCheckpointAPI) {
				api.put(arn, snapshotRow(arn, "snapshot#complete", true, nil))
				api.put(arn, snapshotRow(arn, "snapshot#segment#0", true, nil))
				api.put(arn, memShardRow(arn, "old1", "100"))
			},
			wantQueued: true,
			wantReset:  true,
		},
		{
			name:        "complete and fresh table is not queued",
			signalTable: "rpcn_signals",
			rows: func(api *memCheckpointAPI) {
				api.put(arn, snapshotRow(arn, "snapshot#complete", true, nil))
				api.put(arn, memShardRow(arn, "s1", "100"))
			},
		},
		{
			name:       "without signal_table_name a fresh table is queued",
			wantQueued: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			api := newMemCheckpointAPI(nil)
			if tc.rows != nil {
				tc.rows(api)
			}
			streams := &incStreamsStub{
				describe: func(string) []string { return []string{"old1", "s1"} },
				trimmed:  func(shard string) bool { return shard == "old1" },
			}
			d := newIncConnectInput(t, &scanStubTransport{}, streams)
			d.conf.signalTable = tc.signalTable
			ts := newIncTableStream(d, table, arn, memCheckpointer(t, api, table, arn))

			assert.Equal(t, tc.wantQueued, d.prepareTableBackfill(t.Context(), table, ts))
			if tc.wantReset {
				assert.False(t, api.has(arn, "snapshot#complete"), "the stale snapshot is reset")
			}
		})
	}
}
