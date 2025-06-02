// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams"
	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams/changestreamstest"
	"github.com/redpanda-data/connect/v4/internal/license"
)

func runSpannerCDCInputStream(
	t *testing.T,
	h changestreamstest.RealHelper,
	startTimestamp time.Time,
	endTimestamp time.Time,
	msgs chan<- *service.Message,
) (addr string) {
	port, err := integration.GetFreePort()
	require.NoError(t, err)
	httpConf := fmt.Sprintf(`
http:
  enabled: true
  address: localhost:%d`, port)

	inputConf := fmt.Sprintf(`
gcp_spanner_cdc:
  project_id: %s
  instance_id: %s
  database_id: %s
  stream_id: %s
  start_timestamp: %s
  end_timestamp: %s
  heartbeat_interval: "5s"
`,
		h.ProjectID(),
		h.InstanceID(),
		h.DatabaseID(),
		h.Stream(),
		startTimestamp.Format(time.RFC3339),
		endTimestamp.Add(time.Second).Format(time.RFC3339), // end timestamp is exclusive
	)

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetYAML(httpConf))
	require.NoError(t, sb.AddInputYAML(inputConf))
	require.NoError(t, sb.SetLoggerYAML(`level: DEBUG`))
	require.NoError(t, sb.SetMetricsYAML(`json_api: {}`))

	var count int
	require.NoError(t, sb.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		count += 1
		t.Logf("Got message: %d", count)

		select {
		case <-t.Context().Done():
			return t.Context().Err()
		case msgs <- msg:
			return nil
		}
	},
	))

	s, err := sb.Build()
	require.NoError(t, err, "failed to build stream")
	license.InjectTestService(s.Resources())

	t.Cleanup(func() {
		if err := s.StopWithin(time.Second); err != nil {
			t.Log(err)
		}
	})

	go func() {
		if err := s.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("stream error: %v", err)
		}
		close(msgs)
	}()

	return fmt.Sprintf("localhost:%d", port)
}

type SingersTableHelper struct {
	changestreamstest.RealHelper
	t *testing.T
}

func (h SingersTableHelper) CreateTableAndStream() {
	h.RealHelper.CreateTableAndStream(`CREATE TABLE %s (
			SingerId INT64 NOT NULL,
			FirstName STRING(MAX),
			LastName STRING(MAX)
		) PRIMARY KEY (SingerId)`)
}

func (h SingersTableHelper) InsertRows(n int) (time.Time, time.Time) {
	firstCommitTimestamp := h.insertRow(1)
	for i := 2; i < n; i++ {
		h.insertRow(i)
	}
	lastCommitTimestamp := h.insertRow(n)
	return firstCommitTimestamp, lastCommitTimestamp
}

func (h SingersTableHelper) UpdateRows(n int) (time.Time, time.Time) {
	firstCommitTimestamp := h.updateRow(1)
	for i := 2; i < n; i++ {
		h.updateRow(i)
	}
	lastCommitTimestamp := h.updateRow(n)
	return firstCommitTimestamp, lastCommitTimestamp
}

func (h SingersTableHelper) DeleteRows(n int) (time.Time, time.Time) {
	firstCommitTimestamp := h.deleteRow(1)
	for i := 2; i < n; i++ {
		h.deleteRow(i)
	}
	lastCommitTimestamp := h.deleteRow(n)
	return firstCommitTimestamp, lastCommitTimestamp
}

func (h SingersTableHelper) insertRow(singerID int) time.Time {
	ts, err := h.Client().Apply(h.t.Context(),
		[]*spanner.Mutation{h.insertMut(singerID)},
		spanner.TransactionTag("app=rpcn;action=insert"))
	require.NoError(h.t, err)

	return ts
}

func (h SingersTableHelper) insertMut(singerID int) *spanner.Mutation {
	return spanner.InsertMap(h.Table(), map[string]any{
		"SingerId":  singerID,
		"FirstName": fmt.Sprintf("First Name %d", singerID),
		"LastName":  fmt.Sprintf("Last Name %d", singerID),
	})
}

func (h SingersTableHelper) updateRow(singerID int) time.Time {
	ts, err := h.Client().Apply(h.t.Context(),
		[]*spanner.Mutation{h.updateMut(singerID)},
		spanner.TransactionTag("app=rpcn;action=update"))
	require.NoError(h.t, err)

	return ts
}

func (h SingersTableHelper) updateMut(singerID int) *spanner.Mutation {
	mut := spanner.UpdateMap(h.Table(), map[string]any{
		"SingerId":  singerID,
		"FirstName": fmt.Sprintf("Updated First Name %d", singerID),
		"LastName":  fmt.Sprintf("Updated Last Name %d", singerID),
	})
	return mut
}

func (h SingersTableHelper) deleteRow(singerID int) time.Time {
	ts, err := h.Client().Apply(h.t.Context(),
		[]*spanner.Mutation{h.deleteMut(singerID)},
		spanner.TransactionTag("app=rpcn;action=delete"))
	require.NoError(h.t, err)

	return ts
}

func (h SingersTableHelper) deleteMut(singerID int) *spanner.Mutation {
	return spanner.Delete(h.Table(), spanner.Key{singerID})
}

func TestIntegrationRealSpannerCDCInput(t *testing.T) {
	integration.CheckSkip(t)
	changestreamstest.CheckSkipReal(t)

	require.NoError(t, changestreamstest.MaybeDropOrphanedStreams(t.Context()))

	// How many rows to insert/update/delete
	const numRows = 5

	h := SingersTableHelper{changestreamstest.MakeRealHelper(t), t}
	h.CreateTableAndStream()

	// When rows are inserted, updated and deleted
	startTimestamp, _ := h.InsertRows(numRows)
	h.UpdateRows(numRows)
	_, endTimestamp := h.DeleteRows(numRows)

	// And the stream is started
	ch := make(chan *service.Message, 3*numRows)
	addr := runSpannerCDCInputStream(t, h.RealHelper, startTimestamp, endTimestamp, ch)

	// Then all the changes are received
	var inserts, updates, deletes []spannerMod
	for _, v := range collectN(t, numRows*3, ch) {
		mod, msg := v.Mod, v.Msg

		switch mod.ModType {
		case "INSERT":
			transactionTag, _ := msg.MetaGet("transaction_tag")
			require.Equal(t, "app=rpcn;action=insert", transactionTag)
			inserts = append(inserts, mod)
		case "UPDATE":
			transactionTag, _ := msg.MetaGet("transaction_tag")
			require.Equal(t, "app=rpcn;action=update", transactionTag)
			updates = append(updates, mod)
		case "DELETE":
			transactionTag, _ := msg.MetaGet("transaction_tag")
			require.Equal(t, "app=rpcn;action=delete", transactionTag)
			deletes = append(deletes, mod)
		}
	}

	wantInserts := make([]spannerMod, numRows)
	for i := range wantInserts {
		singerID := i + 1
		wantInserts[i] = spannerMod{
			TableName: h.Table(),
			ModType:   "INSERT",
			Mod: &changestreams.Mod{
				Keys: spanner.NullJSON{
					Value: map[string]any{"SingerId": fmt.Sprintf("%d", singerID)},
					Valid: true,
				},
				NewValues: spanner.NullJSON{
					Value: map[string]any{
						"FirstName": fmt.Sprintf("First Name %d", singerID),
						"LastName":  fmt.Sprintf("Last Name %d", singerID),
					},
					Valid: true,
				},
				OldValues: spanner.NullJSON{
					Value: map[string]any{},
					Valid: true,
				},
			},
		}
	}
	assert.Equal(t, wantInserts, inserts)

	wantUpdates := make([]spannerMod, numRows)
	for i := range wantUpdates {
		singerID := i + 1
		wantUpdates[i] = spannerMod{
			TableName: h.Table(),
			ModType:   "UPDATE",
			Mod: &changestreams.Mod{
				Keys: spanner.NullJSON{
					Value: map[string]any{"SingerId": fmt.Sprintf("%d", singerID)},
					Valid: true,
				},
				NewValues: spanner.NullJSON{
					Value: map[string]any{
						"FirstName": fmt.Sprintf("Updated First Name %d", singerID),
						"LastName":  fmt.Sprintf("Updated Last Name %d", singerID),
					},
					Valid: true,
				},
				OldValues: spanner.NullJSON{
					Value: map[string]any{
						"FirstName": fmt.Sprintf("First Name %d", singerID),
						"LastName":  fmt.Sprintf("Last Name %d", singerID),
					},
					Valid: true,
				},
			},
		}
	}
	assert.Equal(t, wantUpdates, updates)

	wantDeletes := make([]spannerMod, numRows)
	for i := range wantDeletes {
		singerID := i + 1
		wantDeletes[i] = spannerMod{
			TableName: h.Table(),
			ModType:   "DELETE",
			Mod: &changestreams.Mod{
				Keys: spanner.NullJSON{
					Value: map[string]any{"SingerId": fmt.Sprintf("%d", singerID)},
					Valid: true,
				},
				NewValues: spanner.NullJSON{
					Value: map[string]any{},
					Valid: true,
				},
				OldValues: spanner.NullJSON{
					Value: map[string]any{
						"FirstName": fmt.Sprintf("Updated First Name %d", singerID),
						"LastName":  fmt.Sprintf("Updated Last Name %d", singerID),
					},
					Valid: true,
				},
			},
		}
	}
	assert.Equal(t, wantDeletes, deletes)

	// And metrics are set...
	resp, err := http.Get("http://" + addr + "/metrics")
	require.NoError(t, err)
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	t.Logf("Metrics:\n%s", string(b))

	ms := parseMetricsSnapshot(t, b)
	require.NotZero(t, ms.PartitionCreatedToScheduled)
	require.NotZero(t, ms.PartitionScheduledToRunning)
	require.NotZero(t, ms.DataChangeRecordCommittedToEmitted)
	ms.PartitionCreatedToScheduled = timeDist{}
	ms.PartitionScheduledToRunning = timeDist{}
	ms.DataChangeRecordCommittedToEmitted = timeDist{}

	// This can be a bit flaky depending on if Spanner decides to split the
	// partition. Adding PartitionRecordSplitCount covers both cases.
	want := metricsSnapshot{
		PartitionRecordCreatedCount:  2 + ms.PartitionRecordSplitCount,
		PartitionRecordRunningCount:  2 + ms.PartitionRecordSplitCount,
		PartitionRecordFinishedCount: 1 + ms.PartitionRecordSplitCount,
		PartitionRecordSplitCount:    ms.PartitionRecordSplitCount,
		PartitionRecordMergeCount:    0,
		QueryCount:                   2 + ms.PartitionRecordSplitCount,
		DataChangeRecordCount:        3 * numRows,
		HeartbeatRecordCount:         1 + ms.PartitionRecordSplitCount,
	}
	assert.Equal(t, want, ms)
}

func TestIntegrationRealSpannerCDCInputMessagesOrderedByTimestampAndTransactionId(t *testing.T) {
	integration.CheckSkip(t)
	changestreamstest.CheckSkipReal(t)

	require.NoError(t, changestreamstest.MaybeDropOrphanedStreams(t.Context()))

	h := SingersTableHelper{changestreamstest.MakeRealHelper(t), t}
	h.CreateTableAndStream()

	writeTransactionsToDatabase := func() time.Time {
		// 1. Insert Singer 1 and Singer 2
		ts, err := h.Client().Apply(h.t.Context(), []*spanner.Mutation{
			h.insertMut(1),
			h.insertMut(2),
		})
		require.NoError(t, err)
		t.Logf("First transaction committed with timestamp: %v", ts)

		// 2. Delete Singer 1 and Insert Singer 3
		ts, err = h.Client().Apply(h.t.Context(), []*spanner.Mutation{
			h.deleteMut(1),
			h.insertMut(3),
		})
		require.NoError(t, err)
		t.Logf("Second transaction committed with timestamp: %v", ts)

		// 3. Delete Singer 2 and Singer 3
		ts, err = h.Client().Apply(h.t.Context(), []*spanner.Mutation{
			h.deleteMut(2),
			h.deleteMut(3),
		})
		require.NoError(t, err)
		t.Logf("Third transaction committed with timestamp: %v", ts)

		// 4. Delete Singer 0 if it exists
		ts, err = h.Client().Apply(h.t.Context(), []*spanner.Mutation{
			h.deleteMut(0),
		})
		require.NoError(t, err)
		t.Logf("Fourth transaction committed with timestamp: %v", ts)

		return ts
	}

	// Given 3 batches of transactions with 2 second gaps
	const expectedMessages = 1 + 7 + 2*6
	startTimestamp := h.insertRow(0)
	writeTransactionsToDatabase()
	time.Sleep(2 * time.Second)
	writeTransactionsToDatabase()
	time.Sleep(2 * time.Second)
	endTimestamp := writeTransactionsToDatabase()

	// When we read from the stream
	ch := make(chan *service.Message, expectedMessages)
	runSpannerCDCInputStream(t, h.RealHelper, startTimestamp, endTimestamp, ch)
	messages := collectN(t, expectedMessages, ch)

	// Then there are 3 batches...

	// Sort messages by commit timestamp and transaction ID
	commitTimestampAt := func(idx int) time.Time {
		s, ok := messages[idx].Msg.MetaGet("commit_timestamp")
		require.True(t, ok)
		v, err := time.Parse(time.RFC3339Nano, s)
		require.NoError(t, err)
		return v
	}
	transactionIdAt := func(idx int) string {
		s, ok := messages[idx].Msg.MetaGet("server_transaction_id")
		require.True(t, ok)
		return s
	}
	sort.SliceStable(messages, func(i, j int) bool { // MUST be stable
		if cmp := commitTimestampAt(i).Compare(commitTimestampAt(j)); cmp == 0 {
			return transactionIdAt(i) < transactionIdAt(j)
		} else {
			return cmp < 0
		}
	})

	// Group by batches with 1.5 second gap threshold
	groupMessagesByBatch := func() [][]spannerMod {
		var (
			batches [][]spannerMod
			cur     []spannerMod
			lastTs  time.Time
		)

		for i, msg := range messages {
			ts := commitTimestampAt(i)

			if len(cur) == 0 || ts.Sub(lastTs) < 1500*time.Millisecond {
				cur = append(cur, msg.Mod)
			} else {
				batches = append(batches, cur)
				cur = []spannerMod{msg.Mod}
			}
			lastTs = ts
		}
		if len(cur) != 0 {
			batches = append(batches, cur)
		}

		return batches
	}
	batches := groupMessagesByBatch()
	require.Len(t, batches, 3)

	// And operation order is preserved...

	var sb strings.Builder
	for i, batch := range batches {
		sb.WriteString(fmt.Sprintf("Batch %d:\n", i))
		for _, m := range batch {
			fmt.Fprintf(&sb, "  %s: %s\n", m.ModType, m.Keys.Value)
		}
	}
	want := `Batch 0:
  INSERT: map[SingerId:0]
  INSERT: map[SingerId:1]
  INSERT: map[SingerId:2]
  DELETE: map[SingerId:1]
  INSERT: map[SingerId:3]
  DELETE: map[SingerId:2]
  DELETE: map[SingerId:3]
  DELETE: map[SingerId:0]
Batch 1:
  INSERT: map[SingerId:1]
  INSERT: map[SingerId:2]
  DELETE: map[SingerId:1]
  INSERT: map[SingerId:3]
  DELETE: map[SingerId:2]
  DELETE: map[SingerId:3]
Batch 2:
  INSERT: map[SingerId:1]
  INSERT: map[SingerId:2]
  DELETE: map[SingerId:1]
  INSERT: map[SingerId:3]
  DELETE: map[SingerId:2]
  DELETE: map[SingerId:3]
`
	assert.Equal(t, want, sb.String())
}

type spannerModMessage struct {
	Mod spannerMod
	Msg *service.Message
}

func collectN(t *testing.T, n int, ch <-chan *service.Message) (mods []spannerModMessage) {
	for range n {
		select {
		case msg := <-ch:
			b, err := msg.AsBytes()
			require.NoError(t, err)
			var sm spannerMod
			require.NoError(t, json.Unmarshal(b, &sm))
			mods = append(mods, spannerModMessage{
				Mod: sm,
				Msg: msg,
			})
		case <-time.After(time.Minute):
			t.Fatalf("timeout waiting for message, got %d messages wanted %d", len(mods), n)
		}
	}
	return
}

type timeDist struct {
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P99 float64 `json:"p99"`
}

type metricsSnapshot struct {
	PartitionRecordCreatedCount        int64    `json:"partition_record_created_count"`
	PartitionRecordRunningCount        int64    `json:"partition_record_running_count"`
	PartitionRecordFinishedCount       int64    `json:"partition_record_finished_count"`
	PartitionRecordSplitCount          int64    `json:"partition_record_split_count"`
	PartitionRecordMergeCount          int64    `json:"partition_record_merge_count"`
	PartitionCreatedToScheduled        timeDist `json:"partition_created_to_scheduled_ns"`
	PartitionScheduledToRunning        timeDist `json:"partition_scheduled_to_running_ns"`
	QueryCount                         int64    `json:"query_count"`
	DataChangeRecordCount              int64    `json:"data_change_record_count"`
	DataChangeRecordCommittedToEmitted timeDist `json:"data_change_record_committed_to_emitted_ns"`
	HeartbeatRecordCount               int64    `json:"heartbeat_record_count"`
}

func parseMetricsSnapshot(t *testing.T, data []byte) metricsSnapshot {
	// First preprocess the JSON to clean up the metric names
	data, err := extractSpannerCDCMetricsJSON(data)
	require.NoError(t, err)

	// Unmarshal the cleaned JSON into the metricsSnapshot struct
	var ms metricsSnapshot
	require.NoError(t, json.Unmarshal(data, &ms))
	return ms
}

// extractSpannerCDCMetricsJSON transforms the raw metrics JSON into a format
// that can be directly unmarshaled into a metricsSnapshot struct.
func extractSpannerCDCMetricsJSON(data []byte) ([]byte, error) {
	// Parse the raw JSON into a map
	var rawData map[string]json.RawMessage
	if err := json.Unmarshal(data, &rawData); err != nil {
		return nil, err
	}

	metricNameRegex := regexp.MustCompile(`spanner_cdc_([^{]+)(?:\{.*\})?`)

	res := make(map[string]json.RawMessage)
	for k, v := range rawData {
		m := metricNameRegex.FindStringSubmatch(k)
		if len(m) < 2 {
			continue
		}
		res[m[1]] = v
	}
	return json.Marshal(res)
}
