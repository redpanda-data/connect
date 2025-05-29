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
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams"
	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams/changestreamstest"
	"github.com/redpanda-data/connect/v4/internal/license"
)

func runSpannerCDCInputStream(t *testing.T, h changestreamstest.RealHelper) <-chan *service.Message {
	inputConf := fmt.Sprintf(`
gcp_spanner_cdc:
  project_id: %s
  instance_id: %s
  database_id: %s
  stream_id: %s
  start_timestamp: %s
  heartbeat_interval: "5s"
`,
		h.ProjectID(),
		h.InstanceID(),
		h.DatabaseID(),
		h.Stream(),
		time.Now().Format(time.RFC3339),
	)

	ch := make(chan *service.Message, 10)

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.AddInputYAML(inputConf))
	require.NoError(t, sb.SetLoggerYAML(`level: DEBUG`))
	require.NoError(t, sb.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		select {
		case <-t.Context().Done():
			return t.Context().Err()
		case ch <- msg:
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
		close(ch)
	}()

	return ch
}

// Run tests with:
// go test -v -run TestIntegrationSpannerCDCInput . -spanner.project_id=sandbox-rpcn -spanner.instance_id=rpcn-tests -spanner.database_id=changestreams
func TestIntegrationSpannerCDCInput(t *testing.T) {
	integration.CheckSkip(t)
	changestreamstest.CheckSkipReal(t)

	require.NoError(t, changestreamstest.MaybeDropOrphanedStreams(t.Context()))

	t.Run("smoke", func(t *testing.T) {
		h := changestreamstest.MakeRealHelper(t)

		// Given table
		h.CreateTableAndStream(`CREATE TABLE %s (id INT64 NOT NULL, active BOOL NOT NULL ) PRIMARY KEY (id)`)
		ch := runSpannerCDCInputStream(t, h)

		// When data is inserted and deleted in a transaction
		_, err := h.Client().ReadWriteTransaction(t.Context(), func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			if _, err := txn.Update(ctx, spanner.NewStatement(fmt.Sprintf("INSERT INTO %s (id, active) VALUES (1, true)", h.Table()))); err != nil {
				return err
			}
			if _, err := txn.Update(ctx, spanner.NewStatement(fmt.Sprintf("DELETE FROM %s WHERE id = 1", h.Table()))); err != nil {
				return err
			}
			return nil
		})
		require.NoError(t, err)

		// Then we get the changes
		want := []spannerMod{
			{
				TableName: h.Table(),
				ModType:   "INSERT",
				Mod: &changestreams.Mod{
					Keys: spanner.NullJSON{
						Value: map[string]any{"id": "1"},
						Valid: true,
					},
					NewValues: spanner.NullJSON{
						Value: map[string]any{"active": true},
						Valid: true,
					},
					OldValues: spanner.NullJSON{
						Value: map[string]any{},
						Valid: true,
					},
				},
			},
			{
				TableName: h.Table(),
				ModType:   "DELETE",
				Mod: &changestreams.Mod{
					Keys: spanner.NullJSON{
						Value: map[string]interface{}{"id": "1"},
						Valid: true,
					},
					NewValues: spanner.NullJSON{
						Value: map[string]interface{}{},
						Valid: true,
					},
					OldValues: spanner.NullJSON{
						Value: map[string]interface{}{"active": true},
						Valid: true,
					},
				},
			},
		}
		assert.Equal(t, want, collectN(t, 2, ch))
	})
}

func collectN(t *testing.T, n int, ch <-chan *service.Message) (mods []spannerMod) {
	for range n {
		select {
		case msg := <-ch:
			b, err := msg.AsBytes()
			require.NoError(t, err)
			var sm spannerMod
			require.NoError(t, json.Unmarshal(b, &sm))
			mods = append(mods, sm)
		case <-time.After(time.Minute):
			t.Fatalf("timeout waiting for message, got %d messages wanted %d", len(mods), n)
		}
	}
	return
}
