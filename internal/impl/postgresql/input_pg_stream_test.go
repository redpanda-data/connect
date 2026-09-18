// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package pgstream

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream"
	"github.com/redpanda-data/connect/v4/internal/license"
)

func TestNewPgStreamInputSignalTableName(t *testing.T) {
	env := service.NewEnvironment()
	spec := newPostgresCDCConfig()

	tests := []struct {
		name        string
		conf        string
		errContains string
	}{
		{
			name: "no signal table configured",
			conf: `
dsn: postgres://user:pass@localhost:5432/db
slot_name: my_slot
schema: dbo
tables:
  - events
`,
		},
		{
			name: "signal table distinct from tables",
			conf: `
dsn: postgres://user:pass@localhost:5432/db
slot_name: my_slot
schema: dbo
tables:
  - events
signal_table_name: rpcn_signal_table
`,
		},
		{
			name: "signal table also listed in tables",
			conf: `
dsn: postgres://user:pass@localhost:5432/db
slot_name: my_slot
schema: dbo
tables:
  - events
  - rpcn_signal_table
signal_table_name: rpcn_signal_table
`,
			errContains: `signal_table_name "rpcn_signal_table" must not also appear in tables`,
		},
		{
			name: "signal table matches tables entry under different case-folding",
			conf: `
dsn: postgres://user:pass@localhost:5432/db
slot_name: my_slot
schema: dbo
tables:
  - events
  - RPCN_SIGNAL_TABLE
signal_table_name: rpcn_signal_table
`,
			errContains: `signal_table_name "rpcn_signal_table" must not also appear in tables`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pConf, err := spec.ParseYAML(test.conf, env)
			require.NoError(t, err)

			mgr := service.MockResources()
			license.InjectTestService(mgr)

			_, err = newPgStreamInput(pConf, mgr)
			if test.errContains != "" {
				require.ErrorContains(t, err, test.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestResolveBatchingPolicy pins how the batching policy and
// batch_transactions resolve: no policy forces count 1 (one message per batch
// by default), an explicit policy including count: 1 is honoured per message,
// and batch_transactions is rejected alongside any policy. It rests on the
// field defaulting to count 0 and on IsNoop treating count <= 1 as no policy.
func TestResolveBatchingPolicy(t *testing.T) {
	env := service.NewEnvironment()
	spec := newPostgresCDCConfig()

	base := `
dsn: postgres://user:pass@localhost:5432/db
slot_name: my_slot
schema: dbo
tables:
  - events
`
	tests := []struct {
		name              string
		batching          string
		batchTransactions bool
		configured        bool
		count             int
		errContains       string
	}{
		{name: "no batching block", configured: false, count: 1},
		{name: "no batching block, batch_transactions", batchTransactions: true, configured: false, count: 1},
		{name: "explicit count 1", batching: "batching:\n  count: 1\n", configured: true, count: 1},
		{name: "count 3", batching: "batching:\n  count: 3\n", configured: true, count: 3},
		// A configured policy is left as written: count stays 0 and the
		// period or byte size alone drives the batcher.
		{name: "period only", batching: "batching:\n  period: 1s\n", configured: true, count: 0},
		{name: "byte size only", batching: "batching:\n  byte_size: 1024\n", configured: true, count: 0},
		{name: "processors only", batching: "batching:\n  processors:\n    - mapping: root = this\n", configured: true, count: 0},
		{name: "count 3 with batch_transactions", batching: "batching:\n  count: 3\n", batchTransactions: true, errContains: "batch_transactions cannot be combined with a batching policy"},
		{name: "count 1 with batch_transactions", batching: "batching:\n  count: 1\n", batchTransactions: true, errContains: "batch_transactions cannot be combined with a batching policy"},
		{name: "period with batch_transactions", batching: "batching:\n  period: 1s\n", batchTransactions: true, errContains: "batch_transactions cannot be combined with a batching policy"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pConf, err := spec.ParseYAML(base+test.batching, env)
			require.NoError(t, err)
			batching, err := pConf.FieldBatchPolicy(fieldBatching)
			require.NoError(t, err)

			policy, configured, err := resolveBatchingPolicy(batching, test.batchTransactions)
			if test.errContains != "" {
				require.ErrorContains(t, err, test.errContains)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.configured, configured)
			require.Equal(t, test.count, policy.Count)
		})
	}
}

// TestBatchTransactionsRejectedWithPolicy checks the combination is refused
// at construction, through the public spec, so a misconfiguration fails the
// config rather than quietly picking one behaviour.
func TestBatchTransactionsRejectedWithPolicy(t *testing.T) {
	pConf, err := newPostgresCDCConfig().ParseYAML(`
dsn: postgres://user:pass@localhost:5432/db
slot_name: my_slot
schema: dbo
tables:
  - events
batch_transactions: true
batching:
  count: 10
`, service.NewEnvironment())
	require.NoError(t, err)
	res := service.MockResources()
	license.InjectTestService(res)
	_, err = newPgStreamInput(pConf, res)
	require.ErrorContains(t, err, "batch_transactions cannot be combined with a batching policy")
}

// TestPassesThrough pins which reader batches are handed on whole: snapshot
// pages always (as before this option existed), streaming batches only with
// batch_transactions, and nothing once a batching policy is configured.
func TestPassesThrough(t *testing.T) {
	lsn := "0/10"
	snapshotPage := []pglogicalstream.StreamMessage{{Operation: pglogicalstream.ReadOpType}, {Operation: pglogicalstream.ReadOpType}}
	txn := []pglogicalstream.StreamMessage{{Operation: pglogicalstream.InsertOpType, LSN: &lsn}, {Operation: pglogicalstream.UpdateOpType, LSN: &lsn}}

	tests := []struct {
		name              string
		configured        bool
		batchTransactions bool
		batch             []pglogicalstream.StreamMessage
		want              bool
	}{
		{name: "default, snapshot page", batch: snapshotPage, want: true},
		{name: "default, transaction", batch: txn, want: false},
		{name: "batch_transactions, snapshot page", batchTransactions: true, batch: snapshotPage, want: true},
		{name: "batch_transactions, transaction", batchTransactions: true, batch: txn, want: true},
		{name: "policy configured, snapshot page", configured: true, batch: snapshotPage, want: false},
		{name: "policy configured, transaction", configured: true, batch: txn, want: false},
		{name: "empty batch", batchTransactions: true, batch: nil, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := &pgStreamInput{batchingConfigured: test.configured, batchTransactions: test.batchTransactions}
			require.Equal(t, test.want, p.passesThrough(test.batch))
		})
	}
}

func TestStreamBatchMaxRowsFor(t *testing.T) {
	require.Equal(t, 512, streamBatchMaxRowsFor(1024), "default checkpoint_limit")
	require.Equal(t, 5, streamBatchMaxRowsFor(10))
	require.Equal(t, 1, streamBatchMaxRowsFor(1), "never zero, or the reader could not batch at all")
	require.Equal(t, 1, streamBatchMaxRowsFor(0))
	require.Equal(t, 50000, streamBatchMaxRowsFor(100000), "the reader clamps this to its own default")
}

type failingBatchProcessor struct{}

func (failingBatchProcessor) ProcessBatch(context.Context, service.MessageBatch) ([]service.MessageBatch, error) {
	return nil, errors.New("processor exploded")
}

func (failingBatchProcessor) Close(context.Context) error { return nil }

// registerFailingProcessor registers the test processor once, globally: the
// batcher builds its processors from the resources it is given, which come
// from the global environment, not from the environment the config was parsed
// with.
var registerFailingProcessor = sync.OnceValue(func() error {
	return service.RegisterBatchProcessor("pgstream_test_failing_processor", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.BatchProcessor, error) {
			return failingBatchProcessor{}, nil
		})
})

func newFailingProcessorBatcher(t *testing.T) *service.Batcher {
	t.Helper()
	require.NoError(t, registerFailingProcessor())
	env := service.NewEnvironment()
	pConf, err := newPostgresCDCConfig().ParseYAML(`
dsn: postgres://user:pass@localhost:5432/db
slot_name: my_slot
schema: dbo
tables:
  - events
batching:
  count: 2
  processors:
    - pgstream_test_failing_processor: {}
`, env)
	require.NoError(t, err)
	policy, err := pConf.FieldBatchPolicy(fieldBatching)
	require.NoError(t, err)
	batcher, err := policy.NewBatcher(service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() { _ = batcher.Close(context.Background()) })
	return batcher
}

// TestFlushBatcherPublishesRowsWhenProcessorsFail: the batcher gives its rows
// up when batching.processors fail. Dropping them is silent loss, and a
// restart re-reads the same rows and fails the same way for good, so the
// input publishes them unprocessed with the error set instead.
func TestFlushBatcherPublishesRowsWhenProcessorsFail(t *testing.T) {
	batcher := newFailingProcessorBatcher(t)
	p := &pgStreamInput{
		msgChan: make(chan asyncMessage, 1),
		logger:  service.MockResources().Logger(),
		stopSig: shutdown.NewSignaller(),
	}
	cp := checkpoint.NewCapped[*string](10)

	var pending service.MessageBatch
	for _, lsn := range []string{"0/1", "0/2"} {
		msg := service.NewMessage([]byte(`{}`))
		msg.MetaSet("lsn", lsn)
		pending = append(pending, msg)
		batcher.Add(msg)
	}

	require.True(t, p.flushBatcher(t.Context(), nil, cp, batcher, &pending), "the stream keeps running")
	require.Nil(t, pending, "the mirror is cleared with the batcher")
	require.False(t, p.stopSig.IsSoftStopSignalled())

	select {
	case got := <-p.msgChan:
		require.Len(t, got.msg, 2)
		for _, msg := range got.msg {
			require.ErrorContains(t, msg.GetError(), "processor exploded")
		}
		lsn, _ := got.msg[1].MetaGet("lsn")
		require.Equal(t, "0/2", lsn, "the rows keep their position so the ack still advances the slot")
	default:
		require.Fail(t, "the rows were not published")
	}
}

// TestFlushBatcherCleanShutdownIsSilent: a flush that fails only because the
// input is stopping must neither log a restart nor soft-stop again.
func TestFlushBatcherCleanShutdownIsSilent(t *testing.T) {
	batcher := newFailingProcessorBatcher(t)
	p := &pgStreamInput{
		msgChan: make(chan asyncMessage),
		logger:  service.MockResources().Logger(),
		stopSig: shutdown.NewSignaller(),
	}
	cp := checkpoint.NewCapped[*string](10)

	msg := service.NewMessage([]byte(`{}`))
	pending := service.MessageBatch{msg}
	batcher.Add(msg)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.False(t, p.flushBatcher(ctx, nil, cp, batcher, &pending))
	require.False(t, p.stopSig.IsSoftStopSignalled(), "shutdown was already in progress")
}
