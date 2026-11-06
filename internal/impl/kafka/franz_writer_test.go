// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/dispatch"
)

func TestFranzWriterTombstone(t *testing.T) {
	spec := service.NewConfigSpec().Fields(FranzWriterConfigFields()...)

	newMsg := func(body []byte, meta map[string]any) *service.Message {
		msg := service.NewMessage(body)
		for k, v := range meta {
			msg.MetaSetMut(k, v)
		}
		return msg
	}

	testCases := []struct {
		name        string
		conf        string
		msg         *service.Message
		wantValue   []byte
		wantNil     bool
		errContains string
	}{
		{
			name:      "field unset keeps empty payload as non-null",
			conf:      `topic: foo`,
			msg:       newMsg([]byte{}, nil),
			wantValue: []byte{},
		},
		{
			name: "mirrored tombstone stays a tombstone",
			conf: `
topic: foo
tombstone: '${! @kafka_tombstone_message.or(false) }'
`,
			msg:     newMsg(nil, map[string]any{"kafka_tombstone_message": true}),
			wantNil: true,
		},
		{
			name: "payload is dropped for tombstones",
			conf: `
topic: foo
tombstone: '${! @kafka_tombstone_message.or(false) }'
`,
			msg:     newMsg([]byte("null"), map[string]any{"kafka_tombstone_message": true}),
			wantNil: true,
		},
		{
			name: "non-tombstone keeps payload",
			conf: `
topic: foo
tombstone: '${! @kafka_tombstone_message.or(false) }'
`,
			msg:       newMsg([]byte("hello"), map[string]any{"kafka_tombstone_message": false}),
			wantValue: []byte("hello"),
		},
		{
			name: "missing metadata with or fallback is not a tombstone",
			conf: `
topic: foo
tombstone: '${! @kafka_tombstone_message.or(false) }'
`,
			msg:       newMsg([]byte(""), nil),
			wantValue: []byte{},
		},
		{
			name: "content based mapping",
			conf: `
topic: foo
tombstone: '${! this.op == "delete" }'
`,
			msg:     newMsg([]byte(`{"op":"delete"}`), nil),
			wantNil: true,
		},
		{
			name: "unparseable value fails the write",
			conf: `
topic: foo
tombstone: '${! @kafka_tombstone_message }'
`,
			msg:         newMsg([]byte("hello"), nil),
			errContains: "parse tombstone",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pConf, err := spec.ParseYAML(tc.conf, nil)
			require.NoError(t, err)

			w, err := NewFranzWriterFromConfig(pConf, franzWriterHooks{})
			require.NoError(t, err)

			records, err := w.messageBatchToFranzRecords(service.MessageBatch{tc.msg})
			if tc.errContains != "" {
				require.ErrorContains(t, err, tc.errContains)
				return
			}
			require.NoError(t, err)
			require.Len(t, records, 1)

			if tc.wantNil {
				assert.Nil(t, records[0].Value)
				return
			}
			require.NotNil(t, records[0].Value)
			assert.Equal(t, tc.wantValue, records[0].Value)
		})
	}
}

// TestBatchWriterTriggersDispatchSignalForRealRecords guards against a
// regression where batchWriter.writeBatch had ended up calling
// dispatch.TriggerSignal only for skipped records, and not for records that
// are actually produced. That signal is what lets a paired ordered/per-partition
// Kafka input release its next batch for a partition without waiting for the
// broker ack, so it must fire once the record is handed to the producer and
// before the broker has acknowledged it. The test holds the Produce request
// inside the fake cluster to prove the "before" part: the signals must all
// have fired while writeBatch is still blocked waiting on the broker.
func TestBatchWriterTriggersDispatchSignalForRealRecords(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.AllowAutoTopicCreation())
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseProduce := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseProduce)

	// Hold every Produce request until released, then let kfake handle it as
	// normal. SleepControl parks only this request's connection (franz-go
	// sends produce requests on a dedicated connection), so metadata and
	// other requests keep flowing while the produce is held.
	cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		cluster.SleepControl(func() { <-release })
		return nil, nil, false
	})

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.AllowAutoTopicCreation(),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	details := &FranzSharedClientInfo{Client: client}

	w := &FranzWriter{
		MessageBatchToFranzRecords: func(batch service.MessageBatch) ([]kgo.Record, error) {
			records := make([]kgo.Record, len(batch))
			for i, m := range batch {
				b, err := m.AsBytes()
				if err != nil {
					return nil, err
				}
				records[i] = kgo.Record{
					Topic:   "dispatch_regression_test",
					Value:   b,
					Context: m.Context(),
				}
			}
			return records, nil
		},
	}

	const n = 3
	var dispatched atomic.Int32
	batch := make(service.MessageBatch, n)
	for i := range batch {
		msg := service.NewMessage([]byte("hello"))
		batch[i] = msg.WithContext(dispatch.CtxOnTriggerSignal(msg.Context(), func() {
			dispatched.Add(1)
		}))
	}

	done := make(chan error, 1)
	go func() {
		done <- w.newBatchWriter(t.Context(), batch).writeBatch(details)
	}()

	require.Eventually(t, func() bool {
		return dispatched.Load() == n
	}, 5*time.Second, 10*time.Millisecond,
		"dispatch.TriggerSignal should fire for every real (non-skipped) record "+
			"once it has been handed to the producer")

	// The broker has not answered yet, so writeBatch must still be blocked:
	// the signals fired pre-ack, not from the produce callback.
	select {
	case err := <-done:
		t.Fatalf("writeBatch returned (%v) before the broker acked; dispatch fired post-ack", err)
	case <-time.After(100 * time.Millisecond):
	}

	releaseProduce()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("writeBatch did not return after the produce request was released")
	}
	require.Equal(t, int32(n), dispatched.Load(), "signal must fire exactly once per record")
}

// TestBatchWriterDispatchSignalForSkippedAndFailedRecords pins the two
// non-produced paths of writeBatch: a SkipRecord sentinel signals dispatch
// without being produced, and a record that fails DecorateRecord does not
// signal at all (the batch errors, and an ordered input must keep holding the
// partition until that nack resolves). The remaining real record is produced
// and signalled as normal.
func TestBatchWriterDispatchSignalForSkippedAndFailedRecords(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.AllowAutoTopicCreation())
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.AllowAutoTopicCreation(),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	details := &FranzSharedClientInfo{Client: client}

	const (
		idxSkipped = 0
		idxFailed  = 1
		idxReal    = 2
	)
	w := &FranzWriter{
		MessageBatchToFranzRecords: func(batch service.MessageBatch) ([]kgo.Record, error) {
			records := make([]kgo.Record, len(batch))
			for i, m := range batch {
				if i == idxSkipped {
					records[i] = SkipRecord
					continue
				}
				b, err := m.AsBytes()
				if err != nil {
					return nil, err
				}
				records[i] = kgo.Record{
					Topic:   "dispatch_skip_fail_test",
					Value:   b,
					Context: m.Context(),
				}
			}
			return records, nil
		},
		DecorateRecord: func(r *kgo.Record) error {
			if string(r.Value) == "fail" {
				return errors.New("boom")
			}
			return nil
		},
	}

	var dispatched [3]atomic.Int32
	batch := make(service.MessageBatch, 3)
	for i := range batch {
		payload := "ok"
		if i == idxFailed {
			payload = "fail"
		}
		msg := service.NewMessage([]byte(payload))
		batch[i] = msg.WithContext(dispatch.CtxOnTriggerSignal(msg.Context(), func() {
			dispatched[i].Add(1)
		}))
	}

	err = w.newBatchWriter(t.Context(), batch).writeBatch(details)
	require.ErrorContains(t, err, "decorate record: boom")

	assert.Equal(t, int32(1), dispatched[idxSkipped].Load(), "skipped record must signal dispatch")
	assert.Equal(t, int32(0), dispatched[idxFailed].Load(), "record that failed decoration must not signal dispatch")
	assert.Equal(t, int32(1), dispatched[idxReal].Load(), "produced record must signal dispatch")
}
