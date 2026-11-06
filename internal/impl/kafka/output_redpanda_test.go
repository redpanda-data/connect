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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/dispatch"
)

const redpandaOutputTestTopic = "redpanda_output_test"

// TestRedpandaOutputProducesBatchesConcurrently guards against a regression
// where the redpanda output with its own connection fields held an exclusive
// lock across each write, including the wait for its acks, so that batches
// were produced one at a time whatever max_in_flight was set to. Every Produce
// request is held inside the fake cluster, so the first write cannot complete,
// and the second must still hand its record to the producer.
func TestRedpandaOutputProducesBatchesConcurrently(t *testing.T) {
	cluster := newRedpandaOutputTestCluster(t)
	w := newRedpandaOutputTestWriter(t, cluster)
	release := holdRedpandaOutputTestProduce(t, cluster)
	require.NoError(t, w.Connect(t.Context()))

	const writes = 2
	var dispatched atomic.Int32
	done := make(chan error, writes)
	for range writes {
		go func() {
			done <- w.WriteBatch(t.Context(), service.MessageBatch{newRedpandaOutputTestMessage(&dispatched)})
		}()
	}

	require.Eventually(t, func() bool {
		return dispatched.Load() == writes
	}, 5*time.Second, 10*time.Millisecond,
		"every write should hand its record to the producer while another write waits on its acks")

	release()
	for range writes {
		require.NoError(t, receiveRedpandaOutputTestResult(t, done, "write"))
	}
}

// TestRedpandaOutputCloseWaitsForInFlightWrite pins that closing the output
// does not close the client under a write that is still waiting on its acks,
// which would fail records the broker may yet accept.
func TestRedpandaOutputCloseWaitsForInFlightWrite(t *testing.T) {
	cluster := newRedpandaOutputTestCluster(t)
	w := newRedpandaOutputTestWriter(t, cluster)
	release := holdRedpandaOutputTestProduce(t, cluster)
	require.NoError(t, w.Connect(t.Context()))

	var dispatched atomic.Int32
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- w.WriteBatch(t.Context(), service.MessageBatch{newRedpandaOutputTestMessage(&dispatched)})
	}()
	require.Eventually(t, func() bool {
		return dispatched.Load() == 1
	}, 5*time.Second, 10*time.Millisecond)

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- w.Close(t.Context())
	}()

	select {
	case err := <-closeDone:
		t.Fatalf("Close returned (%v) while a write was waiting on its acks", err)
	case <-time.After(100 * time.Millisecond):
	}

	release()
	require.NoError(t, receiveRedpandaOutputTestResult(t, writeDone, "write"))
	require.NoError(t, receiveRedpandaOutputTestResult(t, closeDone, "Close"))
}

// TestRedpandaOutputReconnectsAfterClose pins that a write after Close
// creates a new client rather than using the closed one.
func TestRedpandaOutputReconnectsAfterClose(t *testing.T) {
	cluster := newRedpandaOutputTestCluster(t)
	w := newRedpandaOutputTestWriter(t, cluster)

	var dispatched atomic.Int32
	require.NoError(t, w.Connect(t.Context()))
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{newRedpandaOutputTestMessage(&dispatched)}))
	require.NoError(t, w.Close(t.Context()))
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{newRedpandaOutputTestMessage(&dispatched)}))
	require.Equal(t, int32(2), dispatched.Load())
}

func newRedpandaOutputTestCluster(t *testing.T) *kfake.Cluster {
	t.Helper()

	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, redpandaOutputTestTopic))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
	return cluster
}

// newRedpandaOutputTestWriter builds the writer the redpanda output builds
// when it has its own connection fields.
func newRedpandaOutputTestWriter(t *testing.T, cluster *kfake.Cluster) *FranzWriter {
	t.Helper()

	conf, err := redpandaOutputConfig().ParseYAML(fmt.Sprintf(`
seed_brokers: [ %q ]
topic: %s
`, cluster.ListenAddrs()[0], redpandaOutputTestTopic), nil)
	require.NoError(t, err)

	connDetails, err := FranzConnectionDetailsFromConfig(conf, service.MockResources().Logger())
	require.NoError(t, err)
	producerOpts, err := FranzProducerOptsFromConfig(conf)
	require.NoError(t, err)

	w, err := NewFranzWriterFromConfig(conf, newRedpandaOutputClient(connDetails, producerOpts).hooks())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, w.Close(context.Background()))
	})
	return w
}

// holdRedpandaOutputTestProduce holds every Produce request inside the
// cluster until the returned func is called. SleepControl parks only the
// connection of the held request (franz-go produces on a dedicated
// connection), so metadata and other requests keep flowing. The release is
// also registered as a cleanup, which runs before the writer's Close cleanup
// so that a failed test does not leave Close waiting on a held write.
func holdRedpandaOutputTestProduce(t *testing.T, cluster *kfake.Cluster) func() {
	t.Helper()

	held := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(held) }) }
	t.Cleanup(release)

	cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		cluster.SleepControl(func() { <-held })
		return nil, nil, false
	})
	return release
}

// newRedpandaOutputTestMessage returns a message that counts its dispatch
// signal, which the writer fires once the record is handed to the producer
// and before the broker acknowledges it.
func newRedpandaOutputTestMessage(dispatched *atomic.Int32) *service.Message {
	msg := service.NewMessage([]byte("hello"))
	return msg.WithContext(dispatch.CtxOnTriggerSignal(msg.Context(), func() {
		dispatched.Add(1)
	}))
}

func receiveRedpandaOutputTestResult(t *testing.T, results <-chan error, what string) error {
	t.Helper()

	select {
	case err := <-results:
		return err
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not return after the produce request was released", what)
		return nil
	}
}
