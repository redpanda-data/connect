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
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/dispatch"
)

func TestPartitionCacheOrdering(t *testing.T) {
	var commitOffset int64 = -1
	pCache := newPartitionCache(func(r *kgo.Record) {
		atomic.StoreInt64(&commitOffset, r.Offset)
	})

	batches, batchSize := 100000, 10

	go func() {
		for bid := range batches {
			var bwr batchWithRecords

			for i := range batchSize {
				mid := int64((bid * batchSize) + i)
				bwr.b = append(bwr.b, &messageWithRecord{
					m:    service.NewMessage(strconv.AppendInt(nil, mid, 10)),
					r:    &kgo.Record{Offset: mid},
					size: 1,
				})
				bwr.size++
			}
			require.False(t, pCache.push(uint64(batches*10), uint64(batchSize), &bwr))
		}
	}()

	assert.Equal(t, int64(-1), atomic.LoadInt64(&commitOffset))

	workers := 10
	workerBatchChan := make(chan *batchWithAckFn, workers)
	outputBatchChan := make(chan *batchWithAckFn, 1)

	// These workers simulate processing pipelines that naturally want to tangle
	// the ordering of messages.
	for range workers {
		go func() {
			for {
				nextBatch, open := <-workerBatchChan
				if !open {
					return
				}

				// time.Sleep(time.Duration(rand.Intn(100) + 1))
				outputBatchChan <- nextBatch
			}
		}()
	}

	// This routine simulates an input pulling data out as fast as possible
	go func() {
		for range batches {
			var nextBatch *batchWithAckFn
			for nextBatch == nil {
				nextBatch = pCache.pop()
			}

			select {
			case workerBatchChan <- nextBatch:
			case <-t.Context().Done():
				t.Error(t.Context().Err())
			}
		}
		close(workerBatchChan)
	}()

	// This loop simulates an output that expects ordered messages
	var n int

	for range batches {
		select {
		case nextBatch, open := <-outputBatchChan:
			if !open {
				return
			}

			require.Len(t, nextBatch.batch, batchSize)

			for _, m := range nextBatch.batch {
				mBytes, err := m.AsBytes()
				assert.NoError(t, err)

				require.Equal(t, strconv.Itoa(n), string(mBytes))
				n++

				// Immediately trigger the next batch flush
				dispatch.TriggerSignal(m.Context())
			}

			// time.Sleep(time.Duration(rand.Intn(100) + 1))
			nextBatch.onAck()
		case <-t.Context().Done():
			t.Error(t.Context().Err())
			return
		}
	}
}

func TestPartitionCacheBatching(t *testing.T) {
	pCache := newPartitionCache(func(*kgo.Record) {})
	bufSize, batchSize := uint64(1_000_000), uint64(10)

	var i int64
	testBatchIn := func(msgs ...string) *batchWithRecords {
		b := &batchWithRecords{}
		for _, m := range msgs {
			b.b = append(b.b, &messageWithRecord{
				m:    service.NewMessage([]byte(m)),
				r:    &kgo.Record{Offset: i},
				size: uint64(len(m)),
			})
			b.size += uint64(len(m))
			i++
		}
		return b
	}

	popOutStrs := func(pCache *partitionCache) (outStrs []string) {
		tmp := pCache.pop()
		if tmp == nil {
			return
		}

		tmp.onAck()
		for _, m := range tmp.batch {
			outBytes, err := m.AsBytes()
			require.NoError(t, err)

			outStrs = append(outStrs, string(outBytes))
		}
		return
	}

	// Ensure big batches are broken down
	assert.False(t, pCache.push(bufSize, batchSize, testBatchIn(
		"aaaa",
		"bbbb",
		"cccc",
		"dd",
		"ee",
		"ffff",
	)))

	assert.Equal(t, []string{"aaaa", "bbbb"}, popOutStrs(pCache))

	assert.Equal(t, []string{"cccc", "dd", "ee"}, popOutStrs(pCache))

	assert.Equal(t, []string{"ffff"}, popOutStrs(pCache))

	assert.Equal(t, []string(nil), popOutStrs(pCache))

	// Ensure small batches get messages appended to them
	assert.False(t, pCache.push(bufSize, batchSize, testBatchIn(
		"aaaa",
		"bbbb",
	)))

	assert.False(t, pCache.push(bufSize, batchSize, testBatchIn(
		"cc",
		"dddd",
		"eeee",
		"ffff",
	)))

	assert.False(t, pCache.push(bufSize, batchSize, testBatchIn(
		"gg",
		"hh",
	)))

	assert.False(t, pCache.push(bufSize, batchSize, testBatchIn(
		"iiiiiiii",
	)))

	assert.Equal(t, []string{"aaaa", "bbbb", "cc"}, popOutStrs(pCache))

	assert.Equal(t, []string{"dddd", "eeee"}, popOutStrs(pCache))

	assert.Equal(t, []string{"ffff", "gg", "hh"}, popOutStrs(pCache))

	assert.Equal(t, []string{"iiiiiiii"}, popOutStrs(pCache))

	assert.Equal(t, []string(nil), popOutStrs(pCache))
}

// orderedBatch builds a batchWithRecords of `count` sequential records starting
// at `startOffset` for the given topic/partition.
func orderedBatch(topic string, partition int32, startOffset, count int64) *batchWithRecords {
	b := &batchWithRecords{}
	for i := startOffset; i < startOffset+count; i++ {
		b.b = append(b.b, &messageWithRecord{
			m:    service.NewMessage([]byte("x")),
			r:    &kgo.Record{Topic: topic, Partition: partition, Offset: i},
			size: 1,
		})
		b.size++
	}
	return b
}

// TestOrderedCacheRevokedBlocksCommit verifies that a partitionCache flagged as
// revoked does not commit a late ack, while an equivalent non-revoked cache
// does. This is the ordered-reader analogue of the unordered data-loss fix:
// without the guard a late MarkCommitRecords after a revoke would advance the
// committed offset past records the new owner has not processed.
func TestOrderedCacheRevokedBlocksCommit(t *testing.T) {
	rec := newCommitRecorder()

	pc := newPartitionCache(rec.commitFunc)
	require.False(t, pc.push(1<<20, 10, orderedBatch("t", 0, 0, 3)))
	ack := pc.pop()
	require.NotNil(t, ack)

	pc.markRevoked()
	ack.onAck()
	assert.Empty(t, rec.offsetsFor("t", 0), "revoked cache must not commit any offset")

	// Control: a non-revoked cache commits the head of its batch as normal.
	pc2 := newPartitionCache(rec.commitFunc)
	require.False(t, pc2.push(1<<20, 10, orderedBatch("t", 1, 0, 3)))
	ack2 := pc2.pop()
	require.NotNil(t, ack2)
	ack2.onAck()
	assert.Equal(t, []int64{2}, rec.offsetsFor("t", 1), "non-revoked cache commits the batch head offset")
}

// TestOrderedReassignedPartitionUsesFreshCache proves the per-cache design is
// immune to the same-member reassignment race: after revoke and reassignment a
// fresh partitionCache is created and commits resume, while a stale ack still
// referencing the old (revoked) cache can never commit.
func TestOrderedReassignedPartitionUsesFreshCache(t *testing.T) {
	rec := newCommitRecorder()
	ps := newPartitionState(rec.commitFunc)
	const bufSize, maxBatch = uint64(1 << 20), uint64(10)

	// Old generation: a record on t/0 at offset 5, left in flight (not acked).
	require.False(t, ps.addRecords("t", 0, orderedBatch("t", 0, 5, 1), bufSize, maxBatch))
	oldAck := ps.pop()
	require.NotNil(t, oldAck)

	// Revoke t/0.
	ps.removeTopicPartitions(map[string][]int32{"t": {0}})

	// Reassignment: a new record on t/0 at offset 100 creates a fresh cache.
	require.False(t, ps.addRecords("t", 0, orderedBatch("t", 0, 100, 1), bufSize, maxBatch))
	newAck := ps.pop()
	require.NotNil(t, newAck)

	// New generation commits.
	newAck.onAck()
	assert.Equal(t, []int64{100}, rec.offsetsFor("t", 0), "reassigned partition commits via fresh cache")

	// Stale old-generation ack must not commit.
	oldAck.onAck()
	assert.Equal(t, []int64{100}, rec.offsetsFor("t", 0), "stale ack from revoked cache must not commit")
	assert.NotContains(t, rec.offsetsFor("t", 0), int64(5), "old offset 5 must never be committed")
}
