// Copyright 2024 Redpanda Data, Inc.
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

	batches, batchSize := 1000000, 10

	go func() {
		for bid := 0; bid < batches; bid++ {
			var bwr batchWithRecords

			for i := 0; i < batchSize; i++ {
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
	for i := 0; i < workers; i++ {
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
		for bid := 0; bid < batches; bid++ {
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

	for bid := 0; bid < batches; bid++ {
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
	pCache := newPartitionCache(func(r *kgo.Record) {})
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
