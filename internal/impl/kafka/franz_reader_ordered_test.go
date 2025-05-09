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
			var bs service.MessageBatch
			var rs []*kgo.Record
			for i := 0; i < batchSize; i++ {
				mid := int64((bid * batchSize) + i)
				bs = append(bs, service.NewMessage(strconv.AppendInt(nil, mid, 10)))
				rs = append(rs, &kgo.Record{Offset: mid})
			}
			require.False(t, pCache.push(uint64(batches*10), &batchWithRecords{
				b:    bs,
				r:    rs,
				size: 1,
			}))
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
