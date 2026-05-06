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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// closedKgoClient returns a kgo.Client that has already been Close()d. Calling
// Produce on it fires the callback nearly immediately with "client closed",
// which lets us exercise the produce-callback failure path without a broker.
func closedKgoClient(t *testing.T) *kgo.Client {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers("localhost:0"))
	require.NoError(t, err)
	c.Close()
	return c
}

// failedIndices returns the indices that carry a non-nil error inside a
// *service.BatchError, in walk order.
func failedIndices(be *service.BatchError) []int {
	var out []int
	be.WalkMessages(func(i int, _ *service.Message, err error) bool {
		if err != nil {
			out = append(out, i)
		}
		return true
	})
	return out
}

// TestBatchWriterPartialFailures covers the per-record error reporting added
// to (*batchWriter).writeBatch. No broker is required: each test row uses
// MessageBatchToFranzRecords + DecorateRecord (and, where needed, a closed
// kgo.Client) to drive specific records into specific failure paths.
//
// Failure paths exercised:
//   - DecorateRecord returns an error → recorded at that index, no Produce.
//   - Produce callback fires with an error (closed client) → recorded at that
//     index after wg.Wait.
//
// Non-failure paths exercised:
//   - SkipRecord sentinel (empty Topic+Value+Key) → no Produce, no error slot.
//   - Successful path is implicitly covered by the all-skip row returning nil.
func TestBatchWriterPartialFailures(t *testing.T) {
	errDecorate := errors.New("injected decorate error")

	// Each record's Topic encodes its batch index as "idx-N" via
	// MessageBatchToFranzRecords. DecorateRecord parses that to look up the
	// per-index test directives.
	tests := []struct {
		name string

		// skipAt: indices that MessageBatchToFranzRecords returns as
		// SkipRecord (empty kgo.Record).
		skipAt map[int]bool
		// decorateFailAt: indices that DecorateRecord should fail.
		decorateFailAt map[int]bool
		// useClosedClient: when true, records that reach Produce fail with
		// "client closed".
		useClosedClient bool

		wantNil           bool
		wantFailedIndices []int
	}{
		{
			name:    "all skipped returns nil",
			skipAt:  map[int]bool{0: true, 1: true, 2: true},
			wantNil: true,
		},
		{
			name:              "all decorate fail",
			decorateFailAt:    map[int]bool{0: true, 1: true, 2: true},
			wantFailedIndices: []int{0, 1, 2},
		},
		{
			name:              "single decorate fail at middle index",
			decorateFailAt:    map[int]bool{1: true},
			skipAt:            map[int]bool{0: true, 2: true},
			wantFailedIndices: []int{1},
		},
		{
			name:              "decorate fail and skip do not overlap",
			decorateFailAt:    map[int]bool{0: true, 2: true},
			skipAt:            map[int]bool{1: true},
			wantFailedIndices: []int{0, 2},
		},
		{
			name:              "all produce fail via closed client",
			useClosedClient:   true,
			wantFailedIndices: []int{0, 1, 2},
		},
		{
			name:              "produce fail with one skip; skip not in error",
			skipAt:            map[int]bool{1: true},
			useClosedClient:   true,
			wantFailedIndices: []int{0, 2},
		},
		{
			name:              "produce fail and decorate fail combined",
			decorateFailAt:    map[int]bool{1: true},
			useClosedClient:   true,
			wantFailedIndices: []int{0, 1, 2},
		},
	}

	const batchSize = 3

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var client *kgo.Client
			if tc.useClosedClient {
				client = closedKgoClient(t)
			}
			info := &FranzSharedClientInfo{Client: client}

			w := &FranzWriter{
				hooks: NewFranzWriterHooks(
					func(_ context.Context, fn FranzSharedClientUseFn) error {
						return fn(info)
					},
				),
				MessageBatchToFranzRecords: func(batch service.MessageBatch) ([]kgo.Record, error) {
					records := make([]kgo.Record, len(batch))
					for i := range batch {
						if tc.skipAt[i] {
							records[i] = SkipRecord
							continue
						}
						records[i] = kgo.Record{
							Topic: fmt.Sprintf("idx-%d", i),
							Value: []byte("v"),
						}
					}
					return records, nil
				},
				DecorateRecord: func(r *kgo.Record) error {
					var i int
					if _, err := fmt.Sscanf(r.Topic, "idx-%d", &i); err != nil {
						return fmt.Errorf("parse index from topic %q: %w", r.Topic, err)
					}
					if tc.decorateFailAt[i] {
						return errDecorate
					}
					// Rewrite to a real-looking topic so the closed-client
					// Produce path treats this as a normal record.
					r.Topic = "test-topic"
					return nil
				},
			}

			batch := make(service.MessageBatch, batchSize)
			for i := range batch {
				batch[i] = service.NewMessage([]byte("payload"))
			}

			err := w.WriteBatch(t.Context(), batch)

			if tc.wantNil {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			var batchErr *service.BatchError
			require.ErrorAs(t, err, &batchErr)
			assert.Equal(t, len(tc.wantFailedIndices), batchErr.IndexedErrors(),
				"IndexedErrors() count mismatch")
			assert.ElementsMatch(t, tc.wantFailedIndices, failedIndices(batchErr),
				"failed indices mismatch")
		})
	}
}
