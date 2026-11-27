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

package aerospike

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const coalesceSequenceConfig = `
key: '${! meta("k") }'
bins: 'root = this'
operation: '${! meta("op") }'
`

type seqMsg struct {
	op    string
	bins  map[string]any
	fence int
}

func (m seqMsg) String() string {
	s := m.op
	if m.op != "delete" {
		b, _ := json.Marshal(m.bins)
		s += string(b)
	}
	if m.fence > 0 {
		s += "@" + strconv.Itoa(m.fence)
	}
	return s
}

type seqCase struct {
	init map[string]any
	msgs []seqMsg
}

func (c seqCase) String() string {
	parts := make([]string, len(c.msgs))
	for i, m := range c.msgs {
		parts[i] = m.String()
	}
	b, _ := json.Marshal(c.init)
	return fmt.Sprintf("init=%s [%s]", b, strings.Join(parts, ", "))
}

// seqVariants returns every message shape for one position. Distinct values
// per position make it visible which message a stored bin came from.
func seqVariants(pos int, binSets []func(v int) map[string]any) []seqMsg {
	var out []seqMsg
	for _, op := range []string{"write", "replace", "replace_only", "create_only", "update_only"} {
		for _, bins := range binSets {
			out = append(out, seqMsg{op: op, bins: bins(pos + 1)})
		}
	}
	return append(out, seqMsg{op: "delete"})
}

var (
	binsSetA  = func(v int) map[string]any { return map[string]any{"a": v} }
	binsSetB  = func(v int) map[string]any { return map[string]any{"b": v} }
	binsNullA = func(int) map[string]any { return map[string]any{"a": nil} }
)

// seqProducts returns every sequence of length n over the variants.
func seqProducts(n int, binSets []func(v int) map[string]any) [][]seqMsg {
	seqs := [][]seqMsg{nil}
	for pos := range n {
		var next [][]seqMsg
		for _, s := range seqs {
			for _, v := range seqVariants(pos, binSets) {
				next = append(next, append(append([]seqMsg(nil), s...), v))
			}
		}
		seqs = next
	}
	return seqs
}

func seqMessage(t *testing.T, key string, m seqMsg) *service.Message {
	t.Helper()
	body := []byte(`{}`)
	if m.op != "delete" {
		var err error
		body, err = json.Marshal(m.bins)
		require.NoError(t, err)
	}
	msg := service.NewMessage(body)
	msg.MetaSet("k", key)
	msg.MetaSet("op", m.op)
	if m.fence > 0 {
		msg.MetaSet("off", strconv.Itoa(m.fence))
	}
	return msg
}

// writeBatch writes batch and returns the indexes of the messages it failed.
func writeBatch(t *testing.T, w *aerospikeWriter, batch service.MessageBatch) map[int]bool {
	t.Helper()
	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	failed := map[int]bool{}
	if err == nil {
		return failed
	}
	var berr *service.BatchError
	require.ErrorAs(t, err, &berr, "the batch failed as a whole: %v", err)
	berr.WalkMessagesIndexedBy(indexer, func(i int, _ *service.Message, msgErr error) bool {
		if msgErr != nil {
			failed[i] = true
		}
		return true
	})
	return failed
}

// compareSequences writes every case once as one batch and once a message at
// a time, then requires the same failed messages and the same stored record.
//
// A message at a time follows what the output promises for one batch: a
// message the server rejects stops the later messages for that key, because
// they must not land before its retry. A message rejected while the batch is
// planned is never sent and does not stop them.
func compareSequences(t *testing.T, w *aerospikeWriter, client *as.Client, prefix string, cases []seqCase) {
	t.Helper()
	const chunk = 500
	for start := 0; start < len(cases); start += chunk {
		part := cases[start:min(start+chunk, len(cases))]
		compareSequenceChunk(t, w, client, fmt.Sprintf("%s-%d", prefix, start), part)
	}
}

func compareSequenceChunk(t *testing.T, w *aerospikeWriter, client *as.Client, prefix string, cases []seqCase) {
	t.Helper()
	batchKey := func(i int) string { return fmt.Sprintf("%s-b-%d", prefix, i) }
	seqKey := func(i int) string { return fmt.Sprintf("%s-s-%d", prefix, i) }

	var inits []as.BatchRecordIfc
	for i, c := range cases {
		if c.init == nil {
			continue
		}
		for _, k := range []string{batchKey(i), seqKey(i)} {
			key, err := as.NewKey(integrationNamespace, integrationOutputSet, k)
			require.NoError(t, err)
			ops := make([]*as.Operation, 0, len(c.init))
			for name, v := range c.init {
				ops = append(ops, as.PutOp(as.NewBin(name, v)))
			}
			inits = append(inits, as.NewBatchWrite(nil, key, ops...))
		}
	}
	if len(inits) > 0 {
		require.NoError(t, client.BatchOperate(nil, inits))
	}

	var batch service.MessageBatch
	owner := map[int][2]int{}
	for i, c := range cases {
		for j, m := range c.msgs {
			owner[len(batch)] = [2]int{i, j}
			batch = append(batch, seqMessage(t, batchKey(i), m))
		}
	}
	batchFailed := map[[2]int]bool{}
	for idx := range writeBatch(t, w, batch) {
		batchFailed[owner[idx]] = true
	}

	seqFailed := map[[2]int]bool{}
	stopped := map[int]bool{}
	for j := 0; ; j++ {
		var step service.MessageBatch
		var stepOwner []int
		more := false
		for i, c := range cases {
			if j >= len(c.msgs) {
				continue
			}
			more = true
			if stopped[i] {
				seqFailed[[2]int{i, j}] = true
				continue
			}
			m := seqMessage(t, seqKey(i), c.msgs[j])
			if _, planFailures := w.planBatch(service.MessageBatch{m}); len(planFailures) > 0 {
				seqFailed[[2]int{i, j}] = true
				continue
			}
			step = append(step, m)
			stepOwner = append(stepOwner, i)
		}
		if !more {
			break
		}
		if len(step) == 0 {
			continue
		}
		for idx := range writeBatch(t, w, step) {
			i := stepOwner[idx]
			seqFailed[[2]int{i, j}] = true
			stopped[i] = true
		}
	}

	keys := make([]*as.Key, 0, 2*len(cases))
	for i := range cases {
		for _, k := range []string{batchKey(i), seqKey(i)} {
			key, err := as.NewKey(integrationNamespace, integrationOutputSet, k)
			require.NoError(t, err)
			keys = append(keys, key)
		}
	}
	recs, err := client.BatchGet(nil, keys)
	require.NoError(t, err)

	mismatches := 0
	for i, c := range cases {
		var batchBins, seqBins map[string]any
		if r := recs[2*i]; r != nil {
			batchBins = r.Bins
		}
		if r := recs[2*i+1]; r != nil {
			seqBins = r.Bins
		}
		var batchF, seqF []int
		for j := range c.msgs {
			if batchFailed[[2]int{i, j}] {
				batchF = append(batchF, j)
			}
			if seqFailed[[2]int{i, j}] {
				seqF = append(seqF, j)
			}
		}
		if assert.Equal(t, seqBins, batchBins, "stored record differs: %v", c) &&
			assert.Equal(t, seqF, batchF, "failed messages differ: %v", c) {
			continue
		}
		if mismatches++; mismatches >= 20 {
			t.Fatal("too many mismatches")
		}
	}
}

func TestIntegrationCoalesceMatchesSequential(t *testing.T) {
	w, client := outputSetup(t, coalesceSequenceConfig)

	inits := []map[string]any{nil, {"a": 0, "c": 0}}
	binSets := []func(int) map[string]any{binsSetA, binsSetB, binsNullA}
	for _, n := range []int{2, 3} {
		var cases []seqCase
		for _, init := range inits {
			for _, msgs := range seqProducts(n, binSets) {
				cases = append(cases, seqCase{init: init, msgs: msgs})
			}
		}
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			compareSequences(t, w, client, "plain"+strconv.Itoa(n), cases)
		})
	}
}

func TestIntegrationFencedCoalesceMatchesSequential(t *testing.T) {
	w, client := outputSetup(t, coalesceSequenceConfig+`
ttl: 0
fencing:
  enabled: true
  value: '${! meta("off") }'
`)

	// Fence values 3, 7 and 9 fall on both sides of the stored fence 5.
	inits := []map[string]any{
		nil,
		{"a": 0, "c": 0},
		{"a": 0, "c": 0, defaultFenceBin: 5},
		{defaultFenceBin: 5, defaultTombstoneBin: true},
	}
	withFences := func(msgs []seqMsg, fences []int) []seqMsg {
		out := append([]seqMsg(nil), msgs...)
		for i := range out {
			out[i].fence = fences[i]
		}
		return out
	}

	var two []seqCase
	for _, init := range inits {
		for _, msgs := range seqProducts(2, []func(int) map[string]any{binsSetA, binsSetB, binsNullA}) {
			for _, fences := range [][]int{{3, 7}, {7, 9}, {7, 3}, {9, 7}, {7, 7}, {3, 3}} {
				two = append(two, seqCase{init: init, msgs: withFences(msgs, fences)})
			}
		}
	}
	t.Run("2", func(t *testing.T) {
		compareSequences(t, w, client, "fenced2", two)
	})

	var three []seqCase
	for _, init := range inits {
		for _, msgs := range seqProducts(3, []func(int) map[string]any{binsSetA, binsNullA}) {
			for _, fences := range [][]int{{3, 7, 9}, {9, 7, 3}, {7, 9, 3}, {3, 9, 7}} {
				three = append(three, seqCase{init: init, msgs: withFences(msgs, fences)})
			}
		}
	}
	t.Run("3", func(t *testing.T) {
		compareSequences(t, w, client, "fenced3", three)
	})
}

// A write after a fenced delete must not keep the tombstone expiration, in one
// batch or across batches.
func TestIntegrationFencedWriteAfterDeleteDropsTombstoneTTL(t *testing.T) {
	w, client := outputSetup(t, coalesceSequenceConfig+`
ttl: 0
fencing:
  enabled: true
  value: '${! meta("off") }'
  tombstone_ttl: 20s
`)

	msgs := []seqMsg{
		{op: "write", bins: map[string]any{"a": 1}, fence: 1},
		{op: "delete", fence: 2},
		{op: "write", bins: map[string]any{"a": 3}, fence: 3},
	}

	var batch service.MessageBatch
	for _, m := range msgs {
		batch = append(batch, seqMessage(t, "tomb-ttl-batch", m))
	}
	require.NoError(t, w.WriteBatch(t.Context(), batch))

	for _, m := range msgs {
		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{seqMessage(t, "tomb-ttl-seq", m)}))
	}

	for _, id := range []string{"tomb-ttl-batch", "tomb-ttl-seq"} {
		rec := outputRead(t, client, id)
		require.NotNil(t, rec, id)
		assert.Equal(t, 3, rec.Bins["a"], id)
		assert.NotContains(t, rec.Bins, defaultTombstoneBin, id)
		// The test namespace has default-ttl 0, so ttl 0 means never expire.
		assert.Equal(t, uint32(as.TTLDontExpire), rec.Expiration, id)
	}
}

// A later message for a key is not written after an earlier message for that
// key failed in the same batch, so the retry of the failed message cannot land
// after it.
func TestIntegrationLaterRoundSkippedAfterFailure(t *testing.T) {
	w, client := outputSetup(t, coalesceSequenceConfig)

	batch := service.MessageBatch{
		seqMessage(t, "skip-1", seqMsg{op: "update_only", bins: map[string]any{"a": 1}}),
		seqMessage(t, "skip-2", seqMsg{op: "write", bins: map[string]any{"a": 1}}),
		seqMessage(t, "skip-1", seqMsg{op: "write", bins: map[string]any{"a": 2}}),
		seqMessage(t, "skip-2", seqMsg{op: "create_only", bins: map[string]any{"a": 2}}),
	}
	assert.Equal(t, map[int]bool{0: true, 2: true, 3: true}, writeBatch(t, w, batch))

	assert.Nil(t, outputRead(t, client, "skip-1"), "the write must wait for the retry of the update_only")
	rec := outputRead(t, client, "skip-2")
	require.NotNil(t, rec)
	assert.Equal(t, 1, rec.Bins["a"])
}
