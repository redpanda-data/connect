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
	"slices"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func testKey(t *testing.T, k string) *as.Key {
	t.Helper()
	key, err := as.NewKey("test", "users", k)
	require.NoError(t, err)
	return key
}

func opWith(t *testing.T, idx int, k string, kind opKind, bins map[string]any) *pendingOp {
	t.Helper()
	op := newPendingOp(idx, testKey(t, k), kind, 0)
	// Insert in sorted order so binOrder is deterministic, matching what the
	// real mapper produces.
	names := make([]string, 0, len(bins))
	for n := range bins {
		names = append(names, n)
	}
	slices.Sort(names)
	for _, n := range names {
		op.setBin(n, bins[n])
	}
	return op
}

// TestFold covers the merge rules that make coalescing safe. Each case states
// what applying the operations in sequence would leave on the server, and
// asserts the folded single write produces the same thing.
func TestFold(t *testing.T) {
	tests := []struct {
		name     string
		ops      []*pendingOp
		wantKind opKind
		wantBins map[string]any
	}{
		{
			name: "two writes merge their bins",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opWrite, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opWrite, map[string]any{"b": 2}),
			},
			wantKind: opWrite,
			wantBins: map[string]any{"a": 1, "b": 2},
		},
		{
			name: "later write wins on a shared bin",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opWrite, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opWrite, map[string]any{"a": 2}),
			},
			wantKind: opWrite,
			wantBins: map[string]any{"a": 2},
		},
		{
			// replace defines the full record, so anything accumulated before
			// it is irrelevant.
			name: "replace discards earlier bins",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opWrite, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opReplace, map[string]any{"b": 2}),
			},
			wantKind: opReplace,
			wantBins: map[string]any{"b": 2},
		},
		{
			// A write after a replace merges into it, and the result must stay
			// a replace so bins already on the server are still cleared.
			name: "write after replace stays a replace",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opReplace, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opWrite, map[string]any{"b": 2}),
			},
			wantKind: opReplace,
			wantBins: map[string]any{"a": 1, "b": 2},
		},
		{
			name: "delete discards earlier bins",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opWrite, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opDelete, nil),
			},
			wantKind: opDelete,
			wantBins: map[string]any{},
		},
		{
			// The critical case. Skipping the delete and emitting a merging
			// write would leave pre-delete bins on the server, so the fold has
			// to promote to replace.
			name: "write after delete is promoted to replace",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opWrite, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opDelete, nil),
				opWith(t, 2, "u1", opWrite, map[string]any{"b": 2}),
			},
			wantKind: opReplace,
			wantBins: map[string]any{"b": 2},
		},
		{
			name: "delete then write then delete ends deleted",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opDelete, nil),
				opWith(t, 1, "u1", opWrite, map[string]any{"a": 1}),
				opWith(t, 2, "u1", opDelete, nil),
			},
			wantKind: opDelete,
			wantBins: map[string]any{},
		},
		// update_only must not steal the existence action of a preceding
		// write. Folding [write{a}, update_only{b}] into update_only{b}
		// is a poison batch: sequential application creates then updates, but
		// a lone update_only fails KEY_NOT_FOUND and nacks both messages forever.
		{
			name: "update_only after write stays a write",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opWrite, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opUpdateOnly, map[string]any{"b": 2}),
			},
			wantKind: opWrite,
			wantBins: map[string]any{"a": 1, "b": 2},
		},
		{
			name: "update_only after replace stays a replace",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opReplace, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opUpdateOnly, map[string]any{"b": 2}),
			},
			wantKind: opReplace,
			wantBins: map[string]any{"a": 1, "b": 2},
		},
		{
			name: "two update_onlys merge like two writes",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opUpdateOnly, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opUpdateOnly, map[string]any{"b": 2}),
			},
			wantKind: opUpdateOnly,
			wantBins: map[string]any{"a": 1, "b": 2},
		},
		// Sequential delete then create_only is a full-record rewrite.
		{
			name: "create_only after delete is promoted to replace",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opDelete, nil),
				opWith(t, 1, "u1", opCreateOnly, map[string]any{"b": 2}),
			},
			wantKind: opReplace,
			wantBins: map[string]any{"b": 2},
		},
		// replace_only is a full-record write that also requires the key to
		// exist. A preceding write or replace created the record, so the
		// folded command must not stay replace_only.
		{
			name: "replace_only after write is promoted to replace",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opWrite, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opReplaceOnly, map[string]any{"b": 2}),
			},
			wantKind: opReplace,
			wantBins: map[string]any{"b": 2},
		},
		{
			name: "replace_only after replace is promoted to replace",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opReplace, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opReplaceOnly, map[string]any{"b": 2}),
			},
			wantKind: opReplace,
			wantBins: map[string]any{"b": 2},
		},
		{
			name: "two replace_onlys keep the existence check",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opReplaceOnly, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opReplaceOnly, map[string]any{"b": 2}),
			},
			wantKind: opReplaceOnly,
			wantBins: map[string]any{"b": 2},
		},
		{
			name: "replace_only after update_only stays replace_only",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opUpdateOnly, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opReplaceOnly, map[string]any{"b": 2}),
			},
			wantKind: opReplaceOnly,
			wantBins: map[string]any{"b": 2},
		},
		{
			name: "update_only after replace_only stays replace_only",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opReplaceOnly, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opUpdateOnly, map[string]any{"b": 2}),
			},
			wantKind: opReplaceOnly,
			wantBins: map[string]any{"a": 1, "b": 2},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			acc := tc.ops[0]
			for _, next := range tc.ops[1:] {
				acc.fold(next)
			}

			assert.Equal(t, tc.wantKind, acc.kind)
			assert.Equal(t, tc.wantBins, acc.bins)

			// Every folded message must be represented so a failure can be
			// reported against all of them.
			assert.Len(t, acc.indexes, len(tc.ops))
		})
	}
}

// A compare-and-set write must never be folded. Folding it either drops the
// check, letting a write land on a record that changed underneath it, or
// carries a generation the server never reached, failing both messages.
func TestCoalesceExemptsGenerationCheckedKeys(t *testing.T) {
	checked := opWith(t, 0, "u1", opWrite, map[string]any{"a": 1})
	checked.generation, checked.hasGeneration = 7, true
	unchecked := opWith(t, 1, "u1", opWrite, map[string]any{"b": 2})
	other := opWith(t, 2, "u2", opWrite, map[string]any{"c": 3})

	out := coalesceOps([]*pendingOp{checked, unchecked, other}, true)

	// The CAS-checked key keeps one command per message; the untouched key
	// still coalesces normally.
	require.Len(t, out, 3)
	assert.Equal(t, []int{0}, out[0].indexes)
	assert.True(t, out[0].hasGeneration)
	assert.Equal(t, uint32(7), out[0].generation)
	assert.Equal(t, []int{1}, out[1].indexes)
	assert.Equal(t, []int{2}, out[2].indexes)
}

// The exemption has to cover every message for the key, not just the checked
// one: a write folded into a neighbour would still be reordered around the CAS.
func TestCoalesceExemptsWholeKeyWhenAnyMessageIsChecked(t *testing.T) {
	first := opWith(t, 0, "u1", opWrite, map[string]any{"a": 1})
	second := opWith(t, 1, "u1", opWrite, map[string]any{"b": 2})
	third := opWith(t, 2, "u1", opWrite, map[string]any{"c": 3})
	third.generation, third.hasGeneration = 9, true

	out := coalesceOps([]*pendingOp{first, second, third}, true)

	require.Len(t, out, 3)
	for i, op := range out {
		assert.Equal(t, []int{i}, op.indexes)
	}
}

func TestCoalesceFoldsWhenNoGenerationIsInvolved(t *testing.T) {
	out := coalesceOps([]*pendingOp{
		opWith(t, 0, "u1", opWrite, map[string]any{"a": 1}),
		opWith(t, 1, "u1", opWrite, map[string]any{"b": 2}),
		opWith(t, 2, "u2", opWrite, map[string]any{"c": 3}),
	}, true)

	require.Len(t, out, 2)
	assert.Equal(t, []int{0, 1}, out[0].indexes)
	assert.Equal(t, map[string]any{"a": 1, "b": 2}, out[0].bins)
	assert.Equal(t, []int{2}, out[1].indexes)
}

func TestFoldTracksAllSourceIndexes(t *testing.T) {
	acc := opWith(t, 3, "u1", opWrite, map[string]any{"a": 1})
	acc.fold(opWith(t, 7, "u1", opWrite, map[string]any{"b": 2}))
	acc.fold(opWith(t, 9, "u1", opWrite, map[string]any{"c": 3}))

	assert.Equal(t, []int{3, 7, 9}, acc.indexes)
}

func TestPlanBatchCoalescesRepeatedKeys(t *testing.T) {
	w := newTestWriter(t, baseConfig)

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","a":1}`)),
		service.NewMessage([]byte(`{"id":"u2","a":1}`)),
		service.NewMessage([]byte(`{"id":"u1","b":2}`)),
		service.NewMessage([]byte(`{"id":"u1","a":9}`)),
	}

	ops, failures := w.planBatch(batch)
	require.Empty(t, failures)

	// Four messages, two distinct keys, so two writes rather than four
	// commands contending on one record.
	require.Len(t, ops, 2)

	assert.Equal(t, []int{0, 2, 3}, ops[0].indexes)
	assert.Equal(t, int64(9), ops[0].bins["a"])
	assert.Equal(t, int64(2), ops[0].bins["b"])

	assert.Equal(t, []int{1}, ops[1].indexes)
}

func TestPlanBatchWriteThenUpdateOnlyStaysWrite(t *testing.T) {
	w := newTestWriter(t, baseConfig+"\noperation: '${! json(\"op\") }'\n")

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","op":"write","a":1}`)),
		service.NewMessage([]byte(`{"id":"u1","op":"update_only","b":2}`)),
	}

	ops, failures := w.planBatch(batch)
	require.Empty(t, failures)
	require.Len(t, ops, 1)
	assert.Equal(t, opWrite, ops[0].kind)
	assert.Equal(t, []int{0, 1}, ops[0].indexes)
	assert.Equal(t, int64(1), ops[0].bins["a"])
	assert.Equal(t, int64(2), ops[0].bins["b"])
}

func TestPlanBatchWithoutCoalescing(t *testing.T) {
	w := newTestWriter(t, baseConfig+"\ncoalesce_batch_keys: false\n")

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","a":1}`)),
		service.NewMessage([]byte(`{"id":"u1","b":2}`)),
	}

	ops, failures := w.planBatch(batch)
	require.Empty(t, failures)
	require.Len(t, ops, 2)
	assert.Equal(t, 0, ops[0].round)
	assert.Equal(t, 1, ops[1].round, "the second message for the key is sent after the first")
}

// TestCanFold covers every pair of operations. A pair folds only when one
// command leaves the same record and fails the same messages as the two
// commands in order, for an existing key and for a missing one.
func TestCanFold(t *testing.T) {
	kinds := []opKind{opWrite, opReplace, opReplaceOnly, opCreateOnly, opUpdateOnly, opDelete}
	folds := map[[2]opKind]bool{
		{opWrite, opWrite}: true, {opWrite, opReplace}: true, {opWrite, opDelete}: true,
		{opReplace, opWrite}: true, {opReplace, opReplace}: true, {opReplace, opDelete}: true,
		{opDelete, opWrite}: true, {opDelete, opReplace}: true, {opDelete, opDelete}: true,
		{opWrite, opUpdateOnly}: true, {opWrite, opReplaceOnly}: true,
		{opReplace, opUpdateOnly}: true, {opReplace, opReplaceOnly}: true,
		{opUpdateOnly, opUpdateOnly}: true, {opUpdateOnly, opReplaceOnly}: true,
		{opReplaceOnly, opUpdateOnly}: true, {opReplaceOnly, opReplaceOnly}: true,
		{opDelete, opCreateOnly}: true,
	}
	for _, first := range kinds {
		for _, second := range kinds {
			t.Run(first.String()+" then "+second.String(), func(t *testing.T) {
				want := folds[[2]opKind{first, second}]
				p := opWith(t, 0, "u1", first, map[string]any{"a": 1})
				next := opWith(t, 1, "u1", second, map[string]any{"b": 2})
				assert.Equal(t, want, p.canFold(next))

				out := coalesceOps([]*pendingOp{
					opWith(t, 0, "u1", first, map[string]any{"a": 1}),
					opWith(t, 1, "u1", second, map[string]any{"b": 2}),
				}, true)
				if want {
					require.Len(t, out, 1)
					assert.Equal(t, []int{0, 1}, out[0].indexes)
					return
				}
				require.Len(t, out, 2, "a pair that does not fold is sent as two commands")
				assert.Equal(t, []int{0}, out[0].indexes)
				assert.Equal(t, 0, out[0].round)
				assert.Equal(t, []int{1}, out[1].indexes)
				assert.Equal(t, 1, out[1].round, "the second command waits for the first")
			})
		}
	}

	t.Run("nothing folds into a delete that requires the record", func(t *testing.T) {
		p := newPendingOp(0, testKey(t, "u1"), opDelete, 0)
		p.deleteRequiresRecord = true
		for _, kind := range kinds {
			assert.False(t, p.canFold(opWith(t, 1, "u1", kind, map[string]any{"b": 2})), kind.String())
		}
	})
}

// Two create_only messages for a new key store the first payload and fail
// the second with KEY_EXISTS. One command cannot do that.
func TestPlanBatchCreateOnlyTwiceUsesTwoRounds(t *testing.T) {
	w := newTestWriter(t, baseConfig+"\noperation: create_only\n")

	ops, failures := w.planBatch(service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","a":1}`)),
		service.NewMessage([]byte(`{"id":"u2","a":1}`)),
		service.NewMessage([]byte(`{"id":"u1","a":2}`)),
	})
	require.Empty(t, failures)
	require.Len(t, ops, 3)
	assert.Equal(t, []int{0}, ops[0].indexes)
	assert.Equal(t, 0, ops[0].round)
	assert.Equal(t, int64(1), ops[0].bins["a"])
	assert.Equal(t, []int{1}, ops[1].indexes)
	assert.Equal(t, 0, ops[1].round)
	assert.Equal(t, []int{2}, ops[2].indexes)
	assert.Equal(t, 1, ops[2].round)
}

func TestFoldTTL(t *testing.T) {
	withTTL := func(idx int, kind opKind, ttl uint32) *pendingOp {
		op := opWith(t, idx, "u1", kind, map[string]any{"a": idx})
		if kind == opDelete {
			op = newPendingOp(idx, testKey(t, "u1"), opDelete, 0)
		}
		op.ttl = ttl
		return op
	}

	t.Run("a later keep leaves the earlier ttl", func(t *testing.T) {
		acc := withTTL(0, opWrite, 100)
		acc.fold(withTTL(1, opWrite, as.TTLDontUpdate))
		assert.Equal(t, uint32(100), acc.ttl)
	})
	t.Run("a later explicit ttl replaces it", func(t *testing.T) {
		acc := withTTL(0, opWrite, 100)
		acc.fold(withTTL(1, opWrite, 200))
		assert.Equal(t, uint32(200), acc.ttl)
	})
	t.Run("keep after a delete becomes the namespace default", func(t *testing.T) {
		for _, kind := range []opKind{opWrite, opReplace, opCreateOnly} {
			acc := withTTL(0, opDelete, 100)
			acc.fold(withTTL(1, kind, as.TTLDontUpdate))
			assert.Equal(t, opReplace, acc.kind, kind)
			assert.Equal(t, uint32(as.TTLServerDefault), acc.ttl, kind)
		}
	})
	t.Run("an explicit ttl after a delete applies", func(t *testing.T) {
		acc := withTTL(0, opDelete, 100)
		acc.fold(withTTL(1, opWrite, 200))
		assert.Equal(t, uint32(200), acc.ttl)
	})
}

// recordingOperate answers every record with OK and keeps the keys of each
// batch command, so a test can see the rounds.
func recordingOperate(calls *[][]string) func(*as.BatchPolicy, []as.BatchRecordIfc) error {
	return func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		keys := make([]string, len(recs))
		for i, r := range recs {
			rec := r.BatchRec()
			rec.ResultCode = types.OK
			keys[i] = rec.Key.Value().String()
		}
		*calls = append(*calls, keys)
		return nil
	}
}

func TestWriteBatchSendsRoundsInOrder(t *testing.T) {
	w := newTestWriter(t, baseConfig+"\noperation: create_only\n")
	var calls [][]string
	w.operate = recordingOperate(&calls)

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","a":1}`)),
		service.NewMessage([]byte(`{"id":"u2","a":1}`)),
		service.NewMessage([]byte(`{"id":"u1","a":2}`)),
	})
	require.NoError(t, err)
	assert.Equal(t, [][]string{{"u1", "u2"}, {"u1"}}, calls)
}

// A later round failing as a whole must not fail, and so retry, the messages
// an earlier round already wrote.
func TestWriteBatchLaterRoundFailureKeepsEarlierRounds(t *testing.T) {
	w := newTestWriter(t, baseConfig+"\noperation: create_only\n")
	var calls int
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		calls++
		if calls == 1 {
			for _, r := range recs {
				r.BatchRec().ResultCode = types.OK
			}
			return nil
		}
		return &as.AerospikeError{ResultCode: types.NETWORK_ERROR}
	}

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","a":1}`)),
		service.NewMessage([]byte(`{"id":"u2","a":1}`)),
		service.NewMessage([]byte(`{"id":"u1","a":2}`)),
	}
	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)
	assert.NotErrorIs(t, err, service.ErrNotConnected)

	var berr *service.BatchError
	require.ErrorAs(t, err, &berr)
	var failed []int
	berr.WalkMessagesIndexedBy(indexer, func(i int, _ *service.Message, msgErr error) bool {
		if msgErr != nil {
			failed = append(failed, i)
		}
		return true
	})
	assert.Equal(t, []int{2}, failed)
}

func TestPlanBatchRejectsAllNullReplace(t *testing.T) {
	for _, op := range []string{"replace", "replace_only"} {
		t.Run(op, func(t *testing.T) {
			w := newTestWriter(t, baseConfig+"\noperation: "+op+"\n")

			batch := service.MessageBatch{
				service.NewMessage([]byte(`{"id":"u1","a":null}`)),
				service.NewMessage([]byte(`{"id":"u2","a":1}`)),
			}
			ops, failures := w.planBatch(batch)

			require.Len(t, ops, 1)
			assert.Equal(t, []int{1}, ops[0].indexes)
			require.Len(t, failures, 1)
			assert.Contains(t, failures[0].Error(), "only null bin values")
			assert.NotContains(t, failures[0].Error(), "coalesced")
		})
	}
}

func TestPlanBatchKeepsNullBinOnWrite(t *testing.T) {
	w := newTestWriter(t, baseConfig+"\noperation: write\n")

	ops, failures := w.planBatch(service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","a":null}`)),
	})
	require.Empty(t, failures)
	require.Len(t, ops, 1)

	bw, ok := w.buildRecord(ops[0]).(*as.BatchWrite)
	require.True(t, ok)
	assert.Len(t, bw.Ops, 1, "write deletes a bin by putting nil")
}

func TestPlanBatchNullReplaceFoldsToDelete(t *testing.T) {
	w := newTestWriter(t, baseConfig+"\noperation: '${! meta(\"op\") }'\n")

	// Run one at a time, the delete succeeds and the null write only deletes
	// bins on a record that is already gone. Failing both would replay forever.
	// These are plain deletes: the earlier op would have created the record or
	// removed it, so a missing key is already the end state.
	cases := []struct {
		name string
		msgs []*service.Message
	}{
		{
			name: "delete then null write",
			msgs: func() []*service.Message {
				del := service.NewMessage([]byte(`{"id":"u1"}`))
				del.MetaSet("op", "delete")
				nullWrite := service.NewMessage([]byte(`{"id":"u1","a":null}`))
				nullWrite.MetaSet("op", "write")
				return []*service.Message{del, nullWrite}
			}(),
		},
		{
			name: "replace then null write",
			msgs: func() []*service.Message {
				repl := service.NewMessage([]byte(`{"id":"u1","a":1}`))
				repl.MetaSet("op", "replace")
				nullWrite := service.NewMessage([]byte(`{"id":"u1","a":null}`))
				nullWrite.MetaSet("op", "write")
				return []*service.Message{repl, nullWrite}
			}(),
		},
		{
			name: "replace then null update_only",
			msgs: func() []*service.Message {
				repl := service.NewMessage([]byte(`{"id":"u1","a":1}`))
				repl.MetaSet("op", "replace")
				nullUp := service.NewMessage([]byte(`{"id":"u1","a":null}`))
				nullUp.MetaSet("op", "update_only")
				return []*service.Message{repl, nullUp}
			}(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ops, failures := w.planBatch(tc.msgs)
			require.Empty(t, failures)
			require.Len(t, ops, 1)
			assert.Equal(t, opDelete, ops[0].kind)
			assert.False(t, ops[0].deleteRequiresRecord)
			assert.Equal(t, []int{0, 1}, ops[0].indexes)
			_, ok := w.buildRecord(ops[0]).(*as.BatchDelete)
			assert.True(t, ok)
		})
	}
}

func TestPlanBatchNullReplaceFailsOnItsOwn(t *testing.T) {
	w := newTestWriter(t, baseConfig+"\noperation: '${! meta(\"op\") }'\n")

	write := service.NewMessage([]byte(`{"id":"u1","a":1}`))
	write.MetaSet("op", "write")
	nullRepl := service.NewMessage([]byte(`{"id":"u1","a":null}`))
	nullRepl.MetaSet("op", "replace")
	other := service.NewMessage([]byte(`{"id":"u2","a":2}`))
	other.MetaSet("op", "write")

	ops, failures := w.planBatch(service.MessageBatch{write, nullRepl, other})

	require.Len(t, failures, 1)
	assert.Contains(t, failures[1].Error(), "only null bin values")
	assert.NotContains(t, failures[1].Error(), "coalesced")

	require.Len(t, ops, 2)
	assert.Equal(t, opWrite, ops[0].kind)
	assert.Equal(t, []int{0}, ops[0].indexes)
	assert.Equal(t, int64(1), ops[0].bins["a"])
	assert.Equal(t, []int{2}, ops[1].indexes)

	_, ok := w.buildRecord(ops[0]).(*as.BatchWrite)
	assert.True(t, ok, "the earlier write must still be sent")
}

func TestPlanBatchReplaceOnlyNullUpdateKeepsExistenceCheck(t *testing.T) {
	w := newTestWriter(t, baseConfig+"\noperation: '${! meta(\"op\") }'\n")

	t.Run("every bin nulled", func(t *testing.T) {
		repl := service.NewMessage([]byte(`{"id":"u1","a":1}`))
		repl.MetaSet("op", "replace_only")
		nullUp := service.NewMessage([]byte(`{"id":"u1","a":null}`))
		nullUp.MetaSet("op", "update_only")

		ops, failures := w.planBatch(service.MessageBatch{repl, nullUp})
		require.Empty(t, failures)
		require.Len(t, ops, 1)
		assert.Equal(t, opDelete, ops[0].kind)
		assert.True(t, ops[0].deleteRequiresRecord)
		assert.Equal(t, []int{0, 1}, ops[0].indexes)
		_, ok := w.buildRecord(ops[0]).(*as.BatchDelete)
		assert.True(t, ok)
	})

	t.Run("a bin remains", func(t *testing.T) {
		repl := service.NewMessage([]byte(`{"id":"u1","a":1,"b":2}`))
		repl.MetaSet("op", "replace_only")
		nullUp := service.NewMessage([]byte(`{"id":"u1","a":null}`))
		nullUp.MetaSet("op", "update_only")

		ops, failures := w.planBatch(service.MessageBatch{repl, nullUp})
		require.Empty(t, failures)
		require.Len(t, ops, 1)
		assert.Equal(t, opReplaceOnly, ops[0].kind)
		assert.False(t, ops[0].deleteRequiresRecord)
		assert.Equal(t, int64(2), ops[0].bins["b"])

		bw, ok := w.buildRecord(ops[0]).(*as.BatchWrite)
		require.True(t, ok)
		assert.Equal(t, as.REPLACE_ONLY, bw.Policy.RecordExistsAction)
		assert.Len(t, bw.Ops, 1, "the nulled bin is omitted and the other bin is sent")
	})

	// On a missing key the first two messages fail and the third succeeds,
	// so the third goes in a later round rather than folding.
	for _, later := range []string{"delete", "write"} {
		t.Run("a later "+later+" waits for the check", func(t *testing.T) {
			repl := service.NewMessage([]byte(`{"id":"u1","a":1}`))
			repl.MetaSet("op", "replace_only")
			nullUp := service.NewMessage([]byte(`{"id":"u1","a":null}`))
			nullUp.MetaSet("op", "update_only")
			next := service.NewMessage([]byte(`{"id":"u1","b":2}`))
			next.MetaSet("op", later)

			ops, failures := w.planBatch(service.MessageBatch{repl, nullUp, next})
			require.Empty(t, failures)
			require.Len(t, ops, 2)
			assert.Equal(t, opDelete, ops[0].kind)
			assert.True(t, ops[0].deleteRequiresRecord)
			assert.Equal(t, []int{0, 1}, ops[0].indexes)
			assert.Equal(t, 0, ops[0].round)
			assert.Equal(t, []int{2}, ops[1].indexes)
			assert.Equal(t, 1, ops[1].round)
		})
	}
}

func TestPlanBatchIsolatesMappingFailures(t *testing.T) {
	w := newTestWriter(t, baseConfig)

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","a":1}`)),
		service.NewMessage([]byte(`{"no_id":true}`)), // key resolves empty
		service.NewMessage([]byte(`{"id":"u3","a":3}`)),
	}

	ops, failures := w.planBatch(batch)

	// One bad message must not sink the two good ones.
	assert.Len(t, ops, 2)
	require.Len(t, failures, 1)
	assert.Contains(t, failures[1].Error(), "not a usable record key")
}

func TestBuildRecordAppliesOperationAndTTL(t *testing.T) {
	w := newTestWriter(t, baseConfig+"\nttl: 24h\noperation: replace\n")

	op, err := mapOne(t, w, service.NewMessage([]byte(`{"id":"u1","a":1}`)))
	require.NoError(t, err)

	rec := w.buildRecord(op)
	bw, ok := rec.(*as.BatchWrite)
	require.True(t, ok)

	assert.Equal(t, as.REPLACE, bw.Policy.RecordExistsAction)
	assert.Equal(t, uint32(86400), bw.Policy.Expiration)
	assert.Nil(t, bw.Policy.FilterExpression)
	assert.Len(t, bw.Ops, 1)
}

func TestBuildRecordReplaceOmitsNilBin(t *testing.T) {
	// A nil bin deletes that bin. On REPLACE and REPLACE_ONLY the server
	// rejects the put, and omission already deletes the bin.
	for _, kind := range []opKind{opReplace, opReplaceOnly} {
		t.Run(kind.String(), func(t *testing.T) {
			w := newTestWriter(t, baseConfig+"\noperation: "+kind.String()+"\n")
			op := newPendingOp(0, testKey(t, "u1"), kind, 0)
			op.setBin("a", 1)
			op.setBin("b", nil)

			bw, ok := w.buildRecord(op).(*as.BatchWrite)
			require.True(t, ok)
			assert.Len(t, bw.Ops, 1)
		})
	}

	w := newTestWriter(t, baseConfig+"\noperation: write\n")
	op := newPendingOp(0, testKey(t, "u1"), opWrite, 0)
	op.setBin("a", 1)
	op.setBin("b", nil)
	bw, ok := w.buildRecord(op).(*as.BatchWrite)
	require.True(t, ok)
	assert.Len(t, bw.Ops, 2, "write still deletes a bin by putting nil")
}

func TestBuildRecordReplaceOnly(t *testing.T) {
	w := newTestWriter(t, baseConfig+"\noperation: replace_only\n")

	op, err := mapOne(t, w, service.NewMessage([]byte(`{"id":"u1","a":1}`)))
	require.NoError(t, err)
	assert.Equal(t, opReplaceOnly, op.kind)

	bw, ok := w.buildRecord(op).(*as.BatchWrite)
	require.True(t, ok)
	assert.Equal(t, as.REPLACE_ONLY, bw.Policy.RecordExistsAction)
}

func TestBuildRecordDelete(t *testing.T) {
	w := newTestWriter(t, baseConfig)

	op := newPendingOp(0, testKey(t, "u1"), opDelete, 0)
	rec := w.buildRecord(op)

	_, ok := rec.(*as.BatchDelete)
	assert.True(t, ok)
}

func TestClassifyRecord(t *testing.T) {
	tests := []struct {
		name                 string
		code                 types.ResultCode
		kind                 opKind
		deleteRequiresRecord bool
		wantErr              bool
		contains             string
	}{
		{name: "ok", code: types.OK, kind: opWrite},
		// Deleting an absent record is the desired end state, which keeps
		// deletes idempotent under redelivery.
		{name: "missing key on delete is a success", code: types.KEY_NOT_FOUND_ERROR, kind: opDelete},
		{
			name: "missing key on delete folded from replace_only fails", code: types.KEY_NOT_FOUND_ERROR,
			kind: opDelete, deleteRequiresRecord: true,
			wantErr: true, contains: "does not exist and the operation is replace_only",
		},
		{
			name: "missing key on update_only fails", code: types.KEY_NOT_FOUND_ERROR, kind: opUpdateOnly,
			wantErr: true, contains: "does not exist and the operation is update_only",
		},
		{
			name: "missing key on replace_only fails", code: types.KEY_NOT_FOUND_ERROR, kind: opReplaceOnly,
			wantErr: true, contains: "does not exist and the operation is replace_only",
		},
		{
			name: "key busy explains hot keys", code: types.KEY_BUSY, kind: opWrite,
			wantErr: true, contains: "transaction-pending-limit",
		},
		{
			name: "forbidden explains nsup-period", code: types.FAIL_FORBIDDEN, kind: opWrite,
			wantErr: true, contains: "nsup-period",
		},
		{
			name: "record too big explains remodelling", code: types.RECORD_TOO_BIG, kind: opWrite,
			wantErr: true, contains: "max-record-size",
		},
		{
			name: "invalid namespace explains config", code: types.INVALID_NAMESPACE, kind: opWrite,
			wantErr: true, contains: "cannot be created at runtime",
		},
		{
			name: "no response is retryable", code: types.NO_RESPONSE, kind: opWrite,
			wantErr: true, contains: "retryable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := &as.BatchRecord{Key: testKey(t, "u1"), ResultCode: tc.code}
			err := classifyRecord(rec, &pendingOp{kind: tc.kind, deleteRequiresRecord: tc.deleteRequiresRecord})

			if !tc.wantErr {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.contains)
		})
	}
}

func TestClassifyRecordFlagsInDoubt(t *testing.T) {
	rec := &as.BatchRecord{Key: testKey(t, "u1"), ResultCode: types.TIMEOUT, InDoubt: true}
	err := classifyRecord(rec, &pendingOp{kind: opWrite})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "in doubt")
}
