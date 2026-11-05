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
	"context"
	"fmt"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestWriteBatchNotConnected(t *testing.T) {
	w := newTestWriter(t, baseConfig)
	err := w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","v":1}`)),
	})
	require.ErrorIs(t, err, service.ErrNotConnected)
}

func TestWriteBatchConnectionError(t *testing.T) {
	w := newTestWriter(t, baseConfig)
	w.operate = func(*as.BatchPolicy, []as.BatchRecordIfc) error {
		return &as.AerospikeError{ResultCode: types.NETWORK_ERROR}
	}

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","v":1}`)),
	})
	require.ErrorIs(t, err, service.ErrNotConnected)
}

func TestWriteBatchPartialFailure(t *testing.T) {
	w := newTestWriter(t, baseConfig)
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		require.Len(t, recs, 1)
		recs[0].BatchRec().ResultCode = types.KEY_BUSY
		return nil
	}

	batch := service.MessageBatch{service.NewMessage([]byte(`{"id":"u1","v":1}`))}
	indexer := batch.Index()

	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)
	var berr *service.BatchError
	require.ErrorAs(t, err, &berr)
	require.Equal(t, 1, berr.IndexedErrors())
	var msgErr error
	berr.WalkMessagesIndexedBy(indexer, func(_ int, _ *service.Message, e error) bool {
		msgErr = e
		return false
	})
	require.Error(t, msgErr)
	assert.Contains(t, msgErr.Error(), "hot key")
}

func TestWriteBatchSuccess(t *testing.T) {
	w := newTestWriter(t, baseConfig)
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		for _, rec := range recs {
			rec.BatchRec().ResultCode = types.OK
		}
		return nil
	}

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","v":1}`)),
	})
	require.NoError(t, err)
}

func TestWriteBatchIgnoresRecordTooBig(t *testing.T) {
	w := newTestWriter(t, baseConfig+"ignore_error_codes: [13, 21]\n")
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		require.Len(t, recs, 2)
		recs[0].BatchRec().ResultCode = types.RECORD_TOO_BIG
		recs[1].BatchRec().ResultCode = types.OK
		return nil
	}

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"big","v":1}`)),
		service.NewMessage([]byte(`{"id":"ok","v":1}`)),
	})
	require.NoError(t, err)
}

func TestWriteBatchAllIgnoredReturnsNil(t *testing.T) {
	w := newTestWriter(t, baseConfig+"ignore_error_codes: [13]\n")
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		require.Len(t, recs, 2)
		recs[0].BatchRec().ResultCode = types.RECORD_TOO_BIG
		recs[1].BatchRec().ResultCode = types.RECORD_TOO_BIG
		return nil
	}

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"big-1","v":1}`)),
		service.NewMessage([]byte(`{"id":"big-2","v":1}`)),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), w.ignoredTotal.Load())
}

func TestWriteBatchRecordTooBigStillFailsWhenNotListed(t *testing.T) {
	w := newTestWriter(t, baseConfig)
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		recs[0].BatchRec().ResultCode = types.RECORD_TOO_BIG
		return nil
	}

	batch := service.MessageBatch{service.NewMessage([]byte(`{"id":"big","v":1}`))}
	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)
	assert.Contains(t, indexedMessageError(t, indexer, err), "exceeds the namespace max-record-size")
}

func TestWriteBatchMixedIgnoreAndFailure(t *testing.T) {
	w := newTestWriter(t, baseConfig+"ignore_error_codes: [13]\n")
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		require.Len(t, recs, 2)
		recs[0].BatchRec().ResultCode = types.RECORD_TOO_BIG
		recs[1].BatchRec().ResultCode = types.KEY_BUSY
		return nil
	}

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"big","v":1}`)),
		service.NewMessage([]byte(`{"id":"hot","v":1}`)),
	}
	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)
	var berr *service.BatchError
	require.ErrorAs(t, err, &berr)
	require.Equal(t, 1, berr.IndexedErrors())
	assert.Equal(t, int64(1), w.ignoredTotal.Load())

	var failed string
	berr.WalkMessagesIndexedBy(indexer, func(_ int, msg *service.Message, msgErr error) bool {
		if msgErr != nil {
			raw, readErr := msg.AsBytes()
			require.NoError(t, readErr)
			failed = string(raw)
			assert.Contains(t, msgErr.Error(), "hot key")
		}
		return true
	})
	assert.Contains(t, failed, `"id":"hot"`)
}

func TestWriteBatchIgnoresLongBinName(t *testing.T) {
	w := newTestWriter(t, baseConfig+"ignore_error_codes: [21]\n")
	var sent []any
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		for _, rec := range recs {
			sent = append(sent, rec.BatchRec().Key.Value().GetObject())
			rec.BatchRec().ResultCode = types.OK
		}
		return nil
	}

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"p1","ok":1}`)),
		service.NewMessage([]byte(`{"id":"p2","this_bin_name_is_much_too_long":1}`)),
		service.NewMessage([]byte(`{"id":"p3","ok":1}`)),
	})
	require.NoError(t, err)
	assert.Equal(t, []any{"p1", "p3"}, sent)
}

func TestWriteBatchLongBinNameStillFailsWhenNotListed(t *testing.T) {
	w := newTestWriter(t, baseConfig)
	called := false
	w.operate = func(*as.BatchPolicy, []as.BatchRecordIfc) error {
		called = true
		return nil
	}

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"p2","this_bin_name_is_much_too_long":1}`)),
	}
	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)
	assert.False(t, called, "a long bin name is rejected before the batch is sent")
	assert.Contains(t, indexedMessageError(t, indexer, err), "exceeds the Aerospike limit of 15")
}

func TestWriteBatchEmptyBinNameStillFailsWhen21Listed(t *testing.T) {
	w := newTestWriter(t, baseConfig+`
ignore_error_codes: [21]
bins: 'root = {"": this.v}'
`)
	w.operate = func(*as.BatchPolicy, []as.BatchRecordIfc) error {
		t.Fatal("an empty bin name must not be sent")
		return nil
	}

	batch := service.MessageBatch{service.NewMessage([]byte(`{"id":"u1","v":1}`))}
	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)
	assert.Contains(t, indexedMessageError(t, indexer, err), "must not be empty")
}

func TestWriteBatchMaxRecordBytesStillFailsWhen13Listed(t *testing.T) {
	w := newTestWriter(t, baseConfig+`
ignore_error_codes: [13]
max_record_bytes: 4
`)
	w.operate = func(*as.BatchPolicy, []as.BatchRecordIfc) error {
		t.Fatal("max_record_bytes must reject the record before it is sent")
		return nil
	}

	batch := service.MessageBatch{service.NewMessage([]byte(`{"id":"u1","v":"abcdef"}`))}
	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)
	assert.Contains(t, indexedMessageError(t, indexer, err), "max_record_bytes")
}

func TestWriteBatchLongMapKeyIsNotCode21(t *testing.T) {
	w := newTestWriter(t, baseConfig+"ignore_error_codes: [21]\n")
	var sent int
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		sent = len(recs)
		recs[0].BatchRec().ResultCode = types.OK
		return nil
	}

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","tier":{"this_map_key_is_longer_than_fifteen":"gold"}}`)),
	})
	require.NoError(t, err)
	assert.Equal(t, 1, sent)
}

func TestWriteBatchIgnoresInvalidNamespace(t *testing.T) {
	w := newTestWriter(t, baseConfig+"ignore_error_codes: [20]\n")
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		require.Len(t, recs, 2)
		recs[0].BatchRec().ResultCode = types.INVALID_NAMESPACE
		recs[1].BatchRec().ResultCode = types.OK
		return nil
	}

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"bad","v":1}`)),
		service.NewMessage([]byte(`{"id":"ok","v":1}`)),
	})
	require.NoError(t, err)
}

func TestWriteBatchInvalidNamespaceStillFailsWhenNotListed(t *testing.T) {
	w := newTestWriter(t, baseConfig)
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		recs[0].BatchRec().ResultCode = types.INVALID_NAMESPACE
		recs[1].BatchRec().ResultCode = types.OK
		return nil
	}

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"bad","v":1}`)),
		service.NewMessage([]byte(`{"id":"ok","v":1}`)),
	}
	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)
	var berr *service.BatchError
	require.ErrorAs(t, err, &berr)
	require.Equal(t, 1, berr.IndexedErrors())
	assert.Contains(t, indexedMessageError(t, indexer, err), "does not exist")
}

func TestWriteBatchMissingNamespaceStillFailsWhen20Listed(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: '${! json("namespace_name") }'
set: users
key: '${! json("id") }'
bins: 'root = this.without("id")'
ignore_error_codes: [20]
`)
	w.operate = func(*as.BatchPolicy, []as.BatchRecordIfc) error {
		t.Fatal("a missing namespace field must not be sent")
		return nil
	}

	batch := service.MessageBatch{service.NewMessage([]byte(`{"id":"u1","v":1}`))}
	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)
	assert.Contains(t, indexedMessageError(t, indexer, err), `"null"`)
}

func TestWriteBatchIgnoresCommandLevelForbidden(t *testing.T) {
	w := newTestWriter(t, baseConfig+"ignore_error_codes: [22]\n")
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		// A one-record rejection sets the record code and returns it as the
		// command error. Both have to be acknowledged when 22 is listed.
		recs[0].BatchRec().ResultCode = types.FAIL_FORBIDDEN
		return &as.AerospikeError{ResultCode: types.FAIL_FORBIDDEN}
	}

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","v":1}`)),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), w.ignoredTotal.Load())
}

func TestWriteBatchCommandLevelDoesNotCountWrittenRecords(t *testing.T) {
	w := newTestWriter(t, baseConfig+"ignore_error_codes: [22]\n")
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		require.Len(t, recs, 2)
		recs[0].BatchRec().ResultCode = types.FAIL_FORBIDDEN
		recs[1].BatchRec().ResultCode = types.OK
		return &as.AerospikeError{ResultCode: types.FAIL_FORBIDDEN}
	}

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"bad","v":1}`)),
		service.NewMessage([]byte(`{"id":"ok","v":1}`)),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), w.ignoredTotal.Load())
}

func TestWriteBatchInDoubtNoResponseIsNotIgnored(t *testing.T) {
	w := newTestWriter(t, baseConfig+"ignore_error_codes: [22]\n")
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		require.Len(t, recs, 2)
		// One node rejected with a listed code. Another node never answered, so
		// that write stays at NO_RESPONSE and in doubt. The command error is
		// only the first node's code.
		recs[0].BatchRec().ResultCode = types.FAIL_FORBIDDEN
		recs[1].BatchRec().ResultCode = types.NO_RESPONSE
		recs[1].BatchRec().InDoubt = true
		return &as.AerospikeError{ResultCode: types.FAIL_FORBIDDEN}
	}

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"listed","v":1}`)),
		service.NewMessage([]byte(`{"id":"unanswered","v":1}`)),
	}
	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)
	assert.Equal(t, int64(1), w.ignoredTotal.Load())
	assert.Contains(t, indexedMessageError(t, indexer, err), "no result")
}

func TestWriteBatchInDoubtIsNotIgnored(t *testing.T) {
	w := newTestWriter(t, baseConfig+"ignore_error_codes: [13]\n")
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		rec := recs[0].BatchRec()
		rec.ResultCode = types.RECORD_TOO_BIG
		rec.InDoubt = true
		return nil
	}

	batch := service.MessageBatch{service.NewMessage([]byte(`{"id":"u1","v":1}`))}
	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)
	assert.Equal(t, int64(0), w.ignoredTotal.Load())
}

func TestConfigRejectsRetryableIgnoreCode(t *testing.T) {
	exe, err := bloblang.Parse(retryableIgnoreLint())
	require.NoError(t, err)
	bad, err := exe.Query(int64(14))
	require.NoError(t, err)
	assert.Contains(t, fmt.Sprint(bad), "must not be listed")
	ok, err := exe.Query(int64(22))
	require.NoError(t, err)
	assert.Empty(t, ok)

	for _, code := range []int{0, 9, 14, 18} {
		conf, err := outputSpec().ParseYAML(baseConfig+fmt.Sprintf("ignore_error_codes: [%d]\n", code), nil)
		require.NoError(t, err)
		_, err = parseOutputConfig(conf)
		require.Error(t, err, "code %d", code)
		assert.Contains(t, err.Error(), "must not be listed")
	}
}

func TestWriteBatchCommandLevelForbiddenStillFailsWhenNotListed(t *testing.T) {
	w := newTestWriter(t, baseConfig)
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		recs[0].BatchRec().ResultCode = types.FAIL_FORBIDDEN
		return &as.AerospikeError{ResultCode: types.FAIL_FORBIDDEN}
	}

	batch := service.MessageBatch{service.NewMessage([]byte(`{"id":"u1","v":1}`))}
	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)
	assert.Contains(t, indexedMessageError(t, indexer, err), "nsup-period")
}

func TestWriteBatchConnectionErrorIgnoresListedCodes(t *testing.T) {
	w := newTestWriter(t, baseConfig+"ignore_error_codes: [13]\n")
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		recs[0].BatchRec().ResultCode = types.RECORD_TOO_BIG
		return &as.AerospikeError{ResultCode: types.NETWORK_ERROR}
	}

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","v":1}`)),
	})
	require.ErrorIs(t, err, service.ErrNotConnected)
}

func indexedMessageError(t *testing.T, indexer *service.Indexer, err error) string {
	t.Helper()
	var berr *service.BatchError
	require.ErrorAs(t, err, &berr)
	var found string
	berr.WalkMessagesIndexedBy(indexer, func(_ int, _ *service.Message, msgErr error) bool {
		if msgErr != nil && found == "" {
			found = msgErr.Error()
		}
		return true
	})
	require.NotEmpty(t, found)
	return found
}

func TestConnectionTestCancelled(t *testing.T) {
	w := newTestWriter(t, baseConfig)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	results := w.ConnectionTest(ctx)
	require.Len(t, results, 1)
	require.Error(t, results[0].Err)
	assert.ErrorIs(t, results[0].Err, context.Canceled)
}
