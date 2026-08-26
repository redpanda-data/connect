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

package kinesis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type mockPollAPI struct {
	getShardIterator func(ctx context.Context, in *kinesis.GetShardIteratorInput, opts ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error)
	getRecords       func(ctx context.Context, in *kinesis.GetRecordsInput, opts ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error)
}

func (m *mockPollAPI) GetShardIterator(ctx context.Context, in *kinesis.GetShardIteratorInput, opts ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
	return m.getShardIterator(ctx, in, opts...)
}

func (m *mockPollAPI) GetRecords(ctx context.Context, in *kinesis.GetRecordsInput, opts ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
	return m.getRecords(ctx, in, opts...)
}

func staticIterAPI(iter string) *mockPollAPI {
	return &mockPollAPI{
		getShardIterator: func(_ context.Context, _ *kinesis.GetShardIteratorInput, _ ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{ShardIterator: aws.String(iter)}, nil
		},
	}
}

func TestPollingSourceFetchAdvancesIterator(t *testing.T) {
	api := staticIterAPI("iter-1")
	var gotIters []string
	api.getRecords = func(_ context.Context, in *kinesis.GetRecordsInput, _ ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
		gotIters = append(gotIters, *in.ShardIterator)
		return &kinesis.GetRecordsOutput{
			Records:           []types.Record{{SequenceNumber: aws.String("1")}},
			NextShardIterator: aws.String("iter-2"),
		}, nil
	}

	src, err := newPollingRecordSource(t.Context(), api, "arn", "shard-0", "", true, 0, time.Second, func() string { return "" }, service.MockResources().Logger())
	require.NoError(t, err)

	recs, done, err := src.Fetch(t.Context())
	require.NoError(t, err)
	assert.False(t, done)
	require.Len(t, recs, 1)

	_, _, err = src.Fetch(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []string{"iter-1", "iter-2"}, gotIters)
	assert.False(t, src.Blocking())
}

func TestPollingSourceEndOfShard(t *testing.T) {
	api := staticIterAPI("iter-1")
	api.getRecords = func(_ context.Context, _ *kinesis.GetRecordsInput, _ ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
		return &kinesis.GetRecordsOutput{
			Records:           []types.Record{{SequenceNumber: aws.String("1")}},
			NextShardIterator: nil, // closed shard
		}, nil
	}

	src, err := newPollingRecordSource(t.Context(), api, "arn", "shard-0", "", true, 0, time.Second, func() string { return "" }, service.MockResources().Logger())
	require.NoError(t, err)

	recs, done, err := src.Fetch(t.Context())
	require.NoError(t, err)
	assert.True(t, done)
	assert.Len(t, recs, 1)
}

func TestPollingSourceErrorKeepsIterator(t *testing.T) {
	api := staticIterAPI("iter-1")
	calls := 0
	api.getRecords = func(_ context.Context, in *kinesis.GetRecordsInput, _ ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("boom")
		}
		assert.Equal(t, "iter-1", *in.ShardIterator)
		return &kinesis.GetRecordsOutput{NextShardIterator: aws.String("iter-2")}, nil
	}

	src, err := newPollingRecordSource(t.Context(), api, "arn", "shard-0", "", true, 0, time.Second, func() string { return "" }, service.MockResources().Logger())
	require.NoError(t, err)

	_, done, err := src.Fetch(t.Context())
	require.Error(t, err)
	assert.False(t, done)

	_, _, err = src.Fetch(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
}

func TestPollingSourceExpiredIteratorRefreshes(t *testing.T) {
	var iterRequests []*kinesis.GetShardIteratorInput
	api := &mockPollAPI{}
	api.getShardIterator = func(_ context.Context, in *kinesis.GetShardIteratorInput, _ ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
		iterRequests = append(iterRequests, in)
		return &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("fresh-iter")}, nil
	}
	calls := 0
	api.getRecords = func(_ context.Context, in *kinesis.GetRecordsInput, _ ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
		calls++
		if calls == 1 {
			return nil, &types.ExpiredIteratorException{}
		}
		assert.Equal(t, "fresh-iter", *in.ShardIterator)
		return &kinesis.GetRecordsOutput{NextShardIterator: aws.String("iter-2")}, nil
	}

	src, err := newPollingRecordSource(t.Context(), api, "arn", "shard-0", "", true, 0, time.Second, func() string { return "acked-seq" }, service.MockResources().Logger())
	require.NoError(t, err)

	// Expired iterator: no records, no error, iterator refreshed internally.
	recs, done, err := src.Fetch(t.Context())
	require.NoError(t, err)
	assert.False(t, done)
	assert.Empty(t, recs)

	_, _, err = src.Fetch(t.Context())
	require.NoError(t, err)

	// First request is the constructor's, second is the refresh which must
	// resume after the latest acked sequence.
	require.Len(t, iterRequests, 2)
	assert.Equal(t, types.ShardIteratorTypeAfterSequenceNumber, iterRequests[1].ShardIteratorType)
	assert.Equal(t, "acked-seq", *iterRequests[1].StartingSequenceNumber)
}

func TestPollingSourceIterFallbackToTrimHorizon(t *testing.T) {
	var iterTypes []types.ShardIteratorType
	api := &mockPollAPI{}
	api.getShardIterator = func(_ context.Context, in *kinesis.GetShardIteratorInput, _ ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
		iterTypes = append(iterTypes, in.ShardIteratorType)
		if in.ShardIteratorType == types.ShardIteratorTypeAfterSequenceNumber {
			return &kinesis.GetShardIteratorOutput{}, nil // empty iterator triggers fallback
		}
		return &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("iter-1")}, nil
	}

	_, err := newPollingRecordSource(t.Context(), api, "arn", "shard-0", "old-seq", true, 0, time.Second, func() string { return "" }, service.MockResources().Logger())
	require.NoError(t, err)
	assert.Equal(t, []types.ShardIteratorType{
		types.ShardIteratorTypeAfterSequenceNumber,
		types.ShardIteratorTypeTrimHorizon,
	}, iterTypes)
}

func TestPollingSourceIterFallbackOnInvalidSequence(t *testing.T) {
	var iterTypes []types.ShardIteratorType
	api := &mockPollAPI{}
	api.getShardIterator = func(_ context.Context, in *kinesis.GetShardIteratorInput, _ ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
		iterTypes = append(iterTypes, in.ShardIteratorType)
		if in.ShardIteratorType == types.ShardIteratorTypeAfterSequenceNumber {
			// A sequence aged out of the retention window is rejected outright.
			return nil, &types.InvalidArgumentException{}
		}
		return &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("iter-1")}, nil
	}

	src, err := newPollingRecordSource(t.Context(), api, "arn", "shard-0", "old-seq", true, 0, time.Second, func() string { return "" }, service.MockResources().Logger())
	require.NoError(t, err)
	assert.Equal(t, "iter-1", src.iter)
	assert.Equal(t, []types.ShardIteratorType{
		types.ShardIteratorTypeAfterSequenceNumber,
		types.ShardIteratorTypeTrimHorizon,
	}, iterTypes)
}

func TestPollingSourceIterOtherErrorWithSequenceFails(t *testing.T) {
	calls := 0
	api := &mockPollAPI{}
	api.getShardIterator = func(_ context.Context, _ *kinesis.GetShardIteratorInput, _ ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
		calls++
		return nil, errors.New("AccessDeniedException")
	}

	_, err := newPollingRecordSource(t.Context(), api, "arn", "shard-0", "old-seq", true, 0, time.Second, func() string { return "" }, service.MockResources().Logger())
	require.Error(t, err)
	assert.Equal(t, 1, calls)
}

func TestPollingSourceIterInvalidArgumentWithoutSequenceFails(t *testing.T) {
	calls := 0
	api := &mockPollAPI{}
	api.getShardIterator = func(_ context.Context, _ *kinesis.GetShardIteratorInput, _ ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
		calls++
		return nil, &types.InvalidArgumentException{}
	}

	// Without a stored sequence there is nothing to fall back from, so the
	// error must surface rather than triggering a second request.
	_, err := newPollingRecordSource(t.Context(), api, "arn", "shard-0", "", true, 0, time.Second, func() string { return "" }, service.MockResources().Logger())
	require.Error(t, err)
	assert.Equal(t, 1, calls)
}

func TestPollingSourcePollPeriodGate(t *testing.T) {
	api := staticIterAPI("iter-1")
	var callTimes []time.Time
	api.getRecords = func(_ context.Context, _ *kinesis.GetRecordsInput, _ ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
		callTimes = append(callTimes, time.Now())
		return &kinesis.GetRecordsOutput{NextShardIterator: aws.String("iter-2")}, nil
	}

	const period = 50 * time.Millisecond
	src, err := newPollingRecordSource(t.Context(), api, "arn", "shard-0", "", true, period, time.Second, func() string { return "" }, service.MockResources().Logger())
	require.NoError(t, err)

	for range 3 {
		_, _, err = src.Fetch(t.Context())
		require.NoError(t, err)
	}
	require.Len(t, callTimes, 3)
	for i := 1; i < len(callTimes); i++ {
		assert.GreaterOrEqual(t, callTimes[i].Sub(callTimes[i-1]), period)
	}
}

func TestPollingSourcePollPeriodGateCapsFetchWait(t *testing.T) {
	api := staticIterAPI("iter-1")
	var callTimes []time.Time
	api.getRecords = func(_ context.Context, _ *kinesis.GetRecordsInput, _ ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
		callTimes = append(callTimes, time.Now())
		return &kinesis.GetRecordsOutput{NextShardIterator: aws.String("iter-2")}, nil
	}

	const (
		period      = 200 * time.Millisecond
		maxGateWait = 10 * time.Millisecond
	)
	src, err := newPollingRecordSource(t.Context(), api, "arn", "shard-0", "", true, period, maxGateWait, func() string { return "" }, service.MockResources().Logger())
	require.NoError(t, err)

	_, _, err = src.Fetch(t.Context())
	require.NoError(t, err)
	require.Len(t, callTimes, 1)

	// A fetch immediately after a successful one must hand control back within
	// roughly maxGateWait, without polling, rather than sleeping out the
	// remaining poll period. The capped wait is signalled via the
	// errPollGateWaiting sentinel rather than a nil error, so the caller can
	// tell this apart from a genuinely empty shard and skip arming its
	// failure backoff.
	start := time.Now()
	recs, done, err := src.Fetch(t.Context())
	require.ErrorIs(t, err, errPollGateWaiting)
	assert.False(t, done)
	assert.Empty(t, recs)
	assert.Len(t, callTimes, 1)
	assert.Less(t, time.Since(start), period/2)

	// Repeated fetches keep yielding the sentinel until the full period has
	// elapsed since the last GetRecords call.
	deadline := time.Now().Add(5 * time.Second)
	for len(callTimes) < 2 && time.Now().Before(deadline) {
		_, _, err = src.Fetch(t.Context())
		if err != nil {
			require.ErrorIs(t, err, errPollGateWaiting)
		}
	}
	require.Len(t, callTimes, 2)
	assert.GreaterOrEqual(t, callTimes[1].Sub(callTimes[0]), period)
}

// TestPollingSourcePollPeriodGateSpacingUnderCap simulates the consumer
// loop's retry-immediately-on-sentinel behaviour (no backoff armed) by
// calling Fetch in a tight loop, and checks that GetRecords calls still land
// at roughly pollPeriod spacing rather than pollPeriod plus a failure
// backoff, even though almost every Fetch is capped well below the period.
func TestPollingSourcePollPeriodGateSpacingUnderCap(t *testing.T) {
	api := staticIterAPI("iter-1")
	var callTimes []time.Time
	api.getRecords = func(_ context.Context, _ *kinesis.GetRecordsInput, _ ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
		callTimes = append(callTimes, time.Now())
		return &kinesis.GetRecordsOutput{NextShardIterator: aws.String("iter-2")}, nil
	}

	const (
		period      = 100 * time.Millisecond
		maxGateWait = 20 * time.Millisecond
	)
	src, err := newPollingRecordSource(t.Context(), api, "arn", "shard-0", "", true, period, maxGateWait, func() string { return "" }, service.MockResources().Logger())
	require.NoError(t, err)

	const wantCalls = 4
	deadline := time.Now().Add(5 * time.Second)
	for len(callTimes) < wantCalls && time.Now().Before(deadline) {
		_, _, err = src.Fetch(t.Context())
		if err != nil {
			require.ErrorIs(t, err, errPollGateWaiting)
		}
	}
	require.GreaterOrEqual(t, len(callTimes), wantCalls)

	for i := 1; i < len(callTimes); i++ {
		gap := callTimes[i].Sub(callTimes[i-1])
		assert.GreaterOrEqual(t, gap, period)
		assert.Less(t, gap, time.Duration(float64(period)*1.5))
	}
}

func TestPollingSourcePollPeriodZeroNoDelay(t *testing.T) {
	api := staticIterAPI("iter-1")
	api.getRecords = func(_ context.Context, _ *kinesis.GetRecordsInput, _ ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
		return &kinesis.GetRecordsOutput{NextShardIterator: aws.String("iter-2")}, nil
	}

	src, err := newPollingRecordSource(t.Context(), api, "arn", "shard-0", "", true, 0, time.Second, func() string { return "" }, service.MockResources().Logger())
	require.NoError(t, err)

	start := time.Now()
	for range 10 {
		_, _, err = src.Fetch(t.Context())
		require.NoError(t, err)
	}
	assert.Less(t, time.Since(start), 20*time.Millisecond)
}
