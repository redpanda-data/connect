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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type mockConsumerAPI struct {
	describe func(ctx context.Context, in *kinesis.DescribeStreamConsumerInput, opts ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error)
	register func(ctx context.Context, in *kinesis.RegisterStreamConsumerInput, opts ...func(*kinesis.Options)) (*kinesis.RegisterStreamConsumerOutput, error)
}

func (m *mockConsumerAPI) DescribeStreamConsumer(ctx context.Context, in *kinesis.DescribeStreamConsumerInput, opts ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error) {
	return m.describe(ctx, in, opts...)
}

func (m *mockConsumerAPI) RegisterStreamConsumer(ctx context.Context, in *kinesis.RegisterStreamConsumerInput, opts ...func(*kinesis.Options)) (*kinesis.RegisterStreamConsumerOutput, error) {
	return m.register(ctx, in, opts...)
}

func fastEFOWaits(t *testing.T) {
	t.Helper()
	oldTimeout, oldInterval := efoConsumerActiveTimeout, efoConsumerPollInterval
	efoConsumerActiveTimeout, efoConsumerPollInterval = time.Second, time.Millisecond
	t.Cleanup(func() {
		efoConsumerActiveTimeout, efoConsumerPollInterval = oldTimeout, oldInterval
	})
}

func TestEnsureEFOConsumerExistingActive(t *testing.T) {
	fastEFOWaits(t)
	api := &mockConsumerAPI{
		describe: func(_ context.Context, in *kinesis.DescribeStreamConsumerInput, _ ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error) {
			assert.Equal(t, "my-app", *in.ConsumerName)
			assert.Equal(t, "stream-arn", *in.StreamARN)
			return &kinesis.DescribeStreamConsumerOutput{
				ConsumerDescription: &types.ConsumerDescription{
					ConsumerARN:    aws.String("consumer-arn"),
					ConsumerStatus: types.ConsumerStatusActive,
				},
			}, nil
		},
		register: func(_ context.Context, _ *kinesis.RegisterStreamConsumerInput, _ ...func(*kinesis.Options)) (*kinesis.RegisterStreamConsumerOutput, error) {
			t.Fatal("register must not be called for an existing consumer")
			return nil, nil
		},
	}

	arn, err := ensureEFOConsumer(t.Context(), api, "stream-arn", "my-app", service.MockResources().Logger())
	require.NoError(t, err)
	assert.Equal(t, "consumer-arn", arn)
}

func TestEnsureEFOConsumerRegistersMissing(t *testing.T) {
	fastEFOWaits(t)
	registered := false
	describes := 0
	api := &mockConsumerAPI{
		describe: func(_ context.Context, _ *kinesis.DescribeStreamConsumerInput, _ ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error) {
			describes++
			if !registered || describes < 3 {
				return nil, &types.ResourceNotFoundException{}
			}
			return &kinesis.DescribeStreamConsumerOutput{
				ConsumerDescription: &types.ConsumerDescription{
					ConsumerARN:    aws.String("new-arn"),
					ConsumerStatus: types.ConsumerStatusActive,
				},
			}, nil
		},
		register: func(_ context.Context, in *kinesis.RegisterStreamConsumerInput, _ ...func(*kinesis.Options)) (*kinesis.RegisterStreamConsumerOutput, error) {
			assert.Equal(t, "my-app", *in.ConsumerName)
			registered = true
			return &kinesis.RegisterStreamConsumerOutput{Consumer: &types.Consumer{
				ConsumerARN:    aws.String("new-arn"),
				ConsumerStatus: types.ConsumerStatusCreating,
			}}, nil
		},
	}

	arn, err := ensureEFOConsumer(t.Context(), api, "stream-arn", "my-app", service.MockResources().Logger())
	require.NoError(t, err)
	assert.Equal(t, "new-arn", arn)
	assert.True(t, registered)
}

func TestEnsureEFOConsumerWaitsForCreating(t *testing.T) {
	fastEFOWaits(t)
	describes := 0
	api := &mockConsumerAPI{
		describe: func(_ context.Context, _ *kinesis.DescribeStreamConsumerInput, _ ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error) {
			describes++
			status := types.ConsumerStatusCreating
			if describes >= 3 {
				status = types.ConsumerStatusActive
			}
			return &kinesis.DescribeStreamConsumerOutput{
				ConsumerDescription: &types.ConsumerDescription{
					ConsumerARN:    aws.String("consumer-arn"),
					ConsumerStatus: status,
				},
			}, nil
		},
	}

	arn, err := ensureEFOConsumer(t.Context(), api, "stream-arn", "my-app", service.MockResources().Logger())
	require.NoError(t, err)
	assert.Equal(t, "consumer-arn", arn)
	assert.GreaterOrEqual(t, describes, 3)
}

func TestEnsureEFOConsumerPermissionError(t *testing.T) {
	fastEFOWaits(t)
	api := &mockConsumerAPI{
		describe: func(_ context.Context, _ *kinesis.DescribeStreamConsumerInput, _ ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error) {
			return nil, errors.New("AccessDeniedException")
		},
	}

	_, err := ensureEFOConsumer(t.Context(), api, "stream-arn", "my-app", service.MockResources().Logger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kinesis:DescribeStreamConsumer")
}

func TestEnsureEFOConsumerConcurrentRegistration(t *testing.T) {
	fastEFOWaits(t)
	describes := 0
	api := &mockConsumerAPI{
		describe: func(_ context.Context, _ *kinesis.DescribeStreamConsumerInput, _ ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error) {
			describes++
			if describes == 1 {
				return nil, &types.ResourceNotFoundException{}
			}
			return &kinesis.DescribeStreamConsumerOutput{
				ConsumerDescription: &types.ConsumerDescription{
					ConsumerARN:    aws.String("consumer-arn"),
					ConsumerStatus: types.ConsumerStatusActive,
				},
			}, nil
		},
		register: func(_ context.Context, _ *kinesis.RegisterStreamConsumerInput, _ ...func(*kinesis.Options)) (*kinesis.RegisterStreamConsumerOutput, error) {
			// Another instance won the race.
			return nil, &types.ResourceInUseException{}
		},
	}

	arn, err := ensureEFOConsumer(t.Context(), api, "stream-arn", "my-app", service.MockResources().Logger())
	require.NoError(t, err)
	assert.Equal(t, "consumer-arn", arn)
}

func TestEnsureEFOConsumerDeletingFails(t *testing.T) {
	fastEFOWaits(t)
	api := &mockConsumerAPI{
		describe: func(_ context.Context, _ *kinesis.DescribeStreamConsumerInput, _ ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error) {
			return &kinesis.DescribeStreamConsumerOutput{
				ConsumerDescription: &types.ConsumerDescription{
					ConsumerARN:    aws.String("consumer-arn"),
					ConsumerStatus: types.ConsumerStatusDeleting,
				},
			}, nil
		},
	}

	_, err := ensureEFOConsumer(t.Context(), api, "stream-arn", "my-app", service.MockResources().Logger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deleted")
}

func TestEnsureEFOConsumerNilDescription(t *testing.T) {
	fastEFOWaits(t)
	attempts := 0
	api := &mockConsumerAPI{
		describe: func(_ context.Context, _ *kinesis.DescribeStreamConsumerInput, _ ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error) {
			attempts++
			if attempts < 2 {
				// First attempt: nil ConsumerDescription despite nil error (quirky API response)
				return &kinesis.DescribeStreamConsumerOutput{
					ConsumerDescription: nil,
				}, nil
			}
			// Subsequent attempts: normal response with ACTIVE consumer
			return &kinesis.DescribeStreamConsumerOutput{
				ConsumerDescription: &types.ConsumerDescription{
					ConsumerARN:    aws.String("consumer-arn"),
					ConsumerStatus: types.ConsumerStatusActive,
				},
			}, nil
		},
	}

	arn, err := ensureEFOConsumer(t.Context(), api, "stream-arn", "my-app", service.MockResources().Logger())
	require.NoError(t, err)
	assert.Equal(t, "consumer-arn", arn)
	assert.GreaterOrEqual(t, attempts, 2)
}

func TestEnsureEFOConsumerActiveWithNilARN(t *testing.T) {
	fastEFOWaits(t)
	api := &mockConsumerAPI{
		describe: func(_ context.Context, _ *kinesis.DescribeStreamConsumerInput, _ ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error) {
			return &kinesis.DescribeStreamConsumerOutput{
				ConsumerDescription: &types.ConsumerDescription{
					ConsumerARN:    nil, // quirky API response: ACTIVE but no ARN
					ConsumerStatus: types.ConsumerStatusActive,
				},
			}, nil
		},
	}

	_, err := ensureEFOConsumer(t.Context(), api, "stream-arn", "my-app", service.MockResources().Logger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil ARN")
}

type fakeSubscription struct {
	events chan types.SubscribeToShardEventStream
	err    error
	closed chan struct{}
}

func newFakeSubscription() *fakeSubscription {
	return &fakeSubscription{
		events: make(chan types.SubscribeToShardEventStream, 16),
		closed: make(chan struct{}),
	}
}

func (f *fakeSubscription) Events() <-chan types.SubscribeToShardEventStream { return f.events }

func (f *fakeSubscription) Close() error {
	select {
	case <-f.closed:
	default:
		close(f.closed)
	}
	return nil
}

func (f *fakeSubscription) Err() error { return f.err }

func (f *fakeSubscription) send(continuation string, recs ...types.Record) {
	f.events <- &types.SubscribeToShardEventStreamMemberSubscribeToShardEvent{
		Value: types.SubscribeToShardEvent{
			ContinuationSequenceNumber: aws.String(continuation),
			MillisBehindLatest:         aws.Int64(0),
			Records:                    recs,
		},
	}
}

// sendFinal emits the closed-shard terminator (nil continuation).
func (f *fakeSubscription) sendFinal(recs ...types.Record) {
	f.events <- &types.SubscribeToShardEventStreamMemberSubscribeToShardEvent{
		Value: types.SubscribeToShardEvent{
			MillisBehindLatest: aws.Int64(0),
			Records:            recs,
		},
	}
}

func rec(seq string) types.Record {
	return types.Record{SequenceNumber: aws.String(seq), Data: []byte(seq)}
}

// fastEFOResubscribe shrinks the resubscribe floor (and therefore the
// backoff's initial interval) so tests that exercise resubscription don't
// have to wait out the real one-second-per-shard-per-consumer API limit.
func fastEFOResubscribe(t *testing.T) {
	t.Helper()
	old := efoResubscribeFloor
	efoResubscribeFloor = time.Millisecond
	t.Cleanup(func() {
		efoResubscribeFloor = old
	})
}

func TestEFOSourceForwardsRecords(t *testing.T) {
	sub := newFakeSubscription()
	src, err := newEFORecordSource(t.Context(), func(_ context.Context, pos types.StartingPosition) (efoSubscription, error) {
		assert.Equal(t, types.ShardIteratorTypeTrimHorizon, pos.Type)
		return sub, nil
	}, "shard-0", "", true, 100*time.Millisecond, service.MockResources().Logger())
	require.NoError(t, err)
	defer src.Close()

	sub.send("c1", rec("1"), rec("2"))

	recs, done, err := src.Fetch(t.Context())
	require.NoError(t, err)
	assert.False(t, done)
	require.Len(t, recs, 2)
	assert.True(t, src.Blocking())
}

func TestEFOSourceFetchTimesOutEmpty(t *testing.T) {
	sub := newFakeSubscription()
	src, err := newEFORecordSource(t.Context(), func(_ context.Context, _ types.StartingPosition) (efoSubscription, error) {
		return sub, nil
	}, "shard-0", "", true, 20*time.Millisecond, service.MockResources().Logger())
	require.NoError(t, err)
	defer src.Close()

	recs, done, err := src.Fetch(t.Context())
	require.NoError(t, err)
	assert.False(t, done)
	assert.Empty(t, recs)
}

func TestEFOSourceResubscribesAtContinuation(t *testing.T) {
	fastEFOResubscribe(t)

	var mu sync.Mutex
	var positions []types.StartingPosition
	subs := make(chan *fakeSubscription, 2)
	first, second := newFakeSubscription(), newFakeSubscription()
	subs <- first
	subs <- second

	src, err := newEFORecordSource(t.Context(), func(_ context.Context, pos types.StartingPosition) (efoSubscription, error) {
		mu.Lock()
		positions = append(positions, pos)
		mu.Unlock()
		return <-subs, nil
	}, "shard-0", "start-seq", true, 100*time.Millisecond, service.MockResources().Logger())
	require.NoError(t, err)
	defer src.Close()

	first.send("cont-1", rec("1"))
	close(first.events) // AWS ends the ~5 minute subscription

	second.send("cont-2", rec("2"))

	var got []string
	for len(got) < 2 {
		recs, done, err := src.Fetch(t.Context())
		require.NoError(t, err)
		require.False(t, done)
		for _, r := range recs {
			got = append(got, *r.SequenceNumber)
		}
	}
	assert.Equal(t, []string{"1", "2"}, got)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, positions, 2)
	assert.Equal(t, types.ShardIteratorTypeAfterSequenceNumber, positions[0].Type)
	assert.Equal(t, "start-seq", *positions[0].SequenceNumber)
	assert.Equal(t, types.ShardIteratorTypeAtSequenceNumber, positions[1].Type)
	assert.Equal(t, "cont-1", *positions[1].SequenceNumber)
}

func TestEFOSourceStreamErrorResubscribes(t *testing.T) {
	fastEFOResubscribe(t)

	var mu sync.Mutex
	var positions []types.StartingPosition
	subs := make(chan *fakeSubscription, 2)
	first, second := newFakeSubscription(), newFakeSubscription()
	first.err = errors.New("stream error")
	subs <- first
	subs <- second

	src, err := newEFORecordSource(t.Context(), func(_ context.Context, pos types.StartingPosition) (efoSubscription, error) {
		mu.Lock()
		positions = append(positions, pos)
		mu.Unlock()
		return <-subs, nil
	}, "shard-0", "", true, 100*time.Millisecond, service.MockResources().Logger())
	require.NoError(t, err)
	defer src.Close()

	first.send("cont-1", rec("1"))
	close(first.events) // stream ends with a non-nil Err()

	second.send("cont-2", rec("2"))

	var got []string
	for len(got) < 2 {
		recs, done, err := src.Fetch(t.Context())
		require.NoError(t, err)
		require.False(t, done)
		for _, r := range recs {
			got = append(got, *r.SequenceNumber)
		}
	}
	assert.Equal(t, []string{"1", "2"}, got)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, positions, 2)
	assert.Equal(t, types.ShardIteratorTypeAtSequenceNumber, positions[1].Type)
	assert.Equal(t, "cont-1", *positions[1].SequenceNumber)
}

func TestEFOSourceAnchorsTimestampWhenNotOldest(t *testing.T) {
	sub := newFakeSubscription()
	before := time.Now()
	src, err := newEFORecordSource(t.Context(), func(_ context.Context, pos types.StartingPosition) (efoSubscription, error) {
		assert.Equal(t, types.ShardIteratorTypeAtTimestamp, pos.Type)
		if assert.NotNil(t, pos.Timestamp) {
			assert.False(t, pos.Timestamp.Before(before))
		}
		return sub, nil
	}, "shard-0", "", false, 100*time.Millisecond, service.MockResources().Logger())
	require.NoError(t, err)
	defer src.Close()
}

func TestEFOSourceShardClosed(t *testing.T) {
	sub := newFakeSubscription()
	src, err := newEFORecordSource(t.Context(), func(_ context.Context, _ types.StartingPosition) (efoSubscription, error) {
		return sub, nil
	}, "shard-0", "", true, 100*time.Millisecond, service.MockResources().Logger())
	require.NoError(t, err)
	defer src.Close()

	sub.sendFinal(rec("1"))

	recs, done, err := src.Fetch(t.Context())
	require.NoError(t, err)
	assert.False(t, done)
	require.Len(t, recs, 1)

	// The next fetch reports the closed shard.
	deadline := time.Now().Add(2 * time.Second)
	for {
		recs, done, err = src.Fetch(t.Context())
		require.NoError(t, err)
		assert.Empty(t, recs)
		if done || time.Now().After(deadline) {
			break
		}
	}
	assert.True(t, done)
}

func TestEFOSourceInitialSubscribeErrorFailsFast(t *testing.T) {
	_, err := newEFORecordSource(t.Context(), func(_ context.Context, _ types.StartingPosition) (efoSubscription, error) {
		return nil, errors.New("AccessDeniedException")
	}, "shard-0", "", true, 100*time.Millisecond, service.MockResources().Logger())
	require.Error(t, err)
}

func TestEFOSourceInitialSubscribeNonResourceInUseFailsFastWithOneCall(t *testing.T) {
	fastEFOResubscribe(t)

	calls := 0
	_, err := newEFORecordSource(t.Context(), func(_ context.Context, _ types.StartingPosition) (efoSubscription, error) {
		calls++
		return nil, errors.New("AccessDenied")
	}, "shard-0", "", true, 100*time.Millisecond, service.MockResources().Logger())
	require.Error(t, err)
	assert.Equal(t, 1, calls)
}

func TestEFOSourceInvalidSequenceFallsBackToConfiguredStart(t *testing.T) {
	sub := newFakeSubscription()
	var positions []types.StartingPosition
	calls := 0
	src, err := newEFORecordSource(t.Context(), func(_ context.Context, pos types.StartingPosition) (efoSubscription, error) {
		positions = append(positions, pos)
		calls++
		if calls == 1 {
			return nil, &types.InvalidArgumentException{}
		}
		return sub, nil
	}, "shard-0", "stale-seq", true, 100*time.Millisecond, service.MockResources().Logger())
	require.NoError(t, err)
	defer src.Close()

	require.Len(t, positions, 2)
	assert.Equal(t, types.ShardIteratorTypeAfterSequenceNumber, positions[0].Type)
	require.NotNil(t, positions[0].SequenceNumber)
	assert.Equal(t, "stale-seq", *positions[0].SequenceNumber)
	assert.Equal(t, types.ShardIteratorTypeTrimHorizon, positions[1].Type)
}

func TestEFOSourceInvalidSequenceFallsBackToTrimHorizonWhenNotOldest(t *testing.T) {
	sub := newFakeSubscription()
	var positions []types.StartingPosition
	calls := 0
	src, err := newEFORecordSource(t.Context(), func(_ context.Context, pos types.StartingPosition) (efoSubscription, error) {
		positions = append(positions, pos)
		calls++
		if calls == 1 {
			return nil, &types.InvalidArgumentException{}
		}
		return sub, nil
	}, "shard-0", "stale-seq", false, 100*time.Millisecond, service.MockResources().Logger())
	require.NoError(t, err)
	defer src.Close()

	require.Len(t, positions, 2)
	assert.Equal(t, types.ShardIteratorTypeAfterSequenceNumber, positions[0].Type)
	// This shard demonstrably had a committed position, so resuming at "now"
	// would silently skip every record still retained ahead of the expired
	// sequence: the fallback must be TRIM_HORIZON regardless of
	// start_from_oldest.
	assert.Equal(t, types.ShardIteratorTypeTrimHorizon, positions[1].Type)
	assert.Nil(t, positions[1].Timestamp)
}

func TestEFOSourcePumpFallsBackOnInvalidSequence(t *testing.T) {
	fastEFOResubscribe(t)

	var mu sync.Mutex
	var positions []types.StartingPosition
	sub := newFakeSubscription()

	src, err := newEFORecordSource(t.Context(), func(_ context.Context, pos types.StartingPosition) (efoSubscription, error) {
		mu.Lock()
		positions = append(positions, pos)
		n := len(positions)
		mu.Unlock()
		switch n {
		case 1:
			// The shard's previous owner still holds the one allowed
			// subscription, so the pump takes over the retry loop.
			return nil, &types.ResourceInUseException{}
		case 2:
			// The stored sequence has aged out of the retention window.
			return nil, &types.InvalidArgumentException{}
		}
		return sub, nil
	}, "shard-0", "stale-seq", false, 20*time.Millisecond, service.MockResources().Logger())
	require.NoError(t, err)
	defer src.Close()

	sub.send("c1", rec("1"))

	var got int
	deadline := time.Now().Add(5 * time.Second)
	for got == 0 && time.Now().Before(deadline) {
		recs, _, err := src.Fetch(t.Context())
		require.NoError(t, err)
		got = len(recs)
	}
	require.Equal(t, 1, got)

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(positions), 3)
	assert.Equal(t, types.ShardIteratorTypeAfterSequenceNumber, positions[0].Type)
	assert.Equal(t, types.ShardIteratorTypeAfterSequenceNumber, positions[1].Type)
	// Without the fallback the pump would retry the rejected sequence forever.
	assert.Equal(t, types.ShardIteratorTypeTrimHorizon, positions[2].Type)
}

func TestEFOSourceOtherErrorWithSequenceDoesNotFallBack(t *testing.T) {
	calls := 0
	_, err := newEFORecordSource(t.Context(), func(_ context.Context, _ types.StartingPosition) (efoSubscription, error) {
		calls++
		return nil, errors.New("AccessDeniedException")
	}, "shard-0", "stale-seq", true, 100*time.Millisecond, service.MockResources().Logger())
	require.Error(t, err)
	assert.Equal(t, 1, calls)
}

func TestEFOSourceInitialSubscribeResourceInUseDoesNotBlock(t *testing.T) {
	fastEFOResubscribe(t)

	const failCount = 3
	var calls atomic.Int32
	sub := newFakeSubscription()

	start := time.Now()
	src, err := newEFORecordSource(t.Context(), func(_ context.Context, _ types.StartingPosition) (efoSubscription, error) {
		n := calls.Add(1)
		if n <= failCount {
			return nil, &types.ResourceInUseException{}
		}
		return sub, nil
	}, "shard-0", "", true, 100*time.Millisecond, service.MockResources().Logger())
	require.NoError(t, err)
	defer src.Close()

	// The constructor must not block synchronously retrying/waiting out the
	// ResourceInUseException; it should return as soon as the pump goroutine
	// is started.
	assert.Less(t, time.Since(start), time.Second)

	// Once the subscribe fn stops failing, the background pump picks it up
	// and records flow normally.
	sub.send("c1", rec("1"))

	recs, done, err := src.Fetch(t.Context())
	require.NoError(t, err)
	assert.False(t, done)
	require.Len(t, recs, 1)

	assert.GreaterOrEqual(t, calls.Load(), int32(failCount+1))
}

func TestEFOSourceCloseStopsPump(t *testing.T) {
	sub := newFakeSubscription()
	src, err := newEFORecordSource(t.Context(), func(_ context.Context, _ types.StartingPosition) (efoSubscription, error) {
		return sub, nil
	}, "shard-0", "", true, 100*time.Millisecond, service.MockResources().Logger())
	require.NoError(t, err)

	src.Close() // must not hang
	select {
	case <-sub.closed:
	default:
		t.Fatal("expected the subscription to be closed")
	}
}
