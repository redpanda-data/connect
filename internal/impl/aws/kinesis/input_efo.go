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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/cenkalti/backoff/v4"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Overridable in tests.
var (
	// efoConsumerPollInterval is the internal poll cadence used while waiting
	// for a registered consumer to become active. It stays internal (rather
	// than a config field) because it is bounded by the user-configurable
	// consumer_activation_timeout: it only affects how promptly activation is
	// noticed within that window, not the overall time budget, so exposing it
	// for independent tuning would add a knob with no real effect on outcomes.
	efoConsumerPollInterval = time.Second
	// efoResubscribeFloor is the minimum spacing enforced between successive
	// SubscribeToShard calls for a given shard/consumer, matching the API's
	// one-call-per-second limit. It also seeds the resubscribe backoff's
	// initial interval.
	efoResubscribeFloor = time.Second
)

// efoConsumerAPI is the subset of the Kinesis API used to resolve and
// register enhanced fan-out stream consumers.
type efoConsumerAPI interface {
	DescribeStreamConsumer(ctx context.Context, params *kinesis.DescribeStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error)
	RegisterStreamConsumer(ctx context.Context, params *kinesis.RegisterStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.RegisterStreamConsumerOutput, error)
}

// ensureEFOConsumer returns the ARN of the named enhanced fan-out consumer on
// the given stream, registering it if it does not exist and waiting for it to
// become ACTIVE. The consumer is intentionally never deregistered: multiple
// instances of the same pipeline share it as a single logical application.
func ensureEFOConsumer(ctx context.Context, api efoConsumerAPI, streamARN, name string, activationTimeout time.Duration, log *service.Logger) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, activationTimeout)
	defer cancel()

	registered := false
	for {
		res, err := api.DescribeStreamConsumer(ctx, &kinesis.DescribeStreamConsumerInput{
			StreamARN:    &streamARN,
			ConsumerName: &name,
		})
		if err != nil {
			var nf *types.ResourceNotFoundException
			if !errors.As(err, &nf) {
				return "", fmt.Errorf("describing enhanced fan-out consumer '%v' on stream '%v' (requires kinesis:DescribeStreamConsumer): %w", name, streamARN, err)
			}
			if !registered {
				if _, err := api.RegisterStreamConsumer(ctx, &kinesis.RegisterStreamConsumerInput{
					StreamARN:    &streamARN,
					ConsumerName: &name,
				}); err != nil {
					var inUse *types.ResourceInUseException
					if !errors.As(err, &inUse) {
						return "", fmt.Errorf("registering enhanced fan-out consumer '%v' on stream '%v' (requires kinesis:RegisterStreamConsumer): %w", name, streamARN, err)
					}
					// Another instance registered it concurrently; poll for it.
				} else {
					log.Infof("Registered Kinesis enhanced fan-out consumer '%v' on stream '%v'", name, streamARN)
				}
				registered = true
			}
		} else {
			// Guard against quirky API responses with nil ConsumerDescription.
			if res.ConsumerDescription == nil {
				// Not ready yet; poll until it is.
			} else {
				switch res.ConsumerDescription.ConsumerStatus {
				case types.ConsumerStatusActive:
					if res.ConsumerDescription.ConsumerARN == nil {
						return "", fmt.Errorf("enhanced fan-out consumer '%v' on stream '%v' returned ACTIVE status but nil ARN", name, streamARN)
					}
					return *res.ConsumerDescription.ConsumerARN, nil
				case types.ConsumerStatusDeleting:
					return "", fmt.Errorf("enhanced fan-out consumer '%v' on stream '%v' is currently being deleted, wait for the deletion to complete", name, streamARN)
				}
				// CREATING: poll until ACTIVE.
			}
		}

		select {
		case <-time.After(efoConsumerPollInterval):
		case <-ctx.Done():
			return "", fmt.Errorf("waiting for enhanced fan-out consumer '%v' on stream '%v' to become active: %w", name, streamARN, ctx.Err())
		}
	}
}

// efoSubscription is the consumable side of a SubscribeToShard event stream,
// satisfied by *kinesis.SubscribeToShardEventStream.
type efoSubscription interface {
	Events() <-chan types.SubscribeToShardEventStream
	Close() error
	Err() error
}

// efoSubscribeFn opens a shard subscription at the given position.
type efoSubscribeFn func(ctx context.Context, pos types.StartingPosition) (efoSubscription, error)

// kinesisEFOSubscribeFn builds an efoSubscribeFn backed by the real Kinesis
// SubscribeToShard API for a registered consumer.
func kinesisEFOSubscribeFn(svc *kinesis.Client, consumerARN, shardID string) efoSubscribeFn {
	return func(ctx context.Context, pos types.StartingPosition) (efoSubscription, error) {
		out, err := svc.SubscribeToShard(ctx, &kinesis.SubscribeToShardInput{
			ConsumerARN:      &consumerARN,
			ShardId:          &shardID,
			StartingPosition: &pos,
		})
		if err != nil {
			return nil, err
		}
		return out.GetStream(), nil
	}
}

// efoRecordSource consumes a shard via enhanced fan-out push delivery. A
// background goroutine owns the subscription, forwarding record batches into
// a buffered channel and resubscribing at the continuation sequence whenever
// AWS terminates the subscription (roughly every five minutes). Backpressure
// from an unread channel simply pauses event consumption; flow control of
// in-flight messages remains governed by checkpoint_limit.
type efoRecordSource struct {
	subscribe              efoSubscribeFn
	shardID                string
	fetchTimeout           time.Duration
	maxResubscribeInterval time.Duration
	log                    *service.Logger

	recordsChan chan []types.Record
	finished    atomic.Bool

	ctx    context.Context //nolint:containedctx // lifecycle context for the pump goroutine
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// efoStartingPosition computes the StartingPosition for a shard given its
// checkpointed sequence (if any) and the start_from_oldest setting.
//
// When there is no checkpoint and startFromOldest is false, the position is
// anchored to the current wall-clock time (AT_TIMESTAMP) rather than LATEST.
// LATEST is re-evaluated at subscribe time, so if the initial subscription
// dies before delivering any event, a naive retry using LATEST again would
// re-anchor at the new (later) tip and skip any records published in the
// interim. Anchoring a timestamp once, up front, is stable across retries:
// it means "latest as of construction" and never moves, so no records can be
// skipped between subscribe attempts.
func efoStartingPosition(startingSequence string, startFromOldest bool) types.StartingPosition {
	if startingSequence != "" {
		return types.StartingPosition{
			Type:           types.ShardIteratorTypeAfterSequenceNumber,
			SequenceNumber: &startingSequence,
		}
	}
	if startFromOldest {
		return types.StartingPosition{Type: types.ShardIteratorTypeTrimHorizon}
	}
	return types.StartingPosition{Type: types.ShardIteratorTypeAtTimestamp, Timestamp: aws.Time(time.Now())}
}

// newEFORecordSource makes the first subscription attempt synchronously, so
// that ordinary misconfiguration (missing consumer, IAM) fails the shard
// claim fast, then starts the pump goroutine.
//
// Two kinds of failure on that first attempt are handled specially rather
// than failing the claim outright:
//
//   - InvalidArgumentException while starting from a checkpointed sequence
//     number: the stored sequence can no longer be resolved (e.g. it fell
//     behind the shard's retention window). This mirrors the polling
//     source's TRIM_HORIZON fallback: we log a warning and retry once from
//     TRIM_HORIZON rather than failing the claim forever. The fallback is
//     unconditionally TRIM_HORIZON (never the timestamp anchor above) because
//     this shard demonstrably had a committed position: resuming at "now"
//     would silently skip every record still retained ahead of the expired
//     sequence, breaking at-least-once delivery.
//   - ResourceInUseException: the shard's previous owner likely still holds
//     the one allowed subscription during a steal/handoff. Rather than
//     blocking the caller (and therefore the sequential claim/steal loop in
//     runBalancedShards) waiting out AWS's cooldown, the pump is started
//     with no live subscription; run's own resubscribe loop (spaced by
//     efoResubscribeFloor, ctx-aware, indefinite) acquires it in the
//     background.
//
// Any other error still fails fast.
func newEFORecordSource(ctx context.Context, subscribe efoSubscribeFn, shardID, startingSequence string, startFromOldest bool, fetchTimeout, maxResubscribeInterval time.Duration, log *service.Logger) (*efoRecordSource, error) {
	pos := efoStartingPosition(startingSequence, startFromOldest)

	e := &efoRecordSource{
		subscribe:              subscribe,
		shardID:                shardID,
		fetchTimeout:           fetchTimeout,
		maxResubscribeInterval: maxResubscribeInterval,
		log:                    log,
		recordsChan:            make(chan []types.Record, 1),
	}
	e.ctx, e.cancel = context.WithCancel(ctx)

	subscribedAt := time.Now()
	sub, err := e.subscribe(e.ctx, pos)

	if err != nil && pos.Type == types.ShardIteratorTypeAfterSequenceNumber {
		var invalidArg *types.InvalidArgumentException
		if errors.As(err, &invalidArg) {
			log.Warnf("Stored position for shard '%v' was rejected, falling back to the oldest retained record", shardID)
			pos = types.StartingPosition{Type: types.ShardIteratorTypeTrimHorizon}
			subscribedAt = time.Now()
			sub, err = e.subscribe(e.ctx, pos)
		}
	}

	if err != nil {
		var inUse *types.ResourceInUseException
		if !errors.As(err, &inUse) {
			e.cancel()
			return nil, fmt.Errorf("subscribing to shard '%v' (requires kinesis:SubscribeToShard): %w", shardID, err)
		}
		sub = nil
	}

	e.wg.Add(1)
	go e.run(sub, pos, subscribedAt)
	return e, nil
}

// waitForResubscribeFloor blocks until at least efoResubscribeFloor has
// elapsed since lastSubscribeAt, so that SubscribeToShard is never called
// more than once per second for this shard/consumer regardless of how the
// previous subscription ended. Returns false if the context was cancelled
// first.
func (e *efoRecordSource) waitForResubscribeFloor(lastSubscribeAt time.Time) bool {
	if wait := efoResubscribeFloor - time.Since(lastSubscribeAt); wait > 0 {
		select {
		case <-time.After(wait):
		case <-e.ctx.Done():
			return false
		}
	}
	return true
}

func (e *efoRecordSource) run(sub efoSubscription, pos types.StartingPosition, lastSubscribeAt time.Time) {
	defer func() {
		close(e.recordsChan)
		e.wg.Done()
	}()

	boff := backoff.NewExponentialBackOff()
	// SubscribeToShard is limited to one call per second per consumer per
	// shard, so never retry faster than that. RandomizationFactor is zeroed
	// so jitter never pulls a retry below that floor.
	boff.InitialInterval = efoResubscribeFloor
	boff.MaxInterval = e.maxResubscribeInterval
	boff.MaxElapsedTime = 0
	boff.RandomizationFactor = 0
	boff.Reset()

	for {
		if sub == nil {
			if !e.waitForResubscribeFloor(lastSubscribeAt) {
				return
			}
			lastSubscribeAt = time.Now()
			var err error
			if sub, err = e.subscribe(e.ctx, pos); err != nil {
				if e.ctx.Err() != nil {
					return
				}
				// ResourceInUseException is expected, self-healing contention:
				// another consumer (usually the previous lease owner during a
				// steal/handoff) still holds the shard's one allowed
				// subscription, and the escalating resubscribe loop resolves
				// it. Logging it at error level would page operators on every
				// routine rebalance.
				var inUse *types.ResourceInUseException
				if errors.As(err, &inUse) {
					e.log.Debugf("Shard '%v' is still subscribed by another consumer, waiting for the handoff to complete: %v", e.shardID, err)
				} else {
					e.log.Errorf("Failed to subscribe to shard '%v': %v", e.shardID, err)
				}
				// A sequence-derived position that AWS refuses can never
				// succeed on a retry (it has aged out of the shard's retention
				// window), so fall back to the oldest retained record rather
				// than wedging this shard forever whilst the lease renews.
				// After the fallback the position is TRIM_HORIZON, so this can
				// only fire once per stale position.
				var invalidArg *types.InvalidArgumentException
				if errors.As(err, &invalidArg) &&
					(pos.Type == types.ShardIteratorTypeAfterSequenceNumber || pos.Type == types.ShardIteratorTypeAtSequenceNumber) {
					e.log.Warnf("Stored position for shard '%v' was rejected, falling back to the oldest retained record", e.shardID)
					pos = types.StartingPosition{Type: types.ShardIteratorTypeTrimHorizon}
				}
				select {
				case <-time.After(boff.NextBackOff()):
				case <-e.ctx.Done():
					return
				}
				continue
			}
		}

		finished, continuation, sawEvent := e.consume(sub)
		streamErr := sub.Err()
		_ = sub.Close()
		sub = nil

		// Progress, not a successful subscribe, resets the retry interval: a
		// subscription that is accepted and then dies without delivering a
		// single event would otherwise retry at the one-second floor forever,
		// saturating the SubscribeToShard limit. Escalating instead also damps
		// the subscription ping-pong two consumers can produce whilst a shard
		// lease is being stolen.
		if sawEvent {
			boff.Reset()
		}

		if finished {
			e.finished.Store(true)
			return
		}
		if e.ctx.Err() != nil {
			return
		}
		if continuation != "" {
			pos = types.StartingPosition{
				Type:           types.ShardIteratorTypeAtSequenceNumber,
				SequenceNumber: &continuation,
			}
		}
		// Escalate the backoff whenever the subscription ended without
		// delivering any event, whether or not it ended with an error: an
		// errorless, eventless close is exactly how the subscription
		// ping-pong from a shard-steal ends, and without escalating here it
		// would resubscribe at the one-second floor forever, saturating the
		// SubscribeToShard limit rather than backing off.
		if streamErr != nil || !sawEvent {
			var inUse *types.ResourceInUseException
			if streamErr != nil {
				// A live subscription terminated with ResourceInUseException is
				// how the losing side of a shard steal observes the takeover —
				// expected contention, same as the resubscribe case above.
				if errors.As(streamErr, &inUse) {
					e.log.Debugf("Enhanced fan-out subscription for shard '%v' was taken over by another consumer: %v", e.shardID, streamErr)
				} else {
					e.log.Errorf("Enhanced fan-out subscription for shard '%v' failed: %v", e.shardID, streamErr)
				}
			} else {
				e.log.Debugf("Enhanced fan-out subscription for shard '%v' ended without delivering any event", e.shardID)
			}
			select {
			case <-time.After(boff.NextBackOff()):
			case <-e.ctx.Done():
				return
			}
		}
	}
}

// consume reads events until the subscription ends. finished is true when the
// shard is closed and fully read (signalled by a nil continuation sequence).
// sawEvent reports whether the subscription delivered at least one shard event
// (with or without records), which the caller uses to decide whether the
// subscription made any progress before it ended.
func (e *efoRecordSource) consume(sub efoSubscription) (finished bool, continuation string, sawEvent bool) {
	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				return false, continuation, sawEvent
			}
			sev, ok := ev.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent)
			if !ok {
				if ev != nil {
					e.log.Errorf("Received unexpected event type: %T", ev)
				}
				continue
			}
			sawEvent = true
			if len(sev.Value.Records) > 0 {
				if !e.forward(sev.Value.Records) {
					return false, continuation, sawEvent
				}
			}
			if sev.Value.ContinuationSequenceNumber == nil {
				return true, "", sawEvent
			}
			continuation = *sev.Value.ContinuationSequenceNumber
		case <-e.ctx.Done():
			return false, continuation, sawEvent
		}
	}
}

func (e *efoRecordSource) forward(recs []types.Record) bool {
	select {
	case e.recordsChan <- recs:
		return true
	case <-e.ctx.Done():
		return false
	}
}

// Fetch waits (bounded by fetchTimeout) for the next pushed batch, so that
// the calling consumer loop keeps servicing its commit timer.
func (e *efoRecordSource) Fetch(ctx context.Context) ([]types.Record, bool, error) {
	timer := time.NewTimer(e.fetchTimeout)
	defer timer.Stop()
	select {
	case recs, ok := <-e.recordsChan:
		if !ok {
			return nil, e.finished.Load(), nil
		}
		return recs, false, nil
	case <-timer.C:
		return nil, false, nil
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

func (*efoRecordSource) Blocking() bool { return true }

func (e *efoRecordSource) Close() {
	e.cancel()
	e.wg.Wait()
}
