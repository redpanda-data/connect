// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pure

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/dispatch"
	"golang.org/x/sync/errgroup"
)

const (
	bbvFieldChild  = "child"
	bbvFieldValue  = "value"
	bbvFieldPolicy = "policy"
)

func init() {
	spec := service.NewConfigSpec().
		Description("Batch messages by a function interpolated string evaluated per message. This allows for creating batches and processing messages in order based on some value for each message.").
		Categories("Utility").
		Fields(
			service.NewInputField(bbvFieldChild).Description("The child input"),
			service.NewInterpolatedStringField(bbvFieldValue).
				Description("The value to partition each message by").
				Example("${!@kafka_topic}-${!@kafka_partition}"),
			service.NewBatchPolicyField(bbvFieldPolicy),
			service.NewAutoRetryNacksToggleField(),
			// TODO(rockwood): Do we need some kind of additional limit here to prevent an OOM due to the number of messages built up
			// or should can rely on child inputs for that backpressure?
		).Example(
		"Explicit batching for the Redpanda input",
		"This is an example of explicitly batching per topic-partition in the `redpanda` input, which normally does not provide explicit batching controls, but instead relies on the batch sizes returned by the broker.", `
input:
  batched_by_value:
    value: "${!@kafka_topic}-${!@kafka_partition}"
    policy:
      count: 10000
      period: 10s
    child:
      redpanda:
        seed_brokers: [TODO]
        topics: [foo_topic]
        consumer_group: redpanda_connect_foo
`)
	err := service.RegisterBatchInput("batched_by_value", spec, func(conf *service.ParsedConfig, res *service.Resources) (service.BatchInput, error) {
		child, err := conf.FieldInput(bbvFieldChild)
		if err != nil {
			return nil, err
		}
		computeValue, err := conf.FieldInterpolatedString(bbvFieldValue)
		if err != nil {
			return nil, err
		}
		policy, err := conf.FieldBatchPolicy(bbvFieldPolicy)
		if err != nil {
			return nil, err
		}
		input := newBatchedByValueInput(res, computeValue, policy, child)
		return service.AutoRetryNacksBatchedToggled(conf, input)
	})
	if err != nil {
		panic(err)
	}
}

func newBatchedByValueInput(res *service.Resources, computeValue *service.InterpolatedString, policy service.BatchPolicy, child childInput) *batchedByValueInput {
	return &batchedByValueInput{
		res,
		computeValue,
		policy,
		child,
		nil,
		make(map[string]*batchedPartition),
		make(chan flushedBatch),
		make(chan error, 1),
	}
}

type (
	childInput interface {
		ReadBatch(context.Context) (service.MessageBatch, service.AckFunc, error)
		service.Closer
	}
	// At a high level, this input works by reading from the child input in a single background
	// goroutine, then dispatching batches from the child into a number of batchers (aka queues)
	// flushing each batches as the policy dictates. Periodic flushes are achieved via a timeout
	// when reading from the child input (see doLoopIter for the details), there are are no
	// seperate goroutines for periodic flushing as to prevent the need for locking.
	batchedByValueInput struct {
		resources    *service.Resources
		computeValue *service.InterpolatedString
		policy       service.BatchPolicy
		child        childInput
		shutSig      *shutdown.Signaller
		partitions   map[string]*batchedPartition
		readChan     chan flushedBatch
		errChan      chan error
	}
	batchedPartition struct {
		// For the current batch we're building
		batcher *service.Batcher
		acks    []service.AckFunc
		// For the batch we need to send
		pending []flushedBatch
		// For the batches we've sent
		inFlight atomic.Bool
	}
	flushedBatch struct {
		batch service.MessageBatch
		ackFn service.AckFunc
	}
)

func (bbvi *batchedByValueInput) Connect(ctx context.Context) error {
	if bbvi.shutSig != nil {
		// We need to wait for the previous loop to iterate if we're
		// reconnecting
		bbvi.shutSig.TriggerHardStop()
		select {
		case <-bbvi.shutSig.HasStoppedChan():
		case <-ctx.Done():
			return ctx.Err()
		}
		// Reset/drain our err channel
		select {
		case <-bbvi.errChan:
		default:
		}
	}
	shutSig := shutdown.NewSignaller()
	bbvi.shutSig = shutSig
	go func() {
		defer shutSig.TriggerHasStopped()
		err := bbvi.loop(shutSig)
		// We hit an error, let's flush all our batches, then report the issue
		ctx, cancel := shutSig.HardStopCtx(context.Background())
		defer cancel()
		for id := range bbvi.partitions {
			bbvi.flushBatch(ctx, id)
		}
		if err != nil {
			select {
			case bbvi.errChan <- err:
			default:
			}
		}
	}()
	return nil
}

func (bbvi *batchedByValueInput) partitionBatch(batch service.MessageBatch) (map[string]service.MessageBatch, error) {
	exec := batch.InterpolationExecutor(bbvi.computeValue)
	partitioned := make(map[string]service.MessageBatch)
	for i, msg := range batch {
		partID, err := exec.TryString(i)
		if err != nil {
			return nil, fmt.Errorf("mapping %s failed: %w", bbvFieldValue, err)
		}
		partitioned[partID] = append(partitioned[partID], msg)
	}
	return partitioned, nil
}

func (bbvi *batchedByValueInput) addBatch(ctx context.Context, partitionID string, batch service.MessageBatch, ackFn service.AckFunc) {
	part, ok := bbvi.partitions[partitionID]
	if !ok {
		// Initialize the partition
		batcher, err := bbvi.policy.NewBatcher(bbvi.resources)
		if err != nil {
			_ = ackFn(ctx, err)
			return
		}
		part = &batchedPartition{
			batcher: batcher,
			acks:    nil,
		}
		bbvi.partitions[partitionID] = part
	}
	part.acks = append(part.acks, ackFn)
	flush := false
	hasInFlight := part.inFlight.Load()
	for _, msg := range batch {
		if !hasInFlight {
			dispatch.TriggerSignal(msg.Context())
		}
		if part.batcher.Add(msg) {
			flush = true
		}
	}
	if flush {
		bbvi.flushBatch(ctx, partitionID)
	}
}

func (bbvi *batchedByValueInput) flushBatch(ctx context.Context, id string) {
	part := bbvi.partitions[id]
	ackFn := func(ctx context.Context, err error) (rErr error) {
		for _, ack := range part.acks {
			if aErr := ack(ctx, err); err == nil {
				rErr = aErr
			}
		}
		part.inFlight.Store(false)
		return
	}
	batch, err := part.batcher.Flush(ctx)
	if err != nil {
		_ = ackFn(ctx, err)
		return
	}
	if len(part.pending) == 0 && !part.inFlight.Swap(true) {
		select {
		case bbvi.readChan <- flushedBatch{batch, ackFn}:
		case <-ctx.Done():
			_ = ackFn(ctx, ctx.Err())
		}
		return
	}
	part.pending = append(part.pending, flushedBatch{batch, ackFn})
}

func (bbvi *batchedByValueInput) loop(shutSig *shutdown.Signaller) error {
	ctx, cancel := shutSig.SoftStopCtx(context.Background())
	defer cancel()
	for shutSig.IsSoftStopSignalled() {
		if err := bbvi.doLoopIter(ctx); err != nil {
			return err
		}
	}
	return nil
}

var errFlushNeeded = errors.New("yo dawg you need to flush")

func (bbvi *batchedByValueInput) doLoopIter(ctx context.Context) error {
	// Flush any pending batches if we can
	for _, part := range bbvi.partitions {
		if len(part.pending) > 0 && !part.inFlight.Swap(true) {
			sending := part.pending[0]
			part.pending = part.pending[1:]
			select {
			case bbvi.readChan <- sending:
			case <-ctx.Done():
				_ = sending.ackFn(ctx, ctx.Err())
			}
		}
	}
	// Compute the earliest time we need to flush a batch by. If there are no periodic
	// batches, then use a time that is effectively in the infinite future.
	needFlushDeadline := time.Now().Add(math.MaxInt64)
	for id, part := range bbvi.partitions {
		d, ok := part.batcher.UntilNext()
		if !ok {
			continue
		}
		// If we need to flush now, let's do it
		if d == 0 {
			bbvi.flushBatch(ctx, id)
			continue
		}
		// Otherwise stop when we hit the timeout
		deadline := time.Now().Add(d)
		if deadline.Before(needFlushDeadline) {
			needFlushDeadline = deadline
		}
	}
	// We'll abort the read to flush by returning a sentinal error
	readCtx, cancel := context.WithDeadlineCause(ctx, needFlushDeadline, errFlushNeeded)
	defer cancel()
	batch, ack, err := bbvi.child.ReadBatch(readCtx)
	if errors.Is(err, errFlushNeeded) {
		// Return immediately, then let the loop recall this function,
		// which will flush everything that is needed.
		return nil
	} else if err != nil {
		return err
	}
	batches, err := bbvi.partitionBatch(batch)
	if err != nil {
		_ = ack(ctx, err)
		return err
	}
	// Use an atomic to keep track of when to ack.
	// The general algorithm here because atomics are tricky:
	// - Start the counter positive
	// - acks decrement the counter. When the atomic increment hits
	//   zero that means that everything has been acked successful
	//   and we can send the ack upstream
	// - If the counter is ever negative, it means there has been
	//   a nack. The *first* one to set the counter negative is
	//   allowed to report the nack upstream.
	// - we can use any negative as the sentinal value because the
	//   happy path only decrements, so that means they can't hit
	//   zero and we'll never propagate an ack.
	var pendingAcks atomic.Int64
	pendingAcks.Add(int64(len(batches)))
	for id, batch := range batches {
		bbvi.addBatch(ctx, id, batch, func(ctx context.Context, err error) error {
			// If there is an error, we want to propagate the nack immediately
			if err != nil {
				// If no one else has already caused an error (by sending pending to negative)
				// then we can propagate the nack, otherwise do nothing.
				if pendingAcks.Swap(-1) > 0 {
					return ack(ctx, err)
				}
				return nil
			}
			// Decrement our counter, if we didn't hit zero then wait for more acks
			if pendingAcks.Add(-1) != 0 {
				return nil
			}
			// we're the lucky last message and get to ack
			return ack(ctx, nil)
		})
	}
	return nil
}

func (bbvi *batchedByValueInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case flushed := <-bbvi.readChan:
		return flushed.batch, flushed.ackFn, nil
	case err := <-bbvi.errChan:
		return nil, nil, err
	case <-bbvi.shutSig.HasStoppedChan():
		return nil, nil, context.Canceled
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (bbvi *batchedByValueInput) Close(ctx context.Context) error {
	var wg errgroup.Group
	wg.Go(func() error {
		bbvi.shutSig.TriggerSoftStop()
		<-bbvi.shutSig.HasStoppedChan()
		for _, part := range bbvi.partitions {
			_ = part.batcher.Close(ctx)
		}
		return nil
	})
	wg.Go(func() error {
		err := bbvi.child.Close(ctx)
		bbvi.shutSig.TriggerHardStop()
		return err
	})
	return wg.Wait()
}
