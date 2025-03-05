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

package wal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type (
	// The main loop sends these to the output channel
	durableBatch struct {
		batch     service.MessageBatch
		segmentID SegmentID
	}

	bufferState int

	// The main loop sends these to the WAL loop
	pendingWALOp struct {
		persist *pendingBatch
		remove  *ackedSegment
	}

	shard struct {
		sema *semaphore.Weighted
		wal  *WriteAheadLog

		recoveryMu sync.RWMutex

		mainChan chan mainLoopMsg   // unbuffered
		walChan  chan *pendingWALOp // buffered=1

		pendingBySegment sync.Map // map[SegmentID]*atomic.Int64
		durableCond      *sync.Cond
		durable          *chunkedBuffer[durableBatch] // waiting to be read
		durableState     bufferState

		loops *errgroup.Group

		log *service.Logger
	}
)

const (
	bufferStateOpen bufferState = iota
	bufferStateClosed
	bufferStateEnd
)

var _ service.BatchBuffer = &shard{}

func newWALShard(limit int, opts *WALOptions) (*shard, error) {
	wal, err := NewWriteAheadLog(opts)
	if err != nil {
		return nil, err
	}
	s := &shard{
		wal:         wal,
		mainChan:    make(chan mainLoopMsg),
		walChan:     make(chan *pendingWALOp, 1),
		durableCond: sync.NewCond(&sync.Mutex{}),
		durable:     newChunkedBuffer[durableBatch](),
		log:         opts.Log,
		loops:       &errgroup.Group{},
	}
	if limit > 0 {
		s.sema = semaphore.NewWeighted(int64(limit))
	}
	s.recoveryMu.Lock() // unlocked in the wal loop
	ctx, cancel := context.WithCancel(context.Background())
	s.loops.Go(func() error {
		defer cancel()
		err := s.runWALLoop(ctx)
		if err != nil {
			s.log.Errorf("failure in WAL persistence loop: %v", err)
		}
		return err
	})
	s.loops.Go(func() error {
		defer cancel()
		err := s.runMainLoop(ctx)
		if err != nil {
			s.log.Errorf("failure in main WAL buffer loop: %v", err)
		} else {
			s.log.Info("WAL buffer stopped")
		}
		return err
	})
	return s, nil
}

func (s *shard) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	ctx, done := context.WithCancel(ctx)
	defer done()
	go func() {
		<-ctx.Done()
		s.durableCond.Broadcast()
	}()
	s.durableCond.L.Lock()
	defer s.durableCond.L.Unlock()
	for ctx.Err() == nil {
		item, ok := s.durable.PopFront()
		if ok {
			if item.batch == nil {
				return nil, nil, service.ErrEndOfBuffer
			}
			return item.batch, func(ctx context.Context, err error) error {
				return s.ackBatch(ctx, item, err)
			}, nil
		}
		if s.durableState == bufferStateClosed {
			return nil, nil, context.Canceled
		}
		s.durableCond.Wait()
	}
	return nil, nil, ctx.Err()
}

func (s *shard) ackBatch(ctx context.Context, durable durableBatch, err error) error {
	if err != nil {
		s.durableCond.L.Lock()
		s.durable.PushBack(durable)
		s.durableCond.Broadcast()
		s.durableCond.L.Unlock()
		return nil
	}
	if s.sema != nil {
		s.sema.Release(int64(len(durable.batch)))
	}
	c, ok := s.pendingBySegment.Load(durable.segmentID)
	if !ok {
		s.log.Warnf("missing WAL segment %d", durable.segmentID)
		return nil
	}
	if c.(*atomic.Int64).Add(-1) == 0 {
		errChan := make(chan error, 1)
		s.mainChan <- &ackedSegment{id: durable.segmentID, err: errChan}
		return <-errChan
	}
	return nil
}

func (s *shard) WriteBatch(ctx context.Context, batch service.MessageBatch, ackFn service.AckFunc) error {
	if s.sema != nil {
		if err := s.sema.Acquire(ctx, int64(len(batch))); err != nil {
			return err
		}
	}
	s.recoveryMu.RLock()
	defer s.recoveryMu.RUnlock()
	pending := &pendingBatch{
		batch: batch.DeepCopy(), // defensive copy
		ackFn: ackFn,
	}
	select {
	case s.mainChan <- pending:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *shard) EndOfInput() {
	s.recoveryMu.RLock()
	defer s.recoveryMu.RUnlock()
	s.mainChan <- &endOfInput{}
}

func (s *shard) Close(ctx context.Context) error {
	select {
	case s.mainChan <- &closeBuffer{}:
		loopErr := s.loops.Wait()
		walErr := s.wal.Close()
		return errors.Join(loopErr, walErr)
	case <-ctx.Done():
		s.log.Warn("unable to cleanly close WAL buffer")
		return ctx.Err()
	}
}

type (
	mainLoopMsg interface{}
	// The main loop recieves these from WriteBatch
	pendingBatch struct {
		batch service.MessageBatch
		ackFn service.AckFunc
	}
	// The main loop recieves these from WALPersistLoop
	persistedBatch struct {
		id  SegmentID
		err error
	}
	// The main loop recieves these from WALPersistLoop
	deletedSegment struct {
		id SegmentID
	}
	// The main loop recieves these from WALRecoveryLoop
	recoveredBatch struct {
		batch service.MessageBatch
		id    SegmentID
	}
	// The main loop recieves these from EndOfInput
	endOfInput struct{}
	// The main loop recieves these from Close
	closeBuffer struct{}
)

func (s *shard) runMainLoop(ctx context.Context) error {
	pending := newChunkedBuffer[*pendingWALOp]()
	var inflight *pendingWALOp // current writing to the WAL
	currentSegment := InvalidSegmentID
	defer close(s.walChan)
	flushWALOp := &pendingWALOp{persist: nil, remove: nil}

	incrementSegmentCount := func(id SegmentID) {
		v, ok := s.pendingBySegment.Load(id)
		if !ok {
			v, _ = s.pendingBySegment.LoadOrStore(id, &atomic.Int64{})
		}
		v.(*atomic.Int64).Add(1)
	}
	getSegmentCount := func(id SegmentID) int64 {
		v, ok := s.pendingBySegment.Load(id)
		if !ok {
			return 0
		}
		return v.(*atomic.Int64).Load()
	}
	removeSegmentCount := func(id SegmentID) {
		s.pendingBySegment.Delete(id)
	}
	pushToOutput := func(batch service.MessageBatch, id SegmentID) {
		if currentSegment != id {
			s.log.Tracef("WAL rotation: (previous_segment=%v, next_segment=%v)", currentSegment, id)
			// The segment was all acked by the time we are sending this new batch
			if getSegmentCount(currentSegment) == 0 && currentSegment != InvalidSegmentID {
				removeSegmentCount(currentSegment)
				pending.PushBack(&pendingWALOp{
					persist: nil,
					remove:  &ackedSegment{id: currentSegment, err: make(chan error, 1)},
				})
			}
		}
		currentSegment = id
		if batch != nil {
			incrementSegmentCount(id)
		}
		s.durableCond.L.Lock()
		s.durable.PushBack(durableBatch{
			batch: batch, segmentID: id,
		})
		s.durableCond.Broadcast()
		s.durableCond.L.Unlock()
	}
	var nextWALOp func()
	nextWALOp = func() {
		if inflight != nil {
			return
		}
		front, ok := pending.PeekFront()
		if !ok {
			return
		}
		select {
		case s.walChan <- front:
			if front == flushWALOp {
				s.log.Trace("WAL flushed, marking the buffer as done")
				// WAL has been flushed, write to the output that we're done
				pushToOutput(nil, currentSegment)
				pending.PopFront()
				nextWALOp()
			} else {
				inflight = front
				pending.PopFront()
			}
		default:
		}
	}
mainLoop:
	for ctx.Err() == nil {
		msg := <-s.mainChan
	recvSwitch:
		switch m := msg.(type) {
		case *pendingBatch:
			pending.PushBack(&pendingWALOp{persist: m, remove: nil})
		case *ackedSegment:
			c := getSegmentCount(m.id)
			if c != 0 || currentSegment == m.id {
				m.err <- nil
				break recvSwitch
			}
			removeSegmentCount(currentSegment)
			pending.PushBack(&pendingWALOp{persist: nil, remove: m})
		case *deletedSegment:
			if inflight == nil || inflight.remove == nil {
				panic("invariant failure")
			}
			// only thing to do is schedule the next WAL op
			inflight = nil
		case *persistedBatch:
			if inflight == nil || inflight.persist == nil {
				panic("invariant failure")
			}
			_ = inflight.persist.ackFn(context.Background(), m.err)
			if m.err != nil {
				break recvSwitch
			}
			pushToOutput(inflight.persist.batch, m.id)
			inflight = nil
		case *recoveredBatch:
			pushToOutput(m.batch, m.id)
		case *endOfInput:
			pending.PushBack(flushWALOp)
			s.durableCond.L.Lock()
			s.durableState = bufferStateEnd
			s.durableCond.L.Unlock()
		case *closeBuffer:
			s.durableCond.L.Lock()
			s.durableState = bufferStateClosed
			s.durableCond.L.Unlock()
			defer s.durableCond.Broadcast()
			break mainLoop
		default:
			panic(fmt.Sprintf("unknown main loop channel message: %T", msg))
		}

		// Schedule the next WAL operation to send out
		nextWALOp()
	}

	for {
		item, ok := pending.PopFront()
		if !ok {
			break
		}
		if item.persist != nil {
			_ = item.persist.ackFn(context.Background(), context.Canceled)
		}
		if item.remove != nil {
			item.remove.err <- s.wal.DeleteSegment(item.remove.id)
		}
	}

	// Best effort clean up the current segment if everything is clean
	if currentSegment != InvalidSegmentID && getSegmentCount(currentSegment) == 0 && inflight == nil {
		if err := s.wal.RotateSegment(); err != nil {
			return fmt.Errorf("unable to rotate last segment during shutdown, some messages will be replayed on startup: %v", err)
		}
		if err := s.wal.DeleteSegment(currentSegment); err != nil {
			return fmt.Errorf("unable to delete last segment during shutdown, some messages will be replayed on startup: %v", err)
		}
	}

	return nil
}

type (
	walLoopMsg   interface{}
	persistBatch struct {
		batch service.MessageBatch
	}
	// The WAL loop recieves these from ack'ing messages
	ackedSegment struct {
		id  SegmentID
		err chan<- error
	}
)

func (s *shard) runWALLoop(ctx context.Context) error {
	err := s.wal.Replay(func(id SegmentID, b []byte) error {
		batch, err := readBatch(b)
		if err != nil {
			return err
		}
		if s.sema != nil {
			if err := s.sema.Acquire(ctx, int64(len(batch))); err != nil {
				return err
			}
		}
		select {
		case s.mainChan <- &recoveredBatch{batch: batch, id: id}:
		case <-ctx.Done():
		}
		return nil
	})
	s.recoveryMu.Unlock()
	if err != nil {
		return err
	}
	for ctx.Err() == nil {
		msg, ok := <-s.walChan
		if !ok {
			break
		}
		if msg.persist != nil {
			b, err := appendBatchV0(nil, msg.persist.batch)
			id := InvalidSegmentID
			if err == nil {
				id, err = s.wal.Append(b)
			}
			select {
			case s.mainChan <- &persistedBatch{id: id, err: err}:
			case <-ctx.Done():
			}
		}
		if msg.remove != nil {
			msg.remove.err <- s.wal.DeleteSegment(msg.remove.id)
			select {
			case s.mainChan <- &deletedSegment{id: InvalidSegmentID}:
			case <-ctx.Done():
			}
		}
	}
	return nil
}
