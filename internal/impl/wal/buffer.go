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
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type (
	// The main loop sends these to the output channel
	durableBatch struct {
		batch     service.MessageBatch
		segmentID SegmentID
	}

	shard struct {
		wal *WriteAheadLog

		outgoing chan durableBatch
		mainChan chan mainLoopMsg
		walChan  chan walLoopMsg

		pendingBySegment sync.Map // map[SegmentID]*atomic.Int64

		// only modified from `runLoop` below
		currentSegment SegmentID
		pending        []pendingBatch // waiting to be written to WAL
		durable        []durableBatch // waiting to be read
	}
)

var _ service.BatchBuffer = &shard{}

func newWALShard() (*shard, error) {
	s := &shard{
		currentSegment: InvalidSegmentID,
	}
	go func() {
		if err := s.runWALLoop(); err != nil {

		}
	}()
	go func() {
		if err := s.runMainLoop(); err != nil {

		}
	}()
	return s, nil
}

func (s *shard) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case batch, closed := <-s.outgoing:
		if closed {
			return nil, nil, service.ErrEndOfBuffer
		}
		return batch.batch, func(ctx context.Context, err error) error {
			if err != nil {
				// retry!
				return nil
			}
			c, ok := s.pendingBySegment.Load(batch.segmentID)
			if !ok {
				// Log warning?
				return nil
			}
			if c.(*atomic.Int64).Add(-1) == 0 {
				s.walChan <- ackedSegment{batch.segmentID}
			}
			return nil
		}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (s *shard) WriteBatch(ctx context.Context, batch service.MessageBatch, ackFn service.AckFunc) error {
	pending := &pendingBatch{
		batch: batch,
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
	s.mainChan <- &endOfInput{}
}

func (s *shard) Close(ctx context.Context) error {
	return nil
}

type (
	mainLoopMsg interface{}
	// The main loop recieves these from WriteBatch
	pendingBatch struct {
		batch service.MessageBatch
		ackFn service.AckFunc
	}
	// The main loop recieves these from WALPersistLoop
	appendedBatch struct {
		id  SegmentID
		err error
	}
	// The main loop recieves these from WALRecoveryLoop
	recoveredBatch struct {
		batch service.MessageBatch
		id    SegmentID
	}
	// The main loop recieves these from WALRecoveryLoop
	recoveryPhase struct {
		start bool
	}
	// The main loop recieves these from EndOfInput
	endOfInput struct{}
	// The main loop recieves these from nack'ing messages
	retryBatch struct {
		batch service.MessageBatch
		id    SegmentID
	}
)

func (s *shard) runMainLoop() error {
	pauseWrites := func() {
		// TODO
	}
	resumeWrites := func() {
		// TODO
	}
	defer func() {
		resumeWrites()
	}()

	for {
		msg := <-s.mainChan
		switch t := msg.(type) {
		case *pendingBatch:
			_ = t
		case *appendedBatch:
			_ = t
		case *recoveryPhase:
			if t.start {
				pauseWrites()
			} else {
				resumeWrites()
			}
		case *recoveredBatch:
			_ = t
		case *retryBatch:
			_ = t
		case *endOfInput:
		default:
			panic(fmt.Sprintf("unknown main loop channel message: %T", msg))
		}
	}
}

type (
	walLoopMsg interface{}
	// The WAL loop recieves these from ack'ing messages
	ackedSegment struct {
		id SegmentID
	}
)

func (s *shard) runWALLoop() error {
	s.mainChan <- &recoveryPhase{start: true}
	err := s.wal.Replay(func(id SegmentID, b []byte) error {
		batch, err := readBatch(b)
		if err != nil {
			return err
		}
		s.mainChan <- &recoveredBatch{batch: batch, id: id}
		return nil
	})
	s.mainChan <- &recoveryPhase{start: false}
	if err != nil {
		return err
	}
	for {
		msg := <-s.walChan
		_ = msg
	}
}
