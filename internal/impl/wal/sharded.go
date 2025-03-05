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
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/Jeffail/shutdown"
	"github.com/dustin/go-humanize"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type readyBatch struct {
	batch   service.MessageBatch
	ackFunc service.AckFunc
	err     error
}

type sharded struct {
	w atomic.Int64

	multiplexer chan readyBatch
	shutSig     *shutdown.Signaller

	shards []service.BatchBuffer
	mask   int64
}

var _ service.BatchBuffer = &sharded{}

func init() {
	err := service.RegisterBatchBuffer(
		"wal",
		service.NewConfigSpec().
			Summary(`A write ahead log (WAL) buffer that keeps writes in memory but also persists batches to disk.`).
			Description(`A write ahead log (WAL) buffer that keeps writes in memory but also persists batches to disk.
Messages are not acknowledge from the input until they have been safely written to disk.

Each batch is written to a segment file, which is a chunk of the WAL. When a segment file is full based on the 'max_segment_age' and 'max_segment_size'
configuration, a new segment file is created. The old segment file will be deleted when all the messages within it have been acknowledged, which preserves
at least once message processing.

Upon startup, writes are not accepted from the 'input', and all the segment files are read to reply any leftover messages from the previous process. Note that
because WAL cleanup happens at the segment level, this means that if a single message is not acknowledged within a segment file, all messages in the segment file
will be replayed upon startup.
`).
			Fields(
				service.NewStringField("path").Description("The directory in which WAL files are saved."),
				service.NewObjectField(
					"limits",
					service.NewIntField("count").
						Default(0).
						Description(`The maximum number of messages inflight at once from the buffer. If the limit is 0 then there is no maximum on the number of messages.`),
				).Description(`Limits that bound the amount of memory used by the buffer. When a limit is hit, backpressure is applied to the input.`),
				service.NewStringField("max_segment_size").
					Description("The maximum size that a single segment of the WAL will grow before being rotated.").
					Default("20MiB").Example("50MB").Example("1GB"),
				service.NewDurationField("max_segment_age").
					Description("The maximum age of a single segment of the WAL will be before being rotated.").
					Default("24h").Example("1h").Example("72h"),
				service.NewIntField("shards").
					Description("The number of shards or seperate WAL files there are. When there is only a single shard messages are processed in order. Setting this to greater than 1 will support multiple log files, which can improve performance, but messages can be processed out of order.").
					Default(1),
			),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchBuffer, error) {
			path, err := conf.FieldString("path")
			if err != nil {
				return nil, err
			}
			limit, err := conf.FieldInt("limits", "count")
			if err != nil {
				return nil, err
			}
			maxSegmentSizeStr, err := conf.FieldString("max_segment_size")
			if err != nil {
				return nil, err
			}
			maxSegmentSize, err := humanize.ParseBytes(maxSegmentSizeStr)
			if err != nil {
				return nil, fmt.Errorf("failed to parse 'max_segment_size' as a bytes string: %w", err)
			}
			maxSegmentAge, err := conf.FieldDuration("max_segment_age")
			if err != nil {
				return nil, err
			}
			shardCount, err := conf.FieldInt("shards")
			if err != nil {
				return nil, err
			}
			if shardCount == 1 {
				return newWALShard(limit, &Options{
					LogDir:     filepath.Join(path, "shard_0"),
					MaxLogSize: int64(maxSegmentSize),
					MaxLogAge:  maxSegmentAge,
					Log:        res.Logger(),
				})
			}
			var shards []service.BatchBuffer
			for i := range shardCount {
				s, err := newWALShard(limit, &Options{
					LogDir:     filepath.Join(path, fmt.Sprintf("shard_%d", i)),
					MaxLogSize: int64(maxSegmentSize),
					MaxLogAge:  maxSegmentAge,
					Log:        res.Logger(),
				})
				if err != nil {
					return nil, err
				}
				shards = append(shards, s)
			}
			return newShardedBuffer(shards)
		})
	if err != nil {
		panic(err)
	}
}

func newShardedBuffer(shards []service.BatchBuffer) (*sharded, error) {
	if len(shards) <= 1 {
		return nil, errors.New("sharded buffers must have more than 1 shard")
	}
	if len(shards)%2 != 0 {
		return nil, fmt.Errorf("sharded buffers must be a power of 2, got: %v", len(shards))
	}
	shutSig := shutdown.NewSignaller()
	multiplexer := make(chan readyBatch)
	ctx, cancel := shutSig.SoftStopCtx(context.Background())
	var wg sync.WaitGroup
	for _, s := range shards {
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				b, ackFn, err := s.ReadBatch(ctx)
				if errors.Is(err, service.ErrEndOfBuffer) {
					return
				}
				multiplexer <- readyBatch{b, ackFn, err}
			}
		}()
	}
	go func() {
		wg.Wait()
		cancel()
		close(multiplexer)
		shutSig.TriggerHasStopped()
	}()
	return &sharded{
		multiplexer: multiplexer,
		shards:      shards,
		shutSig:     shutSig,
		mask:        int64(len(shards)) - 1,
	}, nil
}

func (s *sharded) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case rb, open := <-s.multiplexer:
		if !open {
			return nil, nil, service.ErrEndOfBuffer
		}
		return rb.batch, rb.ackFunc, rb.err
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (s *sharded) WriteBatch(ctx context.Context, batch service.MessageBatch, ackFn service.AckFunc) error {
	return s.shards[s.w.Add(1)&s.mask].WriteBatch(ctx, batch, ackFn)
}

func (s *sharded) EndOfInput() {
	for _, ss := range s.shards {
		ss.EndOfInput()
	}
}

func (s *sharded) Close(ctx context.Context) error {
	var errs []error
	for _, ss := range s.shards {
		errs = append(errs, ss.Close(ctx))
	}
	return errors.Join(errs...)
}
