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
	"sync/atomic"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type sharded struct {
	r atomic.Int64
	w atomic.Int64

	shards []service.BatchBuffer
	mask   int64
}

var _ service.BatchBuffer = &sharded{}

func newShardedBuffer(shards []service.BatchBuffer) (*sharded, error) {
	if len(shards) <= 1 {
		return nil, errors.New("sharded buffers must have more than 1 shard")
	}
	if len(shards)%2 != 0 {
		return nil, fmt.Errorf("sharded buffers must be a power of 2, got: %v", len(shards))
	}
	return &sharded{
		shards: shards,
		mask:   int64(len(shards)) - 1,
	}, nil
}

func (s *sharded) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	return s.shards[s.r.Add(1)&s.mask].ReadBatch(ctx)
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
