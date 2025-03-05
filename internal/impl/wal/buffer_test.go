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
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

func makeBatch(nums ...int) service.MessageBatch {
	b := service.MessageBatch{}
	for _, n := range nums {
		b = append(b, service.NewMessage([]byte(strconv.Itoa(n))))
	}
	return b
}

func batchAsJSON(t *testing.T, batch service.MessageBatch) string {
	var msgs []any
	for _, msg := range batch {
		m, err := msg.AsStructured()
		require.NoError(t, err)
		msgs = append(msgs, m)
	}
	b, err := json.Marshal(msgs)
	require.NoError(t, err)
	return string(b)
}

func makeAckFn(t *testing.T) service.AckFunc {
	return func(ctx context.Context, err error) error {
		require.NoError(t, err)
		return nil
	}
}

func TestBufferSingle(t *testing.T) {
	t.Parallel()
	shard, err := newWALShard(0, &WALOptions{
		LogDir:     t.TempDir(),
		MaxLogSize: humanize.MiByte,
		MaxLogAge:  time.Minute,
	})
	require.NoError(t, err)
	ctx := t.Context()
	err = shard.WriteBatch(ctx, makeBatch(0), makeAckFn(t))
	require.NoError(t, err)
	b, ackFn, err := shard.ReadBatch(ctx)
	require.NoError(t, err)
	require.NoError(t, ackFn(ctx, nil))
	require.JSONEq(t, `[0]`, batchAsJSON(t, b))
	require.NoError(t, shard.Close(ctx))
}

func TestBufferSequential(t *testing.T) {
	t.Parallel()
	shard, err := newWALShard(0, &WALOptions{
		LogDir:     t.TempDir(),
		MaxLogSize: humanize.MiByte,
		MaxLogAge:  time.Minute,
	})
	require.NoError(t, err)
	ctx := t.Context()
	for i := range 10 {
		b := makeBatch(i)
		err = shard.WriteBatch(ctx, b, makeAckFn(t))
		require.NoError(t, err)
	}
	for i := range 10 {
		b, ackFn, err := shard.ReadBatch(ctx)
		require.NoError(t, err)
		require.NoError(t, ackFn(ctx, nil))
		require.JSONEq(t, fmt.Sprintf("[%d]", i), batchAsJSON(t, b))
	}
	require.NoError(t, shard.Close(ctx))
}

func TestBufferInterleaved(t *testing.T) {
	t.Parallel()
	shard, err := newWALShard(0, &WALOptions{
		LogDir:     t.TempDir(),
		MaxLogSize: humanize.MiByte,
		MaxLogAge:  time.Minute,
	})
	require.NoError(t, err)
	ctx := t.Context()
	for i := range 10 {
		b := makeBatch(i)
		err = shard.WriteBatch(ctx, b, makeAckFn(t))
		require.NoError(t, err)
		b, ackFn, err := shard.ReadBatch(ctx)
		require.NoError(t, err)
		require.NoError(t, ackFn(ctx, nil))
		require.JSONEq(t, fmt.Sprintf("[%d]", i), batchAsJSON(t, b))
	}
	require.NoError(t, shard.Close(ctx))
}

func TestBufferRandomized(t *testing.T) {
	runTest := func(t *testing.T, ackHandler func(service.AckFunc)) {
		t.Parallel()
		shard, err := newWALShard(0, &WALOptions{
			LogDir:     t.TempDir(),
			MaxLogSize: humanize.MiByte,
			MaxLogAge:  time.Minute,
		})
		require.NoError(t, err)
		ctx := t.Context()
		results := make(chan string, 10000)
		for i := range 10 {
			go func() {
				for j := range 1000 {
					b := makeBatch(i, j)
					err := shard.WriteBatch(ctx, b, makeAckFn(t))
					require.NoError(t, err)
				}
			}()
			go func() {
				for range 1000 {
					b, ackFn, err := shard.ReadBatch(ctx)
					require.NoError(t, err)
					results <- batchAsJSON(t, b)
					ackHandler(ackFn)
				}
			}()
		}
		actual := map[string]bool{}
		expected := map[string]bool{}
		for i := range 10 {
			for j := range 1000 {
				actual[<-results] = true
				expected[fmt.Sprintf("[%d,%d]", i, j)] = true
			}
		}
		require.Equal(t, expected, actual)
		ackHandler(nil)
		require.NoError(t, shard.Close(ctx))
	}
	t.Run("AckImmediate", func(t *testing.T) {
		runTest(t, func(ackFn service.AckFunc) {
			if ackFn == nil {
				return
			}
			require.NoError(t, ackFn(t.Context(), nil))
		})
	})
	t.Run("AckDelayed", func(t *testing.T) {
		runTest(t, func(ackFn service.AckFunc) {
			if ackFn == nil {
				return
			}
			time.Sleep(time.Millisecond)
			require.NoError(t, ackFn(t.Context(), nil))
		})
	})
	t.Run("AckAsync", func(t *testing.T) {
		var wg sync.WaitGroup
		runTest(t, func(ackFn service.AckFunc) {
			if ackFn == nil {
				wg.Wait()
			} else {
				wg.Add(1)
				go func() {
					defer wg.Done()
					time.Sleep(time.Millisecond)
					require.NoError(t, ackFn(t.Context(), nil))
				}()
			}
		})
	})
	t.Run("AckNever", func(t *testing.T) {
		runTest(t, func(ackFn service.AckFunc) {
			_ = ackFn
		})
	})
}

func TestFileCleanup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	shard, err := newWALShard(0, &WALOptions{
		LogDir:     dir,
		MaxLogSize: humanize.MiByte,
		MaxLogAge:  time.Minute,
	})
	require.NoError(t, err)
	ctx := t.Context()
	var ackFns []service.AckFunc
	for range 1024 {
		// Make big batches
		b := makeBatch(slices.Repeat([]int{9999}, 10000)...)
		err = shard.WriteBatch(ctx, b, makeAckFn(t))
		require.NoError(t, err)
		_, ackFn, err := shard.ReadBatch(ctx)
		require.NoError(t, err)
		ackFns = append(ackFns, ackFn)
	}
	segments, err := shard.wal.loadAllSegmentsFromDisk()
	require.NoError(t, err)
	require.Greater(t, len(segments), 1)

	for _, ackFn := range ackFns {
		require.NoError(t, ackFn(ctx, nil))
	}
	require.NoError(t, shard.Close(ctx))

	segments, err = shard.wal.loadAllSegmentsFromDisk()
	require.NoError(t, err)
	require.Len(t, segments, 1)
}

func TestReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	shard, err := newWALShard(0, &WALOptions{
		LogDir:     dir,
		MaxLogSize: humanize.MiByte,
		MaxLogAge:  time.Minute,
	})
	require.NoError(t, err)
	ctx := t.Context()
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		b := makeBatch(i)
		err = shard.WriteBatch(ctx, b, func(ctx context.Context, err error) error {
			require.NoError(t, err)
			wg.Done()
			return nil
		})
		require.NoError(t, err)
	}
	wg.Wait()
	require.NoError(t, shard.Close(ctx))
	shard, err = newWALShard(0, &WALOptions{
		LogDir:     dir,
		MaxLogSize: humanize.MiByte,
		MaxLogAge:  time.Minute,
	})
	require.NoError(t, err)
	for i := range 10 {
		b, ackFn, err := shard.ReadBatch(ctx)
		require.NoError(t, err)
		require.NoError(t, ackFn(ctx, nil))
		require.JSONEq(t, fmt.Sprintf("[%d]", i), batchAsJSON(t, b))
	}
	require.NoError(t, shard.Close(ctx))
}

func TestEndOfInput(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	shard, err := newWALShard(0, &WALOptions{
		LogDir:     dir,
		MaxLogSize: humanize.MiByte,
		MaxLogAge:  time.Minute,
	})
	require.NoError(t, err)
	ctx := t.Context()
	for i := range 10 {
		b := makeBatch(i)
		err = shard.WriteBatch(ctx, b, makeAckFn(t))
		require.NoError(t, err)
	}
	shard.EndOfInput()
	for i := range 10 {
		b, ackFn, err := shard.ReadBatch(ctx)
		require.NoError(t, err)
		require.NoError(t, ackFn(ctx, nil))
		require.JSONEq(t, fmt.Sprintf("[%d]", i), batchAsJSON(t, b))
	}
	_, _, err = shard.ReadBatch(ctx)
	require.ErrorIs(t, err, service.ErrEndOfBuffer)
	require.NoError(t, shard.Close(ctx))
}

func TestEndOfInputWithReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	shard, err := newWALShard(0, &WALOptions{
		LogDir:     dir,
		MaxLogSize: humanize.MiByte,
		MaxLogAge:  time.Minute,
	})
	require.NoError(t, err)
	ctx := t.Context()
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		b := makeBatch(i)
		err = shard.WriteBatch(ctx, b, func(ctx context.Context, err error) error {
			require.NoError(t, err)
			wg.Done()
			return nil
		})
		require.NoError(t, err)
	}
	wg.Wait()
	require.NoError(t, shard.Close(ctx))
	shard, err = newWALShard(0, &WALOptions{
		LogDir:     dir,
		MaxLogSize: humanize.MiByte,
		MaxLogAge:  time.Minute,
	})
	require.NoError(t, err)
	shard.EndOfInput()
	for i := range 10 {
		b, ackFn, err := shard.ReadBatch(ctx)
		require.NoError(t, err)
		require.NoError(t, ackFn(ctx, nil))
		require.JSONEq(t, fmt.Sprintf("[%d]", i), batchAsJSON(t, b))
	}
	require.NoError(t, shard.Close(ctx))
	require.NoError(t, fs.WalkDir(os.DirFS(dir), ".", func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		stat, err := d.Info()
		if err != nil {
			return err
		}
		if stat.Size() > 0 {
			return fmt.Errorf("non empty file %s found after close", path)
		}
		return nil
	}))
}
