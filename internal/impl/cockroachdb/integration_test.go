// Copyright 2024 Redpanda Data, Inc.
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

package crdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationCRDB(t *testing.T) {
	integration.CheckSkip(t)

	tmpDir := t.TempDir()

	ctr, err := testcontainers.Run(t.Context(), "cockroachdb/cockroach:latest",
		testcontainers.WithCmd("start-single-node", "--insecure"),
		testcontainers.WithExposedPorts("8080/tcp", "26257/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/health").WithPort("8080/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mappedPort, err := ctr.MappedPort(t.Context(), "26257/tcp")
	require.NoError(t, err)
	port := mappedPort.Port()

	var pgpool *pgxpool.Pool
	require.Eventually(t, func() bool {
		if pgpool == nil {
			if pgpool, err = pgxpool.New(t.Context(), fmt.Sprintf("postgresql://root@localhost:%v/defaultdb?sslmode=disable", port)); err != nil {
				return false
			}
		}
		// Enable changefeeds
		if _, err = pgpool.Exec(t.Context(), "SET CLUSTER SETTING kv.rangefeed.enabled = true;"); err != nil {
			return false
		}
		// Create table
		_, err = pgpool.Exec(t.Context(), "CREATE TABLE foo (a INT PRIMARY KEY);")
		return err == nil
	}, time.Minute, time.Second)
	t.Cleanup(func() {
		pgpool.Close()
	})

	// Create a backlog of rows
	for i := range 100 {
		// Insert some rows
		if _, err = pgpool.Exec(t.Context(), fmt.Sprintf("INSERT INTO foo VALUES (%v);", i)); err != nil {
			return
		}
	}

	template := fmt.Sprintf(`
cockroachdb_changefeed:
  dsn: postgres://root@localhost:%v/defaultdb?sslmode=disable
  tables:
    - foo
  cursor_cache: foocache
  options:
    - resolved='1s'
    - min_checkpoint_frequency='1s'
`, port)

	cacheConf := fmt.Sprintf(`
label: foocache
file:
  directory: %v
`, tmpDir)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outBatches []string
	var outBatchMut sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		msgBytes, err := mb[0].AsBytes()
		require.NoError(t, err)
		outBatchMut.Lock()
		outBatches = append(outBatches, string(msgBytes))
		outBatchMut.Unlock()
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)

	go func() {
		_ = streamOut.Run(t.Context())
	}()

	for i := range 900 {
		// Insert some more rows in
		if _, err = pgpool.Exec(t.Context(), fmt.Sprintf("INSERT INTO foo VALUES (%v);", 100+i)); err != nil {
			t.Error(err)
		}
	}

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 1000
	}, time.Second*5, time.Millisecond*100)

	// The cursor only advances to RESOLVED timestamps whose rows are all
	// acked. Wait for a resolved checkpoint that postdates the moment every
	// row above was received, so the restart below resumes without
	// redelivery. Cursor values are "<nanos>.<logical>" HLC timestamps.
	cutoffNanos := time.Now().UnixNano()
	require.Eventually(t, func() bool {
		b, err := os.ReadFile(filepath.Join(tmpDir, "crdb_changefeed_cursor"))
		if err != nil {
			return false
		}
		nanos, err := strconv.ParseInt(strings.SplitN(string(b), ".", 2)[0], 10, 64)
		if err != nil {
			return false
		}
		return nanos > cutoffNanos
	}, time.Second*30, time.Millisecond*100, "cursor never advanced past the delivered rows")

	require.NoError(t, streamOut.StopWithin(time.Second*10))

	//--------------------------------------------------------------------------

	// Execute once more and ensure we don't backfil
	streamOutBuilder = service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	outBatches = nil
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		msgBytes, err := mb[0].AsBytes()
		require.NoError(t, err)
		outBatchMut.Lock()
		outBatches = append(outBatches, string(msgBytes))
		outBatchMut.Unlock()
		return nil
	}))

	streamOut, err = streamOutBuilder.Build()
	require.NoError(t, err)

	go func() {
		if err := streamOut.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()

	time.Sleep(time.Second)
	for i := range 50 {
		// Insert some more rows
		if _, err = pgpool.Exec(t.Context(), fmt.Sprintf("INSERT INTO foo VALUES (%v);", 1000+i)); err != nil {
			t.Error(err)
		}
	}

	var tmpSize int
	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		tmpSize = len(outBatches)
		return tmpSize == 50
	}, time.Second*10, time.Millisecond*100, "length: %v", tmpSize)

	require.NoError(t, streamOut.StopWithin(time.Second*10))
}

// TestIntegrationCRDBBackfillAckCrash verifies that acknowledging a single
// backfill row never persists a cursor that skips the rest of the backfill:
// every row of the initial scan shares one `updated` timestamp and CURSOR
// resume is exclusive, so the pre-fix per-row cursor lost the entire backfill
// after ack-one-then-crash. Only fully-acknowledged RESOLVED timestamps may
// persist. See CON-504.
func TestIntegrationCRDBBackfillAckCrash(t *testing.T) {
	integration.CheckSkip(t)

	tmpDir := t.TempDir()

	ctr, err := testcontainers.Run(t.Context(), "cockroachdb/cockroach:latest",
		testcontainers.WithCmd("start-single-node", "--insecure"),
		testcontainers.WithExposedPorts("8080/tcp", "26257/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/health").WithPort("8080/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mappedPort, err := ctr.MappedPort(t.Context(), "26257/tcp")
	require.NoError(t, err)
	port := mappedPort.Port()

	var pgpool *pgxpool.Pool
	require.Eventually(t, func() bool {
		if pgpool == nil {
			if pgpool, err = pgxpool.New(t.Context(), fmt.Sprintf("postgresql://root@localhost:%v/defaultdb?sslmode=disable", port)); err != nil {
				return false
			}
		}
		if _, err = pgpool.Exec(t.Context(), "SET CLUSTER SETTING kv.rangefeed.enabled = true;"); err != nil {
			return false
		}
		_, err = pgpool.Exec(t.Context(), "CREATE TABLE bar (a INT PRIMARY KEY);")
		return err == nil
	}, time.Minute, time.Second)
	t.Cleanup(func() {
		pgpool.Close()
	})

	const rowCount = 100
	for i := range rowCount {
		_, err := pgpool.Exec(t.Context(), fmt.Sprintf("INSERT INTO bar VALUES (%v);", i))
		require.NoError(t, err)
	}

	template := fmt.Sprintf(`
cockroachdb_changefeed:
  dsn: postgres://root@localhost:%v/defaultdb?sslmode=disable
  tables:
    - bar
  cursor_cache: barcache
  options:
    - resolved='1s'
    - min_checkpoint_frequency='1s'
`, port)

	cacheConf := fmt.Sprintf(`
label: barcache
file:
  directory: %v
`, tmpDir)

	readCursor := func() string {
		b, err := os.ReadFile(filepath.Join(tmpDir, "crdb_changefeed_cursor"))
		if err != nil {
			return ""
		}
		return string(b)
	}

	// Run 1: acknowledge exactly ONE backfill row, then block every other
	// delivery, and crash. The pre-fix code persisted the acked row's own
	// `updated` timestamp here, which on restart skipped the rest of the
	// backfill (all rows share that timestamp and CURSOR is exclusive).
	{
		streamOutBuilder := service.NewStreamBuilder()
		require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
		require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
		require.NoError(t, streamOutBuilder.AddInputYAML(template))

		received := make(chan struct{}, 1)
		var acked atomic.Bool
		require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(ctx context.Context, _ service.MessageBatch) error {
			if acked.CompareAndSwap(false, true) {
				return nil // ack the first row only
			}
			select {
			case received <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		}))

		streamOut, err := streamOutBuilder.Build()
		require.NoError(t, err)

		runCtx, crash := context.WithCancel(t.Context())
		runDone := make(chan struct{})
		go func() {
			defer close(runDone)
			_ = streamOut.Run(runCtx)
		}()

		select {
		case <-received:
		case <-time.After(time.Minute):
			t.Fatal("backfill rows were never delivered")
		}
		// Give the input time to (wrongly) persist a cursor from the single
		// acked row before crashing.
		time.Sleep(3 * time.Second)
		require.Empty(t, readCursor(), "no cursor may be persisted while backfill rows are unacknowledged")
		crash()
		select {
		case <-runDone:
		case <-time.After(30 * time.Second):
			t.Fatal("run 1 did not stop after the simulated crash")
		}
	}

	// Run 2: restart with a free-flowing consumer. Every backfill row must be
	// delivered.
	{
		streamOutBuilder := service.NewStreamBuilder()
		require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
		require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
		require.NoError(t, streamOutBuilder.AddInputYAML(template))

		var seenMut sync.Mutex
		seen := map[string]struct{}{}
		require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
			msgBytes, err := mb[0].AsBytes()
			require.NoError(t, err)
			seenMut.Lock()
			seen[string(msgBytes)] = struct{}{}
			seenMut.Unlock()
			return nil
		}))

		streamOut, err := streamOutBuilder.Build()
		require.NoError(t, err)
		go func() {
			if err := streamOut.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		}()

		var got int
		require.Eventually(t, func() bool {
			seenMut.Lock()
			got = len(seen)
			seenMut.Unlock()
			return got == rowCount
		}, time.Minute, time.Millisecond*100, "backfill rows were skipped after ack-one-then-crash: got %v of %v", got, rowCount)

		require.NoError(t, streamOut.StopWithin(time.Second*10))
	}
}
