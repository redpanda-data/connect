# MySQL Snapshot Chunking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in intra-table PK-range chunking and inter-table parallelism to the `mysql_cdc` snapshot phase, enabling a single large table to be read concurrently across multiple workers.

**Architecture:** Two new config fields (`snapshot_max_parallel_tables`, `snapshot_chunks_per_table`) gate the feature. When both are 1 (the default) the existing sequential path is byte-for-bit unchanged. When raised, multiple `REPEATABLE READ / CONSISTENT SNAPSHOT` transactions are opened under a single brief `FLUSH TABLES WITH READ LOCK` window; each table's integer primary-key range is split into N equal half-open chunks dispatched across the worker pool. Non-integer PKs fall back to whole-table reads with a log line.

**Tech Stack:** Go, `database/sql`, `golang.org/x/sync/errgroup`, `github.com/stretchr/testify`, `testcontainers-go` (integration tests)

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `internal/impl/mysql/snapshot.go` | Modify | Add `bounds *chunkBounds` parameter to `querySnapshotTable` |
| `internal/impl/mysql/snapshot_chunking.go` | Create | `chunkBounds`, `snapshotWorkUnit`, `splitIntRange`, `buildChunkPredicate`, `planSnapshotWork`, `isNumericPKColumn`, `tableIntBounds` |
| `internal/impl/mysql/snapshot_chunking_test.go` | Create | Unit tests for all pure functions in snapshot_chunking.go |
| `internal/impl/mysql/parallel_snapshot.go` | Create | `parallelSnapshotSet`, `prepareParallelSnapshotSet`, `distributeWorkToWorkers[T]` |
| `internal/impl/mysql/parallel_snapshot_test.go` | Create | Unit tests for `distributeWorkToWorkers` concurrency properties |
| `internal/impl/mysql/input_mysql_stream.go` | Modify | New config fields, `readSnapshotWorkUnit`, `runSequentialSnapshot`, `runParallelSnapshot`, routing in `startMySQLSync` |
| `internal/impl/mysql/config_test.go` | Modify | Config field defaults, valid values, and out-of-range rejection |
| `internal/impl/mysql/integration_test.go` | Modify | Chunked snapshot integration tests (int PK, composite PK, non-numeric fallback) |

---

## Task 1: Add config fields with validation

**Files:**
- Modify: `internal/impl/mysql/input_mysql_stream.go`
- Modify: `internal/impl/mysql/config_test.go`

- [ ] **Step 1: Write failing config tests**

Add to `internal/impl/mysql/config_test.go`:

```go
func TestConfig_SnapshotMaxParallelTables_DefaultAndExplicit(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		expected int
	}{
		{
			name: "default",
			yaml: `
dsn: user:password@tcp(localhost:3306)/db
tables: [a]
stream_snapshot: true
checkpoint_cache: foo
`,
			expected: 1,
		},
		{
			name: "explicit=8",
			yaml: `
dsn: user:password@tcp(localhost:3306)/db
tables: [a]
stream_snapshot: true
checkpoint_cache: foo
snapshot_max_parallel_tables: 8
`,
			expected: 8,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conf, err := mysqlStreamConfigSpec.ParseYAML(tc.yaml, nil)
			require.NoError(t, err)
			got, err := conf.FieldInt(fieldSnapshotMaxParallelTables)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestConfig_SnapshotMaxParallelTables_InvalidValuesRejected(t *testing.T) {
	tests := []struct {
		name  string
		value int
	}{
		{"zero", 0},
		{"negative", -5},
		{"above_upper_bound", maxSnapshotParallelTables + 1},
		{"absurdly_large", 10000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			yaml := fmt.Sprintf(`
dsn: user:password@tcp(localhost:3306)/db
tables: [a]
stream_snapshot: true
checkpoint_cache: foo
snapshot_max_parallel_tables: %d
`, tc.value)
			conf, err := mysqlStreamConfigSpec.ParseYAML(yaml, nil)
			require.NoError(t, err)
			got, err := conf.FieldInt(fieldSnapshotMaxParallelTables)
			require.NoError(t, err)
			assert.True(t, got < 1 || got > maxSnapshotParallelTables,
				"value should violate [1, %d]", maxSnapshotParallelTables)
		})
	}
}

func TestConfig_SnapshotChunksPerTable_DefaultAndExplicit(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		expected int
	}{
		{
			name: "default",
			yaml: `
dsn: user:password@tcp(localhost:3306)/db
tables: [a]
stream_snapshot: true
checkpoint_cache: foo
`,
			expected: 1,
		},
		{
			name: "explicit=16",
			yaml: `
dsn: user:password@tcp(localhost:3306)/db
tables: [a]
stream_snapshot: true
checkpoint_cache: foo
snapshot_chunks_per_table: 16
`,
			expected: 16,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conf, err := mysqlStreamConfigSpec.ParseYAML(tc.yaml, nil)
			require.NoError(t, err)
			got, err := conf.FieldInt(fieldSnapshotChunksPerTable)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestConfig_SnapshotChunksPerTable_InvalidValuesRejected(t *testing.T) {
	tests := []struct {
		name  string
		value int
	}{
		{"zero", 0},
		{"negative", -1},
		{"above_upper_bound", maxSnapshotChunksPerTable + 1},
		{"absurdly_large", 100000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			yaml := fmt.Sprintf(`
dsn: user:password@tcp(localhost:3306)/db
tables: [a]
stream_snapshot: true
checkpoint_cache: foo
snapshot_chunks_per_table: %d
`, tc.value)
			conf, err := mysqlStreamConfigSpec.ParseYAML(yaml, nil)
			require.NoError(t, err)
			got, err := conf.FieldInt(fieldSnapshotChunksPerTable)
			require.NoError(t, err)
			assert.True(t, got < 1 || got > maxSnapshotChunksPerTable,
				"value should violate [1, %d]", maxSnapshotChunksPerTable)
		})
	}
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
go test ./internal/impl/mysql/ -run 'TestConfig_Snapshot' -v 2>&1 | grep -E 'FAIL|PASS|undefined'
```

Expected: compile errors referencing undefined `fieldSnapshotMaxParallelTables`, `fieldSnapshotChunksPerTable`, `maxSnapshotParallelTables`, `maxSnapshotChunksPerTable`.

- [ ] **Step 3: Add constants and config spec fields**

In `internal/impl/mysql/input_mysql_stream.go`, extend the `const` block:

```go
const (
	fieldMySQLFlavor               = "flavor"
	fieldMySQLDSN                  = "dsn"
	fieldMySQLTables               = "tables"
	fieldStreamSnapshot            = "stream_snapshot"
	fieldSnapshotMaxBatchSize      = "snapshot_max_batch_size"
	fieldSnapshotMaxParallelTables = "snapshot_max_parallel_tables"
	fieldSnapshotChunksPerTable    = "snapshot_chunks_per_table"
	fieldMaxReconnectAttempts      = "max_reconnect_attempts"
	fieldBatching                  = "batching"
	fieldCheckpointKey             = "checkpoint_key"
	fieldCheckpointCache           = "checkpoint_cache"
	fieldCheckpointLimit           = "checkpoint_limit"
	fieldAWSIAMAuth                = "aws"
	// FieldAWSIAMAuthEnabled enabled field.
	FieldAWSIAMAuthEnabled = "enabled"

	shutdownTimeout = 5 * time.Second

	maxSnapshotParallelTables = 256
	maxSnapshotChunksPerTable = 256
)
```

In `mysqlStreamConfigSpec`, after the existing `fieldSnapshotMaxBatchSize` field:

```go
service.NewIntField(fieldSnapshotMaxParallelTables).
    Description("The maximum number of tables that may be snapshotted in parallel. When `1` (the default) tables are read sequentially using a single transaction. When higher, multiple `REPEATABLE READ` transactions are opened under a single brief `FLUSH TABLES ... WITH READ LOCK` window so every worker observes identical state at the same binlog position. Must be between `1` and `256`.").
    Advanced().
    Default(1),
service.NewIntField(fieldSnapshotChunksPerTable).
    Description("The number of primary-key chunks each table is split into during the snapshot. When `1` (the default) each table is read as a single unit. When higher, each table's first primary-key column is probed for `MIN` and `MAX` and the integer range is split into N equal half-open chunks dispatched across the `"+fieldSnapshotMaxParallelTables+"` worker pool. Only tables whose first PK column is an integer type (`tinyint`, `smallint`, `mediumint`, `int`, `integer`, `bigint`) are chunked; others fall back to a whole-table read. Must be between `1` and `256`.").
    Advanced().
    Default(1),
```

Add struct fields to `mysqlStreamInput`:

```go
fieldSnapshotMaxParallelTables int
fieldSnapshotChunksPerTable    int
```

Add parsing and validation in `newMySQLStreamInput`, after the existing `fieldSnapshotMaxBatchSize` block:

```go
if i.fieldSnapshotMaxParallelTables, err = conf.FieldInt(fieldSnapshotMaxParallelTables); err != nil {
    return nil, err
}
if i.fieldSnapshotMaxParallelTables < 1 {
    return nil, fmt.Errorf("field '%s' must be at least 1, got %d", fieldSnapshotMaxParallelTables, i.fieldSnapshotMaxParallelTables)
}
if i.fieldSnapshotMaxParallelTables > maxSnapshotParallelTables {
    return nil, fmt.Errorf("field '%s' must be at most %d, got %d", fieldSnapshotMaxParallelTables, maxSnapshotParallelTables, i.fieldSnapshotMaxParallelTables)
}

if i.fieldSnapshotChunksPerTable, err = conf.FieldInt(fieldSnapshotChunksPerTable); err != nil {
    return nil, err
}
if i.fieldSnapshotChunksPerTable < 1 {
    return nil, fmt.Errorf("field '%s' must be at least 1, got %d", fieldSnapshotChunksPerTable, i.fieldSnapshotChunksPerTable)
}
if i.fieldSnapshotChunksPerTable > maxSnapshotChunksPerTable {
    return nil, fmt.Errorf("field '%s' must be at most %d, got %d", fieldSnapshotChunksPerTable, maxSnapshotChunksPerTable, i.fieldSnapshotChunksPerTable)
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
go test ./internal/impl/mysql/ -run 'TestConfig_Snapshot' -v
```

Expected: all 8 subtests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/impl/mysql/input_mysql_stream.go internal/impl/mysql/config_test.go
git commit -m "mysql_cdc: add snapshot_max_parallel_tables and snapshot_chunks_per_table config fields"
```

---

## Task 2: Build the parallel snapshot infrastructure

**Files:**
- Create: `internal/impl/mysql/parallel_snapshot.go`
- Create: `internal/impl/mysql/parallel_snapshot_test.go`

- [ ] **Step 1: Write failing tests**

Create `internal/impl/mysql/parallel_snapshot_test.go`:

```go
// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDistributeWorkToWorkers_CoversEveryItemExactlyOnce(t *testing.T) {
	tables := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	for _, workers := range []int{1, 2, 3, 4, 8, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			var mu sync.Mutex
			var visited []string
			err := distributeWorkToWorkers(t.Context(), tables, workers, func(_ context.Context, _ int, table string) error {
				mu.Lock()
				visited = append(visited, table)
				mu.Unlock()
				return nil
			})
			require.NoError(t, err)
			sort.Strings(visited)
			expected := append([]string{}, tables...)
			sort.Strings(expected)
			assert.Equal(t, expected, visited)
		})
	}
}

func TestDistributeWorkToWorkers_WorkerCountCappedByItemCount(t *testing.T) {
	tables := []string{"a", "b"}
	var activeWorkers atomic.Int32
	var maxActive atomic.Int32
	err := distributeWorkToWorkers(t.Context(), tables, 16, func(_ context.Context, _ int, _ string) error {
		n := activeWorkers.Add(1)
		for {
			cur := maxActive.Load()
			if n <= cur || maxActive.CompareAndSwap(cur, n) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		activeWorkers.Add(-1)
		return nil
	})
	require.NoError(t, err)
	assert.LessOrEqual(t, int(maxActive.Load()), len(tables))
}

func TestDistributeWorkToWorkers_SingleWorkerIsSequential(t *testing.T) {
	tables := []string{"a", "b", "c", "d"}
	var mu sync.Mutex
	var inFlight, maxInFlight int
	err := distributeWorkToWorkers(t.Context(), tables, 1, func(_ context.Context, _ int, _ string) error {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, maxInFlight)
}

func TestDistributeWorkToWorkers_ErrorCancelsSiblings(t *testing.T) {
	tables := make([]string, 50)
	for i := range tables {
		tables[i] = fmt.Sprintf("t%d", i)
	}
	sentinel := errors.New("boom")
	var calls atomic.Int32
	err := distributeWorkToWorkers(t.Context(), tables, 4, func(ctx context.Context, _ int, table string) error {
		calls.Add(1)
		if table == "t5" {
			return sentinel
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
			return nil
		}
	})
	require.ErrorIs(t, err, sentinel)
	assert.Less(t, int(calls.Load()), len(tables))
}

func TestDistributeWorkToWorkers_ContextCancellation(t *testing.T) {
	tables := make([]string, 100)
	for i := range tables {
		tables[i] = fmt.Sprintf("t%d", i)
	}
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	err := distributeWorkToWorkers(ctx, tables, 4, func(ctx context.Context, _ int, _ string) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return nil
		}
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestDistributeWorkToWorkers_ZeroWorkersRejected(t *testing.T) {
	err := distributeWorkToWorkers(t.Context(), []string{"a"}, 0, func(context.Context, int, string) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), ">= 1")
}

func TestDistributeWorkToWorkers_EmptyItemsIsNoop(t *testing.T) {
	var called atomic.Bool
	err := distributeWorkToWorkers(t.Context(), nil, 4, func(context.Context, int, string) error {
		called.Store(true)
		return nil
	})
	require.NoError(t, err)
	assert.False(t, called.Load())
}

func TestDistributeWorkToWorkers_WorkerIdxInBounds(t *testing.T) {
	tables := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	const workerCount = 3
	var mu sync.Mutex
	seenIdxs := map[int]struct{}{}
	err := distributeWorkToWorkers(t.Context(), tables, workerCount, func(_ context.Context, idx int, _ string) error {
		mu.Lock()
		seenIdxs[idx] = struct{}{}
		mu.Unlock()
		assert.GreaterOrEqual(t, idx, 0)
		assert.Less(t, idx, workerCount)
		return nil
	})
	require.NoError(t, err)
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
go test ./internal/impl/mysql/ -run 'TestDistributeWorkToWorkers' -v 2>&1 | grep -E 'FAIL|undefined'
```

Expected: compile error — `distributeWorkToWorkers` undefined.

- [ ] **Step 3: Implement parallel_snapshot.go**

Create `internal/impl/mysql/parallel_snapshot.go`:

```go
// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/sync/errgroup"
)

// parallelSnapshotSet owns the shared *sql.DB and a pool of per-worker
// Snapshot instances. Every worker holds its own connection and its own
// REPEATABLE READ / CONSISTENT SNAPSHOT transaction, but all transactions
// were opened within a single FLUSH TABLES ... WITH READ LOCK window so
// they view identical state at the same binlog position.
type parallelSnapshotSet struct {
	db      *sql.DB
	workers []*Snapshot
	logger  *service.Logger
}

// prepareParallelSnapshotSet opens workerCount reader connections that all
// share a single globally-consistent MySQL snapshot:
//
//  1. Acquire FLUSH TABLES <tables> WITH READ LOCK on a dedicated connection.
//  2. Open workerCount snapshot connections, each starting a REPEATABLE READ
//     transaction followed by START TRANSACTION WITH CONSISTENT SNAPSHOT.
//  3. Capture the binlog position once (all workers share this position).
//  4. Release the table locks and return.
//
// Takes ownership of db. On error db is closed before returning.
func prepareParallelSnapshotSet(ctx context.Context, logger *service.Logger, db *sql.DB, tables []string, workerCount int) (*parallelSnapshotSet, *position, error) {
	if workerCount < 1 {
		_ = db.Close()
		return nil, nil, fmt.Errorf("parallel snapshot worker count must be >= 1, got %d", workerCount)
	}
	if len(tables) == 0 {
		_ = db.Close()
		return nil, nil, errors.New("no tables provided")
	}

	set := &parallelSnapshotSet{db: db, logger: logger}
	failWith := func(errs ...error) (*parallelSnapshotSet, *position, error) {
		errs = append(errs, set.close())
		return nil, nil, errors.Join(errs...)
	}

	lockConn, err := db.Conn(ctx)
	if err != nil {
		return failWith(fmt.Errorf("create lock connection: %w", err))
	}
	defer func() { _ = lockConn.Close() }()

	lockQuery := buildFlushAndLockTablesQuery(tables)
	logger.Infof("Acquiring table-level read locks for parallel snapshot (%d workers): %s", workerCount, lockQuery)
	if _, err := lockConn.ExecContext(ctx, lockQuery); err != nil {
		return failWith(fmt.Errorf("acquire table-level read locks: %w", err))
	}
	unlockTables := func() error {
		if _, err := lockConn.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
			return fmt.Errorf("release table-level read locks: %w", err)
		}
		return nil
	}

	for idx := 0; idx < workerCount; idx++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return failWith(fmt.Errorf("open snapshot connection %d: %w", idx, err), unlockTables())
		}
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
		if err != nil {
			_ = conn.Close()
			return failWith(fmt.Errorf("begin snapshot transaction %d: %w", idx, err), unlockTables())
		}
		// NOTE: this is a little sneaky because we're implicitly closing the
		// transaction started with BeginTx above and replacing it with this
		// one. We have to do this because the database/sql driver does not
		// support WITH CONSISTENT SNAPSHOT directly.
		if _, err := tx.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
			_ = tx.Rollback()
			_ = conn.Close()
			return failWith(fmt.Errorf("start consistent snapshot %d: %w", idx, err), unlockTables())
		}
		set.workers = append(set.workers, &Snapshot{
			tx:           tx,
			snapshotConn: conn,
			logger:       logger,
		})
	}

	pos, err := set.workers[0].getCurrentBinlogPosition(ctx)
	if err != nil {
		return failWith(fmt.Errorf("get binlog position: %w", err), unlockTables())
	}
	if err := unlockTables(); err != nil {
		return failWith(err)
	}
	return set, &pos, nil
}

func (p *parallelSnapshotSet) release(ctx context.Context) error {
	var errs []error
	for _, w := range p.workers {
		if err := w.releaseSnapshot(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (p *parallelSnapshotSet) close() error {
	var errs []error
	for _, w := range p.workers {
		if err := w.close(); err != nil {
			errs = append(errs, err)
		}
	}
	if p.db != nil {
		if err := p.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close db: %w", err))
		}
		p.db = nil
	}
	return errors.Join(errs...)
}

// distributeWorkToWorkers fans out items across workerCount goroutines via an
// errgroup. The first error cancels the shared context and is returned from
// Wait. Exposed as a generic helper so the fan-out logic can be unit-tested
// independently of MySQL types.
func distributeWorkToWorkers[T any](ctx context.Context, items []T, workerCount int, readFn func(context.Context, int, T) error) error {
	if workerCount < 1 {
		return fmt.Errorf("workerCount must be >= 1, got %d", workerCount)
	}
	if workerCount > len(items) {
		workerCount = len(items)
	}
	if workerCount == 0 {
		return nil
	}

	g, gctx := errgroup.WithContext(ctx)
	itemCh := make(chan T)

	g.Go(func() error {
		defer close(itemCh)
		for _, it := range items {
			select {
			case itemCh <- it:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})

	for w := 0; w < workerCount; w++ {
		workerIdx := w
		g.Go(func() error {
			for item := range itemCh {
				if err := readFn(gctx, workerIdx, item); err != nil {
					return err
				}
			}
			return nil
		})
	}

	return g.Wait()
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
go test ./internal/impl/mysql/ -run 'TestDistributeWorkToWorkers' -v
```

Expected: all 8 subtests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/impl/mysql/parallel_snapshot.go internal/impl/mysql/parallel_snapshot_test.go
git commit -m "mysql_cdc: add parallel snapshot infrastructure and distributeWorkToWorkers helper"
```

---

## Task 3: Build the chunking logic

**Files:**
- Create: `internal/impl/mysql/snapshot_chunking.go`
- Create: `internal/impl/mysql/snapshot_chunking_test.go`

- [ ] **Step 1: Write failing tests**

Create `internal/impl/mysql/snapshot_chunking_test.go`:

```go
// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitIntRange_SingleChunkWhenNLEOne(t *testing.T) {
	for _, n := range []int{0, 1, -3} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			got := splitIntRange(0, 100, n)
			require.Len(t, got, 1)
			assert.Nil(t, got[0].lo, "single chunk must be unbounded below")
			assert.Nil(t, got[0].hi, "single chunk must be unbounded above")
		})
	}
}

func TestSplitIntRange_SingleChunkWhenRangeCollapsed(t *testing.T) {
	for _, tc := range []struct{ lo, hi int64 }{{5, 5}, {10, 3}} {
		t.Run(fmt.Sprintf("lo=%d,hi=%d", tc.lo, tc.hi), func(t *testing.T) {
			got := splitIntRange(tc.lo, tc.hi, 4)
			require.Len(t, got, 1)
			assert.Nil(t, got[0].lo)
			assert.Nil(t, got[0].hi)
		})
	}
}

func TestSplitIntRange_OutermostChunksAreOpenEnded(t *testing.T) {
	got := splitIntRange(0, 100, 4)
	require.Len(t, got, 4)
	assert.Nil(t, got[0].lo, "first chunk must be unbounded below")
	assert.NotNil(t, got[0].hi)
	assert.NotNil(t, got[len(got)-1].lo)
	assert.Nil(t, got[len(got)-1].hi, "last chunk must be unbounded above")
}

func TestSplitIntRange_ChunksCoverAllIntegersExactlyOnce(t *testing.T) {
	lo, hi := int64(0), int64(50)
	for _, n := range []int{2, 3, 5, 7, 10, 16} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			got := splitIntRange(lo, hi, n)
			require.NotEmpty(t, got)
			for v := lo; v <= hi; v++ {
				covers := 0
				for _, c := range got {
					lower := c.lo == nil || v >= c.lo.(int64)
					upper := c.hi == nil || v < c.hi.(int64)
					if lower && upper {
						covers++
					}
				}
				assert.Equal(t, 1, covers, "value %d must belong to exactly one chunk", v)
			}
		})
	}
}

func TestSplitIntRange_WhenNExceedsSpanCoverageHolds(t *testing.T) {
	// Span < n — step is floored to 1. Open-ended outer chunks ensure every
	// row is still covered even if some chunks overlap pk values.
	got := splitIntRange(0, 3, 10)
	require.NotEmpty(t, got)
	for v := int64(0); v <= 3; v++ {
		covers := 0
		for _, c := range got {
			lower := c.lo == nil || v >= c.lo.(int64)
			upper := c.hi == nil || v < c.hi.(int64)
			if lower && upper {
				covers++
			}
		}
		assert.GreaterOrEqual(t, covers, 1, "value %d must be covered by at least one chunk", v)
	}
}

func TestSplitIntRange_LargeSpanNoOverflow(t *testing.T) {
	got := splitIntRange(math.MinInt64/2, math.MaxInt64/2, 8)
	require.Len(t, got, 8)
	assert.Nil(t, got[0].lo)
	assert.Nil(t, got[len(got)-1].hi)
}

func TestBuildChunkPredicate_NilReturnsEmpty(t *testing.T) {
	frag, args := buildChunkPredicate(nil)
	assert.Empty(t, frag)
	assert.Nil(t, args)
}

func TestBuildChunkPredicate_BothBounds(t *testing.T) {
	frag, args := buildChunkPredicate(&chunkBounds{firstPKCol: "id", lowerIncl: int64(10), upperExcl: int64(20)})
	assert.Equal(t, "`id` >= ? AND `id` < ?", frag)
	assert.Equal(t, []any{int64(10), int64(20)}, args)
}

func TestBuildChunkPredicate_OnlyLower(t *testing.T) {
	frag, args := buildChunkPredicate(&chunkBounds{firstPKCol: "id", lowerIncl: int64(10)})
	assert.Equal(t, "`id` >= ?", frag)
	assert.Equal(t, []any{int64(10)}, args)
}

func TestBuildChunkPredicate_OnlyUpper(t *testing.T) {
	frag, args := buildChunkPredicate(&chunkBounds{firstPKCol: "id", upperExcl: int64(20)})
	assert.Equal(t, "`id` < ?", frag)
	assert.Equal(t, []any{int64(20)}, args)
}

func TestBuildChunkPredicate_AllNilBoundsReturnsEmpty(t *testing.T) {
	frag, args := buildChunkPredicate(&chunkBounds{firstPKCol: "id"})
	assert.Empty(t, frag)
	assert.Nil(t, args)
}

func TestDistributeWorkToWorkers_WorksWithSnapshotWorkUnit(t *testing.T) {
	units := []snapshotWorkUnit{
		{table: "a"},
		{table: "b", bounds: &chunkBounds{firstPKCol: "id", upperExcl: int64(100)}},
		{table: "b", bounds: &chunkBounds{firstPKCol: "id", lowerIncl: int64(100)}},
	}
	var mu sync.Mutex
	var visited []snapshotWorkUnit
	var workerIdxMax atomic.Int32
	err := distributeWorkToWorkers(t.Context(), units, 2, func(_ context.Context, idx int, u snapshotWorkUnit) error {
		mu.Lock()
		visited = append(visited, u)
		mu.Unlock()
		for {
			cur := workerIdxMax.Load()
			if int32(idx) <= cur || workerIdxMax.CompareAndSwap(cur, int32(idx)) {
				break
			}
		}
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, visited, len(units))
	assert.LessOrEqual(t, int(workerIdxMax.Load()), 1)
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
go test ./internal/impl/mysql/ -run 'TestSplitIntRange|TestBuildChunkPredicate|TestDistributeWorkToWorkers_WorksWithSnapshot' -v 2>&1 | grep -E 'FAIL|undefined'
```

Expected: compile errors — `splitIntRange`, `buildChunkPredicate`, `chunkBounds`, `snapshotWorkUnit` undefined.

- [ ] **Step 3: Implement snapshot_chunking.go**

Create `internal/impl/mysql/snapshot_chunking.go`:

```go
// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// chunkBounds is a half-open range [lowerIncl, upperExcl) on the first column
// of a table's primary key. A nil bound means unbounded on that side. Combined
// with the keyset pagination in querySnapshotTable, a chunkBounds partitions
// one table's rows across multiple workers with neither overlap nor gap.
type chunkBounds struct {
	firstPKCol string
	lowerIncl  any
	upperExcl  any
}

// snapshotWorkUnit is one unit of work dispatched to a snapshot worker. Every
// table produces at least one unit: either a whole-table unit (bounds == nil)
// or multiple chunked units covering the table's primary-key space.
type snapshotWorkUnit struct {
	table  string
	bounds *chunkBounds
}

// numericPKDataTypes is the set of MySQL DATA_TYPE tokens for which snapshot
// chunking is supported. Tables whose first PK column is outside this set fall
// back to a whole-table read.
var numericPKDataTypes = map[string]struct{}{
	"tinyint":   {},
	"smallint":  {},
	"mediumint": {},
	"int":       {},
	"integer":   {},
	"bigint":    {},
}

// planSnapshotWork turns a table list into a work-unit list. For each table:
//
//   - chunksPerTable <= 1: emit one whole-table unit (no MIN/MAX query).
//   - First PK column is a supported integer type: query MIN/MAX under the
//     planner's snapshot transaction and split into chunksPerTable ranges.
//   - Otherwise: emit one whole-table unit and log the fallback reason.
//
// For composite PKs only the first column is used for chunking; keyset
// pagination within each chunk continues to respect the full PK ordering.
func planSnapshotWork(ctx context.Context, planner *Snapshot, tables []string, chunksPerTable int) ([]snapshotWorkUnit, error) {
	if chunksPerTable < 1 {
		chunksPerTable = 1
	}

	units := make([]snapshotWorkUnit, 0, len(tables))
	for _, table := range tables {
		if chunksPerTable == 1 {
			units = append(units, snapshotWorkUnit{table: table})
			continue
		}

		pks, err := planner.getTablePrimaryKeys(ctx, table)
		if err != nil {
			return nil, fmt.Errorf("chunk planning for %s: %w", table, err)
		}
		firstPK := pks[0]

		numeric, err := isNumericPKColumn(ctx, planner, table, firstPK)
		if err != nil {
			return nil, fmt.Errorf("inspect PK type for %s.%s: %w", table, firstPK, err)
		}
		if !numeric {
			planner.logger.Infof(
				"Snapshot chunking disabled for table %s: first PK column %s is non-numeric; reading as a single unit",
				table, firstPK)
			units = append(units, snapshotWorkUnit{table: table})
			continue
		}

		lo, hi, empty, err := tableIntBounds(ctx, planner, table, firstPK)
		if err != nil {
			return nil, fmt.Errorf("compute MIN/MAX for %s.%s: %w", table, firstPK, err)
		}
		if empty {
			units = append(units, snapshotWorkUnit{table: table})
			continue
		}

		for _, r := range splitIntRange(lo, hi, chunksPerTable) {
			units = append(units, snapshotWorkUnit{
				table: table,
				bounds: &chunkBounds{
					firstPKCol: firstPK,
					lowerIncl:  r.lo,
					upperExcl:  r.hi,
				},
			})
		}
	}
	return units, nil
}

func isNumericPKColumn(ctx context.Context, s *Snapshot, table, column string) (bool, error) {
	const q = `
SELECT DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?
`
	var dt string
	if err := s.tx.QueryRowContext(ctx, q, table, column).Scan(&dt); err != nil {
		return false, err
	}
	_, ok := numericPKDataTypes[strings.ToLower(dt)]
	return ok, nil
}

// tableIntBounds returns MIN(col), MAX(col) for an integer PK column.
// empty == true when the table has no rows (MIN and MAX return NULL).
func tableIntBounds(ctx context.Context, s *Snapshot, table, column string) (lo, hi int64, empty bool, err error) {
	q := fmt.Sprintf("SELECT MIN(`%s`), MAX(`%s`) FROM `%s`", column, column, table)
	var loN, hiN sql.NullInt64
	if err := s.tx.QueryRowContext(ctx, q).Scan(&loN, &hiN); err != nil {
		return 0, 0, false, err
	}
	if !loN.Valid || !hiN.Valid {
		return 0, 0, true, nil
	}
	return loN.Int64, hiN.Int64, false, nil
}

type intRange struct {
	lo any
	hi any
}

// splitIntRange splits [lo, hi] into n half-open chunks. The outermost chunks
// use nil bounds so rows at the exact MIN/MAX endpoints are never lost and
// any row outside [MIN, MAX] (which should not exist but might under unusual
// conditions) is still read. Every integer in [lo, hi] falls into exactly one
// chunk.
func splitIntRange(lo, hi int64, n int) []intRange {
	if n <= 1 || hi <= lo {
		return []intRange{{lo: nil, hi: nil}}
	}
	span := uint64(hi - lo)
	step := span / uint64(n)
	if step == 0 {
		step = 1
	}

	out := make([]intRange, 0, n)
	for i := 0; i < n; i++ {
		var loV, hiV any
		if i > 0 {
			loV = lo + int64(step*uint64(i))
		}
		if i < n-1 {
			hiV = lo + int64(step*uint64(i+1))
		}
		out = append(out, intRange{lo: loV, hi: hiV})
	}
	return out
}

// buildChunkPredicate returns a SQL fragment bounding the first PK column and
// the values to bind. Returns ("", nil) when b is nil or both bounds are nil —
// the caller should omit the WHERE clause in that case.
func buildChunkPredicate(b *chunkBounds) (string, []any) {
	if b == nil {
		return "", nil
	}
	var parts []string
	var args []any
	if b.lowerIncl != nil {
		parts = append(parts, fmt.Sprintf("`%s` >= ?", b.firstPKCol))
		args = append(args, b.lowerIncl)
	}
	if b.upperExcl != nil {
		parts = append(parts, fmt.Sprintf("`%s` < ?", b.firstPKCol))
		args = append(args, b.upperExcl)
	}
	if len(parts) == 0 {
		return "", nil
	}
	return strings.Join(parts, " AND "), args
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
go test ./internal/impl/mysql/ -run 'TestSplitIntRange|TestBuildChunkPredicate|TestDistributeWorkToWorkers_WorksWithSnapshot' -v
```

Expected: all subtests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/impl/mysql/snapshot_chunking.go internal/impl/mysql/snapshot_chunking_test.go
git commit -m "mysql_cdc: add snapshot chunking — splitIntRange, buildChunkPredicate, planSnapshotWork"
```

---

## Task 4: Extend querySnapshotTable to accept chunk bounds

**Files:**
- Modify: `internal/impl/mysql/snapshot.go`
- Modify: `internal/impl/mysql/input_mysql_stream.go` (single call site)

- [ ] **Step 1: Update the signature in snapshot.go**

In `internal/impl/mysql/snapshot.go`, change the `querySnapshotTable` signature from:

```go
func (s *Snapshot) querySnapshotTable(ctx context.Context, table string, pk []string, lastSeenPkVal *map[string]any, limit int) (*sql.Rows, error) {
```

to:

```go
func (s *Snapshot) querySnapshotTable(ctx context.Context, table string, pk []string, bounds *chunkBounds, lastSeenPkVal *map[string]any, limit int) (*sql.Rows, error) {
```

Replace the function body with:

```go
	snapshotQueryParts := []string{
		"SELECT * FROM " + table,
	}

	var whereParts []string
	var args []any

	if chunkPred, chunkArgs := buildChunkPredicate(bounds); chunkPred != "" {
		whereParts = append(whereParts, chunkPred)
		args = append(args, chunkArgs...)
	}

	if lastSeenPkVal != nil {
		var placeholders []string
		for _, pkCol := range pk {
			val, ok := (*lastSeenPkVal)[pkCol]
			if !ok {
				return nil, fmt.Errorf("primary key column '%s' not found in last seen values", pkCol)
			}
			args = append(args, val)
			placeholders = append(placeholders, "?")
		}
		whereParts = append(whereParts, fmt.Sprintf("(%s) > (%s)", strings.Join(pk, ", "), strings.Join(placeholders, ", ")))
	}

	if len(whereParts) > 0 {
		snapshotQueryParts = append(snapshotQueryParts, "WHERE "+strings.Join(whereParts, " AND "))
	}
	snapshotQueryParts = append(snapshotQueryParts, buildOrderByClause(pk))
	snapshotQueryParts = append(snapshotQueryParts, "LIMIT ?")
	args = append(args, limit)

	q := strings.Join(snapshotQueryParts, " ")
	s.logger.Infof("Querying snapshot: %s", q)
	return s.tx.QueryContext(ctx, q, args...)
```

- [ ] **Step 2: Fix the existing call site in input_mysql_stream.go**

In `internal/impl/mysql/input_mysql_stream.go`, the `readSnapshot` function calls `querySnapshotTable` twice. Update both calls to pass `nil` for bounds (preserving current behavior):

Change:
```go
batchRows, err = snapshot.querySnapshotTable(ctx, table, tablePks, nil, i.fieldSnapshotMaxBatchSize)
```
to:
```go
batchRows, err = snapshot.querySnapshotTable(ctx, table, tablePks, nil, nil, i.fieldSnapshotMaxBatchSize)
```

Change:
```go
batchRows, err = snapshot.querySnapshotTable(ctx, table, tablePks, &lastSeenPksValues, i.fieldSnapshotMaxBatchSize)
```
to:
```go
batchRows, err = snapshot.querySnapshotTable(ctx, table, tablePks, nil, &lastSeenPksValues, i.fieldSnapshotMaxBatchSize)
```

- [ ] **Step 3: Confirm the build still compiles**

```bash
go build ./internal/impl/mysql/
```

Expected: clean build.

- [ ] **Step 4: Run all unit tests to confirm nothing regressed**

```bash
go test ./internal/impl/mysql/ -run 'Test' -v -count=1 2>&1 | grep -E '^(---|\s+(FAIL|PASS)|FAIL|ok)'
```

Expected: all non-integration tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/impl/mysql/snapshot.go internal/impl/mysql/input_mysql_stream.go
git commit -m "mysql_cdc: add bounds parameter to querySnapshotTable for chunk-range filtering"
```

---

## Task 5: Extract readSnapshotWorkUnit and wire up the parallel and chunked paths

**Files:**
- Modify: `internal/impl/mysql/input_mysql_stream.go`

- [ ] **Step 1: Extract readSnapshotWorkUnit from readSnapshot**

In `internal/impl/mysql/input_mysql_stream.go`, replace the `readSnapshot` function and add the extracted function. The goal is:
- `readSnapshotWorkUnit` contains the per-unit loop (schema cache, PK lookup, keyset pagination, row emission)
- `readSnapshot` becomes a thin loop that calls `readSnapshotWorkUnit` for each table as a whole-table unit

Replace the existing `readSnapshot` body with:

```go
func (i *mysqlStreamInput) readSnapshot(ctx context.Context, snapshot *Snapshot) error {
	for _, table := range i.tables {
		if err := i.readSnapshotWorkUnit(ctx, snapshot, snapshotWorkUnit{table: table}); err != nil {
			return err
		}
	}
	return nil
}

func (i *mysqlStreamInput) readSnapshotWorkUnit(ctx context.Context, snapshot *Snapshot, unit snapshotWorkUnit) error {
	table := unit.table
	if tbl, err := i.canal.GetTable(i.mysqlConfig.DBName, table); err == nil {
		if _, err := i.getTableSchema(tbl); err != nil {
			i.logger.Warnf("Failed to pre-populate schema for table %s during snapshot: %v", table, err)
		}
	} else {
		i.logger.Warnf("Failed to fetch schema for table %s during snapshot: %v", table, err)
	}

	tablePks, err := snapshot.getTablePrimaryKeys(ctx, table)
	if err != nil {
		return err
	}
	i.logger.Tracef("primary keys for table %s: %v", table, tablePks)

	lastSeenPksValues := map[string]any{}
	for _, pk := range tablePks {
		lastSeenPksValues[pk] = nil
	}

	var numRowsProcessed int
	for {
		var batchRows *sql.Rows
		if numRowsProcessed == 0 {
			batchRows, err = snapshot.querySnapshotTable(ctx, table, tablePks, unit.bounds, nil, i.fieldSnapshotMaxBatchSize)
		} else {
			batchRows, err = snapshot.querySnapshotTable(ctx, table, tablePks, unit.bounds, &lastSeenPksValues, i.fieldSnapshotMaxBatchSize)
		}
		if err != nil {
			return fmt.Errorf("executing snapshot table query: %s", err)
		}

		types, err := batchRows.ColumnTypes()
		if err != nil {
			return fmt.Errorf("fetching column types: %s", err)
		}
		values, mappers := prepSnapshotScannerAndMappers(types)
		columns, err := batchRows.Columns()
		if err != nil {
			return fmt.Errorf("fetching columns: %s", err)
		}

		var batchRowsCount int
		for batchRows.Next() {
			numRowsProcessed++
			batchRowsCount++
			if err := batchRows.Scan(values...); err != nil {
				return err
			}
			row := map[string]any{}
			for idx, value := range values {
				v, err := mappers[idx](value)
				if err != nil {
					return err
				}
				row[columns[idx]] = v
				if _, ok := lastSeenPksValues[columns[idx]]; ok {
					lastSeenPksValues[columns[idx]] = value
				}
			}
			select {
			case i.rawMessageEvents <- MessageEvent{
				Row:       row,
				Operation: MessageOperationRead,
				Table:     table,
				Position:  nil,
			}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := batchRows.Err(); err != nil {
			return fmt.Errorf("iterating snapshot table: %s", err)
		}
		if batchRowsCount < i.fieldSnapshotMaxBatchSize {
			break
		}
	}
	return nil
}
```

- [ ] **Step 2: Add runSequentialSnapshot and runParallelSnapshot**

Add these two functions to `input_mysql_stream.go`:

```go
func (i *mysqlStreamInput) runSequentialSnapshot(ctx context.Context, snapshot *Snapshot) (*position, error) {
	startPos, err := snapshot.prepareSnapshot(ctx, i.tables)
	if err != nil {
		_ = snapshot.close()
		return nil, fmt.Errorf("unable to prepare snapshot: %w", err)
	}
	if err = i.readSnapshot(ctx, snapshot); err != nil {
		_ = snapshot.close()
		return nil, fmt.Errorf("failed reading snapshot: %w", err)
	}
	if err = snapshot.releaseSnapshot(ctx); err != nil {
		_ = snapshot.close()
		return nil, fmt.Errorf("unable to release snapshot: %w", err)
	}
	if err = snapshot.close(); err != nil {
		return nil, fmt.Errorf("unable to close snapshot: %w", err)
	}
	return startPos, nil
}

func (i *mysqlStreamInput) runParallelSnapshot(ctx context.Context, snapshot *Snapshot) (*position, error) {
	// Transfer db ownership to the parallel set before any failure path:
	// prepareParallelSnapshotSet will close db on error, and we want
	// snapshot.close() to be a safe no-op afterwards.
	db := snapshot.db
	snapshot.db = nil

	workerCount := i.fieldSnapshotMaxParallelTables
	if maxUnits := len(i.tables) * i.fieldSnapshotChunksPerTable; workerCount > maxUnits {
		workerCount = maxUnits
	}

	set, startPos, err := prepareParallelSnapshotSet(ctx, i.logger, db, i.tables, workerCount)
	if err != nil {
		return nil, fmt.Errorf("unable to prepare parallel snapshot: %w", err)
	}

	units, err := planSnapshotWork(ctx, set.workers[0], i.tables, i.fieldSnapshotChunksPerTable)
	if err != nil {
		_ = set.close()
		return nil, fmt.Errorf("plan snapshot work: %w", err)
	}
	i.logger.Infof("Parallel snapshot planned: %d tables -> %d work units across %d workers", len(i.tables), len(units), len(set.workers))

	if err := distributeWorkToWorkers(ctx, units, len(set.workers), func(ctx context.Context, workerIdx int, unit snapshotWorkUnit) error {
		return i.readSnapshotWorkUnit(ctx, set.workers[workerIdx], unit)
	}); err != nil {
		_ = set.close()
		return nil, fmt.Errorf("failed reading snapshot: %w", err)
	}

	if err := set.release(ctx); err != nil {
		_ = set.close()
		return nil, fmt.Errorf("unable to release parallel snapshot: %w", err)
	}
	if err := set.close(); err != nil {
		return nil, fmt.Errorf("unable to close parallel snapshot: %w", err)
	}
	return startPos, nil
}
```

- [ ] **Step 3: Update the routing in startMySQLSync**

In `startMySQLSync`, replace the existing snapshot block:

```go
if snapshot != nil {
    startPos, err := snapshot.prepareSnapshot(ctx, i.tables)
    if err != nil {
        _ = snapshot.close()
        return fmt.Errorf("unable to prepare snapshot: %w", err)
    }
    if err = i.readSnapshot(ctx, snapshot); err != nil {
        _ = snapshot.close()
        return fmt.Errorf("failed reading snapshot: %w", err)
    }
    if err = snapshot.releaseSnapshot(ctx); err != nil {
        _ = snapshot.close()
        return fmt.Errorf("unable to release snapshot: %w", err)
    }
    if err = snapshot.close(); err != nil {
        return fmt.Errorf("unable to close snapshot: %w", err)
    }
```

with:

```go
if snapshot != nil {
    var startPos *position
    var err error
    if i.fieldSnapshotMaxParallelTables <= 1 && i.fieldSnapshotChunksPerTable <= 1 {
        startPos, err = i.runSequentialSnapshot(ctx, snapshot)
    } else {
        startPos, err = i.runParallelSnapshot(ctx, snapshot)
    }
    if err != nil {
        return err
    }
```

- [ ] **Step 4: Build and run all unit tests**

```bash
go build ./internal/impl/mysql/ && go test ./internal/impl/mysql/ -run 'Test' -v -count=1 2>&1 | grep -E '^(---|\s+(FAIL|PASS)|FAIL|ok)'
```

Expected: clean build, all non-integration tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/impl/mysql/input_mysql_stream.go
git commit -m "mysql_cdc: wire chunked parallel snapshot path — readSnapshotWorkUnit, runSequentialSnapshot, runParallelSnapshot"
```

---

## Task 6: Integration tests

**Files:**
- Modify: `internal/impl/mysql/integration_test.go`

Integration tests require Docker. They are skipped automatically when Docker is unavailable. Check an existing integration test (e.g. `TestIntegrationMySQLCDCAllTypes`) for the exact helper pattern used to spin up a MySQL container.

- [ ] **Step 1: Add TestIntegrationMySQLChunkedSnapshot**

Add to `internal/impl/mysql/integration_test.go`:

```go
// TestIntegrationMySQLChunkedSnapshot verifies that snapshot_chunks_per_table
// splits a table into N chunks and that the union of all chunks contains every
// row with no duplicates. Exercises both a single-column int PK table and a
// composite (int, int) PK table.
func TestIntegrationMySQLChunkedSnapshot(t *testing.T) {
	integration.CheckSkip(t)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	resource, dsn := startMySQLContainer(t, pool)
	defer func() { _ = pool.Purge(resource) }()

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Single-column int PK table
	_, err = db.Exec(`CREATE TABLE chunk_test (
		id   INT PRIMARY KEY,
		val  VARCHAR(64)
	)`)
	require.NoError(t, err)

	const rowCount = 100
	for i := 0; i < rowCount; i++ {
		_, err = db.Exec("INSERT INTO chunk_test (id, val) VALUES (?, ?)", i, fmt.Sprintf("row-%d", i))
		require.NoError(t, err)
	}

	// Composite (int, int) PK table
	_, err = db.Exec(`CREATE TABLE chunk_composite (
		a    INT,
		b    INT,
		val  VARCHAR(64),
		PRIMARY KEY (a, b)
	)`)
	require.NoError(t, err)

	for a := 0; a < 10; a++ {
		for b := 0; b < 10; b++ {
			_, err = db.Exec("INSERT INTO chunk_composite (a, b, val) VALUES (?, ?, ?)", a, b, fmt.Sprintf("%d-%d", a, b))
			require.NoError(t, err)
		}
	}

	conf := buildTestMySQLConfig(t, dsn, []string{"chunk_test", "chunk_composite"}, map[string]any{
		"stream_snapshot":              true,
		"snapshot_max_parallel_tables": 4,
		"snapshot_chunks_per_table":    8,
	})

	rows := runSnapshotAndCollect(t, conf, rowCount+100)

	// Verify no duplicate primary keys per table
	chunkTestIDs := map[int]int{}
	compositeKeys := map[string]int{}
	for _, row := range rows {
		switch row["table"] {
		case "chunk_test":
			id := row["id"].(int32)
			chunkTestIDs[int(id)]++
		case "chunk_composite":
			key := fmt.Sprintf("%v:%v", row["a"], row["b"])
			compositeKeys[key]++
		}
	}
	assert.Len(t, chunkTestIDs, rowCount, "all rows from chunk_test must be present")
	for id, count := range chunkTestIDs {
		assert.Equal(t, 1, count, "row %d in chunk_test appeared %d times", id, count)
	}
	assert.Len(t, compositeKeys, 100, "all rows from chunk_composite must be present")
	for key, count := range compositeKeys {
		assert.Equal(t, 1, count, "row %s in chunk_composite appeared %d times", key, count)
	}
}

// TestIntegrationMySQLChunkedSnapshotNonNumericPKFallback confirms that a
// table with a VARCHAR primary key falls back to a whole-table read without
// error when chunking is requested.
func TestIntegrationMySQLChunkedSnapshotNonNumericPKFallback(t *testing.T) {
	integration.CheckSkip(t)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	resource, dsn := startMySQLContainer(t, pool)
	defer func() { _ = pool.Purge(resource) }()

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE varchar_pk_test (
		id  VARCHAR(64) PRIMARY KEY,
		val INT
	)`)
	require.NoError(t, err)

	const rowCount = 20
	for i := 0; i < rowCount; i++ {
		_, err = db.Exec("INSERT INTO varchar_pk_test (id, val) VALUES (?, ?)", fmt.Sprintf("key-%d", i), i)
		require.NoError(t, err)
	}

	conf := buildTestMySQLConfig(t, dsn, []string{"varchar_pk_test"}, map[string]any{
		"stream_snapshot":              true,
		"snapshot_max_parallel_tables": 2,
		"snapshot_chunks_per_table":    4,
	})

	rows := runSnapshotAndCollect(t, conf, rowCount)
	assert.Len(t, rows, rowCount, "all rows must be read even with non-numeric PK")
}
```

Note: `buildTestMySQLConfig`, `startMySQLContainer`, and `runSnapshotAndCollect` are helpers already used by existing integration tests in this file. Check how existing tests like `TestIntegrationMySQLCDCAllTypes` spin up their container and collect messages, and follow the same pattern exactly. The helpers above are representative of what you need; rename them to match what already exists in the file.

- [ ] **Step 2: Run the integration tests**

```bash
go test ./internal/impl/mysql/ -run 'TestIntegrationMySQLChunked' -v -tags integration
```

Expected: both tests PASS (requires Docker).

- [ ] **Step 3: Commit**

```bash
git add internal/impl/mysql/integration_test.go
git commit -m "mysql_cdc: add chunked snapshot integration tests"
```

---

## Task 7: Final verification

- [ ] **Step 1: Run the full unit test suite**

```bash
go test ./internal/impl/mysql/ -run 'Test' -count=1 -v 2>&1 | grep -E '^(---|\s+(FAIL|PASS)|FAIL|ok)'
```

Expected: all non-integration tests PASS.

- [ ] **Step 2: Run the linter**

```bash
task lint
```

Expected: no new lint errors.

- [ ] **Step 3: Build all distributions**

```bash
task build:all
```

Expected: clean build across all four distributions.
