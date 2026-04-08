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

//go:build (darwin && (amd64 || arm64)) || (freebsd && (amd64 || arm64)) || (linux && (386 || amd64 || arm || arm64 || riscv64)) || (windows && (amd64 || arm64))

package sql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	_ "modernc.org/sqlite"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func testOutput(db *sql.DB, queries []rawQueryStatement, hasWhen bool) *sqlRawOutput {
	return &sqlRawOutput{
		driver:            "sqlite",
		dsn:               ":memory:",
		db:                db,
		queries:           queries,
		hasWhenConditions: hasWhen,
		argsConverter:     func(v []any) []any { return v },
		connSettings:      &connSettings{},
		logger:            service.MockResources().Logger(),
		shutSig:           shutdown.NewSignaller(),
	}
}

func TestSQLRawOutputSingleQueryBatch(t *testing.T) {
	db := openTestDB(t)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, `CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
	require.NoError(t, err)

	out := testOutput(db, []rawQueryStatement{
		{static: "INSERT INTO test (id, name) VALUES (?, ?)"},
	}, false)

	// Batch of 3 messages — single query, per-message execution.
	batch := service.MessageBatch{
		service.NewMessage(nil),
		service.NewMessage(nil),
		service.NewMessage(nil),
	}
	env := bloblang.NewEnvironment()
	argsMap, err := env.Parse(`root = [this.id, this.name]`)
	require.NoError(t, err)
	out.queries[0].argsMapping = argsMap

	for i, msg := range batch {
		msg.SetStructured(map[string]any{"id": i + 1, "name": fmt.Sprintf("user%d", i+1)})
	}

	require.NoError(t, out.WriteBatch(ctx, batch))

	var count int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&count))
	assert.Equal(t, 3, count)
}

func TestSQLRawOutputBatchTransactionRollback(t *testing.T) {
	db := openTestDB(t)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, `CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE audit (id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT)`)
	require.NoError(t, err)

	// Pre-insert a row that will conflict.
	_, err = db.ExecContext(ctx, `INSERT INTO test (id, name) VALUES (2, 'existing')`)
	require.NoError(t, err)

	env := bloblang.NewEnvironment()
	insertArgs, err := env.Parse(`root = [this.id, this.name]`)
	require.NoError(t, err)
	auditArgs, err := env.Parse(`root = ["inserted " + this.name]`)
	require.NoError(t, err)

	// Use two queries to exercise the transactional batch path.
	out := testOutput(db, []rawQueryStatement{
		{static: "INSERT INTO test (id, name) VALUES (?, ?)", argsMapping: insertArgs},
		{static: "INSERT INTO audit (action) VALUES (?)", argsMapping: auditArgs},
	}, false)

	// Batch where msg[1] will cause a PK conflict on the first query.
	batch := service.MessageBatch{
		service.NewMessage(nil),
		service.NewMessage(nil), // id=2 conflicts
		service.NewMessage(nil),
	}
	batch[0].SetStructured(map[string]any{"id": 10, "name": "new1"})
	batch[1].SetStructured(map[string]any{"id": 2, "name": "conflict"})
	batch[2].SetStructured(map[string]any{"id": 11, "name": "new2"})

	err = out.WriteBatch(ctx, batch)
	require.Error(t, err)

	// Because of transaction rollback, only the pre-existing row should remain.
	var testCount int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&testCount))
	assert.Equal(t, 1, testCount, "transaction should have rolled back, leaving only the pre-existing row")

	// Audit table should also be empty — the whole transaction rolled back.
	var auditCount int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&auditCount))
	assert.Equal(t, 0, auditCount, "audit rows should have been rolled back too")
}

func TestSQLRawOutputSingleQueryPerMessageErrors(t *testing.T) {
	db := openTestDB(t)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, `CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
	require.NoError(t, err)

	// Pre-insert a row that will conflict.
	_, err = db.ExecContext(ctx, `INSERT INTO test (id, name) VALUES (2, 'existing')`)
	require.NoError(t, err)

	env := bloblang.NewEnvironment()
	argsMap, err := env.Parse(`root = [this.id, this.name]`)
	require.NoError(t, err)

	// Single query — uses non-transactional per-message path.
	out := testOutput(db, []rawQueryStatement{
		{static: "INSERT INTO test (id, name) VALUES (?, ?)", argsMapping: argsMap},
	}, false)

	batch := service.MessageBatch{
		service.NewMessage(nil),
		service.NewMessage(nil), // id=2 conflicts
		service.NewMessage(nil),
	}
	batch[0].SetStructured(map[string]any{"id": 10, "name": "new1"})
	batch[1].SetStructured(map[string]any{"id": 2, "name": "conflict"})
	batch[2].SetStructured(map[string]any{"id": 11, "name": "new2"})

	err = out.WriteBatch(ctx, batch)
	require.Error(t, err)

	// Per-message execution: messages 0 and 2 succeed, only message 1 fails.
	// So we should have the pre-existing row + 2 new rows = 3 total.
	var count int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&count))
	assert.Equal(t, 3, count, "successful messages should persist despite one message failing")
}

func TestSQLRawOutputConditionalQueries(t *testing.T) {
	db := openTestDB(t)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, `CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
	require.NoError(t, err)

	// Pre-insert rows that may be deleted.
	_, err = db.ExecContext(ctx, `INSERT INTO test (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')`)
	require.NoError(t, err)

	env := bloblang.NewEnvironment()

	whenDelete, err := env.Parse(`root = this.op == "delete"`)
	require.NoError(t, err)
	deleteArgs, err := env.Parse(`root = [this.id]`)
	require.NoError(t, err)
	upsertArgs, err := env.Parse(`root = [this.id, this.name]`)
	require.NoError(t, err)

	out := testOutput(db, []rawQueryStatement{
		{
			static:      "DELETE FROM test WHERE id = ?",
			whenMapping: whenDelete,
			argsMapping: deleteArgs,
		},
		{
			static:      "INSERT OR REPLACE INTO test (id, name) VALUES (?, ?)",
			argsMapping: upsertArgs,
		},
	}, true)

	batch := service.MessageBatch{
		service.NewMessage(nil), // delete id=1
		service.NewMessage(nil), // upsert id=4
		service.NewMessage(nil), // upsert id=2 (update)
	}
	batch[0].SetStructured(map[string]any{"op": "delete", "id": 1})
	batch[1].SetStructured(map[string]any{"op": "upsert", "id": 4, "name": "dave"})
	batch[2].SetStructured(map[string]any{"op": "upsert", "id": 2, "name": "bob_updated"})

	require.NoError(t, out.WriteBatch(ctx, batch))

	// id=1 should be deleted, id=2 updated, id=3 unchanged, id=4 inserted.
	rows, err := db.QueryContext(ctx, "SELECT id, name FROM test ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		id   int
		name string
	}
	var results []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.id, &r.name))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	expected := []row{
		{2, "bob_updated"},
		{3, "charlie"},
		{4, "dave"},
	}
	assert.Equal(t, expected, results)
}

func TestSQLRawOutputConditionalQueryNoMatch(t *testing.T) {
	db := openTestDB(t)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, `CREATE TABLE test (id INTEGER PRIMARY KEY)`)
	require.NoError(t, err)

	env := bloblang.NewEnvironment()
	whenNever, err := env.Parse(`root = false`)
	require.NoError(t, err)
	argsMap, err := env.Parse(`root = [this.id]`)
	require.NoError(t, err)

	out := testOutput(db, []rawQueryStatement{
		{
			static:      "INSERT INTO test (id) VALUES (?)",
			whenMapping: whenNever,
			argsMapping: argsMap,
		},
	}, true)

	batch := service.MessageBatch{service.NewMessage(nil)}
	batch[0].SetStructured(map[string]any{"id": 1})

	// Should not error even though no query matched.
	require.NoError(t, out.WriteBatch(ctx, batch))

	var count int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&count))
	assert.Equal(t, 0, count)
}

func TestSQLRawOutputPartitionOrdering(t *testing.T) {
	db := openTestDB(t)
	ctx := t.Context()

	// Use a table with an autoincrement to track insertion order.
	_, err := db.ExecContext(ctx, `CREATE TABLE test (seq INTEGER PRIMARY KEY AUTOINCREMENT, partition_id INTEGER, offset_id INTEGER)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE audit (id INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT)`)
	require.NoError(t, err)

	env := bloblang.NewEnvironment()
	argsMap, err := env.Parse(`root = [this.partition, this.offset]`)
	require.NoError(t, err)
	auditArgs, err := env.Parse(`root = ["ok"]`)
	require.NoError(t, err)

	// Use two queries to exercise the transactional + partition-ordered path.
	out := testOutput(db, []rawQueryStatement{
		{static: "INSERT INTO test (partition_id, offset_id) VALUES (?, ?)", argsMapping: argsMap},
		{static: "INSERT INTO audit (msg) VALUES (?)", argsMapping: auditArgs},
	}, false)

	// Create a batch with interleaved partitions.
	// Batch order: p0-o0, p1-o0, p0-o1, p1-o1, p2-o0
	batch := service.MessageBatch{
		service.NewMessage(nil),
		service.NewMessage(nil),
		service.NewMessage(nil),
		service.NewMessage(nil),
		service.NewMessage(nil),
	}

	data := []struct {
		partition int
		offset    int
	}{
		{0, 0}, {1, 0}, {0, 1}, {1, 1}, {2, 0},
	}
	for i, d := range data {
		batch[i].SetStructured(map[string]any{"partition": d.partition, "offset": d.offset})
		batch[i].MetaSetMut("kafka_partition", fmt.Sprintf("%d", d.partition))
	}

	require.NoError(t, out.WriteBatch(ctx, batch))

	// After partition ordering, execution order should be:
	// partition 0: offset 0, offset 1
	// partition 1: offset 0, offset 1
	// partition 2: offset 0
	rows, err := db.QueryContext(ctx, "SELECT partition_id, offset_id FROM test ORDER BY seq")
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		partition, offset int
	}
	var results []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.partition, &r.offset))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	expected := []row{
		{0, 0}, {0, 1}, // partition 0 in order
		{1, 0}, {1, 1}, // partition 1 in order
		{2, 0}, // partition 2
	}
	assert.Equal(t, expected, results)
}

func TestSQLRawOutputSingleQueryBackwardCompat(t *testing.T) {
	db := openTestDB(t)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, `CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)`)
	require.NoError(t, err)

	env := bloblang.NewEnvironment()
	argsMap, err := env.Parse(`root = [this.id, this.val]`)
	require.NoError(t, err)

	// Single query (no `queries` list), mimicking the legacy `query` field.
	out := testOutput(db, []rawQueryStatement{
		{static: "INSERT INTO test (id, val) VALUES (?, ?)", argsMapping: argsMap},
	}, false)

	batch := service.MessageBatch{
		service.NewMessage(nil),
		service.NewMessage(nil),
	}
	batch[0].SetStructured(map[string]any{"id": 1, "val": "a"})
	batch[1].SetStructured(map[string]any{"id": 2, "val": "b"})

	require.NoError(t, out.WriteBatch(ctx, batch))

	var count int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&count))
	assert.Equal(t, 2, count)
}

func TestSQLRawOutputMultiStatementTransaction(t *testing.T) {
	db := openTestDB(t)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, `CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE audit (id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT)`)
	require.NoError(t, err)

	env := bloblang.NewEnvironment()
	insertArgs, err := env.Parse(`root = [this.id, this.name]`)
	require.NoError(t, err)
	auditArgs, err := env.Parse(`root = ["inserted " + this.name]`)
	require.NoError(t, err)

	// Two queries per message (no when conditions) — both execute for each msg.
	out := testOutput(db, []rawQueryStatement{
		{static: "INSERT INTO test (id, name) VALUES (?, ?)", argsMapping: insertArgs},
		{static: "INSERT INTO audit (action) VALUES (?)", argsMapping: auditArgs},
	}, false)

	batch := service.MessageBatch{
		service.NewMessage(nil),
		service.NewMessage(nil),
	}
	batch[0].SetStructured(map[string]any{"id": 1, "name": "alice"})
	batch[1].SetStructured(map[string]any{"id": 2, "name": "bob"})

	require.NoError(t, out.WriteBatch(ctx, batch))

	var testCount, auditCount int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&testCount))
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&auditCount))
	assert.Equal(t, 2, testCount)
	assert.Equal(t, 2, auditCount)
}

func TestSQLRawOutputSingleQueryWithWhenPerMessage(t *testing.T) {
	db := openTestDB(t)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, `CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
	require.NoError(t, err)

	env := bloblang.NewEnvironment()
	whenMap, err := env.Parse(`root = this.active == true`)
	require.NoError(t, err)
	argsMap, err := env.Parse(`root = [this.id, this.name]`)
	require.NoError(t, err)

	// Single query with a when condition — should use per-message path.
	out := testOutput(db, []rawQueryStatement{
		{
			static:      "INSERT INTO test (id, name) VALUES (?, ?)",
			whenMapping: whenMap,
			argsMapping: argsMap,
		},
	}, true)

	batch := service.MessageBatch{
		service.NewMessage(nil), // active=true, inserted
		service.NewMessage(nil), // active=false, skipped
		service.NewMessage(nil), // active=true, PK conflict with id=1
		service.NewMessage(nil), // active=true, inserted
	}
	batch[0].SetStructured(map[string]any{"id": 1, "name": "alice", "active": true})
	batch[1].SetStructured(map[string]any{"id": 99, "name": "skipped", "active": false})
	batch[2].SetStructured(map[string]any{"id": 1, "name": "conflict", "active": true})
	batch[3].SetStructured(map[string]any{"id": 3, "name": "charlie", "active": true})

	err = out.WriteBatch(ctx, batch)
	// Message 2 (index 2) fails with PK conflict, but others succeed.
	require.Error(t, err)

	// Per-message: msg 0 and 3 succeed, msg 1 skipped, msg 2 fails.
	var count int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&count))
	assert.Equal(t, 2, count, "messages 0 and 3 should be inserted; msg 1 skipped, msg 2 failed")
}

func TestSQLRawOutputWhenConditionNonBoolean(t *testing.T) {
	db := openTestDB(t)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, `CREATE TABLE test (id INTEGER PRIMARY KEY)`)
	require.NoError(t, err)

	env := bloblang.NewEnvironment()
	// Returns an integer, not a boolean.
	whenMap, err := env.Parse(`root = 42`)
	require.NoError(t, err)
	argsMap, err := env.Parse(`root = [this.id]`)
	require.NoError(t, err)

	out := testOutput(db, []rawQueryStatement{
		{
			static:      "INSERT INTO test (id) VALUES (?)",
			whenMapping: whenMap,
			argsMapping: argsMap,
		},
	}, true)

	batch := service.MessageBatch{service.NewMessage(nil)}
	batch[0].SetStructured(map[string]any{"id": 1})

	err = out.WriteBatch(ctx, batch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-boolean")
}

func TestSQLRawOutputLintUnreachableQuery(t *testing.T) {
	linter := service.NewEnvironment().NewComponentConfigLinter()

	// A catch-all (no when) followed by another query — should lint-warn.
	conf := `
sql_raw:
  driver: sqlite
  dsn: ":memory:"
  queries:
    - query: "INSERT INTO a (id) VALUES (?)"
      args_mapping: 'root = [this.id]'
    - query: "INSERT INTO b (id) VALUES (?)"
      args_mapping: 'root = [this.id]'
      when: 'root = this.type == "b"'
`
	lints, err := linter.LintOutputYAML([]byte(conf))
	require.NoError(t, err)
	require.NotEmpty(t, lints, "expected lint warning for unreachable query")
	assert.Contains(t, lints[0].Error(), "unreachable")
}

func TestSQLRawOutputLintReachableQueries(t *testing.T) {
	linter := service.NewEnvironment().NewComponentConfigLinter()

	// Catch-all is last — no lint warning.
	conf := `
sql_raw:
  driver: sqlite
  dsn: ":memory:"
  queries:
    - query: "DELETE FROM t WHERE id = ?"
      args_mapping: 'root = [this.id]'
      when: 'root = this.op == "delete"'
    - query: "INSERT INTO t (id) VALUES (?)"
      args_mapping: 'root = [this.id]'
`
	lints, err := linter.LintOutputYAML([]byte(conf))
	require.NoError(t, err)
	assert.Empty(t, lints, "no lint warnings expected when catch-all is last")
}

func TestSQLRawOutputContextDoneSkipsRollback(t *testing.T) {
	setup := func(t *testing.T) (*sqlRawOutput, service.MessageBatch) {
		t.Helper()
		db := openTestDB(t)

		_, err := db.ExecContext(t.Context(), `CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)`)
		require.NoError(t, err)

		env := bloblang.NewEnvironment()
		argsMapping, err := env.Parse(`root = [this.id, this.name]`)
		require.NoError(t, err)

		out := testOutput(db, []rawQueryStatement{
			{static: "INSERT INTO test (id, name) VALUES (?, ?)", argsMapping: argsMapping},
			{static: "SELECT 1", argsMapping: nil},
		}, false)

		batch := service.MessageBatch{service.NewMessage(nil)}
		batch[0].SetStructured(map[string]any{"id": 1, "name": "alice"})
		return out, batch
	}

	t.Run("canceled", func(t *testing.T) {
		out, batch := setup(t)
		cancelCtx, cancel := context.WithCancel(t.Context())
		cancel()

		err := out.WriteBatch(cancelCtx, batch)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("deadline_exceeded", func(t *testing.T) {
		out, batch := setup(t)
		deadlineCtx, cancel := context.WithDeadline(t.Context(), time.Now())
		defer cancel()

		err := out.WriteBatch(deadlineCtx, batch)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestSQLRawOutputEmptyShutdown(t *testing.T) {
	conf := `
driver: sqlite
dsn: ":memory:"
query: "INSERT INTO test (id) VALUES (?)"
args_mapping: 'root = [ this.id ]'
`
	spec := sqlRawOutputConfig()
	env := service.NewEnvironment()

	parsedConf, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	out, err := newSQLRawOutputFromConfig(parsedConf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out.Close(t.Context()))
}
