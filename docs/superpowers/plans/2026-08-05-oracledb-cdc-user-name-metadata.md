# Oracle CDC `user_name` Metadata Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `user_name` metadata field to `oracledb_cdc` messages, sourced from `V$LOGMNR_CONTENTS.USERNAME` (the DB user who executed the change), alongside the existing `scn`/`database_schema`/`table_name`/`operation`/`transaction_id`/`source_ts_ms`/`commit_ts_ms`/`schema` fields.

**Architecture:** `USERNAME` is added to the LogMiner `SELECT` and scanned into `sqlredo.RedoEvent`. It is copied through `sqlredo.DMLEvent` (at parse time) and `replication.MessageEvent` (at commit time), then unconditionally set as `user_name` message metadata in the batcher — mirroring the existing `transaction_id` field's pattern exactly. Snapshot (`read`) events never populate it, since there is no DML session for a plain table read.

**Tech Stack:** Go, `database/sql` (`sql.NullString` for nullable Oracle columns), benthos `service.Message` metadata API, testify (`assert`/`require`).

## Global Constraints

- No new config field: metadata fields in this connector are always emitted unconditionally (subject to per-field emptiness checks) — no allow/deny list exists today, and `user_name` must not introduce one (see feasibility investigation, config.go/logminer/config.go have no `metadata` field).
- `user_name` must be **absent from message metadata** (not an empty string) when `USERNAME` is `NULL` — this happens for internal/recursive/background Oracle transactions and is expected. Follow the exact `if m.TransactionID != "" { msg.MetaSet(...) }` guard pattern already used in `internal/impl/oracledb/batcher.go`.
- `user_name` must be absent on snapshot (`read`) messages — there is no DML session to attribute a user to. Do not set `replication.MessageEvent.UserName` anywhere in `internal/impl/oracledb/replication/snapshot.go`; the zero-value (`""`) already produces this behavior via the same emptiness guard.
- No checkpoint schema version bump needed — `user_name` is never persisted to the checkpoint cache table, only flows through the live in-memory pipeline before publish.
- No license/enterprise gating changes needed — `oracledb_cdc` is already gated at the connector level in `internal/impl/oracledb/input_oracledb_cdc.go` via `license.CheckRunningEnterprise`.
- Field naming in Go structs: `UserName` (matches existing sibling fields `SchemaName`, `TableName` — not `Username`).
- This repo's Go changes go through TDD: write the failing test first, watch it fail (compile error counts as a legitimate Go "red"), then implement the minimal change.

---

### Task 1: Thread `UserName` through the `sqlredo` parsing layer

**Files:**
- Modify: `internal/impl/oracledb/logminer/sqlredo/events.go:154-178` (`DMLEvent` and `RedoEvent` structs)
- Modify: `internal/impl/oracledb/logminer/sqlredo/parser.go:53-85` (`RedoEventToDMLEvent`)
- Test: `internal/impl/oracledb/logminer/sqlredo/parser_test.go`

**Interfaces:**
- Consumes: nothing from other tasks (this is the innermost layer).
- Produces: `sqlredo.RedoEvent.UserName sql.NullString` and `sqlredo.DMLEvent.UserName string` — Task 2 reads `dml.UserName` (a plain `string`) when building `replication.MessageEvent`, and Task 3 writes to `event.UserName` (the `sql.NullString`) as a new `rows.Scan` target.

- [ ] **Step 1: Write the failing test**

Add to `internal/impl/oracledb/logminer/sqlredo/parser_test.go`. Add `"database/sql"` to its import block (it currently imports `encoding/json`, `testing`, `time`, `testify/assert`, `testify/require`, and the `sqlredo` package — `database/sql` is new).

```go
func TestRedoEventToDMLEvent_UserName(t *testing.T) {
	p := sqlredo.NewParser()

	t.Run("valid username is copied through", func(t *testing.T) {
		redoEvent := &sqlredo.RedoEvent{
			Operation:  sqlredo.OpInsert,
			SQLRedo:    sql.NullString{String: `insert into "TESTDB"."USERS"("ID") values ('1')`, Valid: true},
			SchemaName: sql.NullString{String: "TESTDB", Valid: true},
			TableName:  sql.NullString{String: "USERS", Valid: true},
			UserName:   sql.NullString{String: "ALICE", Valid: true},
		}

		event, err := p.RedoEventToDMLEvent(redoEvent)
		require.NoError(t, err)
		assert.Equal(t, "ALICE", event.UserName)
	})

	t.Run("NULL username leaves DMLEvent.UserName empty", func(t *testing.T) {
		redoEvent := &sqlredo.RedoEvent{
			Operation:  sqlredo.OpInsert,
			SQLRedo:    sql.NullString{String: `insert into "TESTDB"."USERS"("ID") values ('1')`, Valid: true},
			SchemaName: sql.NullString{String: "TESTDB", Valid: true},
			TableName:  sql.NullString{String: "USERS", Valid: true},
			UserName:   sql.NullString{Valid: false},
		}

		event, err := p.RedoEventToDMLEvent(redoEvent)
		require.NoError(t, err)
		assert.Empty(t, event.UserName)
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/impl/oracledb/logminer/sqlredo/... -run TestRedoEventToDMLEvent_UserName -v`
Expected: compile FAIL — `unknown field UserName in struct literal of type sqlredo.RedoEvent` and `event.UserName undefined (type sqlredo.DMLEvent has no field or method UserName)`.

- [ ] **Step 3: Write minimal implementation**

In `internal/impl/oracledb/logminer/sqlredo/events.go`, add a field to each struct (append after the existing `TransactionID` field in both):

```go
// DMLEvent represents a parsed DML (Data Manipulation Language) operation
type DMLEvent struct {
	Operation Operation
	Schema    string
	Table     string
	SQLRedo   string
	Data      map[string]any
	// OldValues holds the WHERE-clause column values for UPDATE and DELETE events.
	// For LOB-init UPDATE events these are used to identify the source row for PK matching.
	OldValues     map[string]any
	Timestamp     time.Time
	TransactionID TransactionID
	// UserName is the Oracle database user that executed the change, from
	// V$LOGMNR_CONTENTS.USERNAME. Empty for internal/recursive transactions.
	UserName string
}

// RedoEvent represents a redo log row from V$LOGMNR_CONTENTS
type RedoEvent struct {
	SCN           uint64
	SQLRedo       sql.NullString
	Data          map[string]any
	Operation     Operation
	TableName     sql.NullString
	SchemaName    sql.NullString
	Timestamp     time.Time
	TransactionID TransactionID
	// UserName is NULL for internal/recursive/background transactions.
	UserName sql.NullString
}
```

In `internal/impl/oracledb/logminer/sqlredo/parser.go`, add a copy-through in `RedoEventToDMLEvent` right after the existing `TableName` copy-through:

```go
	if redoEvent.SchemaName.Valid {
		event.Schema = redoEvent.SchemaName.String
	}
	if redoEvent.TableName.Valid {
		event.Table = redoEvent.TableName.String
	}
	if redoEvent.UserName.Valid {
		event.UserName = redoEvent.UserName.String
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/impl/oracledb/logminer/sqlredo/... -run TestRedoEventToDMLEvent_UserName -v`
Expected: PASS (both subtests).

Also run the full package to catch regressions: `go test ./internal/impl/oracledb/logminer/sqlredo/...`

- [ ] **Step 5: Commit**

```bash
git add internal/impl/oracledb/logminer/sqlredo/events.go internal/impl/oracledb/logminer/sqlredo/parser.go internal/impl/oracledb/logminer/sqlredo/parser_test.go
git commit -m "oracledb_cdc: thread UserName through sqlredo RedoEvent/DMLEvent"
```

---

### Task 2: Thread `UserName` through `replication.MessageEvent` and `toMessageEvent`

**Files:**
- Modify: `internal/impl/oracledb/replication/stream_message.go:118-130` (`MessageEvent` struct)
- Modify: `internal/impl/oracledb/logminer/logminer.go:1145-1181` (`toMessageEvent`)
- Test: `internal/impl/oracledb/logminer/logminer_test.go`

**Interfaces:**
- Consumes: `sqlredo.DMLEvent.UserName string` (from Task 1).
- Produces: `replication.MessageEvent.UserName string` — Task 4 reads `m.UserName` in `batcher.go`'s `Publish` method.

- [ ] **Step 1: Write the failing test**

Add to `internal/impl/oracledb/logminer/logminer_test.go` (package `logminer`, already imports `database/sql`, `log/slog`, `testing`, `testify/assert`, `testify/require`, `sqlredo`, and has the `newLogMiner`/`publisherStub` test helpers at the bottom of the file):

```go
func TestProcessRedoEventCapturesUserName(t *testing.T) {
	pub := &publisherStub{}
	cache := NewInMemoryCache(0, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
	lm := newLogMiner(pub, cache)

	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN:           100,
		Operation:     sqlredo.OpInsert,
		SQLRedo:       sql.NullString{String: `insert into "TESTDB"."USERS"("ID") values ('1')`, Valid: true},
		SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
		TableName:     sql.NullString{String: "USERS", Valid: true},
		UserName:      sql.NullString{String: "ALICE", Valid: true},
		TransactionID: "txB",
	}))

	require.NoError(t, lm.processRedoEvent(t.Context(), &sqlredo.RedoEvent{
		SCN: 200, Operation: sqlredo.OpCommit, TransactionID: "txB",
	}))

	require.Len(t, pub.messages, 1)
	assert.Equal(t, "ALICE", pub.messages[0].UserName)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/impl/oracledb/logminer/... -run TestProcessRedoEventCapturesUserName -v`
Expected: compile FAIL — `pub.messages[0].UserName undefined (type replication.MessageEvent has no field or method UserName)`.

- [ ] **Step 3: Write minimal implementation**

In `internal/impl/oracledb/replication/stream_message.go`, add a field to `MessageEvent` (append after `TransactionID`):

```go
// MessageEvent represents a single change from Table's change table in the database.
type MessageEvent struct {
	SCN             SCN
	CheckpointSCN   SCN
	Operation       OpType
	Schema          string
	Table           string
	Data            any
	Timestamp       time.Time
	CommitTimestamp time.Time
	ColumnMeta      []ColumnMeta
	TransactionID   string
	// UserName is the Oracle database user that executed the change. Empty on
	// snapshot (read) events and for internal/recursive transactions.
	UserName string
}
```

In `internal/impl/oracledb/logminer/logminer.go`, add `UserName: dml.UserName,` to the `replication.MessageEvent` literal inside `toMessageEvent`:

```go
	m := &replication.MessageEvent{
		SCN:             replication.SCN(scn),
		CheckpointSCN:   replication.SCN(checkpointSCN),
		Schema:          dml.Schema,
		Table:           dml.Table,
		Data:            data,
		Timestamp:       dml.Timestamp,
		TransactionID:   dml.TransactionID.String(),
		CommitTimestamp: commitTimestamp,
		UserName:        dml.UserName,
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/impl/oracledb/logminer/... -run TestProcessRedoEventCapturesUserName -v`
Expected: PASS.

Also run the full package to catch regressions: `go test ./internal/impl/oracledb/logminer/...`

- [ ] **Step 5: Commit**

```bash
git add internal/impl/oracledb/replication/stream_message.go internal/impl/oracledb/logminer/logminer.go internal/impl/oracledb/logminer/logminer_test.go
git commit -m "oracledb_cdc: thread UserName into replication.MessageEvent"
```

---

### Task 3: Add `USERNAME` to the LogMiner `SELECT` and row scan

**Files:**
- Modify: `internal/impl/oracledb/logminer/logminer.go:103` (query string built in `NewMiner`)
- Modify: `internal/impl/oracledb/logminer/logminer.go:928-938` (`rows.Scan` in `queryLogMinerContents`)
- Test: `internal/impl/oracledb/logminer/logminer_test.go`

**Interfaces:**
- Consumes: `sqlredo.RedoEvent.UserName sql.NullString` (from Task 1) as the new scan target.
- Produces: a populated `RedoEvent.UserName` for every row LogMiner returns — Task 1's `RedoEventToDMLEvent` (already implemented) then copies it onward. No new exported symbols.

- [ ] **Step 1: Write the failing test**

Add to `internal/impl/oracledb/logminer/logminer_test.go`:

```go
func TestNewMinerQueryIncludesUserName(t *testing.T) {
	pub := &publisherStub{}
	cache := NewInMemoryCache(0, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
	tables := []replication.UserTable{{Schema: "TESTDB", Name: "USERS"}}

	lm := NewMiner(nil, tables, pub, NewDefaultConfig(), cache, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))

	assert.Contains(t, lm.logMinerQuery, "USERNAME")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/impl/oracledb/logminer/... -run TestNewMinerQueryIncludesUserName -v`
Expected: FAIL — assertion failure, `lm.logMinerQuery` does not contain "USERNAME" (this is a real behavioral failure, not a compile error, since the struct field already exists from Task 1).

- [ ] **Step 3: Write minimal implementation**

In `internal/impl/oracledb/logminer/logminer.go`, update the query string (line 103):

```go
	logMinerQuery := "SELECT SCN, SQL_REDO, OPERATION_CODE, TABLE_NAME, SEG_OWNER, TIMESTAMP, XID, COMMIT_SCN, CSF, USERNAME FROM V$LOGMNR_CONTENTS WHERE SCN > :1 AND SCN <= :2" + buf.String()
```

And add the matching scan target in `queryLogMinerContents` (append `&event.UserName` last, matching the column order added to the `SELECT`):

```go
		if err := rows.Scan(
			&event.SCN,
			&event.SQLRedo,
			&event.Operation,
			&event.TableName,
			&event.SchemaName,
			&event.Timestamp,
			&event.TransactionID,
			&commitSCN,
			&csf,
			&event.UserName,
		); err != nil {
			return err
		}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/impl/oracledb/logminer/... -run TestNewMinerQueryIncludesUserName -v`
Expected: PASS.

Also run the full package to catch regressions: `go test ./internal/impl/oracledb/logminer/...`

**Note:** this task cannot be fully verified without a live Oracle instance — no `sqlmock` exists for `V$LOGMNR_CONTENTS`, so the actual column-order/scan correctness against a real cursor is only exercised by the Docker-gated integration test in Task 4. Flag this explicitly when running the integration suite: watch for a `sql: Scan error on column index 9` or similar type-mismatch error, which would mean the real `USERNAME` column type needs a different scan target than `sql.NullString`.

- [ ] **Step 5: Commit**

```bash
git add internal/impl/oracledb/logminer/logminer.go internal/impl/oracledb/logminer/logminer_test.go
git commit -m "oracledb_cdc: select USERNAME from V\$LOGMNR_CONTENTS"
```

---

### Task 4: Emit `user_name` metadata, document it, and extend the integration test

**Files:**
- Modify: `internal/impl/oracledb/batcher.go:169-194` (`Publish` method)
- Modify: `internal/impl/oracledb/input_oracledb_cdc.go:78-97` (`Description(...)` metadata doc block)
- Modify: `internal/impl/oracledb/integration_test.go:587-633` (`mustAssertMetadata`)
- Regenerate: `docs/modules/components/pages/inputs/oracledb_cdc.adoc` (via `task docs`, generated — do not hand-edit)

**Interfaces:**
- Consumes: `replication.MessageEvent.UserName string` (from Task 2).
- Produces: the `user_name` message metadata key — this is the final, user-visible deliverable; no later task depends on it.

- [ ] **Step 1: Extend the integration test assertion (the failing "test" for this task)**

In `internal/impl/oracledb/integration_test.go`, inside `mustAssertMetadata`, add a new block immediately after the existing `transaction_id` assertion (after the line asserting `^\d+\.\d+\.\d+$`):

```go
			// assert user_name metadata
			userName, ok := msg.MetaGet("user_name")
			require.Truef(t, ok, "message %d missing 'user_name' metadata", i)
			assert.NotEmptyf(t, userName, "message %d: user_name should not be empty", i)
```

- [ ] **Step 2: Run integration tests to verify it fails**

Run: `task test:integration -- oracledb` (requires Docker running; this test suite is skipped by default otherwise — see `CLAUDE.md`).
Expected: FAIL — `message 0 missing 'user_name' metadata` (the batcher doesn't set this key yet).

- [ ] **Step 3: Write minimal implementation**

In `internal/impl/oracledb/batcher.go`, inside `Publish`, add the metadata set immediately after the existing `transaction_id` block:

```go
	if m.TransactionID != "" {
		msg.MetaSet("transaction_id", m.TransactionID)
	}
	if m.UserName != "" {
		msg.MetaSet("user_name", m.UserName)
	}
```

In `internal/impl/oracledb/input_oracledb_cdc.go`, add a new bullet to the `Description(...)` metadata list, immediately after the `transaction_id` bullet:

```go
- user_name: The Oracle database user that executed the change, sourced from ` + "`V$LOGMNR_CONTENTS.USERNAME`" + `. Not present for internal/recursive transactions, and not present on snapshot (` + "`read`" + `) messages.
```

- [ ] **Step 4: Run integration tests to verify it passes**

Run: `task test:integration -- oracledb`
Expected: PASS. If it fails with a scan/type error referencing column index 9 or `USERNAME`, return to Task 3 and adjust the scan target type (see the note in Task 3 Step 4).

Then regenerate and validate docs:

Run: `task docs`
Expected: `docs/modules/components/pages/inputs/oracledb_cdc.adoc` updates to include the new bullet, and doc example validation passes.

- [ ] **Step 5: Commit**

```bash
git add internal/impl/oracledb/batcher.go internal/impl/oracledb/input_oracledb_cdc.go internal/impl/oracledb/integration_test.go docs/modules/components/pages/inputs/oracledb_cdc.adoc
git commit -m "oracledb_cdc: emit user_name metadata and document it"
```

---

## After all tasks

- [ ] Run the full unit suite once more: `task test:unit` (or `task test:ut`).
- [ ] Run `task lint` and `task fmt` to catch any formatting/lint drift.
- [ ] Run `/code-review` (per this repo's CLAUDE.md) before opening a PR.
