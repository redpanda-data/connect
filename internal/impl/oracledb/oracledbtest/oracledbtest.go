// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledbtest

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/sijms/go-ora/v2"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/license"
)

// Batch represents the expected test output.
type Batch struct {
	sync.Mutex
	Msgs []string
}

// Reset sets the messages in the batch to nil.
func (c *Batch) Reset() {
	c.Lock()
	defer c.Unlock()
	c.Msgs = nil
}

// Count returns the total number of messages in the batch.
func (c *Batch) Count() int {
	c.Lock()
	defer c.Unlock()
	return len(c.Msgs)
}

// Clone returns a clone of the underlying Msgs.
func (c *Batch) Clone() []string {
	c.Lock()
	defer c.Unlock()
	return slices.Clone(c.Msgs)
}

// Append adds messages to the batch.
func (c *Batch) Append(msgs ...string) {
	c.Lock()
	defer c.Unlock()
	c.Msgs = append(c.Msgs, msgs...)
}

// Consumer returns a batch consumer that appends the raw bytes of each message
// to the batch as a string.
func (c *Batch) Consumer(t *testing.T) service.MessageBatchHandlerFunc {
	return func(_ context.Context, mb service.MessageBatch) error {
		c.Lock()
		defer c.Unlock()
		for _, msg := range mb {
			msgBytes, err := msg.AsBytes()
			assert.NoError(t, err)
			c.Msgs = append(c.Msgs, string(msgBytes))
		}
		return nil
	}
}

// MsgBatch collects raw messages for tests that assert on message metadata.
type MsgBatch struct {
	sync.Mutex
	Msgs []*service.Message
}

// Reset sets the messages in the batch to nil.
func (c *MsgBatch) Reset() {
	c.Lock()
	defer c.Unlock()
	c.Msgs = nil
}

// Count returns the total number of messages in the batch.
func (c *MsgBatch) Count() int {
	c.Lock()
	defer c.Unlock()
	return len(c.Msgs)
}

// Clone returns a clone of the underlying Msgs.
func (c *MsgBatch) Clone() []*service.Message {
	c.Lock()
	defer c.Unlock()
	return slices.Clone(c.Msgs)
}

// Append adds messages to the batch.
func (c *MsgBatch) Append(msgs ...*service.Message) {
	c.Lock()
	defer c.Unlock()
	c.Msgs = append(c.Msgs, msgs...)
}

// Consumer returns a batch consumer that appends each message to the batch.
func (c *MsgBatch) Consumer() service.MessageBatchHandlerFunc {
	return func(_ context.Context, mb service.MessageBatch) error {
		c.Append(mb...)
		return nil
	}
}

// StartPipeline builds a stream from the input config, logs at INFO level, and
// runs it in the background. The caller must stop the stream with StopWithin.
func StartPipeline(t *testing.T, cfg string, consume service.MessageBatchHandlerFunc) *service.Stream {
	t.Helper()
	return StartPipelineWithLogLevel(t, cfg, "INFO", consume)
}

// StartPipelineWithLogLevel is StartPipeline with a custom log level.
func StartPipelineWithLogLevel(t *testing.T, cfg, logLevel string, consume service.MessageBatchHandlerFunc) *service.Stream {
	t.Helper()
	return startPipeline(t, cfg, consume, func(sb *service.StreamBuilder) {
		require.NoError(t, sb.SetLoggerYAML("level: "+logLevel))
	})
}

// StartPipelineWithLogger is StartPipeline with a custom logger. Use it with a
// SyncBuffer when a test must assert on log output.
func StartPipelineWithLogger(t *testing.T, cfg string, logger *slog.Logger, consume service.MessageBatchHandlerFunc) *service.Stream {
	t.Helper()
	return startPipeline(t, cfg, consume, func(sb *service.StreamBuilder) {
		sb.SetLogger(logger)
	})
}

func startPipeline(t *testing.T, cfg string, consume service.MessageBatchHandlerFunc, setLogger func(*service.StreamBuilder)) *service.Stream {
	t.Helper()

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(cfg))
	setLogger(streamBuilder)
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(consume))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	go func() {
		if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	return stream
}

// WaitForCount waits until count returns at least want, then asserts that the
// last observed value is exactly want. It polls once per second.
func WaitForCount(t *testing.T, count func() int, want int, wait time.Duration) {
	t.Helper()

	var got int
	assert.Eventually(t, func() bool {
		got = count()
		return got >= want
	}, wait, time.Second)
	assert.Equalf(t, want, got, "Wanted %d messages but got %d", want, got)
}

// TestDB wraps sql.DB with testing utilities for Oracle database integration tests.
// It provides helper methods for table creation, supplemental logging enablement, and assertions.
type TestDB struct {
	*sql.DB

	T *testing.T

	// Schema and Schema2 are the two schemas that belong to this test only.
	// The names have a suffix that is unique per test, because all tests share
	// one Oracle container. The setup functions create them and drop them when
	// the test ends.
	Schema  string
	Schema2 string
}

// CheckpointTable returns a checkpoint cache table in the schema of this test.
// Set it as checkpoint_cache_table_name in each connector config. The default
// table RPCN.CDC_CHECKPOINT_CACHE and its key are the same for all tests, so
// a test that uses it can resume from the checkpoint of an earlier test.
func (db *TestDB) CheckpointTable() string {
	return db.Schema + ".CDC_CHECKPOINT_CACHE"
}

// MustExec executes a SQL query and fails the test if an error occurs.
func (db *TestDB) MustExec(query string, args ...any) {
	_, err := db.Exec(query, args...)
	require.NoError(db.T, err)
}

// MustExecContext takes a context and executes a SQL query and fails the test if an error occurs.
func (db *TestDB) MustExecContext(ctx context.Context, query string, args ...any) {
	_, err := db.ExecContext(ctx, query, args...)
	require.NoError(db.T, err)
}

// MustExecInContainer enables executing SQL against the running contanier.
func MustExecInContainer(t *testing.T, ctx context.Context, ctr testcontainers.Container, script string, opts ...tcexec.ProcessOption) string {
	t.Helper()

	opts = append(opts, tcexec.Multiplexed())
	code, reader, err := ctr.Exec(ctx, []string{"bash", "-c", script}, opts...)
	require.NoError(t, err)

	outBytes, err := io.ReadAll(reader)
	require.NoError(t, err)
	out := string(outBytes)

	t.Logf("container exec %q exited with code %d, output:\n%s", script, code, out)
	require.Zero(t, code, "container exec failed (%q): %s", script, out)
	return out
}

// MustEnableSupplementalLogging enables supplemental logging on the specified table.
// The fullTableName should be in format "schema.table" (e.g., "SYSTEM.all_data_types").
// If only a table name is provided, defaults to "SYSTEM" schema.
// This enables supplemental logging for all columns, which is required for CDC.
func (db *TestDB) MustEnableSupplementalLogging(ctx context.Context, fullTableName string) {
	db.T.Logf("Enabling supplemental logging for table %q", fullTableName)
	table := strings.Split(fullTableName, ".")
	if len(table) != 2 {
		table = []string{"SYSTEM", table[0]}
	}
	schema := strings.ToUpper(table[0])
	tableName := strings.ToUpper(table[1])

	// Enable supplemental logging for all columns on the table
	// This ensures all column values (before and after) are captured in redo logs
	query := fmt.Sprintf(`ALTER TABLE %s.%s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS`, schema, tableName)

	_, err := db.ExecContext(ctx, query)
	require.NoError(db.T, err)

	db.T.Logf("Supplemental logging enabled for table %q", fullTableName)
}

// MustDisableSupplementalLogging disables supplemental logging on the specified table.
// The fullTableName should be in format "schema.table" (e.g., "SYSTEM.all_data_types").
// If only a table name is provided, defaults to "SYSTEM" schema.
func (db *TestDB) MustDisableSupplementalLogging(ctx context.Context, fullTableName string) {
	db.T.Logf("Disabling supplemental logging for table %q", fullTableName)
	table := strings.Split(fullTableName, ".")
	if len(table) != 2 {
		table = []string{"SYSTEM", table[0]}
	}
	schema := strings.ToUpper(table[0])
	tableName := strings.ToUpper(table[1])

	// Drop supplemental logging for all columns on the table
	query := fmt.Sprintf(`ALTER TABLE %s.%s DROP SUPPLEMENTAL LOG DATA (ALL) COLUMNS`, schema, tableName)

	_, err := db.ExecContext(ctx, query)
	require.NoError(db.T, err)

	db.T.Logf("Supplemental logging disabled for table %q", fullTableName)
}

// CreateTableWithSupplementalLoggingIfNotExists creates the given test tables ensuring supplemental logging is enabled.
func (db *TestDB) CreateTableWithSupplementalLoggingIfNotExists(ctx context.Context, fullTableName, createTableQuery string, _ ...any) error {
	// default to SYSTEM if not found
	table := strings.Split(fullTableName, ".")
	if len(table) != 2 {
		table = []string{"SYSTEM", table[0]}
	}
	schema := strings.ToUpper(table[0])
	tableName := strings.ToUpper(table[1])

	// Check if table exists using Oracle's user_tables view
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2",
		schema, tableName).Scan(&count)
	if err != nil {
		return err
	}

	// Only create table if it doesn't exist
	if count == 0 {
		// Create the table
		if _, err := db.ExecContext(ctx, createTableQuery); err != nil {
			return err
		}

		// Enable supplemental logging for all columns on the table
		enableSupplementalLogging := fmt.Sprintf(
			"ALTER TABLE %s.%s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
			schema, tableName)
		if _, err := db.ExecContext(ctx, enableSupplementalLogging); err != nil {
			return err
		}
	}

	return nil
}

// SetupTestWithOracleDBVersion connects to the Oracle Free container that all
// tests in the package share, and starts it on first use. It creates the two
// schemas of this test (see TestDB.Schema) and returns the connection string
// and TestDB wrapper.
func SetupTestWithOracleDBVersion(t *testing.T) (string, *TestDB) {
	t.Helper()
	cfg := sharedContainer(t)

	// Local users in CDB$ROOT need _ORACLE_SCRIPT (to avoid ORA-65096). It is a
	// session setting, so createSchemas uses one dedicated connection.
	db := newTestDB(t, cfg.dbConn)
	createSchemas(t, cfg.dbConn, true, db.Schema, db.Schema2)
	return cfg.connStr, db
}

// SetupTestWithOracleDBVersionAndContainer is SetupTestWithOracleDBVersion, but also
// returns the container handle so tests can Exec admin commands (e.g. SQL*Plus
// SHUTDOWN/STARTUP MOUNT/FLASHBACK DATABASE/OPEN RESETLOGS) that a plain SQL connection
// can't issue.
//
// The next test uses the same container. A test that restarts or changes the
// database instance must make it available again before it returns, and undo
// its instance-level changes in a t.Cleanup.
func SetupTestWithOracleDBVersionAndContainer(t *testing.T) (string, *TestDB, testcontainers.Container) {
	t.Helper()
	connStr, db := SetupTestWithOracleDBVersion(t)
	cfg := sharedContainer(t)
	t.Cleanup(func() {
		// A restart of the instance breaks the idle pooled connections. Close
		// them, so that the next test opens new ones.
		for _, pool := range []*sql.DB{cfg.dbConn, cfg.pdbConn} {
			pool.SetMaxIdleConns(0)
			pool.SetMaxIdleConns(5)
		}
	})
	return connStr, db, cfg.container
}

// newTestDB returns a TestDB with schema names that are unique to t. The
// suffix is a hash of t.Name(), so a schema that stays after a failed cleanup
// identifies its test.
func newTestDB(t *testing.T, conn *sql.DB) *TestDB {
	h := fnv.New32a()
	_, _ = h.Write([]byte(t.Name()))
	suffix := fmt.Sprintf("%08X", h.Sum32())
	return &TestDB{DB: conn, T: t, Schema: "TESTDB_" + suffix, Schema2: "TESTDB2_" + suffix}
}

// createSchemas creates each user on one connection of db and drops it with
// CASCADE when the test ends. It drops a user with the same name first, if a
// previous run did not remove it. Set oracleScript for local users in CDB$ROOT.
func createSchemas(t *testing.T, db *sql.DB, oracleScript bool, users ...string) {
	t.Helper()
	ctx := t.Context()

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	if oracleScript {
		_, err = conn.ExecContext(ctx, `ALTER SESSION SET "_ORACLE_SCRIPT"=TRUE`)
		require.NoError(t, err)
	}
	for _, user := range users {
		require.NoError(t, dropUser(ctx, conn.ExecContext, user))
		for _, q := range []string{
			"CREATE USER " + user + " IDENTIFIED BY testdb123",
			"GRANT CONNECT, RESOURCE, DBA TO " + user,
			"GRANT UNLIMITED TABLESPACE TO " + user,
		} {
			_, err = conn.ExecContext(ctx, q)
			require.NoErrorf(t, err, "creating schema %s: %s", user, q)
		}
	}

	t.Cleanup(func() {
		// t.Context() is cancelled before cleanup runs.
		ctx := context.Background()
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Errorf("dropping test schemas: %v", err)
			return
		}
		defer conn.Close()
		if oracleScript {
			if _, err := conn.ExecContext(ctx, `ALTER SESSION SET "_ORACLE_SCRIPT"=TRUE`); err != nil {
				t.Errorf("dropping test schemas: %v", err)
				return
			}
		}
		for _, user := range users {
			if err := dropUser(ctx, conn.ExecContext, user); err != nil {
				t.Errorf("dropping test schema %s: %v", user, err)
			}
		}
	})
}

// dropUser drops the user with CASCADE. It ignores ORA-01918 (user does not exist).
func dropUser(ctx context.Context, exec func(context.Context, string, ...any) (sql.Result, error), user string) error {
	_, err := exec(ctx, "DROP USER "+user+" CASCADE")
	if err != nil && strings.Contains(err.Error(), "ORA-01918") {
		return nil
	}
	return err
}

// ---------------------------------------------------------------------------
// Schema metadata integration tests
// ---------------------------------------------------------------------------

// ExtractSchema extracts and parses the schema metadata from a service.Message.
// Returns a zero-value schema.Common if the metadata is absent.
func ExtractSchema(t *testing.T, msg *service.Message) schema.Common {
	t.Helper()
	var raw any
	_ = msg.MetaWalkMut(func(k string, v any) error {
		if k == "schema" {
			raw = v
		}
		return nil
	})
	if raw == nil {
		return schema.Common{}
	}
	c, err := schema.ParseFromAny(raw)
	require.NoError(t, err)
	return c
}

// ExtractFingerprint extracts the fingerprint string from schema metadata.
func ExtractFingerprint(t *testing.T, msg *service.Message) string {
	t.Helper()
	var raw any
	_ = msg.MetaWalkMut(func(k string, v any) error {
		if k == "schema" {
			raw = v
		}
		return nil
	})
	if raw == nil {
		return ""
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return ""
	}
	fp, _ := m["fingerprint"].(string)
	return fp
}

// ChildByName finds a child by name in a Common schema for test assertions.
func ChildByName(t *testing.T, c schema.Common, name string) schema.Common {
	t.Helper()
	for i := range c.Children {
		if c.Children[i].Name == name {
			return c.Children[i]
		}
	}
	t.Fatalf("child %q not found in schema %q", name, c.Name)
	return schema.Common{}
}

// SetupCDBTestWithPDB connects to the shared Oracle Free container and
// configures it for CDB mode testing. It creates the C##RPCN checkpoint user in
// CDB$ROOT, and the two schemas of this test (see TestDB.Schema) in FREEPDB1.
// It drops C##RPCN when the test ends, because the connector auto-derives the
// same checkpoint table name for every CDB mode test.
//
// Returns:
//   - cdbConnStr: connection string targeting CDB$ROOT (use as connection_string in the connector config with pdb_name set)
//   - pdbDB: TestDB connected to FREEPDB1 for creating tables and inserting test data
//   - pdbName: "FREEPDB1"
func SetupCDBTestWithPDB(t *testing.T) (string, *TestDB, string) {
	t.Helper()
	cfg := sharedContainer(t)

	// In CDB mode the connector auto-derives the checkpoint cache table as
	// C##RPCN.CDC_CHECKPOINT_<PDBNAME>, so the common user must exist as C##RPCN.
	// Common users require the C## prefix but do not need _ORACLE_SCRIPT workaround.
	ctx := t.Context()
	require.NoError(t, dropUser(ctx, cfg.dbConn.ExecContext, `"C##RPCN"`))
	for _, q := range []string{
		`CREATE USER "C##RPCN" IDENTIFIED BY rpcn123`,
		`GRANT CONNECT, RESOURCE TO "C##RPCN"`,
		`GRANT UNLIMITED TABLESPACE TO "C##RPCN"`,
	} {
		_, err := cfg.dbConn.ExecContext(ctx, q)
		require.NoError(t, err, q)
	}
	t.Cleanup(func() {
		if err := dropUser(context.Background(), cfg.dbConn.ExecContext, `"C##RPCN"`); err != nil {
			t.Errorf("dropping C##RPCN: %v", err)
		}
	})

	// PDB-local users do not require the C## prefix or _ORACLE_SCRIPT workaround.
	db := newTestDB(t, cfg.pdbConn)
	createSchemas(t, cfg.pdbConn, false, db.Schema, db.Schema2)
	return cfg.connStr, db, "FREEPDB1"
}

// CreatePDBTableWithSupplementalLoggingIfNotExists creates a table in a PDB and
// enables supplemental logging on it. Unlike CreateTableWithSupplementalLoggingIfNotExists,
// it requires the schema in fullTableName.
func (db *TestDB) CreatePDBTableWithSupplementalLoggingIfNotExists(ctx context.Context, fullTableName, createTableQuery string) error {
	parts := strings.SplitN(fullTableName, ".", 2)
	if len(parts) != 2 {
		return fmt.Errorf("fullTableName must be schema.table, got %q", fullTableName)
	}
	schemaName := strings.ToUpper(parts[0])
	tableName := strings.ToUpper(parts[1])

	var count int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2",
		schemaName, tableName).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	if _, err := db.ExecContext(ctx, createTableQuery); err != nil {
		return err
	}

	_, err := db.ExecContext(ctx, fmt.Sprintf(
		"ALTER TABLE %s.%s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
		schemaName, tableName))
	return err
}

type containerCfg struct {
	container testcontainers.Container
	dbConn    *sql.DB // CDB$ROOT
	pdbConn   *sql.DB // FREEPDB1
	connStr   string
}

// shared is the Oracle Free container that all tests in a package use. Oracle
// Free takes up to three minutes to boot, and a small Docker VM, such as the
// one on a developer laptop, has memory for one instance only.
var shared struct {
	once sync.Once
	cfg  containerCfg
	err  error
}

// sharedContainer returns the shared container. The first call starts it. If
// the start fails, all calls fail with the same error, so the tests do not boot
// the container again one by one. TerminateShared stops the container.
func sharedContainer(t *testing.T) containerCfg {
	t.Helper()
	shared.once.Do(func() {
		// context.Background(), not t.Context(): the container outlives the
		// test that starts it.
		shared.cfg, shared.err = startContainer(context.Background())
	})
	require.NoError(t, shared.err, "starting the shared Oracle container")
	return shared.cfg
}

// TerminateShared closes the connections to the shared container and stops
// it, if a test started it. Call it from TestMain after the tests of the
// package complete.
func TerminateShared() error {
	var errs []error
	for _, db := range []*sql.DB{shared.cfg.pdbConn, shared.cfg.dbConn} {
		if db != nil {
			errs = append(errs, db.Close())
		}
	}
	if shared.cfg.container != nil {
		errs = append(errs, shared.cfg.container.Terminate(context.Background()))
	}
	return errors.Join(errs...)
}

// startContainer starts an Oracle Free container, opens connections to
// CDB$ROOT and FREEPDB1, and enables the database-level supplemental logging
// that CDC needs. On error, it stops the container.
func startContainer(ctx context.Context) (cfg containerCfg, err error) {
	container, err := testcontainers.Run(ctx, "container-registry.oracle.com/database/free:latest-lite",
		testcontainers.WithExposedPorts("1521/tcp"),
		testcontainers.WithEnv(map[string]string{
			"ORACLE_PWD": "YourPassword123",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("DATABASE IS READY TO USE!").WithStartupTimeout(3*time.Minute),
		),
	)
	if container != nil {
		defer func() {
			if err != nil {
				_ = container.Terminate(context.Background())
			}
		}()
	}
	if err != nil {
		return cfg, err
	}
	cfg.container = container

	port, err := container.MappedPort(ctx, "1521/tcp")
	if err != nil {
		return cfg, err
	}
	host, err := container.Host(ctx)
	if err != nil {
		return cfg, err
	}

	// CDB$ROOT connection string — the connector uses this with pdb_name set.
	cfg.connStr = fmt.Sprintf("oracle://system:YourPassword123@%s:%s/FREE", host, port.Port())
	if cfg.dbConn, err = openDB(ctx, cfg.connStr); err != nil {
		return cfg, err
	}
	if cfg.pdbConn, err = openDB(ctx, fmt.Sprintf("oracle://system:YourPassword123@%s:%s/FREEPDB1", host, port.Port())); err != nil {
		_ = cfg.dbConn.Close()
		return cfg, err
	}

	for _, q := range []string{
		"ALTER DATABASE ADD SUPPLEMENTAL LOG DATA",
		// Enable minimal supplemental logging for primary keys at CDB level
		"ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS",
	} {
		if _, err = cfg.dbConn.ExecContext(ctx, q); err != nil {
			_ = cfg.pdbConn.Close()
			_ = cfg.dbConn.Close()
			return cfg, fmt.Errorf("%s: %w", q, err)
		}
	}
	return cfg, nil
}

func openDB(ctx context.Context, connStr string) (*sql.DB, error) {
	db, err := sql.Open("oracle", connStr)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Minute * 5)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// SyncBuffer a buffer used for buffering log output
type SyncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *SyncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *SyncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
