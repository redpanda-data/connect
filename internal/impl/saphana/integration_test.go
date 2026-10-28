// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package saphana

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/license"

	_ "github.com/redpanda-data/connect/v4/public/components/pure"
)

// The integration tests run against one SAP HANA Express (HXE) container
// shared by the whole package: its cold start takes several minutes, so one
// per test is not an option. Set SAP_HANA_TEST_DSN to point the tests at an
// existing instance instead (a HANA Cloud instance, for example, which also
// exercises the TLS DSN parameters); every test creates uniquely named tables
// in the connecting user's schema and drops them afterwards.
//
// HXE is published for linux/amd64 only and its licence agreement and
// passwords are command-line arguments (the image reads no environment
// variables). Its start-up checks also ask for the shared-memory sysctls, the
// open-files ulimit and the mbind/move_pages syscalls configured below.
//
// The tests connect to the system database (SYSTEMDB, port 39017) rather than
// the HXE tenant: under Docker Desktop's Rosetta emulation on Apple Silicon
// hdbdaemon spins at 100% CPU and never starts the tenant's indexserver, so
// tenant creation fails after a five-minute timeout. SYSTEMDB is up two to
// three minutes into the cold start, runs the same SQL engine and catalog
// views, and --dont-exit-on-error keeps the container alive when the tenant
// step fails. On an amd64 Linux host the tenant is created normally and the
// tests still use SYSTEMDB.
//
// The image is pinned so a new HANA revision cannot change behaviour or break
// start-up without a code change, and the container runs at UTC+3 (TZ is
// honoured by the HANA processes) so CURRENT_TIMESTAMP and
// CURRENT_UTCTIMESTAMP differ and the timestamp_clock tests can tell them
// apart.
const (
	hanaImage       = "saplabs/hanaexpress:2.00.088.00.20251110.1"
	hanaTimezone    = "Etc/GMT-3" // POSIX sign convention: UTC+3
	hanaSystemPort  = "39017/tcp" // SQL port of SYSTEMDB
	hanaUser        = "SYSTEM"
	hanaPassword    = "HXEHana1" // must satisfy the HXE policy: upper, lower, digit, 8+ chars
	hanaStartupWait = 20 * time.Minute
	hanaDSNEnv      = "SAP_HANA_TEST_DSN"
)

var (
	hanaOnce    sync.Once
	hanaDSN     string
	hanaErr     error
	hanaCleanup func()
	hanaTableID atomic.Int64
)

func TestMain(m *testing.M) {
	code := m.Run()
	if hanaCleanup != nil {
		hanaCleanup()
	}
	os.Exit(code)
}

// requireHANA returns a DSN for the shared HANA instance, starting the HXE
// container on first use.
func requireHANA(t *testing.T) string {
	t.Helper()
	integration.CheckSkip(t)

	hanaOnce.Do(func() {
		if dsn := os.Getenv(hanaDSNEnv); dsn != "" {
			hanaDSN = dsn
			return
		}
		hanaDSN, hanaCleanup, hanaErr = startHANA(context.Background())
	})
	require.NoError(t, hanaErr, "starting SAP HANA Express")

	db := openTestDB(t, hanaDSN)
	require.Eventually(t, func() bool {
		return db.PingContext(t.Context()) == nil
	}, 5*time.Minute, 2*time.Second, "HANA did not accept connections")
	return hanaDSN
}

func startHANA(ctx context.Context) (string, func(), error) {
	ctr, err := testcontainers.Run(
		ctx,
		hanaImage,
		testcontainers.WithImagePlatform("linux/amd64"),
		testcontainers.WithExposedPorts(hanaSystemPort),
		testcontainers.WithCmd("--agree-to-sap-license", "--dont-exit-on-error", "--master-password", hanaPassword),
		testcontainers.WithEnv(map[string]string{"TZ": hanaTimezone}),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.SecurityOpt = append(hc.SecurityOpt, "seccomp=unconfined")
			hc.Ulimits = append(hc.Ulimits, &container.Ulimit{Name: "nofile", Soft: 1048576, Hard: 1048576})
			if hc.Sysctls == nil {
				hc.Sysctls = map[string]string{}
			}
			hc.Sysctls["kernel.shmmax"] = "1073741824"
			hc.Sysctls["kernel.shmall"] = "8388608"
			hc.Sysctls["net.ipv4.ip_local_port_range"] = "40000 60999"
		}),
		// "HANA is up" is printed once SYSTEMDB has started, before the tenant
		// is created; requireHANA then waits for SQL connections to succeed.
		testcontainers.WithWaitStrategyAndDeadline(hanaStartupWait,
			wait.ForLog("HANA is up").WithStartupTimeout(hanaStartupWait),
			wait.ForListeningPort(hanaSystemPort).WithStartupTimeout(hanaStartupWait),
		),
	)
	cleanup := func() {
		if ctr != nil {
			_ = testcontainers.TerminateContainer(ctr)
		}
	}
	if err != nil {
		cleanup()
		return "", nil, err
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		cleanup()
		return "", nil, err
	}
	port, err := ctr.MappedPort(ctx, hanaSystemPort)
	if err != nil {
		cleanup()
		return "", nil, err
	}
	u := url.URL{
		Scheme: "hdb",
		User:   url.UserPassword(hanaUser, hanaPassword),
		Host:   fmt.Sprintf("%s:%s", host, port.Port()),
	}
	return u.String(), cleanup, nil
}

func openTestDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("hdb", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

// createTable creates a uniquely named table from the column definition list
// and drops it when the test ends. It returns the table name.
func createTable(t *testing.T, db *sql.DB, prefix, columns string) string {
	t.Helper()
	name := fmt.Sprintf("%s_%d_%d", strings.ToUpper(prefix), time.Now().UnixNano()%1_000_000, hanaTableID.Add(1))
	_, err := db.ExecContext(t.Context(), `CREATE TABLE `+quoteIdentifier(name)+` (`+columns+`)`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DROP TABLE `+quoteIdentifier(name))
	})
	return name
}

// currentSchema returns the schema unqualified names resolve against for the
// test connection (the connecting user's own schema).
func currentSchema(t *testing.T, db *sql.DB) string {
	t.Helper()
	var schema string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT CURRENT_SCHEMA FROM DUMMY`).Scan(&schema))
	return schema
}

// createRestrictedUser creates a user holding nothing but SELECT on the given
// objects of schema, the way a least-privilege cloud user would be set up,
// and returns a DSN connecting as it. The user is dropped when the test ends.
func createRestrictedUser(t *testing.T, db *sql.DB, dsn, schema string, objects ...string) (string, string) {
	t.Helper()
	const password = "Rpcn1Restricted"
	// CREATE USER takes a bare identifier, so the name is plain upper case.
	user := fmt.Sprintf("RPCN_R_%d_%d", time.Now().UnixNano()%1_000_000, hanaTableID.Add(1))
	_, err := db.ExecContext(t.Context(), `CREATE USER `+user+` PASSWORD "`+password+`" NO FORCE_FIRST_PASSWORD_CHANGE`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DROP USER `+user+` CASCADE`)
	})
	for _, obj := range objects {
		_, err := db.ExecContext(t.Context(), `GRANT SELECT ON `+quoteIdentifier(schema)+`.`+quoteIdentifier(obj)+` TO `+user)
		require.NoError(t, err)
	}
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	u.User = url.UserPassword(user, password)
	return user, u.String()
}

// disconnectSessions kills every open session of user from the admin
// connection, as an operator, a network reset or an idle-session timeout
// would, and returns how many were killed.
func disconnectSessions(t *testing.T, db *sql.DB, user string) int {
	t.Helper()
	rows, err := db.QueryContext(t.Context(),
		`SELECT CONNECTION_ID FROM M_CONNECTIONS WHERE USER_NAME = ? AND CONNECTION_ID <> CURRENT_CONNECTION AND CONNECTION_STATUS <> 'DISCONNECTED'`, user)
	require.NoError(t, err)
	var ids []int64
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	killed := 0
	for _, id := range ids {
		if _, err := db.ExecContext(t.Context(), fmt.Sprintf(`ALTER SYSTEM DISCONNECT SESSION '%d'`, id)); err == nil {
			killed++
		}
	}
	return killed
}

// newIntegrationInput builds and connects an input from YAML; the test owns
// its lifetime.
func newIntegrationInput(t *testing.T, res *service.Resources, yaml string) *sapHANAInput {
	t.Helper()
	conf, err := sapHANAInputConfigSpec.ParseYAML(yaml, nil)
	require.NoError(t, err)
	input, err := newSAPHANAInput(conf, res)
	require.NoError(t, err)
	t.Cleanup(func() { _ = input.Close(context.Background()) })
	require.NoError(t, input.Connect(t.Context()))
	return input
}

// decodeRow unmarshals a message payload into a map.
func decodeRow(t *testing.T, msg *service.Message) map[string]any {
	t.Helper()
	raw, err := msg.AsBytes()
	require.NoError(t, err)
	var row map[string]any
	require.NoError(t, json.Unmarshal(raw, &row))
	return row
}

// readRows reads batches, acknowledging each, until want rows have been
// collected or the timeout passes. A one-shot mode ending early returns what
// was read so far.
func readRows(t *testing.T, input *sapHANAInput, want int, timeout time.Duration) []map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	var rows []map[string]any
	for len(rows) < want {
		batch, ack, err := input.ReadBatch(ctx)
		if errors.Is(err, service.ErrEndOfInput) || ctx.Err() != nil {
			break
		}
		require.NoError(t, err)
		require.NoError(t, ack(context.Background(), nil))
		for _, msg := range batch {
			rows = append(rows, decodeRow(t, msg))
		}
	}
	return rows
}

// readAllMessages drains a one-shot (bulk or query) input.
func readAllMessages(t *testing.T, yaml string) []map[string]any {
	t.Helper()
	input := newIntegrationInput(t, enterpriseResources(), yaml)
	return readRows(t, input, 1<<30, 30*time.Second)
}

// expectNoRows asserts that a polling input delivers nothing within d.
func expectNoRows(t *testing.T, input *sapHANAInput, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	defer cancel()
	batch, ack, err := input.ReadBatch(ctx)
	if err == nil {
		_ = ack(context.Background(), nil)
		require.Empty(t, batch, "unexpected rows: %v", batch)
	}
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func intsOf(t *testing.T, rows []map[string]any, col string) []int64 {
	t.Helper()
	out := make([]int64, 0, len(rows))
	for _, row := range rows {
		v, ok := row[col].(float64)
		require.True(t, ok, "column %s: expected a JSON number, got %T (%v)", col, row[col], row[col])
		out = append(out, int64(v))
	}
	return out
}

func seq(from, to int64) []int64 {
	out := make([]int64, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

func TestIntegrationSAPHANAInputBulk(t *testing.T) {
	dsn := requireHANA(t)
	db := openTestDB(t, dsn)
	t.Log("Given: a table with three rows")
	table := createTable(t, db, "bulk", `ID INTEGER, NAME NVARCHAR(100)`)
	for i := 1; i <= 3; i++ {
		_, err := db.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (?, ?)`, i, fmt.Sprintf("row-%d", i))
		require.NoError(t, err)
	}

	t.Log("When: a bulk input reads the table")
	rows := readAllMessages(t, fmt.Sprintf(`
dsn: %q
mode: bulk
table: %s
`, dsn, table))

	t.Log("Then: every row is emitted once with its columns")
	assert.ElementsMatch(t, []int64{1, 2, 3}, intsOf(t, rows, "ID"))
	for _, row := range rows {
		assert.Equal(t, fmt.Sprintf("row-%v", row["ID"]), row["NAME"])
	}
}

func TestIntegrationSAPHANAInputQuery(t *testing.T) {
	dsn := requireHANA(t)
	db := openTestDB(t, dsn)
	t.Log("Given: a table with active and inactive rows")
	table := createTable(t, db, "query", `ID INTEGER, ACTIVE TINYINT`)
	for i, active := range []int{1, 0, 1, 0} {
		_, err := db.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (?, ?)`, i+1, active)
		require.NoError(t, err)
	}

	t.Log("When: a query input selects the active rows")
	rows := readAllMessages(t, fmt.Sprintf(`
dsn: %q
mode: query
query: SELECT * FROM %s WHERE ACTIVE = 1
`, dsn, quoteIdentifier(table)))

	t.Log("Then: only the active rows are emitted")
	assert.ElementsMatch(t, []int64{1, 3}, intsOf(t, rows, "ID"))
}

// Incrementing mode emits the backlog on the first poll, honours a configured
// initial value, and then picks up rows inserted later without re-delivering
// anything.
func TestIntegrationSAPHANAInputIncrementing(t *testing.T) {
	dsn := requireHANA(t)
	db := openTestDB(t, dsn)
	table := createTable(t, db, "inc", `ID BIGINT, VAL NVARCHAR(50)`)
	insert := func(from, to int64) {
		for i := from; i <= to; i++ {
			_, err := db.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (?, ?)`, i, fmt.Sprintf("v-%d", i))
			require.NoError(t, err)
		}
	}
	t.Log("Given: a table with a backlog of five rows")
	insert(1, 5)

	conf := fmt.Sprintf(`
dsn: %q
mode: incrementing
table: %s
incrementing_column: ID
poll_interval: 200ms
fetch_size: 2
`, dsn, table)

	t.Log("When: an incrementing input starts")
	input := newIntegrationInput(t, enterpriseResources(), conf)
	t.Log("Then: the backlog is emitted in key order")
	assert.Equal(t, seq(1, 5), intsOf(t, readRows(t, input, 5, 30*time.Second), "ID"))
	expectNoRows(t, input, time.Second)

	t.Log("When: three more rows are inserted")
	insert(6, 8)
	t.Log("Then: only the new rows are emitted")
	assert.Equal(t, seq(6, 8), intsOf(t, readRows(t, input, 3, 30*time.Second), "ID"))
	expectNoRows(t, input, time.Second)

	t.Log("And: an input with incrementing_initial_value 3 starts after it")
	withInitial := newIntegrationInput(t, enterpriseResources(), conf+"incrementing_initial_value: \"3\"\n")
	assert.Equal(t, seq(4, 8), intsOf(t, readRows(t, withInitial, 5, 30*time.Second), "ID"))
}

// A restart resumes from the checkpoint written by the last acknowledged
// batch: nothing before it is re-delivered, nothing after it is missed.
func TestIntegrationSAPHANAIncrementingResumesFromCheckpoint(t *testing.T) {
	dsn := requireHANA(t)
	db := openTestDB(t, dsn)
	t.Log("Given: a table with ten rows")
	table := createTable(t, db, "incres", `ID BIGINT PRIMARY KEY, VAL NVARCHAR(50)`)
	for i := int64(1); i <= 10; i++ {
		_, err := db.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (?, ?)`, i, fmt.Sprintf("v-%d", i))
		require.NoError(t, err)
	}
	conf := fmt.Sprintf(`
dsn: %q
mode: incrementing
table: %s
incrementing_column: ID
poll_interval: 200ms
fetch_size: 3
checkpoint_cache: %s
`, dsn, table, testCacheName)

	t.Log("When: one batch is acknowledged, a second is read but not acknowledged, and the input stops")
	res := enterpriseResourcesWithCache()
	first := newIntegrationInput(t, res, conf)
	batch, ack, err := first.ReadBatch(t.Context())
	require.NoError(t, err)
	require.NoError(t, ack(context.Background(), nil))
	assert.Len(t, batch, 3)
	// A second batch is read but never acknowledged before the "crash".
	_, _, err = first.ReadBatch(t.Context())
	require.NoError(t, err)
	require.NoError(t, first.Close(context.Background()))

	t.Log("Then: only the acknowledged batch is checkpointed")
	cp := readCheckpoint(t, res)
	require.NotNil(t, cp)
	require.NotNil(t, cp.IncrHWMInt)
	assert.Equal(t, int64(3), *cp.IncrHWMInt, "only the acknowledged batch is checkpointed")
	assert.Equal(t, quoteIdentifier(table), cp.Source)

	t.Log("And: a restarted input resumes after it, re-delivering the unacknowledged batch")
	second := newIntegrationInput(t, res, conf)
	assert.Equal(t, seq(4, 10), intsOf(t, readRows(t, second, 7, 30*time.Second), "ID"))
	expectNoRows(t, second, time.Second)
	cp = readCheckpoint(t, res)
	require.NotNil(t, cp.IncrHWMInt)
	assert.Equal(t, int64(10), *cp.IncrHWMInt)
}

// Timestamp mode bounds each poll by the clock timestamp_clock names minus
// timestamp_delay (a fractional delay binds a non-integer ADD_SECONDS
// argument), sees rows inserted later, and never re-delivers a row. The
// container runs ahead of UTC, so a row stamped between the UTC clock and the
// local clock is inside the database window but outside the database_utc
// one: the subtests fail if the clock choice were ignored.
func TestIntegrationSAPHANAInputTimestamp(t *testing.T) {
	dsn := requireHANA(t)
	db := openTestDB(t, dsn)

	var offset int64
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT SECONDS_BETWEEN(CURRENT_UTCTIMESTAMP, CURRENT_TIMESTAMP) FROM DUMMY`).Scan(&offset))
	if offset <= 0 {
		if os.Getenv(hanaDSNEnv) != "" {
			t.Skipf("the instance behind %s runs at UTC%+ds; the clock subtests need a local clock ahead of UTC", hanaDSNEnv, offset)
		}
		t.Fatalf("the HANA container should run ahead of UTC (TZ=%s), got an offset of %ds", hanaTimezone, offset)
	}

	for _, tc := range []struct {
		clock, now  string
		wantBetween bool
	}{
		{clock: "database", now: "CURRENT_TIMESTAMP", wantBetween: true},
		{clock: "database_utc", now: "CURRENT_UTCTIMESTAMP", wantBetween: false},
	} {
		t.Run(tc.clock, func(t *testing.T) {
			t.Logf("Given: rows stamped with %s, and row 99 stamped between the UTC and local clocks", tc.now)
			table := createTable(t, db, "ts", `ID INTEGER, UPDATED_AT TIMESTAMP`)
			insertNow := func(ids ...int) {
				for _, id := range ids {
					_, err := db.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (?, `+tc.now+`)`, id)
					require.NoError(t, err)
				}
			}
			insertNow(1, 2, 3)
			_, err := db.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (99, ADD_SECONDS(CURRENT_UTCTIMESTAMP, ?))`, offset/2)
			require.NoError(t, err)

			t.Logf("When: a timestamp input polls with timestamp_clock %s", tc.clock)
			res := enterpriseResourcesWithCache()
			input := newIntegrationInput(t, res, fmt.Sprintf(`
dsn: %q
mode: timestamp
table: %s
timestamp_column: UPDATED_AT
timestamp_clock: %s
timestamp_delay: 500ms
poll_interval: 200ms
checkpoint_cache: %s
`, dsn, table, tc.clock, testCacheName))

			want := []int64{1, 2, 3}
			if tc.wantBetween {
				want = append(want, 99)
			}
			t.Logf("Then: it emits %v", want)
			assert.ElementsMatch(t, want, intsOf(t, readRows(t, input, len(want), 30*time.Second), "ID"))
			expectNoRows(t, input, time.Second)

			t.Log("And: rows inserted later are emitted once")
			insertNow(4, 5)
			assert.ElementsMatch(t, []int64{4, 5}, intsOf(t, readRows(t, input, 2, 30*time.Second), "ID"))
			expectNoRows(t, input, time.Second)

			var dbNow time.Time
			require.NoError(t, db.QueryRowContext(t.Context(), `SELECT `+tc.now+` FROM DUMMY`).Scan(&dbNow))
			cp := readCheckpoint(t, res)
			require.NotNil(t, cp)
			require.NotNil(t, cp.TimestampHWM, "the window bound is checkpointed")
			assert.WithinDuration(t, dbNow, *cp.TimestampHWM, time.Minute,
				"the checkpointed bound must come from the %s clock", tc.now)
		})
	}
}

// timestamp+incrementing splits a group of rows sharing one timestamp across
// fetch_size boundaries and across a restart without duplicating or skipping
// any of them, and its checkpoint carries the (timestamp, id) resume pair.
func TestIntegrationSAPHANAInputTimestampIncrementingTiesAndResume(t *testing.T) {
	dsn := requireHANA(t)
	db := openTestDB(t, dsn)
	t.Log("Given: ten rows, ids 1 to 6 sharing one timestamp")
	table := createTable(t, db, "tsinc", `ID INTEGER, UPDATED_AT TIMESTAMP`)
	// Ten rows: ids 1..6 share one timestamp, 7..10 a later one.
	tieTime := time.Date(2024, 5, 1, 10, 0, 0, 0, time.UTC)
	for i := 1; i <= 10; i++ {
		ts := tieTime
		if i > 6 {
			ts = tieTime.Add(time.Second)
		}
		_, err := db.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (?, ?)`, i, ts)
		require.NoError(t, err)
	}
	conf := fmt.Sprintf(`
dsn: %q
mode: timestamp+incrementing
table: %s
timestamp_column: UPDATED_AT
incrementing_column: ID
timestamp_delay: 0s
poll_interval: 200ms
fetch_size: 4
checkpoint_cache: %s
`, dsn, table, testCacheName)

	t.Log("When: the first batch, ending inside the tie group, is acknowledged and the input stops")
	res := enterpriseResourcesWithCache()
	first := newIntegrationInput(t, res, conf)
	batch, ack, err := first.ReadBatch(t.Context())
	require.NoError(t, err)
	require.NoError(t, ack(context.Background(), nil))
	require.Len(t, batch, 4, "first batch stops at fetch_size inside the tie group")
	require.NoError(t, first.Close(context.Background()))

	t.Log("Then: the checkpoint is the last delivered (timestamp, id) pair")
	cp := readCheckpoint(t, res)
	require.NotNil(t, cp)
	require.NotNil(t, cp.IncrHWMInt)
	require.NotNil(t, cp.TimestampHWM)
	assert.Equal(t, int64(4), *cp.IncrHWMInt, "mid-window checkpoint is the last delivered row's id")
	assert.Equal(t, tieTime, cp.TimestampHWM.UTC())

	t.Log("And: a restarted input continues with id 5 and later rows")
	second := newIntegrationInput(t, res, conf)
	assert.Equal(t, seq(5, 10), intsOf(t, readRows(t, second, 6, 30*time.Second), "ID"))
	expectNoRows(t, second, time.Second)

	// Rows arriving later with a fresh timestamp are seen on the next poll.
	_, err = db.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (11, CURRENT_TIMESTAMP)`)
	require.NoError(t, err)
	assert.Equal(t, []int64{11}, intsOf(t, readRows(t, second, 1, 30*time.Second), "ID"))
}

// timestamp_column is checked against the catalog at connect time, with and
// without schema_name (the latter resolves the table against CURRENT_SCHEMA).
func TestIntegrationSAPHANATimestampColumnTypeChecked(t *testing.T) {
	dsn := requireHANA(t)
	db := openTestDB(t, dsn)
	t.Log("Given: a timestamp_column stored as DECIMAL(15,0)")
	table := createTable(t, db, "tstype", `ID INTEGER, CHANGED DECIMAL(15,0)`)
	schema := currentSchema(t, db)

	for name, schemaLine := range map[string]string{"current_schema": "", "schema_name": "schema_name: " + schema} {
		t.Run(name, func(t *testing.T) {
			conf, err := sapHANAInputConfigSpec.ParseYAML(fmt.Sprintf(`
dsn: %q
mode: timestamp
table: %s
timestamp_column: CHANGED
%s
`, dsn, table, schemaLine), nil)
			require.NoError(t, err)
			input, err := newSAPHANAInput(conf, enterpriseResources())
			require.NoError(t, err)
			t.Cleanup(func() { _ = input.Close(context.Background()) })
			t.Log("When: a timestamp input connects")
			err = input.Connect(t.Context())
			t.Log("Then: Connect fails naming the column type")
			require.ErrorContains(t, err, "has type DECIMAL")
		})
	}
}

// The configured incrementing_initial_value is bound with the key column's
// catalog type: text keys keep leading zeros, timestamp keys parse RFC3339,
// binary keys are hexadecimal.
func TestIntegrationSAPHANAIncrementingInitialValueTypes(t *testing.T) {
	dsn := requireHANA(t)
	db := openTestDB(t, dsn)

	cases := []struct {
		name, columnType, initial string
		values                    []string
		insert                    func(table string, v string)
		wantIDs                   []int64
	}{
		{
			name: "nvarchar", columnType: "NVARCHAR(10)", initial: "0005",
			values:  []string{"0001", "0005", "0010", "0100"},
			wantIDs: []int64{3, 4},
		},
		{
			name: "timestamp", columnType: "TIMESTAMP", initial: "2024-01-02T00:00:00Z",
			values:  []string{"2024-01-01 00:00:00", "2024-01-02 00:00:00", "2024-01-02 00:00:01", "2024-06-01 12:00:00"},
			wantIDs: []int64{3, 4},
		},
		{
			name: "varbinary", columnType: "VARBINARY(4)", initial: "0000aa00",
			values:  []string{"00000100", "0000aa00", "0000aa01", "ff000000"},
			wantIDs: []int64{3, 4},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Given: a %s key with values %v", tc.columnType, tc.values)
			table := createTable(t, db, "init_"+tc.name, `ID INTEGER, KEY `+tc.columnType)
			for i, v := range tc.values {
				// go-hdb converts bind parameters client-side and accepts no
				// string for a TIMESTAMP or VARBINARY column (the same rule
				// the input's initial-value coercion exists for).
				var arg any = v
				switch tc.name {
				case "varbinary":
					b, err := hex.DecodeString(v)
					require.NoError(t, err)
					arg = b
				case "timestamp":
					ts, err := time.Parse("2006-01-02 15:04:05", v)
					require.NoError(t, err)
					arg = ts
				}
				_, err := db.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (?, ?)`, i+1, arg)
				require.NoError(t, err)
			}
			t.Logf("When: an input starts with incrementing_initial_value %q", tc.initial)
			res := enterpriseResourcesWithCache()
			input := newIntegrationInput(t, res, fmt.Sprintf(`
dsn: %q
mode: incrementing
table: %s
incrementing_column: KEY
incrementing_initial_value: %q
poll_interval: 200ms
checkpoint_cache: %s
`, dsn, table, tc.initial, testCacheName))
			t.Log("Then: only the rows after the initial value are emitted")
			assert.Equal(t, tc.wantIDs, intsOf(t, readRows(t, input, len(tc.wantIDs), 30*time.Second), "ID"))
			expectNoRows(t, input, time.Second)

			t.Log("And: the persisted HWM binds on the next connect too")
			require.NoError(t, input.Close(context.Background()))
			resumed := newIntegrationInput(t, res, fmt.Sprintf(`
dsn: %q
mode: incrementing
table: %s
incrementing_column: KEY
poll_interval: 200ms
checkpoint_cache: %s
`, dsn, table, testCacheName))
			expectNoRows(t, resumed, time.Second)
		})
	}
}

// A DECIMAL key with a fractional scale under best_fit is tracked as a
// float64 HWM; the bind must not re-match the last delivered row forever.
func TestIntegrationSAPHANABestFitDecimalKey(t *testing.T) {
	dsn := requireHANA(t)
	db := openTestDB(t, dsn)
	t.Log("Given: a DECIMAL(10,2) key read with numeric_mapping best_fit")
	table := createTable(t, db, "bestfit", `ID INTEGER, SEQ DECIMAL(10,2)`)
	for i, v := range []string{"1.10", "2.20", "3.30"} {
		_, err := db.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (?, ?)`, i+1, v)
		require.NoError(t, err)
	}
	input := newIntegrationInput(t, enterpriseResources(), fmt.Sprintf(`
dsn: %q
mode: incrementing
table: %s
incrementing_column: SEQ
numeric_mapping: best_fit
poll_interval: 200ms
`, dsn, table))
	t.Log("When: the backlog is read")
	rows := readRows(t, input, 3, 30*time.Second)
	t.Log("Then: each row is emitted once and the poll does not re-match the last one")
	assert.Equal(t, seq(1, 3), intsOf(t, rows, "ID"))
	assert.Equal(t, 3.3, rows[2]["SEQ"], "best_fit emits a DECIMAL(10,2) as a double")
	expectNoRows(t, input, time.Second)

	t.Log("And: a later row is picked up")
	_, err := db.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (4, 4.40)`)
	require.NoError(t, err)
	assert.Equal(t, []int64{4}, intsOf(t, readRows(t, input, 1, 30*time.Second), "ID"))
	expectNoRows(t, input, time.Second)
}

// Identifiers are quoted on the way to HANA, so SAP's namespaced column names
// work as the key and reach the payload and the schema metadata unchanged.
func TestIntegrationSAPHANASlashColumnNames(t *testing.T) {
	dsn := requireHANA(t)
	db := openTestDB(t, dsn)
	t.Log("Given: a /BIC/ table with /BIC/ column names")
	table := createTable(t, db, "/BIC/AZ", `"/BIC/ZID" INTEGER PRIMARY KEY, "/BIC/ZNAME" NVARCHAR(20)`)
	for i := 1; i <= 2; i++ {
		_, err := db.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (?, ?)`, i, fmt.Sprintf("n%d", i))
		require.NoError(t, err)
	}
	input := newIntegrationInput(t, enterpriseResources(), fmt.Sprintf(`
dsn: %q
mode: incrementing
schema_name: %s
table: %q
incrementing_column: /BIC/ZID
poll_interval: 200ms
`, dsn, currentSchema(t, db), table))
	t.Log("When: the input reads keyed on /BIC/ZID")
	batch, ack, err := input.ReadBatch(t.Context())
	require.NoError(t, err)
	require.NoError(t, ack(context.Background(), nil))
	require.Len(t, batch, 2)
	t.Log("Then: the names reach the payload and the schema metadata unchanged")
	assert.Equal(t, []int64{1, 2}, intsOf(t, []map[string]any{decodeRow(t, batch[0]), decodeRow(t, batch[1])}, "/BIC/ZID"))

	pk, ok := batch[0].MetaGet("primary_key_columns")
	require.True(t, ok)
	assert.Equal(t, `["/BIC/ZID"]`, pk)
	schemaVal, ok := batch[0].MetaGetMut("schema")
	require.True(t, ok)
	assert.Contains(t, fieldNames(t, schemaVal), "/BIC/ZNAME")
}

// fieldNames returns the names of the top-level fields of a schema tree.
func fieldNames(t *testing.T, schemaVal any) []string {
	t.Helper()
	tree, ok := schemaVal.(map[string]any)
	require.True(t, ok, "schema metadata should be a tree, got %T", schemaVal)
	children, ok := tree["children"].([]any)
	require.True(t, ok, "schema tree has no children: %v", tree)
	names := make([]string, 0, len(children))
	for _, c := range children {
		cm, ok := c.(map[string]any)
		require.True(t, ok)
		names = append(names, fmt.Sprint(cm["name"]))
	}
	return names
}

// Every HANA column type the type map knows about round-trips through a
// real result set, both with catalog schema metadata (schema_name set) and
// with only the driver's column metadata (schema_name unset), where go-hdb
// reports the wire type names (LONGDATE, FIXED8, ...) rather than the
// catalog's.
func TestIntegrationSAPHANAColumnTypes(t *testing.T) {
	dsn := requireHANA(t)
	db := openTestDB(t, dsn)
	t.Log("Given: one row covering every column type the type map knows")
	table := createTable(t, db, "types", `
ID INTEGER PRIMARY KEY,
C_TINYINT TINYINT, C_SMALLINT SMALLINT, C_BIGINT BIGINT,
C_DEC_INT DECIMAL(12,0), C_DEC_SCALED DECIMAL(10,2), C_DEC_WIDE DECIMAL(30,5), C_DEC_FLOATING DECIMAL, C_SMALLDEC SMALLDECIMAL,
C_REAL REAL, C_DOUBLE DOUBLE,
C_VARCHAR VARCHAR(20), C_NVARCHAR NVARCHAR(20), C_ALPHANUM ALPHANUM(10), C_NCLOB NCLOB, C_BLOB BLOB, C_VARBINARY VARBINARY(8),
C_DATE DATE, C_TIME TIME, C_TIMESTAMP TIMESTAMP, C_SECONDDATE SECONDDATE, C_BOOLEAN BOOLEAN,
C_NULL NVARCHAR(5)`)
	_, err := db.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (
1, 7, 300, 9007199254740993,
123456789012, 12.34, 1234567890123456789012345.12345, 0.000001234, 1.5,
2.5, 3.25,
'ascii', 'ünïcödé', 'A1', 'a long text', ?, ?,
'2024-02-29', '13:45:30', '2024-02-29 13:45:30.123456', '2024-02-29 13:45:30', TRUE,
NULL)`, []byte("blob\x00bytes"), []byte{0xde, 0xad, 0xbe, 0xef})
	require.NoError(t, err)

	schema := currentSchema(t, db)
	for name, schemaLine := range map[string]string{"driver_types": "", "catalog_types": "schema_name: " + schema} {
		t.Run(name, func(t *testing.T) {
			input := newIntegrationInput(t, enterpriseResources(), fmt.Sprintf(`
dsn: %q
mode: bulk
table: %s
%s
`, dsn, table, schemaLine))
			t.Logf("When: a bulk input reads it (%s)", name)
			batch, ack, err := input.ReadBatch(t.Context())
			require.NoError(t, err)
			require.NoError(t, ack(context.Background(), nil))
			require.Len(t, batch, 1)
			row := decodeRow(t, batch[0])
			t.Log("Then: each column has its expected JSON value and schema type")

			want := map[string]any{
				"ID":             float64(1),
				"C_TINYINT":      float64(7),
				"C_SMALLINT":     float64(300),
				"C_BIGINT":       float64(9007199254740993), // JSON number, compared as float64 by the decoder
				"C_DEC_INT":      float64(123456789012),
				"C_DEC_SCALED":   "12.34",
				"C_DEC_WIDE":     "1234567890123456789012345.12345",
				"C_SMALLDEC":     "1.5",
				"C_REAL":         2.5,
				"C_DOUBLE":       3.25,
				"C_VARCHAR":      "ascii",
				"C_NVARCHAR":     "ünïcödé",
				"C_ALPHANUM":     "A1",
				"C_NCLOB":        "a long text",
				"C_BLOB":         base64.StdEncoding.EncodeToString([]byte("blob\x00bytes")),
				"C_VARBINARY":    base64.StdEncoding.EncodeToString([]byte{0xde, 0xad, 0xbe, 0xef}),
				"C_DATE":         "2024-02-29T00:00:00Z",
				"C_TIME":         "0001-01-01T13:45:30Z",
				"C_TIMESTAMP":    "2024-02-29T13:45:30.123456Z",
				"C_SECONDDATE":   "2024-02-29T13:45:30Z",
				"C_BOOLEAN":      true,
				"C_NULL":         nil,
				"C_DEC_FLOATING": "0.000001234",
			}
			for col, w := range want {
				assert.Equal(t, w, row[col], "column %s", col)
			}
			assert.Len(t, row, len(want), "unexpected extra columns: %v", row)

			if schemaLine == "" {
				_, ok := batch[0].MetaGet("schema")
				assert.False(t, ok, "no schema metadata without schema_name")
				return
			}
			schemaVal, ok := batch[0].MetaGetMut("schema")
			require.True(t, ok)
			assert.Equal(t, map[string]string{
				"ID": "INT64", "C_TINYINT": "INT64", "C_SMALLINT": "INT64", "C_BIGINT": "INT64",
				"C_DEC_INT": "INT64", "C_DEC_SCALED": "DECIMAL", "C_DEC_WIDE": "DECIMAL", "C_DEC_FLOATING": "BIG_DECIMAL", "C_SMALLDEC": "BIG_DECIMAL",
				"C_REAL": "FLOAT32", "C_DOUBLE": "FLOAT64",
				"C_VARCHAR": "STRING", "C_NVARCHAR": "STRING", "C_ALPHANUM": "STRING", "C_NCLOB": "STRING",
				"C_BLOB": "BYTE_ARRAY", "C_VARBINARY": "BYTE_ARRAY",
				"C_DATE": "DATE", "C_TIME": "TIMESTAMP", "C_TIMESTAMP": "TIMESTAMP", "C_SECONDDATE": "TIMESTAMP",
				"C_BOOLEAN": "BOOLEAN", "C_NULL": "STRING",
			}, fieldTypes(t, schemaVal))
			pk, ok := batch[0].MetaGet("primary_key_columns")
			require.True(t, ok)
			assert.Equal(t, `["ID"]`, pk)
			tn, _ := batch[0].MetaGet("table_name")
			assert.Equal(t, table, tn)
			ds, _ := batch[0].MetaGet("database_schema")
			assert.Equal(t, schema, ds)
		})
	}
}

// fieldTypes maps each top-level schema field to its type name.
func fieldTypes(t *testing.T, schemaVal any) map[string]string {
	t.Helper()
	tree, ok := schemaVal.(map[string]any)
	require.True(t, ok, "schema metadata should be a tree, got %T", schemaVal)
	children, ok := tree["children"].([]any)
	require.True(t, ok, "schema tree has no children: %v", tree)
	out := make(map[string]string, len(children))
	for _, c := range children {
		cm, ok := c.(map[string]any)
		require.True(t, ok)
		out[fmt.Sprint(cm["name"])] = fmt.Sprint(cm["type"])
	}
	return out
}

// The registered component runs end to end inside a stream: the enterprise
// license check, auto_replay_nacks wrapping and a real cache resource holding
// the checkpoint.
func TestIntegrationSAPHANAStream(t *testing.T) {
	dsn := requireHANA(t)
	db := openTestDB(t, dsn)
	t.Log("Given: a table with five rows and a stream with a memory checkpoint cache")
	table := createTable(t, db, "stream", `ID INTEGER PRIMARY KEY, VAL NVARCHAR(20)`)
	for i := 1; i <= 5; i++ {
		_, err := db.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (?, ?)`, i, fmt.Sprintf("v%d", i))
		require.NoError(t, err)
	}

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetLoggerYAML(`level: DEBUG`))
	require.NoError(t, sb.AddCacheYAML(`
label: hana_cp
memory: {}
`))
	require.NoError(t, sb.AddInputYAML(fmt.Sprintf(`
sap_hana:
  dsn: %q
  mode: incrementing
  schema_name: %s
  table: %s
  incrementing_column: ID
  poll_interval: 200ms
  fetch_size: 2
  checkpoint_cache: hana_cp
  checkpoint_cache_key: stream_test
`, dsn, currentSchema(t, db), table)))

	var (
		mut  sync.Mutex
		seen []int64
	)
	require.NoError(t, sb.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		mut.Lock()
		defer mut.Unlock()
		for _, msg := range mb {
			seen = append(seen, intsOf(t, []map[string]any{decodeRow(t, msg)}, "ID")...)
		}
		return nil
	}))
	stream, err := sb.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	t.Log("When: the stream runs and a sixth row is inserted")
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- stream.Run(ctx) }()

	require.Eventually(t, func() bool {
		mut.Lock()
		defer mut.Unlock()
		return len(seen) == 5
	}, 30*time.Second, 100*time.Millisecond)
	_, err = db.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (6, 'v6')`)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		mut.Lock()
		defer mut.Unlock()
		return len(seen) == 6
	}, 30*time.Second, 100*time.Millisecond)
	t.Log("Then: all six rows arrive in order and the checkpoint holds the last id")
	assert.Equal(t, seq(1, 6), seen)

	var raw []byte
	require.NoError(t, stream.Resources().AccessCache(t.Context(), "hana_cp", func(c service.Cache) {
		var gErr error
		raw, gErr = c.Get(t.Context(), "stream_test")
		require.NoError(t, gErr)
	}))
	var cp sapHANACheckpointState
	require.NoError(t, json.Unmarshal(raw, &cp))
	require.NotNil(t, cp.IncrHWMInt)
	assert.Equal(t, int64(6), *cp.IncrHWMInt)

	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("stream stopped with error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("stream did not stop")
	}
}

// A session killed while a cursor is open (an operator, a network reset, or
// HANA Cloud's idle-session timeout) must not lose rows: go-hdb surfaces the
// broken fetch as an error, the input rewinds to its last safe position, and
// the rows already scanned (the peeked one and the rest of the fetch-ahead)
// are read again after the reconnect. Pure timestamp mode is covered too: it
// is the one mode that peeks ahead to close a timestamp window.
func TestIntegrationSAPHANASessionKilledMidCursor(t *testing.T) {
	dsn := requireHANA(t)
	db := openTestDB(t, dsn)
	schema := currentSchema(t, db)
	base := time.Date(2024, 3, 1, 8, 0, 0, 0, time.UTC)
	// HANA answers the statement itself with a first chunk of rows larger
	// than fetch_size; enough rows keep the cursor open on the server.
	const n = int64(500)

	for _, mode := range []string{"incrementing", "timestamp", "timestamp+incrementing"} {
		t.Run(mode, func(t *testing.T) {
			t.Logf("Given: %d rows with distinct ids and timestamps, read by a dedicated user with fetch_size 2", n)
			table := createTable(t, db, "kill", `ID BIGINT PRIMARY KEY, UPDATED_AT TIMESTAMP`)
			for i := int64(1); i <= n; i++ {
				_, err := db.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (?, ?)`, i, base.Add(time.Duration(i)*time.Second))
				require.NoError(t, err)
			}
			user, userDSN := createRestrictedUser(t, db, dsn, schema, table)
			conf := fmt.Sprintf(`
dsn: %q
mode: %s
schema_name: %s
table: %s
poll_interval: 200ms
fetch_size: 2
retry_backoff: 100ms
checkpoint_cache: %s
`, userDSN, mode, schema, table, testCacheName)
			if mode != "timestamp" {
				conf += "incrementing_column: ID\n"
			}
			if mode != "incrementing" {
				conf += "timestamp_column: UPDATED_AT\ntimestamp_delay: 0s\n"
			}
			res := enterpriseResourcesWithCache()
			input := newIntegrationInput(t, res, conf)

			seen := map[int64]int{}
			batch, ack, err := input.ReadBatch(t.Context())
			require.NoError(t, err)
			require.NoError(t, ack(context.Background(), nil))
			for _, id := range intsOf(t, decodeBatch(t, batch), "ID") {
				seen[id]++
			}
			require.Len(t, batch, 2)

			t.Log("When: the input's sessions are disconnected with the cursor still open")
			require.Positive(t, disconnectSessions(t, db, user), "no session of %s to disconnect", user)

			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			var readErrs int
			for int64(len(seen)) < n && ctx.Err() == nil {
				batch, ack, err := input.ReadBatch(ctx)
				if err != nil {
					// The framework retries a failed ReadBatch and reconnects
					// on ErrNotConnected; do the same.
					readErrs++
					t.Logf("ReadBatch error (expected after the disconnect): %v", err)
					if errors.Is(err, service.ErrNotConnected) {
						require.NoError(t, input.Connect(ctx))
					}
					continue
				}
				require.NoError(t, ack(context.Background(), nil))
				for _, id := range intsOf(t, decodeBatch(t, batch), "ID") {
					seen[id]++
				}
			}

			t.Log("Then: the broken fetch surfaced as an error, and every id arrived at least once")
			assert.Positive(t, readErrs, "the disconnect should have broken the open cursor")
			for id := int64(1); id <= n; id++ {
				assert.Positive(t, seen[id], "id %d was skipped", id)
			}
			expectNoRows(t, input, time.Second)
			cp := readCheckpoint(t, res)
			require.NotNil(t, cp)
			if mode != "timestamp" {
				require.NotNil(t, cp.IncrHWMInt)
				assert.Equal(t, n, *cp.IncrHWMInt, "the final checkpoint is the highest id")
			}
			if mode != "incrementing" {
				require.NotNil(t, cp.TimestampHWM)
				// A drained window moves the HWM to its upper bound (the
				// database clock minus timestamp_delay), past the last row.
				latest := base.Add(time.Duration(n) * time.Second)
				assert.False(t, cp.TimestampHWM.Before(latest),
					"the final checkpoint %v is before the latest row timestamp %v", *cp.TimestampHWM, latest)
			}
		})
	}
}

// timestamp_delay is what stands between the timestamp modes and silent loss
// under MVCC: a row whose timestamp is set inside a transaction that commits
// after the poll covering it is never read unless the delay outlasts the
// transaction. Both sides are pinned here against a real snapshot.
func TestIntegrationSAPHANATimestampCommitLag(t *testing.T) {
	dsn := requireHANA(t)
	db := openTestDB(t, dsn)

	for _, tc := range []struct {
		name, delay string
		wantLate    bool
	}{
		{name: "delay outlasts the transaction", delay: "5s", wantLate: true},
		{name: "zero delay misses the row", delay: "0s", wantLate: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Given: an empty table polled every 200ms with timestamp_delay %s", tc.delay)
			table := createTable(t, db, "lag", `ID INTEGER, UPDATED_AT TIMESTAMP`)
			input := newIntegrationInput(t, enterpriseResources(), fmt.Sprintf(`
dsn: %q
mode: timestamp
table: %s
timestamp_column: UPDATED_AT
timestamp_delay: %s
poll_interval: 200ms
`, dsn, table, tc.delay))
			expectNoRows(t, input, 500*time.Millisecond)

			t.Log("When: a second connection inserts row 1 stamped CURRENT_TIMESTAMP and commits only after several polls")
			tx, err := db.BeginTx(t.Context(), nil)
			require.NoError(t, err)
			_, err = tx.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (1, CURRENT_TIMESTAMP)`)
			require.NoError(t, err)
			expectNoRows(t, input, 2*time.Second)
			require.NoError(t, tx.Commit())

			if tc.wantLate {
				t.Log("Then: the row is delivered once the window passes its timestamp")
				assert.Equal(t, []int64{1}, intsOf(t, readRows(t, input, 1, 30*time.Second), "ID"))
				return
			}
			t.Log("Then: the row is never delivered (the documented limitation), while later rows still are")
			expectNoRows(t, input, 2*time.Second)
			_, err = db.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (2, CURRENT_TIMESTAMP)`)
			require.NoError(t, err)
			assert.Equal(t, []int64{2}, intsOf(t, readRows(t, input, 1, 30*time.Second), "ID"))
		})
	}
}

// A least-privilege user holding only SELECT on the object it reads. HANA
// shows catalog rows for objects a user has a privilege on, so on a table the
// schema metadata and catalog-typed initial value still work. A view has no
// SYS.TABLE_COLUMNS rows at all, so metadata is omitted, but its
// SYS.VIEW_COLUMNS rows still type incrementing_initial_value and the
// timestamp_column check.
func TestIntegrationSAPHANARestrictedUser(t *testing.T) {
	dsn := requireHANA(t)
	db := openTestDB(t, dsn)
	schema := currentSchema(t, db)

	table := createTable(t, db, "restricted", `ID INTEGER PRIMARY KEY, DOC_NO NVARCHAR(10), UPDATED_AT TIMESTAMP`)
	base := time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)
	for i, doc := range []string{"005", "007", "008", "070"} {
		_, err := db.ExecContext(t.Context(), `INSERT INTO `+quoteIdentifier(table)+` VALUES (?, ?, ?)`, i+1, doc, base.Add(time.Duration(i)*time.Second))
		require.NoError(t, err)
	}
	view := table + "_V"
	_, err := db.ExecContext(t.Context(), `CREATE VIEW `+quoteIdentifier(view)+` AS SELECT * FROM `+quoteIdentifier(table))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP VIEW `+quoteIdentifier(view)) })

	for _, tc := range []struct {
		object       string
		wantMetadata bool
	}{
		{object: table, wantMetadata: true},
		{object: view, wantMetadata: false},
	} {
		kind := "table"
		if !tc.wantMetadata {
			kind = "view"
		}
		t.Run(kind, func(t *testing.T) {
			t.Logf("Given: a user with nothing but SELECT on the %s", kind)
			_, userDSN := createRestrictedUser(t, db, dsn, schema, tc.object)

			t.Run("incrementing", func(t *testing.T) {
				t.Log(`When: it reads by an NVARCHAR key from incrementing_initial_value "007"`)
				input := newIntegrationInput(t, enterpriseResources(), fmt.Sprintf(`
dsn: %q
mode: incrementing
schema_name: %s
table: %s
incrementing_column: DOC_NO
incrementing_initial_value: "007"
poll_interval: 200ms
`, userDSN, schema, tc.object))
				assert.Equal(t, "007", input.hwm, "the initial value binds as the literal string")

				t.Log("Then: the rows after 007 flow, with schema metadata only where the catalog describes the object")
				batch, ack, err := input.ReadBatch(t.Context())
				require.NoError(t, err)
				require.NoError(t, ack(context.Background(), nil))
				assert.Equal(t, []int64{3, 4}, intsOf(t, decodeBatch(t, batch), "ID"))
				_, hasSchema := batch[0].MetaGet("schema")
				_, hasPK := batch[0].MetaGet("primary_key_columns")
				assert.Equal(t, tc.wantMetadata, hasSchema, "schema metadata")
				assert.Equal(t, tc.wantMetadata, hasPK, "primary_key_columns metadata")
				tn, _ := batch[0].MetaGet("table_name")
				assert.Equal(t, tc.object, tn)
				expectNoRows(t, input, time.Second)
			})

			t.Run("timestamp", func(t *testing.T) {
				t.Log("When: it reads in timestamp mode")
				input := newIntegrationInput(t, enterpriseResources(), fmt.Sprintf(`
dsn: %q
mode: timestamp
schema_name: %s
table: %s
timestamp_column: UPDATED_AT
timestamp_delay: 0s
poll_interval: 200ms
`, userDSN, schema, tc.object))
				t.Log("Then: every row flows")
				assert.ElementsMatch(t, []int64{1, 2, 3, 4}, intsOf(t, readRows(t, input, 4, 30*time.Second), "ID"))
				expectNoRows(t, input, time.Second)
			})
		})
	}

	t.Run("initial value that does not match the column fails Connect", func(t *testing.T) {
		t.Log("Given: a SELECT-only user reading the view with an initial value that is no timestamp, for a TIMESTAMP column")
		_, userDSN := createRestrictedUser(t, db, dsn, schema, view)
		conf, err := sapHANAInputConfigSpec.ParseYAML(fmt.Sprintf(`
dsn: %q
mode: incrementing
schema_name: %s
table: %s
incrementing_column: UPDATED_AT
incrementing_initial_value: "not-a-timestamp"
`, userDSN, schema, view), nil)
		require.NoError(t, err)
		input, err := newSAPHANAInput(conf, enterpriseResources())
		require.NoError(t, err)
		t.Cleanup(func() { _ = input.Close(context.Background()) })

		t.Log("When: it connects")
		err = input.Connect(t.Context())

		t.Log("Then: Connect fails with a clear error instead of polling forever")
		require.ErrorContains(t, err, `incrementing_initial_value "not-a-timestamp" does not match incrementing_column "UPDATED_AT" of type TIMESTAMP`)
	})
}
