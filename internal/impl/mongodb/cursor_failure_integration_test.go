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

// These tests cover the two exits from the mongodb input's cursor error branch
// in ReadBatch: a cursor that dies mid-read while the read context is still
// live (a real failure, which must tear down and re-query) and a cursor that
// stops because the read context was cancelled (an ordinary shutdown, which
// must terminate cleanly). The file is package mongodb rather than mongodb_test
// because the shutdown test drives newMongoInput's ReadBatch directly - see the
// comment on that test for why the stream level cannot distinguish the paths.

package mongodb

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

// cursorTestAppName scopes the failCommand failpoint to the input's own
// connections, so an unlucky getMore from the test's admin client (or from the
// driver's internal monitoring) cannot consume the single armed failure.
const cursorTestAppName = "rpcn-cursor-failure-test"

// startMongoForCursorTests runs a standalone mongod with enableTestCommands=1,
// which is what unlocks the failCommand failpoint used below. The extra mongod
// arguments are appended by the image's entrypoint, which still injects --auth
// for the root credentials, so the served config matches the other integration
// tests in this package.
func startMongoForCursorTests(t *testing.T) (port string, admin *mongo.Client) {
	t.Helper()

	ctr, err := testcontainers.Run(t.Context(), "mongo:latest",
		testcontainers.WithExposedPorts("27017/tcp"),
		testcontainers.WithEnv(map[string]string{
			"MONGO_INITDB_ROOT_USERNAME": "mongoadmin",
			"MONGO_INITDB_ROOT_PASSWORD": "secret",
		}),
		testcontainers.WithCmd("mongod", "--setParameter", "enableTestCommands=1"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("27017/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "27017/tcp")
	require.NoError(t, err)
	port = mp.Port()

	var client *mongo.Client
	require.Eventually(t, func() bool {
		client, err = mongo.Connect(options.Client().
			SetConnectTimeout(10 * time.Second).
			SetTimeout(30 * time.Second).
			SetServerSelectionTimeout(30 * time.Second).
			SetAppName("cursor-failure-test-admin").
			SetAuth(options.Credential{Username: "mongoadmin", Password: "secret"}).
			ApplyURI("mongodb://localhost:" + port))
		return err == nil && client.Ping(context.Background(), nil) == nil
	}, time.Minute, time.Second, "the mongod never became reachable")
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

	return port, client
}

// seedDocs inserts documents with _id 1..count into the named collection.
func seedDocs(t *testing.T, client *mongo.Client, database, collection string, count int) {
	t.Helper()
	docs := make([]any, 0, count)
	for id := 1; id <= count; id++ {
		docs = append(docs, bson.M{"_id": id, "data": "hello"})
	}
	_, err := client.Database(database).Collection(collection).InsertMany(t.Context(), docs)
	require.NoError(t, err)
}

// cursorLogCapture is a slog.Handler that keeps every record it is given. The
// input reports a dead cursor by returning an error from ReadBatch, which the
// framework logs and then recovers from by reconnecting; the log is the only
// place a test can observe that the failure was seen rather than mistaken for
// the end of the result set.
type cursorLogCapture struct {
	mu      sync.Mutex
	records []string
}

func (*cursorLogCapture) Enabled(context.Context, slog.Level) bool { return true }

func (l *cursorLogCapture) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, r.Level.String()+" "+r.Message)
	return nil
}

func (l *cursorLogCapture) WithAttrs([]slog.Attr) slog.Handler { return l }

func (l *cursorLogCapture) WithGroup(string) slog.Handler { return l }

// matching returns the captured messages containing sub. It snapshots under the
// mutex so it is safe to call while the stream is still running.
func (l *cursorLogCapture) matching(sub string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, r := range l.records {
		if strings.Contains(r, sub) {
			out = append(out, r)
		}
	}
	return out
}

// TestIntegrationMongoInputCursorFailureRequery is the end-to-end proof that a
// cursor which dies mid-read is recovered from rather than mistaken for the end
// of the result set. ReadBatch reports the dead cursor as an error after tearing
// down the cursor and the client, the next read then reports ErrNotConnected,
// and the framework's reconnect re-runs the query from the start - at-least-once
// delivery, so duplicates are expected and only the distinct set is pinned.
//
// The failure is staged with the failCommand failpoint on getMore rather than by
// killing the connection, because that fails exactly one getMore at a
// deterministic point: batch_size 2 over 6 documents means the first batch is
// served by the find itself and every later batch needs a getMore, so the
// failure always lands after the first batch has been delivered.
//
// errorCode 43 (CursorNotFound) is deliberate. getMore is not a retryable read,
// and 43 carries no retryable error label, so the driver surfaces it to
// cursor.Err() instead of transparently retrying it and hiding the failure from
// the input (verified: the driver reports
// "(CursorNotFound) Failing command via 'failCommand' failpoint" and iteration
// stops after exactly 2 documents).
func TestIntegrationMongoInputCursorFailureRequery(t *testing.T) {
	integration.CheckSkip(t)

	const (
		database   = "TestDB"
		collection = "cursorfail"
		docCount   = 6
	)

	port, admin := startMongoForCursorTests(t)
	seedDocs(t, admin, database, collection, docCount)

	// Armed before the stream starts: mode {times: 1} consumes itself on the
	// first getMore the input issues, which is the read that follows the first
	// delivered batch.
	res := admin.Database("admin").RunCommand(t.Context(), bson.D{
		{Key: "configureFailPoint", Value: "failCommand"},
		{Key: "mode", Value: bson.M{"times": 1}},
		{Key: "data", Value: bson.M{
			"errorCode":    43,
			"failCommands": bson.A{"getMore"},
			"appName":      cursorTestAppName,
		}},
	})
	require.NoError(t, res.Err(), "arming the failpoint requires a mongod started with enableTestCommands=1")
	t.Cleanup(func() {
		_ = admin.Database("admin").RunCommand(context.Background(), bson.D{
			{Key: "configureFailPoint", Value: "failCommand"},
			{Key: "mode", Value: "off"},
		}).Err()
	})

	logs := &cursorLogCapture{}
	builder := service.NewStreamBuilder()
	builder.SetLogger(slog.New(logs))
	require.NoError(t, builder.AddInputYAML(`
mongodb:
  url: mongodb://localhost:`+port+`
  database: `+database+`
  collection: `+collection+`
  username: mongoadmin
  password: secret
  app_name: `+cursorTestAppName+`
  json_marshal_mode: relaxed
  batch_size: 2
  query: 'root = {}'
`))

	var (
		mu   sync.Mutex
		ids  []int
		errs []error
	)
	require.NoError(t, builder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		mu.Lock()
		defer mu.Unlock()
		for _, msg := range batch {
			b, err := msg.AsBytes()
			if err != nil {
				errs = append(errs, err)
				continue
			}
			// json_marshal_mode relaxed renders _id as a plain number.
			var doc struct {
				ID int `json:"_id"`
			}
			if err := json.Unmarshal(b, &doc); err != nil {
				errs = append(errs, err)
				continue
			}
			ids = append(ids, doc.ID)
		}
		return nil
	}))

	stream, err := builder.Build()
	require.NoError(t, err)

	// The input is one-shot: once the re-run query is exhausted it reports
	// end-of-input and the stream terminates on its own, so Run returning nil is
	// also the assertion that the recovery ended in a clean shutdown rather than
	// in a reconnect loop.
	runCtx, cancelRun := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancelRun()
	require.NoError(t, stream.Run(runCtx))

	mu.Lock()
	defer mu.Unlock()
	require.Empty(t, errs)

	distinct := map[int]bool{}
	for _, id := range ids {
		distinct[id] = true
	}
	t.Logf("delivered %d messages, %d distinct ids: %v", len(ids), len(distinct), ids)

	// Every document must arrive despite the cursor dying part way through.
	for id := 1; id <= docCount; id++ {
		require.True(t, distinct[id], "document %d was never delivered, got: %v", id, ids)
	}
	require.Len(t, distinct, docCount)
	// The re-query restarts from the beginning, so the documents delivered
	// before the failure arrive a second time. Asserting the duplicates exist
	// pins that the recovery really was a fresh query rather than the cursor
	// somehow resuming.
	require.Greater(t, len(ids), docCount, "expected the re-query to redeliver the first batch")

	failures := logs.matching("mongodb cursor failure")
	require.NotEmpty(t, failures, "the dead cursor must be reported, captured logs: %v", logs.matching(""))
	t.Logf("cursor failure (x%d): %s", len(failures), failures[0])
	require.Contains(t, failures[0], "CursorNotFound")
}

// TestIntegrationMongoInputShutdownIsNotCursorFailure pins the ctx.Err() == nil
// guard on the same branch: when the cursor stops because the read context was
// cancelled, that is an ordinary shutdown and ReadBatch must report
// ErrEndOfInput rather than a cursor failure, and must leave the cursor and
// client for Close to release instead of tearing them down as a failure.
// Verified against the driver: a getMore interrupted by a cancelled context
// leaves cursor.Err() == context.Canceled, which without the guard would be
// wrapped and returned as "mongodb cursor failure: context canceled".
//
// ReadBatch is driven directly rather than through a running stream because the
// distinction is invisible at the stream level: the framework's reader cancels
// the read context by way of the same soft-stop signal it checks immediately
// after ReadBatch returns, so it stops on either answer, and it deliberately
// does not log errors that wrap context.Canceled. A stream-level version of
// this test would therefore pass just as happily with the guard deleted.
//
// auto_replay_nacks is disabled so that newMongoInput returns the input itself:
// the auto-retry wrapper answers a cancelled context with ctx.Err() from its own
// queue before the underlying ReadBatch is consulted, which would mask the value
// under test.
func TestIntegrationMongoInputShutdownIsNotCursorFailure(t *testing.T) {
	integration.CheckSkip(t)

	const (
		database   = "TestDB"
		collection = "shutdown"
		docCount   = 6
	)

	port, admin := startMongoForCursorTests(t)
	seedDocs(t, admin, database, collection, docCount)

	conf, err := mongoConfigSpec().ParseYAML(`
url: mongodb://localhost:`+port+`
database: `+database+`
collection: `+collection+`
username: mongoadmin
password: secret
app_name: `+cursorTestAppName+`
json_marshal_mode: relaxed
auto_replay_nacks: false
batch_size: 2
query: 'root = {}'
`, service.NewEnvironment())
	require.NoError(t, err)

	in, err := newMongoInput(conf, service.MockResources().Logger())
	require.NoError(t, err)
	input, ok := in.(*mongoInput)
	require.True(t, ok, "auto_replay_nacks: false must leave the input unwrapped")
	t.Cleanup(func() { _ = in.Close(context.Background()) })

	// readCtx stands in for the framework's soft-stop context, which is what
	// ReadBatch receives and what shutdown cancels.
	readCtx, stop := context.WithCancel(t.Context())
	defer stop()

	require.NoError(t, in.Connect(readCtx))

	// Drain the batch the find itself served, so the cursor is mid-read: the
	// next document can only come from a getMore.
	batch, ack, err := in.ReadBatch(readCtx)
	require.NoError(t, err)
	require.Len(t, batch, 2)
	require.NoError(t, ack(readCtx, nil))

	stop()

	_, _, err = in.ReadBatch(readCtx)
	require.ErrorIs(t, err, service.ErrEndOfInput, "a cancelled read context is a shutdown, not a cursor failure")
	require.NotContains(t, err.Error(), "cursor failure")
	require.NotNil(t, input.cursor, "shutdown must not tear the cursor down")
	require.NotNil(t, input.client, "shutdown must not tear the client down")
}
