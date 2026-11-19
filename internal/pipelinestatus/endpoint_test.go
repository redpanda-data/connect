// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package pipelinestatus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecentErrorsBoundedPerComponent(t *testing.T) {
	r := newRecentErrors(3)

	for i := range 5 {
		r.add(logEntry{Label: "a", Path: "input", Message: fmt.Sprintf("a%d", i), Timestamp: int64(i)})
	}
	r.add(logEntry{Label: "b", Path: "output", Message: "b0", Timestamp: 1})

	got := r.entries()
	require.Len(t, got, 4)

	var msgs []string
	for _, e := range got {
		msgs = append(msgs, e.Message)
	}
	// In recording order; component a kept only its last three.
	assert.Equal(t, []string{"a2", "a3", "a4", "b0"}, msgs)
}

func TestRecentErrorsEmpty(t *testing.T) {
	r := newRecentErrors(3)
	assert.Empty(t, r.entries())
	assert.NotNil(t, r.entries(), "must encode as [] not null")
}

func TestErrorRecorderAttributesAndRedaction(t *testing.T) {
	ep := New()
	ep.SetEnabled(true)
	logger := slog.New(ep.LogHandler())

	// Attributes arrive the way the component manager attaches them: via
	// With, which goes through WithAttrs on the handler.
	compLogger := logger.With("label", "my_cache", "path", "root.pipeline.processors.0.cache")

	compLogger.Info("not recorded")
	compLogger.Warn("not recorded either")
	compLogger.Error("failed to connect to postgres://user:secretpass@db.example.com:5432/app")
	compLogger.With("error", errors.New("dial tcp: amqp://guest:hunter2@mq:5672: refused")).Error("Failed to send")

	// A root-level entry with no component context.
	logger.Error("boot failed")

	// Trailing newlines from printf-style log calls are trimmed.
	compLogger.With("error", errors.New("boom\n")).Error("Failed to flush\n")

	entries := ep.recent.entries()
	require.Len(t, entries, 4)

	byMsgPrefix := func(prefix string) logEntry {
		for _, e := range entries {
			if len(e.Message) >= len(prefix) && e.Message[:len(prefix)] == prefix {
				return e
			}
		}
		t.Fatalf("no entry starting %q in %#v", prefix, entries)
		return logEntry{}
	}

	conn := byMsgPrefix("failed to connect")
	assert.Equal(t, "my_cache", conn.Label)
	assert.Equal(t, "pipeline.processors.0.cache", conn.Path)
	assert.NotContains(t, conn.Message, "secretpass")
	assert.Contains(t, conn.Message, "db.example.com")
	assert.NotZero(t, conn.Timestamp)

	send := byMsgPrefix("Failed to send")
	assert.Equal(t, "my_cache", send.Label)
	assert.NotContains(t, send.Message, "hunter2")
	assert.Contains(t, send.Message, "mq:5672")

	boot := byMsgPrefix("boot failed")
	assert.Empty(t, boot.Label)
	assert.Empty(t, boot.Path)

	flush := byMsgPrefix("Failed to flush")
	assert.Equal(t, "Failed to flush: boom", flush.Message)
}

func TestErrorRecorderDisabledRecordsNothing(t *testing.T) {
	ep := New()
	logger := slog.New(ep.LogHandler())

	assert.False(t, logger.Handler().Enabled(context.Background(), slog.LevelError))
	logger.With("label", "a", "path", "root.input").Error("ignored")
	assert.Empty(t, ep.recent.entries())

	ep.SetEnabled(true)
	logger.With("label", "a", "path", "root.input").Error("kept")
	assert.Len(t, ep.recent.entries(), 1)
}

func TestErrorRecorderComponentIdentityFromHandlerAttrsOnly(t *testing.T) {
	ep := New()
	ep.SetEnabled(true)
	compLogger := slog.New(ep.LogHandler()).With("label", "files", "path", "root.input")

	// A component logging its own path and err attributes keeps its identity
	// and folds the error into the message.
	compLogger.Error("Failed to open file", "path", "/data/in/file-123.csv", "err", "permission denied")
	compLogger.Error("Failed to open file", "path", "/data/in/file-456.csv", "err", "permission denied")

	entries := ep.recent.entries()
	require.Len(t, entries, 2)
	for _, e := range entries {
		assert.Equal(t, "files", e.Label)
		assert.Equal(t, "input", e.Path)
		assert.Equal(t, "Failed to open file: permission denied", e.Message)
	}
	assert.Len(t, ep.recent.rings, 1, "one ring per component, not per file path")
}

func TestErrorRecorderIgnoresComponentPathOverride(t *testing.T) {
	ep := New()
	ep.SetEnabled(true)
	root := slog.New(ep.LogHandler())
	// The manager scopes the path incrementally, most specific last.
	compLogger := root.With("path", "root.input").With("label", "files", "path", "root.input.broker.inputs.0")

	// A component scoping its own logger with a path of its own, the way the
	// sftp input does for the file it is reading, keeps the manager's identity.
	compLogger.With("path", "/data/in/a.csv").Error("Failed to open")
	compLogger.With("path", "/data/in/b.csv").Error("Failed to open")

	// A logger with no component scope and a non-root path stays unscoped.
	root.With("path", "/tmp/x").Error("boot failed")

	entries := ep.recent.entries()
	require.Len(t, entries, 3)
	assert.Equal(t, "files", entries[0].Label)
	assert.Equal(t, "input.broker.inputs.0", entries[0].Path)
	assert.Equal(t, "files", entries[1].Label)
	assert.Equal(t, "input.broker.inputs.0", entries[1].Path)
	assert.Empty(t, entries[2].Label)
	assert.Empty(t, entries[2].Path)
	assert.Len(t, ep.recent.rings, 2, "one ring per component, not per overriding attribute")
}

func TestErrorRecorderKeysByStream(t *testing.T) {
	ep := New()
	ep.SetEnabled(true)
	root := slog.New(ep.LogHandler())
	root.With("stream", "a", "label", "in", "path", "root.input").Error("a failed")
	root.With("stream", "b", "label", "in", "path", "root.input").Error("b failed")

	entries := ep.recent.entries()
	require.Len(t, entries, 2)
	assert.Equal(t, "a", entries[0].Stream)
	assert.Equal(t, "b", entries[1].Stream)
	assert.Len(t, ep.recent.rings, 2, "the same component in two streams is two rings")

	body, err := json.Marshal(entries[0])
	require.NoError(t, err)
	assert.Contains(t, string(body), `"stream":"a"`)
}

func TestRecentErrorsBoundedRings(t *testing.T) {
	r := newRecentErrors(2)
	for i := range maxRings {
		r.add(logEntry{Stream: fmt.Sprint(i), Path: "input", Message: "x"})
	}
	require.Len(t, r.rings, maxRings)

	// Writing to an old ring keeps it fresh, so stream 0 survives the next
	// eviction and stream 1, now the stalest, goes.
	r.add(logEntry{Stream: "0", Path: "input", Message: "y"})
	r.add(logEntry{Stream: "new", Path: "input", Message: "z"})
	assert.Len(t, r.rings, maxRings)
	assert.Contains(t, r.rings, "0\x00\x00input")
	assert.Contains(t, r.rings, "new\x00\x00input")
	assert.NotContains(t, r.rings, "1\x00\x00input")
}

func TestRecentErrorsOrderedByRecordingNotName(t *testing.T) {
	r := newRecentErrors(3)
	r.add(logEntry{Label: "zed", Path: "output", Message: "first", Timestamp: 100})
	r.add(logEntry{Label: "alpha", Path: "input", Message: "second", Timestamp: 100})

	got := r.entries()
	require.Len(t, got, 2)
	assert.Equal(t, "first", got[0].Message)
	assert.Equal(t, "second", got[1].Message)
}

func TestConnectionResponseIgnoresClosedConnections(t *testing.T) {
	// An input that has finished, or a component not attempted yet, is
	// inactive without an error. It is not a problem to report.
	closed := connectionResponse([]ConnectionIssue{{Path: "input"}})
	assert.Equal(t, "OK", closed.Status)
	assert.Empty(t, closed.Errors)

	mixed := connectionResponse([]ConnectionIssue{
		{Path: "input"},
		{Path: "output", Err: errors.New("refused")},
	})
	assert.Equal(t, "ERROR", mixed.Status)
	require.Len(t, mixed.Errors, 1)
	assert.Equal(t, "output", mixed.Errors[0].Path)
}

func TestConnectionResponseRedactsAndOrders(t *testing.T) {
	conn := connectionResponse([]ConnectionIssue{
		{Label: "out", Path: "output", Err: errors.New("dial amqp://guest:hunter2@mq:5672: connection refused")},
		{Path: "input"},
	})
	assert.Equal(t, "ERROR", conn.Status)
	require.Len(t, conn.Errors, 1)
	assert.Equal(t, "out", conn.Errors[0].Label)
	assert.Equal(t, "output", conn.Errors[0].Path)
	assert.NotContains(t, conn.Errors[0].Message, "hunter2")
	assert.Contains(t, conn.Errors[0].Message, "mq:5672")

	empty := connectionResponse(nil)
	assert.Equal(t, "OK", empty.Status)
	assert.NotNil(t, empty.Errors)
	assert.Empty(t, empty.Errors)
}

func TestErrorRecorderZeroTimeFallsBackToNow(t *testing.T) {
	ep := New()
	ep.SetEnabled(true)
	h := ep.LogHandler()

	before := time.Now().Unix()
	rec := slog.Record{Level: slog.LevelError, Message: "no time"}
	require.NoError(t, h.Handle(context.Background(), rec))

	entries := ep.recent.entries()
	require.Len(t, entries, 1)
	assert.GreaterOrEqual(t, entries[0].Timestamp, before)
}

func TestRecentErrorsConcurrentAccess(t *testing.T) {
	r := newRecentErrors(10)
	keys := []string{"a", "b", "c", "d"}

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 100 {
				r.add(logEntry{Label: keys[(g+i)%len(keys)], Path: "input", Message: fmt.Sprint(g, i)})
				if i%10 == 0 {
					_ = r.entries()
				}
			}
		})
	}
	wg.Wait()

	got := r.entries()
	assert.Len(t, got, len(keys)*10)
	for i := 1; i < len(got); i++ {
		assert.Less(t, got[i-1].seq, got[i].seq, "entries must be in recording order")
	}
}

type failingHandler struct{ err error }

func (failingHandler) Enabled(context.Context, slog.Level) bool    { return true }
func (f failingHandler) Handle(context.Context, slog.Record) error { return f.err }
func (f failingHandler) WithAttrs([]slog.Attr) slog.Handler        { return f }
func (f failingHandler) WithGroup(string) slog.Handler             { return f }

func TestFanoutHandlerJoinsErrors(t *testing.T) {
	var recs []slog.Record
	var groups []string
	ok := recordingHandler{level: slog.LevelInfo, records: &recs, groups: &groups}
	f := &fanoutHandler{handlers: []slog.Handler{failingHandler{errors.New("boom")}, ok, failingHandler{errors.New("bang")}}}

	err := f.Handle(context.Background(), slog.Record{Level: slog.LevelInfo, Message: "x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
	assert.Contains(t, err.Error(), "bang")
	assert.Len(t, recs, 1, "a failing sibling must not stop delivery to the others")
}

func TestStatusPath(t *testing.T) {
	assert.Empty(t, componentPath(""))
	assert.Empty(t, componentPath("root"))
	assert.Equal(t, "input", componentPath("root.input"))
	assert.Equal(t, "pipeline.processors.0", componentPath("root.pipeline.processors.0"))
	assert.Equal(t, "rooted.thing", componentPath("rooted.thing"))
}

func TestMessagesConsumedTeeIgnoredWhenDisabled(t *testing.T) {
	ep := New()
	counter := ep.MetricsTee().NewCounterCtor("input_received", "label", "path")("a", "root.input")
	counter.Incr(3)
	assert.Zero(t, ep.consumed.Load())
	assert.Zero(t, ep.lastReceivedAt.Load())

	// Enabling after the counter was built still takes effect.
	ep.SetEnabled(true)
	counter.Incr(3)
	assert.Equal(t, int64(3), ep.consumed.Load())
	assert.NotZero(t, ep.lastReceivedAt.Load())
}

func TestLastMessageReceivedAtOnlyMovesForward(t *testing.T) {
	ep := New()
	ep.SetEnabled(true)
	counter := ep.MetricsTee().NewCounterCtor("input_received", "label", "path")("a", "root.input")

	counter.Incr(0)
	assert.Zero(t, ep.lastReceivedAt.Load(), "an empty batch is not a message")
	assert.Zero(t, ep.consumed.Load())

	future := time.Now().Add(time.Hour).Unix()
	ep.lastReceivedAt.Store(future)
	counter.Incr(1)
	assert.Equal(t, future, ep.lastReceivedAt.Load(), "a later timestamp is never lowered")
	assert.Equal(t, int64(1), ep.consumed.Load())
}

func BenchmarkConsumedCounterIncr(b *testing.B) {
	ep := New()
	ep.SetEnabled(true)
	counter := ep.MetricsTee().NewCounterCtor("input_received", "label", "path")("a", "root.input")
	b.ReportAllocs()
	for b.Loop() {
		counter.Incr(1)
	}
}

func TestLastMessageReceivedAt(t *testing.T) {
	ep := New()
	ep.SetEnabled(true)

	assert.Nil(t, ep.response().LastMessageReceivedAt, "null before any message")

	before := time.Now().Unix()
	ep.MetricsTee().NewCounterCtor("input_received", "label", "path")("a", "root.input").Incr(2)
	got := ep.response().LastMessageReceivedAt
	require.NotNil(t, got)
	assert.GreaterOrEqual(t, *got, before)
	assert.LessOrEqual(t, *got, time.Now().Unix())

	// Other metrics don't move it.
	ep.lastReceivedAt.Store(1)
	ep.MetricsTee().NewCounterCtor("output_sent", "label", "path")("c", "root.output").Incr(5)
	assert.Equal(t, int64(1), *ep.response().LastMessageReceivedAt)

	body, err := json.Marshal(New().response())
	require.NoError(t, err)
	assert.Contains(t, string(body), `"last_message_received_at":null`)
}

func TestMessagesConsumedTeeOnlyCountsTopLevelInput(t *testing.T) {
	ep := New()
	ep.SetEnabled(true)
	tee := ep.MetricsTee()

	// A `metric` processor can emit a counter of the same name.
	tee.NewCounterCtor("input_received", "label", "path")("m", "root.pipeline.processors.0").Incr(3)
	// Without a path there is no way to tell, so it doesn't count either.
	tee.NewCounterCtor("input_received")().Incr(3)
	// A wrapping input's children are not counted: the wrapper itself counts
	// every batch they produce.
	tee.NewCounterCtor("input_received", "label", "path")("b", "root.input.broker.inputs.1").Incr(2)
	assert.Zero(t, ep.consumed.Load())

	tee.NewCounterCtor("input_received", "label", "path")("a", "root.input").Incr(1)
	tee.NewCounterCtor("input_received", "stream", "label", "path")("s", "c", "root.input").Incr(4)
	assert.Equal(t, int64(5), ep.consumed.Load())
}

func TestMessagesConsumedTeeSumsOnlyInputReceived(t *testing.T) {
	ep := New()
	ep.SetEnabled(true)
	tee := ep.MetricsTee()

	// The top-level input counts, under whichever label values it carries;
	// the children of a wrapping input don't, since the wrapper counts their
	// batches already.
	received := tee.NewCounterCtor("input_received", "stream", "label", "path")
	received("", "a", "root.input").Incr(3)
	received("s", "b", "root.input").Incr(4)
	received("", "a", "root.input").Incr(1)
	received("", "c", "root.input.broker.inputs.0").Incr(100)

	tee.NewCounterCtor("output_sent", "label", "path")("c", "root.output").Incr(100)
	tee.NewTimerCtor("input_latency_ns", "label", "path")("a", "root.input").Timing(50)
	tee.NewGaugeCtor("input_connection_up", "label", "path")("a", "root.input").Set(1)

	assert.Equal(t, int64(8), ep.consumed.Load())
	require.NoError(t, tee.Close(context.Background()))
}

func TestStatusEndpointResponseBeforeStreamStart(t *testing.T) {
	ep := New()
	ep.SetEnabled(true)
	ep.MetricsTee().NewCounterCtor("input_received", "label", "path")("a", "root.input").Incr(5)
	slog.New(ep.LogHandler()).With("label", "out", "path", "root.output").Error("nope")

	rec := httptest.NewRecorder()
	ep.Handler()(rec, httptest.NewRequest(http.MethodGet, "/status", http.NoBody))

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

	conn, ok := body["connection"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "INITIALIZING", conn["status"])
	assert.Equal(t, []any{}, conn["errors"], "errors must be an empty array, not null")

	assert.Equal(t, float64(5), body["messages_consumed"])

	recent, ok := body["recent_errors"].([]any)
	require.True(t, ok)
	require.Len(t, recent, 1)
	entry, ok := recent[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "out", entry["label"])
	assert.Equal(t, "output", entry["path"])
	assert.Equal(t, "nope", entry["message"])
	assert.NotZero(t, entry["timestamp"])
}

type recordingHandler struct {
	level   slog.Level
	attrs   []slog.Attr
	records *[]slog.Record
	groups  *[]string
}

func (h recordingHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.level }
func (h recordingHandler) Handle(_ context.Context, r slog.Record) error {
	r.AddAttrs(h.attrs...)
	*h.records = append(*h.records, r)
	return nil
}

func (h recordingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return h
}

func (h recordingHandler) WithGroup(name string) slog.Handler {
	*h.groups = append(*h.groups, name)
	return h
}

func TestFanoutHandler(t *testing.T) {
	var infoRecs, errRecs []slog.Record
	var groups []string
	info := recordingHandler{level: slog.LevelInfo, records: &infoRecs, groups: &groups}
	errOnly := recordingHandler{level: slog.LevelError, records: &errRecs, groups: &groups}

	logger := slog.New(&fanoutHandler{handlers: []slog.Handler{info, errOnly}})
	logger = logger.With("label", "foo").WithGroup("g")

	logger.Info("hello")
	logger.Error("oops")

	require.Len(t, infoRecs, 2)
	require.Len(t, errRecs, 1)
	assert.Equal(t, "oops", errRecs[0].Message)

	// Attributes added through the fanout reach each child.
	var keys []string
	errRecs[0].Attrs(func(a slog.Attr) bool {
		keys = append(keys, a.Key)
		return true
	})
	assert.Contains(t, keys, "label")

	// WithGroup is forwarded to every child.
	assert.Equal(t, []string{"g", "g"}, groups)

	// Enabled is the union of the children.
	assert.True(t, logger.Handler().Enabled(context.Background(), slog.LevelInfo))
	assert.False(t, logger.Handler().Enabled(context.Background(), slog.LevelDebug))
}
