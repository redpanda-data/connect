// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package pgtest

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/require"
)

// ReceivedMessages is a thread-safe accessor for messages collected by the
// consumer func startSignallingStream registers.
type ReceivedMessages struct {
	mu   sync.Mutex
	msgs []any
}

// Add records a received message.
func (r *ReceivedMessages) Add(m any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, m)
}

// Len returns the number of messages received so far.
func (r *ReceivedMessages) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.msgs)
}

// All returns a snapshot of the messages received so far.
func (r *ReceivedMessages) All() []any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]any(nil), r.msgs...)
}

// Reset discards every message received so far.
func (r *ReceivedMessages) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = nil
}

// TestLogCapture is an implemention of the slog.Logger interface to support
// verifying log output, including attributes bound via With().
type TestLogCapture struct {
	sink  *logSink
	attrs []slog.Attr
}

type logSink struct {
	mu       sync.Mutex
	messages []string
}

func (s *logSink) add(msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, msg)
}

func (s *logSink) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.messages...)
}

// Handle records the log record's message, plus any attributes bound via
// With() or attached directly to the record.
func (c *TestLogCapture) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Message)
	for _, a := range c.attrs {
		fmt.Fprintf(&b, " %s=%v", a.Key, a.Value)
	}
	r.Attrs(func(a slog.Attr) bool {
		fmt.Fprintf(&b, " %s=%v", a.Key, a.Value)
		return true
	})
	c.sink.add(b.String())
	return nil
}

// Messages returns a snapshot of every message logged so far.
func (c *TestLogCapture) Messages() []string {
	return c.sink.snapshot()
}

// WithAttrs returns a handler carrying the given attributes, sharing this
// capture's underlying sink so messages logged through it are still visible
// via Messages().
func (c *TestLogCapture) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &TestLogCapture{sink: c.sink, attrs: append(append([]slog.Attr{}, c.attrs...), attrs...)}
}

// WithGroup returns the receiver unchanged; groups are not recorded.
func (c *TestLogCapture) WithGroup(string) slog.Handler { return c }

// Enabled always returns true so every log record is captured.
func (*TestLogCapture) Enabled(context.Context, slog.Level) bool { return true }

// NewTestLogCapture creates a TestLogCapture ready to use as a slog.Handler.
func NewTestLogCapture() *TestLogCapture {
	return &TestLogCapture{sink: &logSink{}}
}

// FakeFlightRecord is a fake row shape used to generate test data for
// integration tests.
type FakeFlightRecord struct {
	RealAddress faker.RealAddress `faker:"real_address"`
	CreatedAt   int64             `fake:"unix_time"`
}

// GetFakeFlightRecord generates a random FakeFlightRecord, panicking if
// generation fails.
func GetFakeFlightRecord() FakeFlightRecord {
	flightRecord := FakeFlightRecord{}
	err := faker.FakeData(&flightRecord)
	if err != nil {
		panic(err)
	}

	return flightRecord
}

// TestDB wraps sql.DB with testing utilities for database integration tests.
type TestDB struct {
	*sql.DB
}

// MustExec executes a SQL query and fails t if an error occurs. t is taken
// per call, not stored on TestDB, so a call made from inside a subtest fails
// that subtest rather than reaching for FailNow on a parent *testing.T from
// the wrong goroutine.
func (db *TestDB) MustExec(t *testing.T, query string, args ...any) {
	t.Helper()
	_, err := db.Exec(query, args...)
	require.NoError(t, err)
}
