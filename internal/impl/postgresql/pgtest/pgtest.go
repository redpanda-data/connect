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
	"log/slog"
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

// TestLogCapture is an implemention of the slog.Logger interface
// to support verifying log output.
type TestLogCapture struct {
	mu       sync.Mutex
	messages []string
}

// Handle records the log record's message.
func (c *TestLogCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = append(c.messages, r.Message)
	return nil
}

// Messages returns a snapshot of every message logged so far.
func (c *TestLogCapture) Messages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.messages...)
}

// WithAttrs returns the receiver unchanged; attributes are not recorded.
func (c *TestLogCapture) WithAttrs([]slog.Attr) slog.Handler { return c }

// WithGroup returns the receiver unchanged; groups are not recorded.
func (c *TestLogCapture) WithGroup(string) slog.Handler { return c }

// Enabled always returns true so every log record is captured.
func (*TestLogCapture) Enabled(context.Context, slog.Level) bool { return true }

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
