// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// TestHeartbeatTransactionalPerTick: only a transactional message carries a
// transaction id, which an incremental snapshot needs to advance. Each one
// spends an id though, so an idle coordinator gets the cheaper
// non-transactional message.
//
// The predicate is read per tick, so a backfill starting or finishing takes
// effect on the next heartbeat without rebuilding it -- the interval is
// fixed at construction, the message kind is not.
func TestHeartbeatTransactionalPerTick(t *testing.T) {
	var prepared []string
	db := newFakeQueryDBCapturing(t, nil, nil, nil, &prepared)

	var backfilling atomic.Bool
	h := &heartbeat{
		db:            db,
		logger:        service.MockResources().Logger(),
		prefix:        "test",
		value:         `{"type":"heartbeat"}`,
		transactional: backfilling.Load,
	}

	transactionalCalls := func() (yes, no int) {
		for _, q := range prepared {
			switch {
			case strings.Contains(q, "pg_logical_emit_message(true"):
				yes++
			case strings.Contains(q, "pg_logical_emit_message(false"):
				no++
			}
		}
		return yes, no
	}

	// Idle: no transaction id spent.
	h.run(t.Context())
	yes, no := transactionalCalls()
	assert.Equal(t, 0, yes)
	assert.Equal(t, 1, no)

	// A signal queues work, and the very next tick must carry an id.
	backfilling.Store(true)
	h.run(t.Context())
	yes, no = transactionalCalls()
	assert.Equal(t, 1, yes)
	assert.Equal(t, 1, no)

	// Back to idle, and it stops spending them again.
	backfilling.Store(false)
	h.run(t.Context())
	yes, no = transactionalCalls()
	assert.Equal(t, 1, yes)
	assert.Equal(t, 2, no)
}

// TestHeartbeatNilPredicateIsNeverTransactional: a connector with no
// incremental snapshot supplies no predicate, and must never spend a
// transaction id.
func TestHeartbeatNilPredicateIsNeverTransactional(t *testing.T) {
	var prepared []string
	db := newFakeQueryDBCapturing(t, nil, nil, nil, &prepared)

	h := &heartbeat{db: db, logger: service.MockResources().Logger(), prefix: "test", value: "{}"}
	h.run(t.Context())

	require.Len(t, prepared, 1)
	assert.Contains(t, prepared[0], "pg_logical_emit_message(false")
}
