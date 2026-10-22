// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// heldPageDeps is a Deps whose Scan reads one page and waits on it, so the
// page stays held for as long as the shard progress blocks it.
type heldPageDeps struct{ runDeps }

func (*heldPageDeps) Scan(ctx context.Context, _ fakeProgress, hooks ScanHooks) error {
	hooks.BeforeRequest(0)
	item := Item{"pk": &dynamodbtypes.AttributeValueMemberS{Value: "a"}}
	return hooks.Batch(ctx, []Item{item}, 0, nil)
}

// TestRunWarnsWhyPageIsHeld: a page held past the warning threshold logs
// why, naming what the release waits on, so a stalled backfill is
// diagnosable without a debugger.
func TestRunWarnsWhyPageIsHeld(t *testing.T) {
	logs := &syncBuffer{}
	state := NewState(0, time.Hour)
	state.Register("shard-1")
	state.RefreshDone([]string{"shard-1"}) // settled, but shard-1 never read
	b := &Backfill[fakeProgress]{
		Table:         "t",
		KeySchema:     []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash}},
		State:         state,
		Deps:          &heldPageDeps{runDeps{waited: make(chan struct{}, 1), release: make(chan struct{})}},
		Logger:        service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(logs, nil))),
		ReleaseTick:   5 * time.Millisecond,
		HeldWarnAfter: 50 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()

	require.Eventually(t, func() bool { return strings.Contains(logs.String(), "held a page") },
		5*time.Second, 5*time.Millisecond, "a long-held page must log why")
	out := logs.String()
	assert.Contains(t, out, "shard-1", "the warning names the shard the release waits on")
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, 1, strings.Count(logs.String(), "held a page"), "the warning is rate limited")

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the run should return once ctx ends")
	}
}
