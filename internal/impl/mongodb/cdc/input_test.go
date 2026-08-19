// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package cdc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
)

func TestSpecParsesAWSBlock(t *testing.T) {
	sb := service.NewStreamBuilder()
	err := sb.AddInputYAML(`
mongodb_cdc:
  url: "mongodb://localhost:27017"
  database: foo
  collections: [bar]
  checkpoint_cache: foocache
  aws:
    enabled: true
    region: us-east-1
    roles:
      - role: arn:aws:iam::123456789012:role/foo
`)
	require.NoError(t, err)
}

func TestIsUnresumableTokenError(t *testing.T) {
	// The observed shape for a malformed keystring, verified against a real
	// mongo:7 replica set, is `(Location50811) KeyString format error: Unknown
	// type: 222` arriving as a mongo.CommandError - which is why CommandError is
	// the type these cases are built from.
	//
	// opening() reproduces how a rejected start position reaches the classifier:
	// readFromStream tags the Watch failure with errOpeningChangeStream and the
	// caller wraps that again on its way to errorChan.
	opening := func(err error) error {
		return fmt.Errorf("error watching MongoDB change stream: %w", fmt.Errorf("%w: %w", errOpeningChangeStream, err))
	}
	keyStringErr := mongo.CommandError{
		Code:    codeKeyStringFormatError,
		Name:    "Location50811",
		Message: "KeyString format error: Unknown type: 222",
	}
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{
			// History loss is a property of the oplog, not of one token: the
			// capped FIFO has dropped everything at or before the lost position,
			// so the cached position is gone too whichever phase reported it.
			name: "change stream history lost while opening",
			err:  opening(mongo.CommandError{Code: codeChangeStreamHistoryLost, Name: "ChangeStreamHistoryLost"}),
			want: true,
		},
		{
			name: "change stream history lost mid-stream",
			err:  mongo.CommandError{Code: codeChangeStreamHistoryLost, Name: "ChangeStreamHistoryLost"},
			want: true,
		},
		{
			// ChangeStreamFatalError is the rest of the server's
			// NonResumableChangeStreamError category, and is what resuming past a
			// collection drop or rename reports. Like history loss it is a statement
			// about the stream rather than about one token, so the phase is
			// irrelevant.
			name: "change stream fatal error while opening",
			err:  opening(mongo.CommandError{Code: codeChangeStreamFatalError, Name: "ChangeStreamFatalError"}),
			want: true,
		},
		{
			name: "change stream fatal error mid-stream",
			err:  mongo.CommandError{Code: codeChangeStreamFatalError, Name: "ChangeStreamFatalError"},
			want: true,
		},
		{
			name: "shard removed while opening",
			err:  opening(mongo.CommandError{Code: codeShardRemovedError, Name: "ShardRemovedError"}),
			want: true,
		},
		{
			name: "shard removed mid-stream",
			err:  mongo.CommandError{Code: codeShardRemovedError, Name: "ShardRemovedError"},
			want: true,
		},
		{
			name: "invalid resume token while opening",
			err:  opening(mongo.CommandError{Code: codeInvalidResumeToken, Name: "InvalidResumeToken"}),
			want: true,
		},
		{
			// Mid-stream these codes are about the token the driver holds in
			// memory, not the one loaded from the cache. Clearing a cached
			// position that may be perfectly good would cost a needless
			// re-snapshot, so the phase gate keeps it.
			name: "invalid resume token mid-stream is not attributable to the checkpoint",
			err:  mongo.CommandError{Code: codeInvalidResumeToken, Name: "InvalidResumeToken"},
			want: false,
		},
		{
			name: "keystring format error while opening",
			err:  opening(keyStringErr),
			want: true,
		},
		{
			name: "keystring format error mid-stream is not attributable to the checkpoint",
			err:  keyStringErr,
			want: false,
		},
		{
			// 50811 is a generic Location code, so the message has to agree too.
			name: "unrelated keystring-code error while opening",
			err:  opening(mongo.CommandError{Code: codeKeyStringFormatError, Name: "Location50811", Message: "some other location failure"}),
			want: false,
		},
		{
			// A server error that has nothing to do with the resume position must
			// not clear a perfectly good checkpoint.
			name: "unrelated server error while opening",
			err:  opening(mongo.CommandError{Code: 11000, Name: "DuplicateKey"}),
			want: false,
		},
		{
			// Transient failures are the expensive false positive: clearing here
			// would re-snapshot (and duplicate) on every network blip.
			name: "network error",
			err:  errors.New("network"),
			want: false,
		},
		{
			// Even from the open phase, a non-server error proves nothing about
			// the stored position.
			name: "network error while opening",
			err:  opening(errors.New("connection reset")),
			want: false,
		},
		{
			name: "context cancellation",
			err:  fmt.Errorf("reading change stream: %w", context.Canceled),
			want: false,
		},
		{
			name: "nil",
			err:  nil,
			want: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, isUnresumableTokenError(test.err))
		})
	}
}

func TestOpeningChangeStreamErrorKeepsItsMessage(t *testing.T) {
	// The sentinel replaced a plain fmt.Errorf prefix, and both the integration
	// test and anyone reading logs rely on the wording being unchanged.
	err := fmt.Errorf("%w: %w", errOpeningChangeStream, errors.New("boom"))
	require.Equal(t, "error opening change stream: boom", err.Error())
	require.ErrorIs(t, err, errOpeningChangeStream)
}

func TestStoreSnapshotCheckpointWaitsForSnapshotAcks(t *testing.T) {
	cp := checkpoint.NewCapped[bson.Raw](10)
	resolve, err := cp.Track(t.Context(), nil, 5)
	require.NoError(t, err)
	require.Equal(t, int64(5), cp.Pending())

	m := &mongoCDC{}
	token := bson.Raw{5, 0, 0, 0, 0}
	stored := make(chan bson.Raw, 1)
	proceed := make(chan bool, 1)
	go func() {
		proceed <- m.storeSnapshotCheckpoint(t.Context(), m.tokenEpoch, cp, token, func(_ context.Context, rt bson.Raw) error {
			stored <- rt
			return nil
		})
	}()

	// While a snapshot batch is unresolved nothing may be persisted: a restart
	// loading that checkpoint would skip the undelivered part of the snapshot.
	select {
	case rt := <-stored:
		t.Fatalf("checkpoint stored while a snapshot batch was in flight: %v", rt)
	case <-proceed:
		t.Fatal("returned while a snapshot batch was in flight")
	case <-time.After(3 * snapshotAckPollInterval):
	}
	m.resumeTokenMu.Lock()
	require.Nil(t, m.resumeToken)
	m.resumeTokenMu.Unlock()

	resolve()

	select {
	case rt := <-stored:
		require.Equal(t, token, rt)
	case <-time.After(time.Minute):
		t.Fatal("checkpoint was not stored after the snapshot batch resolved")
	}
	require.True(t, <-proceed)
	m.resumeTokenMu.Lock()
	require.Equal(t, token, m.resumeToken)
	m.resumeTokenMu.Unlock()
}

func TestStoreSnapshotCheckpointStopsOnShutdown(t *testing.T) {
	cp := checkpoint.NewCapped[bson.Raw](10)
	_, err := cp.Track(t.Context(), nil, 1)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	m := &mongoCDC{}
	proceed := make(chan bool, 1)
	go func() {
		proceed <- m.storeSnapshotCheckpoint(ctx, m.tokenEpoch, cp, bson.Raw{5, 0, 0, 0, 0}, func(context.Context, bson.Raw) error {
			t.Error("checkpoint stored despite an unresolved snapshot batch")
			return nil
		})
	}()
	cancel()

	select {
	case ok := <-proceed:
		require.False(t, ok)
	case <-time.After(time.Minute):
		t.Fatal("wait loop did not exit on context cancellation")
	}
	m.resumeTokenMu.Lock()
	require.Nil(t, m.resumeToken)
	m.resumeTokenMu.Unlock()
}

func TestStoreSnapshotCheckpointStoredDespiteCancelledContext(t *testing.T) {
	// The context is cancelled but every snapshot batch was already acked: the
	// snapshot genuinely completed, so a shutdown arriving now must not throw
	// the checkpoint away (this method only runs when every batch was
	// delivered — resolve-without-delivery paths error out of the errgroup).
	// The store must use a detached context, as the caller's is already dead.
	cp := checkpoint.NewCapped[bson.Raw](10)
	require.Zero(t, cp.Pending())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	token := bson.Raw{5, 0, 0, 0, 0}
	var stored bson.Raw
	m := &mongoCDC{logger: service.MockResources().Logger()}
	require.True(t, m.storeSnapshotCheckpoint(ctx, m.tokenEpoch, cp, token, func(storeCtx context.Context, tok bson.Raw) error {
		require.NoError(t, storeCtx.Err(), "store must receive a live context")
		stored = tok
		return nil
	}))
	require.Equal(t, token, stored)
	m.resumeTokenMu.Lock()
	require.Equal(t, token, m.resumeToken)
	m.resumeTokenMu.Unlock()
}

// rehearsedTracker scripts a sequence of Pending() results, then cancels a
// context when the script is exhausted, so shutdown-race interleavings can be
// pinned deterministically instead of with sleeps.
type rehearsedTracker struct {
	script []int64
	then   func()
	fired  bool
}

func (r *rehearsedTracker) Pending() int64 {
	var next int64
	if len(r.script) > 0 {
		next = r.script[0]
		r.script = r.script[1:]
	}
	if len(r.script) == 0 && !r.fired {
		r.fired = true
		r.then()
	}
	return next
}

func TestStoreSnapshotCheckpointStoredWhenAckWinsShutdownRace(t *testing.T) {
	// The final ack lands and shutdown cancels the context while the wait loop
	// is sleeping: the loop wakes on ctx.Done, must re-check the pending count
	// before giving up, see zero, and store. The tracker scripts exactly that
	// interleaving: one pending at the loop entry, cancellation fired with the
	// script's exhaustion, zero on every later check.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cp := &rehearsedTracker{script: []int64{1}, then: cancel}

	token := bson.Raw{5, 0, 0, 0, 0}
	var stored bson.Raw
	m := &mongoCDC{logger: service.MockResources().Logger()}
	require.True(t, m.storeSnapshotCheckpoint(ctx, m.tokenEpoch, cp, token, func(_ context.Context, tok bson.Raw) error {
		stored = tok
		return nil
	}))
	require.Equal(t, token, stored)
}

func TestStoreSnapshotCheckpointSkippedWithoutToken(t *testing.T) {
	cp := checkpoint.NewCapped[bson.Raw](10)
	m := &mongoCDC{}
	require.True(t, m.storeSnapshotCheckpoint(t.Context(), m.tokenEpoch, cp, nil, func(context.Context, bson.Raw) error {
		t.Error("checkpoint stored without a resume token to store")
		return nil
	}))
	m.resumeTokenMu.Lock()
	require.Nil(t, m.resumeToken)
	m.resumeTokenMu.Unlock()
}

// TestStoreSnapshotCheckpointWaitsForAcksWithoutToken pins the half of the ack
// gate that has nothing to do with the checkpoint write. On the no-token path
// (replica sets before 4.0.7, which report no pre-snapshot resume token) there is
// nothing to store, but streaming must still not begin while snapshot slots are
// unresolved: the streaming phase tracks into the same checkpointer, and the
// library's resolve copies a resolving node's payload onto its predecessor
// (`newNode.prev.payload = newNode.payload` in Uncapped.Track), so a stream batch
// resolving first hands its token to a snapshot slot. That slot's ack then trips
// the "unexpected resume token for snapshot batch" guard and the commit that
// token should have produced is lost.
func TestStoreSnapshotCheckpointWaitsForAcksWithoutToken(t *testing.T) {
	cp := checkpoint.NewCapped[bson.Raw](10)
	resolve, err := cp.Track(t.Context(), nil, 3)
	require.NoError(t, err)
	require.Equal(t, int64(3), cp.Pending())

	m := &mongoCDC{}
	proceed := make(chan bool, 1)
	go func() {
		proceed <- m.storeSnapshotCheckpoint(t.Context(), m.tokenEpoch, cp, nil, func(context.Context, bson.Raw) error {
			t.Error("checkpoint stored without a resume token to store")
			return nil
		})
	}()

	select {
	case <-proceed:
		t.Fatal("returned while a snapshot batch was in flight, letting the streaming phase share the checkpointer")
	case <-time.After(3 * snapshotAckPollInterval):
	}

	resolve()

	select {
	case ok := <-proceed:
		require.True(t, ok)
	case <-time.After(time.Minute):
		t.Fatal("did not return after the snapshot batch resolved")
	}
	m.resumeTokenMu.Lock()
	require.Nil(t, m.resumeToken, "the no-token path must leave the position untouched")
	m.resumeTokenMu.Unlock()
}

// TestStoreSnapshotCheckpointStopsOnShutdownWithoutToken is the no-token
// counterpart of the shutdown case: the return contract is about whether the
// caller may proceed to streaming, not about whether anything was written, so an
// unresolved slot at cancellation must still stop the caller.
func TestStoreSnapshotCheckpointStopsOnShutdownWithoutToken(t *testing.T) {
	cp := checkpoint.NewCapped[bson.Raw](10)
	_, err := cp.Track(t.Context(), nil, 1)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	m := &mongoCDC{}
	proceed := make(chan bool, 1)
	go func() {
		proceed <- m.storeSnapshotCheckpoint(ctx, m.tokenEpoch, cp, nil, func(context.Context, bson.Raw) error {
			t.Error("checkpoint stored without a resume token to store")
			return nil
		})
	}()
	cancel()

	select {
	case ok := <-proceed:
		require.False(t, ok, "an unresolved slot at cancellation must stop the caller even with no token")
	case <-time.After(time.Minute):
		t.Fatal("wait loop did not exit on context cancellation")
	}
}

// TestSnapshotSlotInheritsStreamTokenWithoutGate demonstrates, against the real
// checkpoint library, the failure the gate above prevents: it is the mechanism
// that makes the no-token ack gate a correctness requirement rather than a
// tidiness one. If streaming were allowed to track into the same checkpointer
// while a snapshot slot is unresolved, resolving the stream slot first makes the
// snapshot slot inherit the stream's token, and the snapshot ack - which asserts
// it never sees a token - fails.
func TestSnapshotSlotInheritsStreamTokenWithoutGate(t *testing.T) {
	cp := checkpoint.NewCapped[bson.Raw](10)
	// A snapshot batch, tracked with no payload, as readSnapshotRange does.
	resolveSnapshot, err := cp.Track(t.Context(), nil, 1)
	require.NoError(t, err)
	// A streaming batch tracked afterwards, carrying a resume token.
	streamToken := bson.Raw{5, 0, 0, 0, 0}
	resolveStream, err := cp.Track(t.Context(), streamToken, 1)
	require.NoError(t, err)

	// The stream batch is acked first, which is entirely possible: acks arrive in
	// whatever order the pipeline finishes batches in.
	require.Nil(t, resolveStream(), "nothing may commit while the earlier snapshot slot is pending")

	// Now the snapshot slot resolves as the head - and hands back the stream's
	// token, which is what the snapshot ackFn refuses.
	inherited := resolveSnapshot()
	require.NotNil(t, inherited)
	require.Equal(t, streamToken, *inherited, "the snapshot slot inherited the stream token")
}

func TestStoreSnapshotCheckpointSkippedWhenEpochMovedOn(t *testing.T) {
	// Waiting for snapshot acks can take arbitrarily long, so the generation may
	// be superseded before the gate opens. The position is then not the one to
	// resume from and must not be written.
	cp := checkpoint.NewCapped[bson.Raw](10)
	require.Zero(t, cp.Pending())
	m := &mongoCDC{logger: service.MockResources().Logger()}
	stale := m.tokenEpoch
	m.beginTokenEpoch(nil)

	require.True(t, m.storeSnapshotCheckpoint(t.Context(), stale, cp, bson.Raw{5, 0, 0, 0, 0}, func(context.Context, bson.Raw) error {
		t.Error("a superseded snapshot position was stored")
		return nil
	}))
	m.resumeTokenMu.Lock()
	require.Nil(t, m.resumeToken, "a superseded snapshot position must not become the token to checkpoint")
	m.resumeTokenMu.Unlock()
}

// TestCommitResumeTokenDropsSupersededEpoch pins the guard that stops a late ack
// from resurrecting a position the input has moved off. It is the unit-level
// counterpart of the unresumable-checkpoint recovery: an ack resolved after the
// clear carries a token at or before the dead position (for
// ChangeStreamHistoryLost the whole prefix is gone from the capped oplog), so
// writing it back would restore an equally dead checkpoint.
//
// checkpointFlusher is left nil throughout, which is the dangerous
// configuration: with checkpoint_interval: 0 an ack writes the cache directly
// rather than parking the token for the flusher, so the epoch check is the only
// thing standing between a stale ack and the cache.
func TestCommitResumeTokenDropsSupersededEpoch(t *testing.T) {
	ctx := t.Context()
	const cacheName = "checkpoints"
	res := service.MockResources(service.MockResourcesOptAddCache(cacheName))
	cp := &checkpointCache{resources: res, cacheName: cacheName, cacheKey: "key"}
	m := &mongoCDC{logger: res.Logger(), checkpoint: cp}

	// An ack from the live generation is honoured, and with no flusher it writes
	// straight through to the cache.
	live := m.beginTokenEpoch(nil)
	fresh, err := bson.Marshal(bson.M{"_data": "live"})
	require.NoError(t, err)
	require.NoError(t, m.commitResumeToken(ctx, live, bson.Raw(fresh)))
	m.resumeTokenMu.Lock()
	require.Equal(t, bson.Raw(fresh), m.resumeToken)
	m.resumeTokenMu.Unlock()
	loaded, err := cp.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, bson.Raw(fresh), loaded, "a live-generation ack must reach the cache")

	// The recovery clears the checkpoint and opens a new generation. The ack for
	// `fresh` was already in flight and only resolves now: it must be dropped,
	// leaving both the in-memory token and the cache untouched.
	require.NoError(t, cp.Delete(ctx))
	m.beginTokenEpoch(nil)
	require.NoError(t, m.commitResumeToken(ctx, live, bson.Raw(fresh)))
	m.resumeTokenMu.Lock()
	require.Nil(t, m.resumeToken, "a superseded ack must not resurrect the cleared position")
	m.resumeTokenMu.Unlock()
	loaded, err = cp.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, loaded, "a superseded ack must not write the cleared position back to the cache")

	// And the new generation still works, so the guard is not simply wedging
	// everything after a clear.
	next, err := bson.Marshal(bson.M{"_data": "next"})
	require.NoError(t, err)
	require.NoError(t, m.commitResumeToken(ctx, m.tokenEpoch, bson.Raw(next)))
	loaded, err = cp.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, bson.Raw(next), loaded, "the generation opened by the clear must accept new positions")
}

func TestCommitResumeTokenDefersToFlusher(t *testing.T) {
	// With a flusher configured the ack only parks the token; the periodic write
	// is what persists it. Pinning this keeps the epoch guard from being confused
	// with the write-through behaviour it also protects.
	ctx := t.Context()
	const cacheName = "checkpoints"
	res := service.MockResources(service.MockResourcesOptAddCache(cacheName))
	cp := &checkpointCache{resources: res, cacheName: cacheName, cacheKey: "key"}
	m := &mongoCDC{
		logger:            res.Logger(),
		checkpoint:        cp,
		checkpointFlusher: asyncroutine.NewPeriodicWithContext(time.Hour, func(context.Context) {}),
	}

	token, err := bson.Marshal(bson.M{"_data": "parked"})
	require.NoError(t, err)
	require.NoError(t, m.commitResumeToken(ctx, m.beginTokenEpoch(nil), bson.Raw(token)))
	m.resumeTokenMu.Lock()
	require.Equal(t, bson.Raw(token), m.resumeToken)
	m.resumeTokenMu.Unlock()
	loaded, err := cp.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, loaded, "the flusher owns the write when one is configured")
}

func TestCheckpointCacheRoundTripAndRecoverableFailures(t *testing.T) {
	ctx := t.Context()
	const cacheName = "checkpoints"
	res := service.MockResources(service.MockResourcesOptAddCache(cacheName))
	cp := &checkpointCache{resources: res, cacheName: cacheName, cacheKey: "key"}

	// A missing checkpoint is not an error, it just means "start fresh".
	loaded, err := cp.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, loaded)

	// Deleting one that was never written is success too: the goal state is
	// already reached.
	require.NoError(t, cp.Delete(ctx))

	token, err := bson.Marshal(bson.M{"_data": "abc"})
	require.NoError(t, err)
	require.NoError(t, cp.Store(ctx, bson.Raw(token)))
	loaded, err = cp.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, bson.Raw(token), loaded)

	require.NoError(t, cp.Delete(ctx))
	loaded, err = cp.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, loaded, "a deleted checkpoint reads back as no checkpoint")

	// Bytes that are not extended JSON can never become decodable, so Load
	// distinguishes them: Connect clears and starts over rather than failing
	// forever.
	var setErr error
	require.NoError(t, res.AccessCache(ctx, cacheName, func(c service.Cache) {
		setErr = c.Set(ctx, "key", []byte("not extended json"), nil)
	}))
	require.NoError(t, setErr)
	loaded, err = cp.Load(ctx)
	require.ErrorIs(t, err, errCorruptCheckpoint)
	require.Nil(t, loaded)
}

func TestBeginTokenEpochAdvancesAndInstallsToken(t *testing.T) {
	m := &mongoCDC{}
	loaded := bson.Raw{5, 0, 0, 0, 0}

	first := m.beginTokenEpoch(loaded)
	m.resumeTokenMu.Lock()
	require.Equal(t, loaded, m.resumeToken, "the loaded checkpoint becomes the generation's starting position")
	m.resumeTokenMu.Unlock()

	second := m.beginTokenEpoch(nil)
	require.Greater(t, second, first, "each generation must be distinguishable from the last")
	m.resumeTokenMu.Lock()
	require.Nil(t, m.resumeToken, "clearing installs no position")
	m.resumeTokenMu.Unlock()
}
