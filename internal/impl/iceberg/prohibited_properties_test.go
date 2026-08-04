// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// These tests pin the error-driven prohibited-property stripping added for
// engine-backed catalogs (Databricks Unity Catalog) that reject commits whose
// set-properties updates touch reserved keys. The live failure this guards
// against: a copy-on-write mutation fails because iceberg-go defensively sets
// schema.name-mapping.default when the table has no name mapping, and UC
// prohibits external clients writing that key.

// liveUCProhibitedKeysError reproduces the exact error a live Databricks Unity
// Catalog run returned for a copy-on-write commit (modulo the offending key
// list, which UC populates with the keys it saw).
func liveUCProhibitedKeysError(keys ...string) error {
	return fmt.Errorf("BadRequestException: Malformed request: INVALID_PARAMETER_VALUE: Table properties contain prohibited keys: %s", strings.Join(keys, ", "))
}

// updatePropertyKeys extracts a set-properties update's key/value map via the
// same JSON round-trip the production filter uses (the concrete update struct
// is unexported in iceberg-go).
func updatePropertyKeys(u table.Update) map[string]string {
	raw, err := json.Marshal(u)
	if err != nil {
		return nil
	}
	var payload struct {
		Updates map[string]string `json:"updates"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}
	return payload.Updates
}

// prohibitingCatalog is a table.CatalogIO that models Unity Catalog's
// prohibited-key enforcement: any CommitTable whose set-properties updates
// contain a prohibited key is rejected with the exact live UC error naming the
// offending keys; every other commit is applied through the embedded
// memCatalog.
type prohibitingCatalog struct {
	*memCatalog
	prohibited []string
	commits    int
	rejections int
}

func (p *prohibitingCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	p.commits++
	var offending []string
	for _, u := range updates {
		if u.Action() != table.UpdateSetProperties {
			continue
		}
		props := updatePropertyKeys(u)
		for _, k := range p.prohibited {
			if _, ok := props[k]; ok {
				offending = append(offending, k)
			}
		}
	}
	if len(offending) > 0 {
		p.rejections++
		return nil, "", liveUCProhibitedKeysError(offending...)
	}
	return p.memCatalog.CommitTable(ctx, ident, reqs, updates)
}

func (p *prohibitingCatalog) snapshot() *table.Table {
	return rebindTable(p.memCatalog.snapshot(), p)
}

// lockedBuffer is a goroutine-safe bytes.Buffer for capturing committer logs
// under -race.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func capturedLogger(buf *lockedBuffer) *service.Logger {
	return service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
}

// TestCOWOverwriteStripsProhibitedKeys drives a real copy-on-write overwrite
// against a catalog that prohibits reserved property keys, reproducing the
// live Unity Catalog failure. The commit must succeed on the retry with the
// prohibited keys stripped, apply the mutation exactly once, keep the
// prohibited keys out of the final metadata, and log the one-time warning.
func TestCOWOverwriteStripsProhibitedKeys(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	// setup seeds id=1,2,3 through the plain in-memory catalog (Transaction.
	// Append does not auto-set a name mapping, mirroring a UC-created table),
	// then wraps it in the prohibiting catalog so only the mutation under test
	// is subject to key enforcement.
	setup := func(t *testing.T, prohibited ...string) (*prohibitingCatalog, *writer, *lockedBuffer) {
		ctx := t.Context()
		seedTbl, mem := newCOWTable(t, sc)
		_ = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})
		require.NotContains(t, mem.snapshot().Properties(), "schema.name-mapping.default",
			"precondition: the seeded table must have no name mapping, so the COW path stages one")

		cat := &prohibitingCatalog{memCatalog: mem, prohibited: prohibited}
		var buf lockedBuffer
		comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3},
			func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, capturedLogger(&buf))
		require.NoError(t, err)
		t.Cleanup(comm.Close)
		w := cowWriter(t, cat.snapshot(), "id")
		w.committer = comm
		return cat, w, &buf
	}

	want := map[int64]string{1: "one", 2: "TWO", 3: "three"}
	upsert := func() service.MessageBatch {
		return service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})}
	}

	t.Run("name mapping stripped after live UC rejection", func(t *testing.T) {
		ctx := t.Context()
		cat, w, buf := setup(t, "schema.name-mapping.default")

		require.NoError(t, w.Write(ctx, upsert()), "the overwrite must succeed once the prohibited key is stripped")

		assert.Equal(t, 1, cat.rejections, "exactly one attempt is rejected before the strip set is armed")
		assert.Equal(t, 2, cat.commits, "reject once, then succeed on the retry")
		assert.Equal(t, 1, countSnapshotsWithCommitID(cat.snapshot()), "mutation applied exactly once")
		assert.Equal(t, want, scanRows(t, ctx, cat.snapshot()))

		props := cat.snapshot().Properties()
		assert.NotContains(t, props, "schema.name-mapping.default",
			"the prohibited key must not reach the committed metadata")
		assert.Equal(t, table.WriteModeCopyOnWrite, props[table.WriteDeleteModeKey],
			"non-prohibited keys in the same commit must still land")

		logs := buf.String()
		assert.Contains(t, logs, "prohibits table property", "the strip warning must be logged")
		assert.Contains(t, logs, "schema.name-mapping.default", "the strip warning must name the key")
		assert.Contains(t, logs, "stripping it from commits", "the strip warning must state the action")
	})

	// Unity Catalog may also prohibit other reserved keys such as
	// write.delete.mode, which our own commitOverwrite sets defensively. A
	// multi-key rejection must strip every named key in one retry.
	t.Run("multi-key rejection stripped in one retry", func(t *testing.T) {
		ctx := t.Context()
		cat, w, _ := setup(t, "write.delete.mode", "schema.name-mapping.default")

		require.NoError(t, w.Write(ctx, upsert()))

		assert.Equal(t, 1, cat.rejections)
		assert.Equal(t, 2, cat.commits, "both keys are learned from a single rejection")
		assert.Equal(t, 1, countSnapshotsWithCommitID(cat.snapshot()), "mutation applied exactly once")
		assert.Equal(t, want, scanRows(t, ctx, cat.snapshot()))

		props := cat.snapshot().Properties()
		assert.NotContains(t, props, "schema.name-mapping.default")
		assert.NotContains(t, props, table.WriteDeleteModeKey)
	})
}

// alwaysProhibitingCatalog rejects every commit with the live UC error naming
// a fixed key list, regardless of the updates' content. Used to prove that a
// catalog prohibiting a reserved redpanda-connect.* key fails loudly rather
// than being stripped.
type alwaysProhibitingCatalog struct {
	*memCatalog
	keys    []string
	commits int
}

func (p *alwaysProhibitingCatalog) CommitTable(context.Context, table.Identifier, []table.Requirement, []table.Update) (table.Metadata, string, error) {
	p.commits++
	return nil, "", liveUCProhibitedKeysError(p.keys...)
}

func (p *alwaysProhibitingCatalog) snapshot() *table.Table {
	return rebindTable(p.memCatalog.snapshot(), p)
}

// TestProhibitedReservedKeyFailsLoudly pins the safety guard: keys under
// redpanda-connect.* carry connector semantics (e.g. the timestamp-encoding
// pin), so a catalog that prohibits them must fail the commit with a clear
// error instead of silently stripping them — and must not burn retries doing
// so.
func TestProhibitedReservedKeyFailsLoudly(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	cat := &alwaysProhibitingCatalog{memCatalog: mem, keys: []string{"redpanda-connect.timestamp-encoding"}}

	c, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3},
		func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, service.MockResources().Logger())
	require.NoError(t, err)
	defer c.Close()

	df := synthDataFile(t, cat.snapshot().Spec(), fmt.Sprintf("%s/data/reserved-%s.parquet", cat.location, uuid.New()))
	err = c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df}, SchemaID: c.currentSchemaID()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "redpanda-connect.timestamp-encoding")
	assert.Contains(t, err.Error(), "refuses to strip")
	assert.Equal(t, 1, cat.commits, "a reserved-key rejection must fail on the first attempt, not retry")
}

// TestProhibitedReservedKeyCaseInsensitive pins that the reserved-prefix guard
// is case-insensitive: a catalog that case-folds key names in its rejection
// ("Redpanda-Connect.…") must still trigger the loud refuses-to-strip
// diagnostic — with the property surviving outside the strip set — rather than
// burning a futile stripped retry and surfacing the raw catalog error.
func TestProhibitedReservedKeyCaseInsensitive(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	cat := &alwaysProhibitingCatalog{memCatalog: mem, keys: []string{"Redpanda-Connect.timestamp-encoding"}}

	c, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3},
		func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, service.MockResources().Logger())
	require.NoError(t, err)
	defer c.Close()

	df := synthDataFile(t, cat.snapshot().Spec(), fmt.Sprintf("%s/data/reserved-fold-%s.parquet", cat.location, uuid.New()))
	err = c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df}, SchemaID: c.currentSchemaID()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Redpanda-Connect.timestamp-encoding")
	assert.Contains(t, err.Error(), "refuses to strip", "a case-variant reserved key must hit the loud diagnostic, not the raw error")
	assert.Equal(t, 1, cat.commits, "a reserved-key rejection must fail on the first attempt, not retry")
	assert.Empty(t, c.stripper.strip.snapshot(), "the reserved key must never enter the strip set")
}

// TestProhibitedKeyNeverSentIsNotLearned pins the strongest parser guard: a
// rejection naming a property key the failed commit never actually sent (the
// append path stages no set-properties updates at all) must not be learned —
// otherwise arbitrary error text could poison the strip set.
func TestProhibitedKeyNeverSentIsNotLearned(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	cat := &alwaysProhibitingCatalog{memCatalog: mem, keys: []string{"some.innocent.key"}}

	c, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3},
		func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, service.MockResources().Logger())
	require.NoError(t, err)
	defer c.Close()

	df := synthDataFile(t, cat.snapshot().Spec(), fmt.Sprintf("%s/data/unsent-%s.parquet", cat.location, uuid.New()))
	err = c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df}, SchemaID: c.currentSchemaID()})
	require.Error(t, err, "with nothing to strip, the rejection is not retryable and must surface")
	assert.Empty(t, c.stripper.strip.snapshot(), "a key the commit never sent must not be learned")
	assert.Equal(t, 1, cat.commits, "no stripped retry may be attempted for a key we never sent")
}

// TestProhibitedKeysPersistAcrossCommitters pins the strip-set persistence the
// router provides via tableEntry.prohibitedProps: writeWithRetry closes the
// writer (and its committer) on every failure, so a NEW committer seeded with
// the SAME shared prohibitedKeySet — exactly what Router.createWriter does —
// must strip already-learned keys on its FIRST attempt instead of burning one
// rejected commit per writer generation.
func TestProhibitedKeysPersistAcrossCommitters(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	seedTbl, mem := newCOWTable(t, sc)
	_ = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})
	cat := &prohibitingCatalog{memCatalog: mem, prohibited: []string{"schema.name-mapping.default"}}

	// One shared set for the table, as Router.createWriter seeds from
	// tableEntry.prohibitedProps.
	shared := newProhibitedKeySet()
	newGenWriter := func() *writer {
		comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3, ProhibitedKeys: shared},
			func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, service.MockResources().Logger())
		require.NoError(t, err)
		t.Cleanup(comm.Close)
		w := cowWriter(t, cat.snapshot(), "id")
		w.committer = comm
		return w
	}

	// First committer generation pays one rejection to learn the key.
	w1 := newGenWriter()
	require.NoError(t, w1.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})}))
	require.Equal(t, 1, cat.rejections)
	require.Equal(t, 2, cat.commits, "generation one: one rejection, one stripped success")

	// Second generation (fresh committer, same shared set — the writer was
	// recreated): the mutation must commit FIRST-TRY, no rejection burned.
	w2 := newGenWriter()
	require.NoError(t, w2.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 3, "payload": "THREE"})}))
	assert.Equal(t, 1, cat.rejections, "a committer seeded with the shared set must not re-learn the key via a rejection")
	assert.Equal(t, 3, cat.commits, "generation two must land its commit on the first attempt")
	assert.Equal(t, map[int64]string{1: "one", 2: "TWO", 3: "THREE"}, scanRows(t, ctx, cat.snapshot()))
}

// unknownStateProhibitedTextCatalog applies the FIRST commit server-side but
// reports an ErrCommitStateUnknown whose text also names a prohibited key that
// the commit really sent. It models a 5xx from a catalog whose error body
// happens to mention prohibited keys: the ambiguity handling (reload +
// commit-id idempotency) must take precedence over prohibited-key learning.
type unknownStateProhibitedTextCatalog struct {
	*memCatalog
	calls int
}

func (c *unknownStateProhibitedTextCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	c.calls++
	if c.calls == 1 {
		if _, _, err := c.memCatalog.CommitTable(ctx, ident, reqs, updates); err != nil {
			return nil, "", err
		}
		return nil, "", fmt.Errorf("500 Internal Server Error: Table properties contain prohibited keys: schema.name-mapping.default: %w", rest.ErrCommitStateUnknown)
	}
	return c.memCatalog.CommitTable(ctx, ident, reqs, updates)
}

func (c *unknownStateProhibitedTextCatalog) snapshot() *table.Table {
	return rebindTable(c.memCatalog.snapshot(), c)
}

// TestUnknownStateTakesPrecedenceOverProhibitedKeys pins the precedence fix: a
// commit that LANDED but returned ErrCommitStateUnknown must go through the
// reload + commit-id idempotency check — treating it as a prohibited-keys
// rejection would re-stage and apply the mutation a second time. Exactly one
// snapshot may carry the commit id, and no key may be learned.
func TestUnknownStateTakesPrecedenceOverProhibitedKeys(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	seedTbl, mem := newCOWTable(t, sc)
	_ = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two"})
	cat := &unknownStateProhibitedTextCatalog{memCatalog: mem}

	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3},
		func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, service.MockResources().Logger())
	require.NoError(t, err)
	t.Cleanup(comm.Close)
	w := cowWriter(t, cat.snapshot(), "id")
	w.committer = comm

	require.NoError(t, w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})}))

	assert.Equal(t, 1, cat.calls, "the landed commit must be detected via the commit-id, not re-sent")
	assert.Equal(t, 1, countSnapshotsWithCommitID(cat.snapshot()), "the mutation must be applied exactly once")
	assert.Equal(t, map[int64]string{1: "one", 2: "TWO"}, scanRows(t, ctx, cat.snapshot()))
	assert.Empty(t, comm.stripper.strip.snapshot(), "an unknown-state error must never teach the stripper")
}

// TestParseProhibitedPropertyKeys pins the rejection-parsing grammar: a
// case-insensitive "prohibited key(s)" marker with arbitrary prefix text, an
// optional colon, and a comma-separated key list that tolerates quotes,
// brackets, trailing prose, and sentence punctuation.
func TestParseProhibitedPropertyKeys(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want []string
	}{
		{
			name: "exact live Unity Catalog error",
			err:  errors.New("committing copy-on-write overwrite: committing transaction: BadRequestException: Malformed request: INVALID_PARAMETER_VALUE: Table properties contain prohibited keys: schema.name-mapping.default"),
			want: []string{"schema.name-mapping.default"},
		},
		{
			name: "multiple keys",
			err:  errors.New("Table properties contain prohibited keys: a.b, c.d"),
			want: []string{"a.b", "c.d"},
		},
		{
			name: "prefix text and case-insensitive marker",
			err:  errors.New("rpc error: SOME WRAPPER: Prohibited Keys: write.delete.mode"),
			want: []string{"write.delete.mode"},
		},
		{
			name: "singular key marker",
			err:  errors.New("prohibited key: schema.name-mapping.default"),
			want: []string{"schema.name-mapping.default"},
		},
		{
			name: "quoted and bracketed list",
			err:  errors.New(`prohibited keys: ["a.b", "c-d.e_f"]`),
			want: []string{"a.b", "c-d.e_f"},
		},
		{
			name: "trailing prose after a key",
			err:  errors.New("prohibited keys: a.b (remove them and retry)"),
			want: []string{"a.b"},
		},
		{
			name: "sentence-terminating period",
			err:  errors.New("prohibited keys: schema.name-mapping.default."),
			want: []string{"schema.name-mapping.default"},
		},
		{
			// Regression: the old lazy-colon regex backtracked "keys" into
			// "key" + list "s" and learned the key "s".
			name: "bare phrase without a list learns nothing",
			err:  errors.New("prohibited keys"),
			want: nil,
		},
		{
			// Regression: without the required colon this learned "detected".
			name: "prose after the marker without a colon learns nothing",
			err:  errors.New("prohibited keys detected in the request"),
			want: nil,
		},
		{
			// Regression: the prose after the sentence break used to yield a
			// bogus second key "then".
			name: "sentence prose after the key list is not learned",
			err:  errors.New("prohibited keys: a.b. Remove them, then retry."),
			want: []string{"a.b"},
		},
		{
			name: "prose tail after multiple keys keeps only the real keys",
			err:  errors.New("prohibited keys: a.b, c.d. Please remove them, then retry."),
			want: []string{"a.b", "c.d"},
		},
		{
			name: "quoted comma-separated keys",
			err:  errors.New("prohibited keys: 'x.y', 'z.w'"),
			want: []string{"x.y", "z.w"},
		},
		{
			// Real property keys are dotted; a bare word is prose, not a key.
			name: "dotless token is not a property key",
			err:  errors.New("prohibited keys: forbidden"),
			want: nil,
		},
		{
			name: "unrelated error",
			err:  errors.New("commit failed, refresh and try again"),
			want: nil,
		},
		{
			name: "nil error",
			err:  nil,
			want: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, parseProhibitedPropertyKeys(tc.err))
		})
	}
}

// TestStripperFiltersOnlySetPropertiesUpdates pins the filtering guarantees:
// with stripping active, non-property updates pass through as the SAME values
// (never rewritten), set-properties updates lose only the stripped keys, an
// update left empty is dropped, and untouched set-properties updates keep
// their original value. It also pins the strip-set rules: dedupe on re-add and
// refusal of reserved redpanda-connect.* keys.
func TestStripperFiltersOnlySetPropertiesUpdates(t *testing.T) {
	s := newPropertyStrippingCatalog(nil, nil)

	// Pass-through while the strip set is empty: same slice, no copies.
	unfilteredProps := table.NewSetPropertiesUpdate(iceberg.Properties{"schema.name-mapping.default": "m"})
	out, err := s.filterUpdates([]table.Update{unfilteredProps})
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Same(t, table.Update(unfilteredProps), out[0], "an empty strip set must not touch any update")

	require.True(t, s.addProhibitedKey("schema.name-mapping.default"))
	assert.False(t, s.addProhibitedKey("schema.name-mapping.default"), "re-learning a key must report not-new (one-time warning)")
	assert.False(t, s.addProhibitedKey("redpanda-connect.timestamp-encoding"), "reserved keys must never enter the strip set")

	snapUpd := table.NewAddSnapshotUpdate(&table.Snapshot{
		SnapshotID:   42,
		TimestampMs:  1,
		ManifestList: "s3://bucket/manifest-list.avro",
		Summary:      &table.Summary{Operation: table.OpAppend},
	})
	fmtUpd := table.NewUpgradeFormatVersionUpdate(2)
	mixedProps := table.NewSetPropertiesUpdate(iceberg.Properties{
		"schema.name-mapping.default": "m",
		"write.delete.mode":           "copy-on-write",
	})
	onlyStripped := table.NewSetPropertiesUpdate(iceberg.Properties{"schema.name-mapping.default": "m"})
	cleanProps := table.NewSetPropertiesUpdate(iceberg.Properties{"redpanda-connect.timestamp-encoding": "spec"})

	out, err = s.filterUpdates([]table.Update{snapUpd, mixedProps, fmtUpd, onlyStripped, cleanProps})
	require.NoError(t, err)
	require.Len(t, out, 4, "the update that only set stripped keys must be dropped")

	assert.Same(t, table.Update(snapUpd), out[0], "an add-snapshot update must pass through untouched")
	assert.Same(t, table.Update(fmtUpd), out[2], "a non-property update must pass through untouched")
	assert.Same(t, table.Update(cleanProps), out[3], "a set-properties update naming no stripped key must pass through untouched")

	require.Equal(t, table.UpdateSetProperties, out[1].Action())
	assert.Equal(t, map[string]string{"write.delete.mode": "copy-on-write"}, updatePropertyKeys(out[1]),
		"only the stripped key may be removed from a mixed set-properties update")
}
