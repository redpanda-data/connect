// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"

	"golang.org/x/sync/errgroup"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
)

// CurrentIcebergVersion is the version of iceberg we use when writing.
// TODO(iceberg): When iceberg-go supports v3, add a config knob on moving to v3.
// For now we assume everything works with at least v2
const CurrentIcebergVersion = 2

// commitIDProp is a namespaced idempotency token written into a mutation
// commit's snapshot summary. iceberg-go copies any custom key in the snapshot
// props into the committed snapshot's Summary.Properties (snapshot_producers.go
// summary() does maps.Copy(summaryProps, props)), and Summary marshals/unmarshals
// through the catalog, so the token survives a table reload. On a retry after an
// ambiguous catalog response (ErrCommitStateUnknown) the committer can then look
// for the token in a reloaded snapshot: if present, the prior attempt actually
// landed server-side, so the retry returns success instead of applying the
// mutation a second time. This makes copy-on-write (Overwrite/Delete) and
// merge-on-read (RowDelta) commits safe to retry on an unknown state.
const commitIDProp = "redpanda-connect.commit-id"

// CommitInput holds data files and the schema ID they were written with.
//
// Files are inserted (appended) data files. DeleteFiles are equality-delete
// files produced for upsert/delete operations (ContentType EntryContentEqDeletes
// with EqualityFieldIDs set). When DeleteFiles is empty the commit is a pure
// append and takes the optimised AddDataFiles fast path; otherwise the changes
// are applied atomically through a RowDelta.
type CommitInput struct {
	Files       []iceberg.DataFile
	DeleteFiles []iceberg.DataFile
	SchemaID    int
}

// OverwriteInput describes a copy-on-write mutation applied as one atomic
// snapshot. Filter selects the existing rows to remove. NewReader, when
// non-nil, is a factory that builds the rows to (re)write; it is a factory
// rather than a reader because array.RecordReader is consumed once and the
// commit stage may run more than once on retry. A nil NewReader is a
// delete-only mutation (no rows written).
type OverwriteInput struct {
	Filter    iceberg.BooleanExpression
	NewReader func() (array.RecordReader, error)
	SchemaID  int
}

// CommitConfig holds configuration for the committer.
type CommitConfig struct {
	ManifestMergeEnabled bool
	MaxSnapshotAge       time.Duration
	MaxRetries           int
	// SkipFormatUpgrade leaves the table at its existing format version instead
	// of upgrading to v2. Set for copy-on-write, which only ever writes plain
	// data files (no v2 delete files) and so works on a v1 table — avoiding an
	// unnecessary, irreversible v1->v2 upgrade. Merge-on-read/append leave this
	// false: their equality-delete path requires v2.
	SkipFormatUpgrade bool
	// DisableCleanupOnFailure turns off every post-failed-commit file cleanup
	// this output performs (writer.cleanupFilesAfterCommitErr and
	// commitOverwrite's authorship-tracked orphan sweep). It is the negation of
	// the positively-named `commit.cleanup_on_failure` config field so that the
	// Go zero value keeps cleanup ENABLED — the safe, default behaviour — for
	// every caller that constructs a CommitConfig literal.
	//
	// It exists purely as an incident escape hatch: disabling cleanup can only
	// ever leak storage (orphaned files that Iceberg orphan-file maintenance
	// reclaims), never corrupt a table, so it is always safe to flip.
	DisableCleanupOnFailure bool
	// ProhibitedKeys, when non-nil, is the shared set of catalog-prohibited
	// property keys the committer's stripper reads and learns into. The
	// router owns one per tableEntry so keys learned from one committer's
	// rejection persist across writer recreation (writeWithRetry closes the
	// writer on every failure) instead of costing a rejected commit per
	// generation. Nil gets a fresh, private set.
	ProhibitedKeys *prohibitedKeySet
}

// StaleSchemaError is returned when data was written with a schema
// that no longer matches the table's current schema.
type StaleSchemaError struct {
	WriterSchemaID  int
	CurrentSchemaID int
}

func (e *StaleSchemaError) Error() string {
	return fmt.Sprintf("stale schema: data written with schema %d but table is at schema %d",
		e.WriterSchemaID, e.CurrentSchemaID)
}

// committer batches data file commits for a single table.
// Commits are serialized - only one commit at a time per committer.
type committer struct {
	table       *table.Table
	cfg         CommitConfig
	reloadTable func(ctx context.Context) (*table.Table, error)
	batcher     *asyncroutine.Batcher[CommitInput, struct{}]
	// stripper wraps the table's catalog at the commit boundary so that
	// property keys a catalog rejects as prohibited (learned from the
	// rejection error in commitLocked) are filtered from later attempts. It
	// is installed unconditionally and is a pass-through until a key is
	// learned. Every table the committer retains (initial, reloaded, and
	// post-commit — the latter inherits the binding from its transaction) is
	// bound to it; see NewCommitter for the rebinding choke points.
	stripper *propertyStrippingCatalog
	// writes records every data-file path written through this committer's
	// table handles (their filesystem factory is wrapped by
	// rebindTableRecording, at the same choke points as the stripper). It is
	// what makes copy-on-write orphan cleanup safe under concurrent writers:
	// cleanup only ever considers paths this committer itself authored, so
	// another process's written-but-uncommitted files are untouchable by
	// construction. Scoped per commitOverwrite call via reset-on-entry, which
	// commitMu makes race-free.
	writes *writeRecorder
	// commitMu serializes all commits and guards c.table. The batcher's
	// doCommit and the direct commitRowDelta path both take it.
	commitMu        sync.Mutex
	upgradeWarnOnce sync.Once
	metrics         *opMetrics
	logger          *service.Logger
}

// NewCommitter creates a new committer for a specific table. cat must be the
// catalog tbl was loaded from (the table.CatalogIO its commits go to); the
// committer rebinds tbl — and every table reloadTable returns — onto a
// wrapper of cat so prohibited property keys can be stripped at the commit
// boundary (see propertyStrippingCatalog), and onto a recording filesystem so
// copy-on-write orphan cleanup knows exactly which data files this committer
// wrote (see writeRecorder).
func NewCommitter(tbl *table.Table, cat table.CatalogIO, cfg CommitConfig, reloadTable func(ctx context.Context) (*table.Table, error), logger *service.Logger) (*committer, error) {
	if cat == nil {
		return nil, errors.New("creating committer: catalog must not be nil")
	}
	// commitLocked dereferences reloadTable on every failure branch, so a nil
	// one would panic mid-commit rather than fail construction; reject it here
	// with a clear error. The production caller (Router.createWriter) always
	// supplies one.
	if reloadTable == nil {
		return nil, errors.New("creating committer: reloadTable must not be nil")
	}
	// Defensively clamp MaxRetries to at least 1: commitLocked's retry loop is
	// `for range cfg.MaxRetries`, so a zero or negative value would never run a
	// single attempt and return a "committing transaction after 0 attempts"
	// error wrapping a nil cause. Config lint rejects it at startup (config.go),
	// but callers constructing a committer directly get the same safety here.
	if cfg.MaxRetries < 1 {
		cfg.MaxRetries = 1
	}
	stripper := newPropertyStrippingCatalog(cat, cfg.ProhibitedKeys)
	writes := newWriteRecorder()
	c := &committer{
		table:    rebindTableRecording(tbl, stripper, writes),
		cfg:      cfg,
		stripper: stripper,
		writes:   writes,
		logger:   logger,
	}
	// Single choke point for reloaded tables: every table handle the
	// committer adopts after a reload is rebound onto the stripper and the
	// write recorder, so retried commits keep flowing through the
	// prohibited-key filter and keep recording the data files they write.
	// (Post-commit tables inherit both bindings from their transaction.)
	c.reloadTable = func(ctx context.Context) (*table.Table, error) {
		fresh, err := reloadTable(ctx)
		if err != nil {
			return nil, err
		}
		return rebindTableRecording(fresh, stripper, writes), nil
	}

	batcher, err := asyncroutine.NewBatcher(100, c.doCommit)
	if err != nil {
		return nil, fmt.Errorf("creating batcher: %w", err)
	}
	c.batcher = batcher

	return c, nil
}

// Commit submits files for commit and waits for the result. Pure appends are
// batched for throughput. Commits that carry equality-delete files are applied
// as their own snapshot (never coalesced with other commits): merge-on-read
// equality deletes only remove rows from earlier snapshots, so merging two
// keyed batches into a single snapshot would leave same-key duplicates.
func (c *committer) Commit(ctx context.Context, input CommitInput) error {
	if len(input.DeleteFiles) > 0 {
		return c.commitRowDelta(ctx, input)
	}
	_, err := c.batcher.Submit(ctx, input)
	if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		// The batcher runs commits on its own background context, so Submit
		// returning the CALLER's context error does not stop the request:
		// still queued or mid-flight, it may yet commit these files. That is
		// an ambiguous outcome exactly like a lost commit response — mark it
		// with the unknown-state sentinel so the writer's cleanup gate (and
		// every other errors.Is(err, rest.ErrCommitStateUnknown) check)
		// leaves the written files alone instead of deleting files a
		// still-landing snapshot may reference.
		return fmt.Errorf("%w: %w", rest.ErrCommitStateUnknown, err)
	}
	return err
}

// doCommit processes a batch of append-only commit inputs (the batcher path).
func (c *committer) doCommit(ctx context.Context, inputs []CommitInput) ([]struct{}, error) {
	c.commitMu.Lock()
	defer c.commitMu.Unlock()

	currentSchemaID := c.currentSchemaID()
	var allFiles []iceberg.DataFile
	for _, input := range inputs {
		if input.SchemaID != currentSchemaID {
			return nil, &StaleSchemaError{WriterSchemaID: input.SchemaID, CurrentSchemaID: currentSchemaID}
		}
		allFiles = append(allFiles, input.Files...)
	}

	// The append path carries BOTH idempotency mechanisms across a reload:
	// dropAlreadyCommitted (keyed on file paths) prunes files a lost-but-landed
	// attempt already committed, and the commit-id token short-circuits the
	// retry entirely when the landed snapshot is visible. The token is what
	// survives an external rewriter: if compaction rewrites our landed files
	// away between the landing and our reload, the path-keyed check can no
	// longer see them — but the token in the (historical) snapshot summary
	// still proves the batch landed, so the retry returns success instead of
	// re-adding the files and duplicating rows.
	commitID := uuid.NewString()
	if _, err := c.commitLocked(ctx, commitID, true, func(txn *table.Transaction, props iceberg.Properties, reloaded bool) error {
		files := allFiles
		if reloaded {
			// A prior attempt can land server-side yet report failure (a lost
			// or ambiguous response). The reload then already references those
			// files, so re-adding them with the duplicate check disabled would
			// register the same path in two snapshots and produce conflicting
			// sequence numbers for downstream readers. Drop any file the
			// reloaded table already has; if all are present the previous
			// attempt succeeded and AddDataFiles + Commit become a no-op.
			remaining, err := c.dropAlreadyCommitted(ctx, allFiles)
			if err != nil {
				return err
			}
			files = remaining
		}
		// WithoutDuplicateCheck: writer.Write stamps each path with a fresh uuid,
		// so iceberg-go's default O(snapshot) manifest collision scan is wasted
		// work on the commit hot path (T6692). On a reloaded retry we run the
		// targeted dropAlreadyCommitted check above instead.
		return txn.AddDataFiles(ctx, files, props,
			table.WithoutAutoNameMapping(),
			table.WithoutDuplicateCheck(),
		)
	}); err != nil {
		return nil, err
	}
	c.logger.Debugf("Committed %d data files", len(allFiles))
	return make([]struct{}, len(inputs)), nil
}

// commitRowDelta applies a single input's inserts and equality deletes as one
// atomic snapshot, outside the batcher so it is never coalesced with another
// keyed commit.
func (c *committer) commitRowDelta(ctx context.Context, input CommitInput) error {
	c.commitMu.Lock()
	defer c.commitMu.Unlock()

	currentSchemaID := c.currentSchemaID()
	if input.SchemaID != currentSchemaID {
		return &StaleSchemaError{WriterSchemaID: input.SchemaID, CurrentSchemaID: currentSchemaID}
	}
	// A stable commit-id, generated once before the retry loop, makes this
	// merge-on-read commit idempotent across a reload: commitLocked stamps it into
	// the snapshot summary and, on a retry after a failed or ambiguous
	// (ErrCommitStateUnknown) response, detects a prior attempt that actually
	// landed by finding the id in a reloaded snapshot — returning success instead
	// of applying the RowDelta (and its equality deletes) a second time. That is
	// why retryOnUnknownState is safe to enable here.
	commitID := uuid.NewString()
	if _, err := c.commitLocked(ctx, commitID, true, func(txn *table.Transaction, props iceberg.Properties, _ bool) error {
		// RowDelta derives the snapshot operation automatically
		// (append/delete/overwrite).
		rd := txn.NewRowDelta(props)
		if len(input.Files) > 0 {
			rd.AddRows(input.Files...)
		}
		rd.AddDeletes(input.DeleteFiles...)
		return rd.Commit(ctx)
	}); err != nil {
		return err
	}
	c.logger.Debugf("Committed row delta: %d data files, %d delete files", len(input.Files), len(input.DeleteFiles))
	return nil
}

// commitOverwrite applies a copy-on-write mutation as one atomic snapshot,
// outside the batcher so it is never coalesced with another commit. When
// input.NewReader is nil it is a delete-only mutation (txn.Delete); otherwise
// it is an overwrite that deletes the rows matching input.Filter and appends
// the reader's rows in a single snapshot (txn.Overwrite). Both produce only
// plain data files — no equality- or positional-delete files — so the result
// is readable by engine-backed catalogs (Snowflake, Databricks Unity Catalog).
func (c *committer) commitOverwrite(ctx context.Context, input OverwriteInput) error {
	c.commitMu.Lock()
	defer c.commitMu.Unlock()

	currentSchemaID := c.currentSchemaID()
	if input.SchemaID != currentSchemaID {
		return &StaleSchemaError{WriterSchemaID: input.SchemaID, CurrentSchemaID: currentSchemaID}
	}

	// Copy-on-write writes its rewritten and new data files to storage before the
	// catalog commit (inside txn.Overwrite/Delete), and — unlike the writer-
	// authored append/row-delta paths — we are never handed their paths.
	// commitLocked re-runs the stage (and so re-writes fresh parquet) on EACH
	// attempt, so even a commit that ultimately succeeds can leave earlier
	// attempts' files behind. The committer's table handles write through a
	// recording filesystem (rebindTableRecording), so every data file this call's
	// stage attempts write is captured in c.writes; reset it now so the set is
	// scoped to exactly this call (commitMu, held here, serializes every commit
	// through this committer, so nothing else records concurrently).
	c.writes.reset()

	// The snapshot current at entry bounds the post-commit orphan scan: only
	// snapshots landed after this marker can reference the files this call
	// writes (paths are uuid-stamped), and among them may be our OWN
	// landed-but-superseded attempt, whose files must survive cleanup even
	// when an external rewriter has already replaced the current snapshot.
	startSnapshotID := int64(-1)
	if snap := c.table.CurrentSnapshot(); snap != nil {
		startSnapshotID = snap.SnapshotID
	}

	// A stable commit-id, generated once before the retry loop, makes this
	// copy-on-write commit idempotent across a reload: commitLocked stamps it into
	// the snapshot summary and, on a retry after a failed or ambiguous
	// (ErrCommitStateUnknown) response, detects a prior attempt that actually
	// landed by finding the id in a reloaded snapshot — returning success instead
	// of re-applying the overwrite. That is why retryOnUnknownState is safe to
	// enable here.
	commitID := uuid.NewString()
	retried, err := c.commitLocked(ctx, commitID, true, func(txn *table.Transaction, props iceberg.Properties, _ bool) error {
		// txn.Delete branches on the table's write.delete.mode; the library
		// default is already copy-on-write, but set it explicitly for safety so
		// the delete-only path can never fall into merge-on-read. txn.Overwrite
		// is always copy-on-write regardless of the property.
		if c.table.Properties()[table.WriteDeleteModeKey] != table.WriteModeCopyOnWrite {
			if err := txn.SetProperties(iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeCopyOnWrite}); err != nil {
				return fmt.Errorf("setting %s: %w", table.WriteDeleteModeKey, err)
			}
		}
		if input.NewReader == nil {
			return txn.Delete(ctx, input.Filter, props)
		}
		rdr, err := input.NewReader()
		if err != nil {
			return err
		}
		defer rdr.Release()
		return txn.Overwrite(ctx, rdr, props, table.WithOverwriteFilter(input.Filter))
	})

	// Authorship-tracked orphan cleanup runs after commitLocked resolves.
	// commitLocked re-runs the stage on every retry, and each stage attempt
	// writes a fresh set of parquet files, so a clean-conflict-then-success
	// sequence lands the winning snapshot's files but leaves the earlier
	// attempt's files orphaned — running cleanup only on error (as we used to)
	// would leak them.
	//
	// The candidate set is exactly the files THIS call recorded writing (see
	// c.writes above), never a listing of the data directory: a concurrent
	// committer's written-but-not-yet-committed files are absent from our
	// recorded set, so they are untouchable by construction — no matter how the
	// commits interleave. A recorded file is deleted only if it is also absent
	// from every snapshot landed since this call began (referencedCandidatePaths),
	// which protects
	// the landed attempt's files on a retried success. Safety argument in one
	// line: authored-by-us AND not-referenced ⇒ orphan of a losing attempt.
	//
	// Two further guards:
	//   - Cleanup on failure runs ONLY when every attempt's outcome was a
	//     definitive server-side rejection (isDefinitiveCommitRejection): the
	//     destructive step requires PROOF that no attempt could still land. Any
	//     other failure — the catalog's explicit ErrCommitStateUnknown (5xx),
	//     but equally a raw transport error (timeout, connection reset, EOF) on
	//     the commit request — may have applied server-side, possibly AFTER we
	//     stopped looking: even a successful reload that does not show our
	//     commit-id token only proves the attempt had not landed YET, never
	//     that it won't (the server may be mid-apply). commitLocked therefore
	//     keeps the ambiguity marker sticky for the whole call — the returned
	//     error is unknown-class whenever ANY attempt's outcome was ambiguous,
	//     even when a later attempt's definitive failure terminated the loop —
	//     and cleanup is skipped, leaving the files for Iceberg orphan-file
	//     maintenance. Deleting them and having the ambiguous attempt land
	//     afterwards would leave a committed snapshot referencing deleted
	//     files: table corruption.
	//   - On SUCCESS we only clean when the commit was retried: a first-attempt
	//     success wrote exactly the files it committed, so there is nothing to
	//     reclaim and the reference scan is skipped.
	//   - `commit.cleanup_on_failure: false` (DisableCleanupOnFailure) switches
	//     the whole sweep off as an incident escape hatch. Skipping it only ever
	//     leaks storage — the recorded files are left for Iceberg orphan-file
	//     maintenance — so it can never corrupt the table.
	//
	// Cleanup is best-effort: failures are logged, never returned. It also
	// fails closed: if the reference scan cannot be completed, cleanup deletes
	// nothing at all (see cleanupOrphanedOverwriteFiles).
	if written := c.writes.snapshot(); len(written) > 0 &&
		!errors.Is(err, rest.ErrCommitStateUnknown) && (err != nil || retried) {
		if c.cfg.DisableCleanupOnFailure {
			c.logger.Debugf("Skipping copy-on-write orphan cleanup of %d recorded files: %s is disabled; leaving them for Iceberg orphan-file maintenance", len(written), ioFieldCleanupOnFailure)
		} else {
			c.cleanupOrphanedOverwriteFiles(ctx, written, startSnapshotID)
		}
	}
	if err != nil {
		return err
	}
	c.logger.Debugf("Committed copy-on-write mutation (delete-only=%t)", input.NewReader == nil)
	return nil
}

// cleanupOrphanedOverwriteFiles removes the data files a copy-on-write commit
// left orphaned: those in `written` — the paths THIS committer's stage attempts
// recorded writing (see writeRecorder) — that are not referenced by c.table's
// current snapshot. It runs after the commit resolves whether it succeeded or
// failed: a retried commit re-writes files on each attempt, so even a
// successful commit can leave an earlier attempt's files behind. Because the
// candidate set is authored-by-us by construction (never a directory listing),
// a concurrent committer's in-flight files can never be deleted here; the
// reference check against the current snapshot is the remaining guard that
// keeps the landed attempt's files safe on a retried success. Best-effort —
// errors are logged, not returned. The caller must have established that the
// commit did not terminate in an ambiguous (possibly-landed) state, since
// deleting a possibly-committed file would corrupt the table.
//
// Cleanup fails closed: if the reference scan cannot be completed
// (referencedCandidatePaths errors), it deletes NOTHING and leaves every
// recorded file for Iceberg orphan-file maintenance. An incomplete scan
// cannot tell a losing attempt's orphan from a file the landed snapshot
// references — on a retried success both attempts' files are in `written`,
// and treating a still-referenced file as an orphan would delete data the
// committed snapshot depends on. Deleting nothing is always safe; deleting on
// an incomplete scan is not.
func (c *committer) cleanupOrphanedOverwriteFiles(ctx context.Context, written map[string]struct{}, sinceSnapshotID int64) {
	fsys, err := c.table.FS(ctx)
	if err != nil {
		c.logger.Warnf("Skipping copy-on-write orphan cleanup: could not resolve the table filesystem; leaving %d recorded files for Iceberg orphan-file maintenance: %v", len(written), err)
		return
	}
	referenced, err := c.referencedCandidatePaths(ctx, written, sinceSnapshotID)
	if err != nil {
		c.logger.Warnf("Skipping copy-on-write orphan cleanup: could not verify which files committed snapshots reference; leaving %d recorded files for Iceberg orphan-file maintenance: %v", len(written), err)
		return
	}
	// Deletes run with bounded concurrency: this sweep executes under commitMu
	// on the write path, and a scattered copy-on-write batch can orphan
	// thousands of files — serial per-object round trips against object
	// storage would stall the batch ack and every queued commit for the table.
	// The bound keeps the burst polite to object-store rate limits while still
	// collapsing the sweep's wall clock by roughly its factor.
	const orphanDeleteConcurrency = 8
	var wg errgroup.Group
	wg.SetLimit(orphanDeleteConcurrency)
	for p := range written {
		if _, ref := referenced[p]; ref {
			continue
		}
		wg.Go(func() error {
			if rmErr := fsys.Remove(p); rmErr != nil {
				c.logger.Warnf("Failed to remove orphaned copy-on-write file %s: %v", p, rmErr)
			} else {
				c.logger.Debugf("Removed orphaned copy-on-write file %s", p)
			}
			return nil
		})
	}
	_ = wg.Wait()
}

// referencedCandidatePaths returns the subset of candidates referenced by any
// snapshot committed SINCE the given start marker (the snapshot that was
// current when the commit call began; -1 when the table had none), the current
// snapshot included. Used to guard orphan cleanup:
//
//   - Scanning snapshots-since-start rather than only the current snapshot
//     protects a file our own landed-but-superseded attempt committed: an
//     external rewriter (compaction, another copy-on-write writer) can replace
//     the current snapshot within the retry window, removing our landed files
//     from it while its historical snapshot still references them.
//   - Candidate paths are uuid-stamped at write time, so snapshots from BEFORE
//     the start marker cannot reference them — scanning those would be pure
//     waste. When nothing landed since the start marker, no manifest is read
//     at all.
//   - Only candidate membership is recorded (never the full path set of the
//     table), so memory is O(candidates) regardless of table size, and the
//     scan stops early once every candidate is accounted for.
//
// It fails closed: every failure to read a snapshot's manifests is propagated
// rather than accumulated past — a partial result would make cleanup mistake a
// still-referenced file for an orphan and delete data a committed snapshot
// depends on.
func (c *committer) referencedCandidatePaths(ctx context.Context, candidates map[string]struct{}, sinceSnapshotID int64) (map[string]struct{}, error) {
	refs := make(map[string]struct{}, len(candidates))
	if len(candidates) == 0 {
		return refs, nil
	}
	// Snapshots() is oldest-first; walk backward and stop at the start marker,
	// mirroring committedSnapshotHasID.
	var scan []table.Snapshot
	for _, s := range slices.Backward(c.table.Metadata().Snapshots()) {
		if s.SnapshotID == sinceSnapshotID {
			break
		}
		scan = append(scan, s)
	}
	if len(scan) == 0 {
		return refs, nil
	}
	fsys, err := c.table.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolving table filesystem: %w", err)
	}
	for _, snap := range scan {
		manifests, err := snap.Manifests(fsys)
		if err != nil {
			return nil, fmt.Errorf("loading snapshot %d manifests: %w", snap.SnapshotID, err)
		}
		for _, m := range manifests {
			for entry, err := range m.Entries(fsys, true) {
				if err != nil {
					return nil, fmt.Errorf("reading manifest entries: %w", err)
				}
				p := entry.DataFile().FilePath()
				if _, ok := candidates[p]; !ok {
					continue
				}
				refs[p] = struct{}{}
				if len(refs) == len(candidates) {
					return refs, nil
				}
			}
		}
	}
	return refs, nil
}

// commitLocked stages a transaction via stage and commits it, retrying on
// concurrent-commit conflicts and reloading table metadata between attempts.
// The stage callback's reloaded argument is false on the first attempt and
// true once the table has been reloaded after a failed attempt, so callers can
// guard against re-adding files a lost-but-landed attempt already committed.
//
// retryOnUnknownState controls whether an ErrCommitStateUnknown result (the
// commit may have landed server-side, e.g. a 5xx/timeout response) is retried.
// It is only safe to set when stage is idempotent across a reload — otherwise a
// retry of a commit that actually landed would duplicate it. Two mechanisms
// provide that idempotency:
//   - a non-empty commitID: it is stamped into the snapshot summary
//     (commitIDProp) so that, after a reload, committedSnapshotHasID can detect a
//     prior attempt that landed and short-circuit to success. Used by the
//     mutation paths (copy-on-write Overwrite/Delete and merge-on-read RowDelta),
//     whose stage callbacks are not path-idempotent on their own.
//   - stage dropping files the reloaded snapshot already references (the append
//     path's dropAlreadyCommitted), in which case commitID is empty.
//
// The first return value is retried: true when more than one attempt ran, i.e.
// the stage executed more than once and so may have written data files that are
// not part of the final committed snapshot. commitOverwrite uses this to decide
// whether success-path orphan cleanup is warranted: a first-attempt success has
// no such leftovers, and cleaning then would be unsafe under concurrent
// committers (see commitOverwrite).
//
// Ambiguity stickiness: an attempt whose txn.Commit error is not a definitive
// server-side rejection (isDefinitiveCommitRejection) may have landed
// server-side — that covers the catalog's explicit ErrCommitStateUnknown
// (5xx) and equally raw transport failures (client timeout, connection reset,
// EOF) where no verdict ever arrived. Within a call, the only observation
// that resolves such an ambiguity is finding our commit-id token in reloaded
// metadata — the attempt landed and we return success. The token's ABSENCE
// resolves nothing: it proves the attempt had not landed YET, not that it
// won't (the server may still be mid-apply). The ambiguity is therefore
// sticky across the WHOLE retry history: whenever commitLocked returns an
// error while some attempt's outcome is ambiguous — regardless of which
// attempt's error terminated the loop (exhausted retries, a non-retryable
// error, a stage failure, ...) — the returned error satisfies
// errors.Is(err, rest.ErrCommitStateUnknown) (each ambiguous error is
// normalised onto that sentinel where it is classified, and
// joinUnresolvedUnknown re-attaches earlier ambiguity to a later,
// differently-classed terminating error — so callers need no new check).
// Callers that gate
// destructive follow-up work on the absence of unknown state
// (commitOverwrite's orphan cleanup) rely on this: cleanup requires proof of
// non-application, and absence of proof of application is not that.
// (A commit that ultimately SUCCEEDS returns nil even after earlier ambiguous
// attempts: our commits carry requirements pinned to the base snapshot, so
// once a later attempt lands, an earlier straggler can no longer apply.)
//
// Callers must hold c.commitMu.
func (c *committer) commitLocked(ctx context.Context, commitID string, retryOnUnknownState bool, stage func(txn *table.Transaction, props iceberg.Properties, reloaded bool) error) (bool, error) {
	props := iceberg.Properties{
		table.ManifestMergeEnabledKey: strconv.FormatBool(c.cfg.ManifestMergeEnabled),
	}
	if c.cfg.MaxSnapshotAge > 0 {
		props[table.MaxSnapshotAgeMsKey] = strconv.FormatInt(c.cfg.MaxSnapshotAge.Milliseconds(), 10)
	}
	// Stamp the idempotency token into the snapshot props so the committed
	// snapshot carries it. props is reused across attempts, so this holds for
	// every stage attempt. iceberg-go copies it verbatim into Summary.Properties.
	if commitID != "" {
		props[commitIDProp] = commitID
	}

	// Record the snapshot current before our first attempt so the post-reload
	// idempotency scan can stop once it walks past it: any snapshot our commit
	// created is strictly newer than this one.
	var startSnapshotID int64 = -1
	if snap := c.table.CurrentSnapshot(); snap != nil {
		startSnapshotID = snap.SnapshotID
	}

	var commitErr error
	// unresolvedUnknown is non-nil once some attempt's outcome is ambiguous:
	// its txn.Commit error was not a definitive server-side rejection
	// (isDefinitiveCommitRejection), so the commit may have landed — or may
	// STILL land — server-side. Nothing on an error path ever clears it: a
	// reloaded snapshot without our commit-id token only proves the ambiguous
	// attempt has not landed YET, never that it won't (the server may be
	// mid-apply), so token absence is deliberately not treated as resolution.
	// The only resolutions are token FOUND (the attempt landed; we return
	// success) and a later attempt SUCCEEDING (its snapshot-pinned
	// requirements then fence out any straggler). Every error return joins the
	// marker into the returned error (joinUnresolvedUnknown) so callers'
	// errors.Is(err, rest.ErrCommitStateUnknown) checks stay true even when a
	// later attempt's differently-classed error terminates the loop.
	var unresolvedUnknown error
	attempt := 0
	reloaded := false
	for range c.cfg.MaxRetries {
		attempt++
		txn := c.table.NewTransaction()
		if !c.cfg.SkipFormatUpgrade && c.table.Metadata().Version() < CurrentIcebergVersion {
			c.upgradeWarnOnce.Do(func() {
				c.logger.Warnf("Upgrading iceberg table to format version %d to support row-level deletes; this change is irreversible", CurrentIcebergVersion)
			})
			if err := txn.UpgradeFormatVersion(CurrentIcebergVersion); err != nil {
				return attempt > 1, fmt.Errorf("upgrading version: %w", joinUnresolvedUnknown(err, unresolvedUnknown))
			}
		}
		if err := stage(txn, props, reloaded); err != nil {
			return attempt > 1, joinUnresolvedUnknown(err, unresolvedUnknown)
		}
		tbl, err := txn.Commit(ctx)
		// ambiguous is THE classification for this attempt's outcome; the
		// guards below reuse it rather than re-deriving it from the error
		// chain, so they cannot drift from the normalisation that follows.
		ambiguous := err != nil && !isDefinitiveCommitRejection(err)
		if ambiguous {
			// Anything short of a definitive server-side rejection may have
			// landed (or may still land) server-side: the catalog's explicit
			// ErrCommitStateUnknown (5xx), but equally a raw transport failure
			// (client timeout, connection reset, EOF) on the commit request.
			// Normalise err itself onto the one existing sentinel so EVERY
			// downstream errors.Is(err, rest.ErrCommitStateUnknown) check —
			// including callers' cleanup gates — sees the whole ambiguous
			// class, not just errors the catalog happened to wrap in the
			// sentinel already. Also sticky for the rest of the call (see
			// unresolvedUnknown above).
			if !errors.Is(err, rest.ErrCommitStateUnknown) {
				err = fmt.Errorf("%w: %w", rest.ErrCommitStateUnknown, err)
			}
			unresolvedUnknown = err
		}
		// Some engine-backed catalogs (Databricks Unity Catalog) reject a
		// commit whose set-properties updates touch reserved keys, naming the
		// offending keys in the error (e.g. "Table properties contain
		// prohibited keys: schema.name-mapping.default"). Learn those keys,
		// arm the stripper, and retry: the rejection is a clean 400 (nothing
		// landed), so re-staging from the same base is safe, and the next
		// attempt commits with the keys filtered out. Keys under
		// reservedTablePropertyPrefix are never stripped — they carry
		// connector semantics (e.g. the timestamp-encoding pin) — so a
		// catalog prohibiting them fails the commit loudly instead.
		//
		// An ambiguous-class error is never treated as a prohibited-keys
		// rejection, even if its text mentions them: the commit may have
		// landed server-side, and the prohibited-keys retry re-stages WITHOUT
		// the reload + commit-id idempotency check below, so it could apply
		// the mutation twice. Unknown-state takes precedence.
		if err != nil && !ambiguous {
			if retry, fatalErr := c.noteProhibitedKeys(attempt, err); fatalErr != nil {
				// Reload so the next call uses fresh metadata, mirroring the
				// non-retryable branch below.
				if reloaded, reloadErr := c.reloadTable(ctx); reloadErr == nil {
					c.table = reloaded
				}
				c.incrCommitFailure()
				return attempt > 1, joinUnresolvedUnknown(fatalErr, unresolvedUnknown)
			} else if retry {
				commitErr = err
				continue
			}
		}
		// A clean conflict (our commit did not land) is retried: a
		// reload-and-retry re-adds our files exactly once. Matched via
		// table.ErrCommitFailed, which rest.ErrCommitFailed (the 409) wraps —
		// iceberg-go's client-side conflict-validation sentinels
		// (ErrConflictingDataFiles etc., armed by commit.retry.num-retries)
		// wrap ONLY the table sentinel, so matching the rest one would let
		// those clean conflicts escape the retry loop after one attempt. An
		// ambiguous outcome means the commit may have landed; retrying is only
		// safe when stage dedupes against the reloaded snapshot
		// (retryOnUnknownState), in which case a landed attempt becomes a
		// no-op and an unlanded one re-adds once.
		if errors.Is(err, table.ErrCommitFailed) ||
			(retryOnUnknownState && ambiguous) {
			commitErr = err
			c.logger.Warnf("Commit attempt %d/%d failed: %v", attempt, c.cfg.MaxRetries, err)
			if reloadedTbl, reloadErr := c.reloadTable(ctx); reloadErr == nil {
				c.table = reloadedTbl
				reloaded = true
				// A successful reload can resolve a pending ambiguity in one
				// direction only: the commit-id check just below finding our
				// token (the ambiguous attempt landed — we return success).
				// The token's ABSENCE resolves nothing — it proves the attempt
				// has not landed YET, not that it won't (the server may still
				// be mid-apply and land it after this reload) — so
				// unresolvedUnknown is deliberately NOT cleared here.
				// Idempotency: a failed or ambiguous response may still have
				// landed the commit server-side. If the reloaded table already
				// carries our commit-id, the prior attempt succeeded — return
				// success rather than re-applying the mutation (which would
				// duplicate it). Every commit path passes a commitID; the
				// append path additionally prunes already-landed files via
				// dropAlreadyCommitted in its stage.
				if commitID != "" && c.committedSnapshotHasID(commitID, startSnapshotID) {
					c.logger.Debugf("Commit %s already landed on a prior attempt (found in reloaded snapshot); treating retry as success", commitID)
					return attempt > 1, nil
				}
			} else {
				c.logger.Warnf("Failed to reload table during commit retry: %v", reloadErr)
			}
			continue
		} else if err != nil {
			// Non-retryable error: reload so the next call uses fresh metadata.
			// (Like every reload, this one cannot clear unresolvedUnknown —
			// only finding the commit-id token, which returns success, proves
			// anything about an ambiguous attempt.)
			if reloaded, reloadErr := c.reloadTable(ctx); reloadErr == nil {
				c.table = reloaded
			}
			c.incrCommitFailure()
			return attempt > 1, fmt.Errorf("committing transaction: %w", joinUnresolvedUnknown(err, unresolvedUnknown))
		}
		c.table = tbl
		return attempt > 1, nil
	}
	c.incrCommitFailure()
	return attempt > 1, fmt.Errorf("committing transaction after %d attempts: %w", attempt, joinUnresolvedUnknown(commitErr, unresolvedUnknown))
}

// joinUnresolvedUnknown attaches an unresolved ambiguous commit outcome — an
// earlier attempt whose failure was not a definitive rejection and whose
// possible landing was never observed (see commitLocked's ambiguity
// stickiness) — to err, so that errors.Is on the result reports the unknown
// state no matter which attempt's error terminated the retry loop. When there
// is no unresolved ambiguity, or err already carries the unknown-state
// sentinel, err is returned unchanged, preserving the existing error message
// exactly.
func joinUnresolvedUnknown(err, unresolved error) error {
	if unresolved == nil || errors.Is(err, rest.ErrCommitStateUnknown) {
		return err
	}
	return errors.Join(err, unresolved)
}

// isDefinitiveCommitRejection reports whether a txn.Commit error is a
// definitive server-side verdict that the commit did NOT apply. Only such
// errors may skip commitLocked's ambiguity marker: the destructive follow-up
// work gated on that marker (commitOverwrite's orphan cleanup) requires proof
// of non-application, and absence of proof of application is not that.
//
// The allowlist mirrors iceberg-go's rest catalog error mapping (catalog/rest
// handleNon200 plus updateTable's per-status overrides):
//   - table.ErrCommitFailed — the clean-conflict verdict. It covers
//     rest.ErrCommitFailed (a 409, which wraps it) and the client-side
//     conflict-validation sentinels, all of which mean the commit was
//     rejected without applying.
//   - table.ErrCommitDiverged — iceberg-go's client-side refresh-and-replay
//     (commit.retry.num-retries > 0) concluding the base snapshot left the
//     branch BEFORE any CommitTable call: nothing was sent, so
//     non-application is proven. It deliberately does not wrap
//     ErrCommitFailed, hence its own entry.
//   - rest.ErrBadRequest (400), catalog.ErrNoSuchTable (404, updateTable's
//     override), rest.ErrUnauthorized (401), rest.ErrForbidden (403) and
//     rest.ErrAuthorizationExpired (419) — 4xx verdicts: the server evaluated
//     the request and refused it. 400 notably includes engine catalogs'
//     prohibited-property rejections, whose learn-strip-retry flow depends on
//     the rejection being definitive.
//
// Axiom worth stating plainly: classifying a 409 as definitive assumes a
// catalog never APPLIES a commit and then reports it as a conflict. The retry
// machinery deliberately does not share that assumption — the in-loop
// commit-id check treats a landed-but-reported-failed commit as success on
// reload (see the commitLandThenFail test outcome) — because for DEDUPE the
// cost of being wrong is a duplicate, cheap insurance. For CLEANUP the cost
// of the same wrongness would be deleting a landed snapshot's files, so this
// classification leans on the REST spec instead: a spec-compliant catalog's
// 409 is evaluated-and-refused, nothing applied. A catalog that violates
// that, combined with reloads failing across the whole retry loop (hiding
// the landed token), is the one residual shape where cleanup could remove a
// landed file — accepted, and worth remembering if a catalog is ever caught
// applying-then-409ing in the wild.
//   - A prohibited-keys rejection recognised by text
//     (parseProhibitedPropertyKeys) — that text is only ever authored by a
//     catalog that evaluated and refused the request (in the wild it arrives
//     as the 400 above; test doubles and non-REST wrappers may surface the
//     bare message). The explicit ambiguous-class check below keeps a 5xx
//     body that happens to mention prohibited keys out of this clause.
//
// Everything else is NOT definitive: the rest catalog's explicit ambiguity
// classes (ErrCommitStateUnknown for 500/502/503/504, ErrServerError /
// ErrServiceUnavailable for other 5xx), raw transport failures (url.Error,
// net.Error timeouts, context deadlines, io.EOF, connection resets), and any
// unrecognised error. Notably, bare rest.ErrRESTError is excluded even though
// a 422 maps to it: it is also the wrapper for undecodable error responses of
// any status, so it cannot prove non-application.
func isDefinitiveCommitRejection(err error) bool {
	if err == nil {
		return false
	}
	// Ambiguous classes first: these may have applied server-side, whatever
	// else the error chain or message carries.
	if errors.Is(err, rest.ErrCommitStateUnknown) ||
		errors.Is(err, rest.ErrServerError) ||
		errors.Is(err, rest.ErrServiceUnavailable) {
		return false
	}
	if errors.Is(err, table.ErrCommitFailed) ||
		errors.Is(err, table.ErrCommitDiverged) ||
		errors.Is(err, rest.ErrBadRequest) ||
		errors.Is(err, rest.ErrUnauthorized) ||
		errors.Is(err, rest.ErrForbidden) ||
		errors.Is(err, rest.ErrAuthorizationExpired) ||
		errors.Is(err, catalog.ErrNoSuchTable) {
		return true
	}
	return len(parseProhibitedPropertyKeys(err)) > 0
}

// noteProhibitedKeys inspects a failed commit's error for a catalog
// prohibited-table-property rejection and updates the stripper accordingly.
// It returns retry=true when at least one new (non-reserved) key was learned —
// the caller should count the attempt and re-stage, letting the stripper
// filter the keys on the next commit. Only keys the failed commit actually
// sent (in its set-properties updates, tracked by the stripper) can be
// learned: a named key we never sent cannot be the cause of THIS rejection,
// so learning it would let arbitrary error text poison the strip set — it is
// logged at debug and skipped instead. It returns a non-nil fatalErr when the
// catalog named a key under reservedTablePropertyPrefix (matched
// case-insensitively): those keys carry connector semantics (the commit-id
// idempotency token, the timestamp-encoding pin) that stripping would
// silently break, so the commit must fail loudly instead. Both zero values
// mean the error is not a prohibited-keys rejection — or it names only keys
// that are already being stripped or were never sent, in which case retrying
// would loop futilely — and the caller's standard error handling applies.
func (c *committer) noteProhibitedKeys(attempt int, err error) (retry bool, fatalErr error) {
	keys := parseProhibitedPropertyKeys(err)
	if len(keys) == 0 {
		return false, nil
	}
	var learned, reserved []string
	for _, k := range keys {
		switch {
		case hasReservedPrefix(k):
			reserved = append(reserved, k)
		case !c.stripper.sentPropertyKey(k):
			c.logger.Debugf("Catalog rejection named prohibited table property %q, which this commit never set; not learning it", k)
		case c.stripper.addProhibitedKey(k):
			learned = append(learned, k)
		}
	}
	if len(reserved) > 0 {
		return false, fmt.Errorf(
			"catalog prohibits table properties %v, which this connector depends on (%s* keys pin semantics such as the table's timestamp encoding) and refuses to strip: %w",
			reserved, reservedTablePropertyPrefix, err)
	}
	if len(learned) == 0 {
		return false, nil
	}
	// One-time warning per key: addProhibitedKey only reports a key the first
	// time it is learned.
	for _, k := range learned {
		c.logger.Warnf("Catalog prohibits table property %q; stripping it from commits — safe because our data files carry Iceberg field IDs, so the property only duplicates optional metadata (e.g. the name-mapping fallback for ID-less files) that readers of this table never need", k)
	}
	c.logger.Warnf("Commit attempt %d/%d rejected for prohibited table properties %v; retrying with them stripped", attempt, c.cfg.MaxRetries, learned)
	return true, nil
}

// dropAlreadyCommitted returns the subset of files whose paths are not already
// referenced by the current snapshot of c.table, which the caller must have just
// reloaded. It exists because commit retries re-add the same DataFile objects
// (identical paths) after a reload: if a prior attempt actually landed
// server-side but reported failure, the reloaded snapshot already contains those
// files, and re-adding them with AddDataFiles' duplicate check disabled would
// register the same path in two snapshots (the "conflicting sequence numbers"
// corruption). The scan is O(current snapshot) but only runs on the rare retry
// path, keeping the first-attempt hot path free of the duplicate scan.
func (c *committer) dropAlreadyCommitted(ctx context.Context, files []iceberg.DataFile) ([]iceberg.DataFile, error) {
	snap := c.table.CurrentSnapshot()
	if snap == nil {
		return files, nil
	}
	fs, err := c.table.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolving table filesystem for duplicate check: %w", err)
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, fmt.Errorf("loading manifests for duplicate check: %w", err)
	}

	// Match against our candidate paths only, so the lookup set stays O(files)
	// regardless of table size.
	want := make(map[string]struct{}, len(files))
	for _, f := range files {
		want[f.FilePath()] = struct{}{}
	}
	committed := make(map[string]struct{}, len(files))
	for _, m := range manifests {
		if m.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		for entry, err := range m.Entries(fs, true) {
			if err != nil {
				return nil, fmt.Errorf("reading manifest entries for duplicate check: %w", err)
			}
			path := entry.DataFile().FilePath()
			if _, ok := want[path]; ok {
				committed[path] = struct{}{}
			}
		}
		if len(committed) == len(want) {
			break // every candidate already present; no need to scan further.
		}
	}

	if len(committed) == 0 {
		return files, nil
	}
	remaining := make([]iceberg.DataFile, 0, len(files)-len(committed))
	for _, f := range files {
		if _, ok := committed[f.FilePath()]; ok {
			c.logger.Warnf("Skipping re-add of data file already committed by a prior attempt: %s", f.FilePath())
			continue
		}
		remaining = append(remaining, f)
	}
	return remaining, nil
}

// committedSnapshotHasID reports whether any snapshot in c.table's current
// metadata carries commitID under commitIDProp in its summary. The caller must
// have just reloaded c.table. It backs the mutation paths' idempotent retry: a
// commit that failed or returned an ambiguous state may still have landed
// server-side, and finding our (UUID) commit-id in a reloaded snapshot proves it
// did — so the retry returns success instead of applying the mutation twice.
//
// Snapshots are scanned newest-first (Metadata().Snapshots() is oldest-first, so
// we walk it in reverse) because a just-landed commit is the most recent. The
// scan stops once it reaches stopAtSnapshotID — the snapshot that was current
// when the commit began — since any snapshot our attempt created is strictly
// newer than that. commitIDs are unique per call, so only a snapshot our own
// attempt produced can match.
func (c *committer) committedSnapshotHasID(commitID string, stopAtSnapshotID int64) bool {
	snaps := c.table.Metadata().Snapshots()
	for _, s := range slices.Backward(snaps) {
		if s.SnapshotID == stopAtSnapshotID {
			break
		}
		if s.Summary != nil && s.Summary.Properties[commitIDProp] == commitID {
			return true
		}
	}
	return false
}

func (c *committer) incrCommitFailure() {
	c.metrics.incrCommitFailure()
}

// currentSchemaID returns the table's current schema ID.
func (c *committer) currentSchemaID() int {
	return c.table.Schema().ID
}

// Close shuts down the committer and waits for pending commits.
func (c *committer) Close() {
	c.batcher.Close()
}
