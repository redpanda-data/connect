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
	"sync/atomic"
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
	// UnsupportedUpdateRetryInterval is how long a catalog's refusal of an
	// optional update (snapshot expiry) stands before the update is tried
	// again, and how often the failed-expiry warning may repeat. A verdict
	// rests on one 400 and one landed retry, strong evidence but not proof (a
	// non-conforming catalog could map a transient failure to 400), so it is
	// re-probed rather than permanent: a refusing catalog costs one wasted
	// round trip per interval, a mis-verdict heals within it. Zero or negative means the default of one hour.
	UnsupportedUpdateRetryInterval time.Duration
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
	// UnsupportedUpdates, when non-nil, is the shared record of optional
	// table updates (snapshot expiry) a catalog has proven it refuses, see
	// commitLocked's elimination trials. Owned by the router per tableEntry
	// for the same reason as ProhibitedKeys: a verdict must outlive writer
	// recreation or every generation pays the rejected trial round trips and
	// warnings again. Nil gets a fresh, private record.
	UnsupportedUpdates *unsupportedUpdates
}

// defaultUnsupportedUpdateRetryInterval backs
// CommitConfig.UnsupportedUpdateRetryInterval when it is unset.
const defaultUnsupportedUpdateRetryInterval = time.Hour

// defaultMaxSnapshotAge backs commit.max_snapshot_age when the commit block
// is absent; the field's spec Default must parse to the same value, which
// TestParseCommitConfigMaxSnapshotAge pins. It is Iceberg's own default for
// history.expire.max-snapshot-age-ms.
const defaultMaxSnapshotAge = 5 * 24 * time.Hour

// unsupportedUpdates records, per table, which optional updates the catalog
// has refused and until when that verdict stands. It also carries the rate
// limit of the warnings that accompany those updates, so writer recreation
// (a new committer generation) does not repeat them.
type unsupportedUpdates struct {
	expiryUntil atomic.Int64 // unix nanos; zero means not refused

	// expiryStageWarnAfter rate-limits the "could not stage expiry" warning
	// to once per UnsupportedUpdateRetryInterval per table. Not once-ever: the
	// cause (a ref left pointing at a removed snapshot, say) can be fixed
	// and recur, and retention silently stopping a second time must not be
	// silent.
	expiryStageWarnAfter atomic.Int64
	// tableAgeWarnOnce: the table-property-vs-config retention mismatch is
	// static for the life of the table, so it is logged once per table.
	tableAgeWarnOnce sync.Once
}

// warnExpiryStage reports whether the expiry-staging warning may be logged
// now, and if so arms the next window.
func (u *unsupportedUpdates) warnExpiryStage(every time.Duration) bool {
	return warnEvery(&u.expiryStageWarnAfter, every)
}

func warnEvery(after *atomic.Int64, every time.Duration) bool {
	now := time.Now().UnixNano()
	next := after.Load()
	if now < next {
		return false
	}
	return after.CompareAndSwap(next, now+int64(every))
}

func newUnsupportedUpdates() *unsupportedUpdates { return &unsupportedUpdates{} }

func refusedNow(until *atomic.Int64) bool {
	u := until.Load()
	return u != 0 && time.Now().UnixNano() < u
}

func refuse(until *atomic.Int64, d time.Duration) {
	until.Store(time.Now().Add(d).UnixNano())
}

func (u *unsupportedUpdates) refusesExpiry() bool          { return refusedNow(&u.expiryUntil) }
func (u *unsupportedUpdates) refuseExpiry(d time.Duration) { refuse(&u.expiryUntil, d) }

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
	// unsupported records which optional updates the catalog has refused:
	// snapshot expiry (remove-snapshots). Each is set once a catalog has
	// rejected a commit carrying the update and then accepted the identical
	// commit without it (see commitLocked's elimination trials). Shared
	// across committer generations via CommitConfig.UnsupportedUpdates.
	unsupported *unsupportedUpdates
	metrics     *opMetrics
	logger      *service.Logger
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
	// Defensively clamp MaxRetries to at least 1: commitLocked makes at most
	// cfg.MaxRetries attempts, so a zero or negative value would never run a
	// single attempt and return a "committing transaction after 0 attempts"
	// error wrapping a nil cause. Config lint rejects it at startup (config.go),
	// but callers constructing a committer directly get the same safety here.
	if cfg.MaxRetries < 1 {
		cfg.MaxRetries = 1
	}
	// Same shape for the retry interval: config lint rejects a non-positive
	// value at startup, so this only covers callers constructing a committer
	// directly (tests), where the zero value should mean "the default".
	if cfg.UnsupportedUpdateRetryInterval <= 0 {
		cfg.UnsupportedUpdateRetryInterval = defaultUnsupportedUpdateRetryInterval
	}
	stripper := newPropertyStrippingCatalog(cat, cfg.ProhibitedKeys)
	writes := newWriteRecorder()
	unsupported := cfg.UnsupportedUpdates
	if unsupported == nil {
		unsupported = newUnsupportedUpdates()
	}
	c := &committer{
		table:       rebindTableRecording(tbl, stripper, writes),
		cfg:         cfg,
		stripper:    stripper,
		writes:      writes,
		unsupported: unsupported,
		logger:      logger,
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
		//
		// An EXPLICIT conflicting value is an error, never a silent override:
		// the property is table-level state other engines' delete jobs read,
		// so overriding a deliberate merge-on-read setting would flip their
		// materialisation behind the operator's back — while honouring it
		// would make the delete-only path write delete files, breaking the
		// zero-delete-file guarantee this strategy exists for. Only the
		// property-absent case is stamped.
		switch mode := c.table.Properties()[table.WriteDeleteModeKey]; mode {
		case table.WriteModeCopyOnWrite:
			// Already pinned; nothing to stage.
		case "":
			if err := txn.SetProperties(iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeCopyOnWrite}); err != nil {
				return fmt.Errorf("setting %s: %w", table.WriteDeleteModeKey, err)
			}
		default:
			return fmt.Errorf(
				"table property %s is explicitly set to %q, which conflicts with merge_strategy: copy-on-write; "+
					"the connector will not silently override a table-level setting other engines read — "+
					"unset the property (or set it to %q) to use copy-on-write, or switch this output to merge_strategy: merge-on-read",
				table.WriteDeleteModeKey, mode, table.WriteModeCopyOnWrite)
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
//     prior attempt that landed and short-circuit to success. Every commit path
//     passes one — the mutation paths (copy-on-write Overwrite/Delete and
//     merge-on-read RowDelta) rely on it exclusively, since their stage
//     callbacks are not path-idempotent on their own.
//   - stage dropping files the reloaded snapshot already references (the append
//     path's dropAlreadyCommitted). The append path carries BOTH mechanisms:
//     the token survives an external rewrite compacting the landed files away,
//     which the path-keyed check cannot see.
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
	// props are the SNAPSHOT summary properties every stage callback passes
	// to iceberg-go. The snapshot-retention keys do not belong here:
	// iceberg-go reads them from TABLE metadata properties and never from a
	// snapshot summary, so placing them here is inert. Retention is instead
	// applied directly via Transaction.ExpireSnapshots inside the retry loop
	// below.
	props := iceberg.Properties{
		table.ManifestMergeEnabledKey: strconv.FormatBool(c.cfg.ManifestMergeEnabled),
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
	// Optional-extras elimination. An optional update rides along with the
	// data that a catalog may refuse wholesale with a 400: the
	// remove-snapshots update that snapshot expiry stages. That refusal would
	// otherwise wedge every write to the table. So a 400 with an extra staged
	// is answered by re-staging the same commit with that extra dropped
	// (trial 1 drops expiry). The trial attempt landing proves the
	// catalog refuses the extra just dropped, and the matching refusal is
	// recorded (for UnsupportedUpdateRetryInterval) with a warning. A trial that hits a clean 409 keeps going: the
	// extra stays dropped, and a later clean landing in the same call still
	// records the verdict (see noteTrialLanded). An ambiguous response or a
	// repeat 400 proves nothing: the extra stays dropped for the rest of
	// this call so the data can still get through, no flag is set, and the
	// next call starts over.
	//
	// This leans on 400 being deterministic for a given request body, which
	// is what the status means: if the extra staged nothing new (expiry with
	// nothing eligible), the trial's body is identical to the rejected one
	// and fails identically, so no flag is set. Trials do not consume the
	// configured retry budget: each one extends it by one attempt, so
	// max_retries: 1 still gets its data committed on a refusing catalog.
	var (
		dropExpiry   bool
		trial        int  // 0 none, 1 expiry dropped
		trialAttempt int  // the attempt index that runs the current trial
		trialTainted bool // an ambiguous outcome since the trial began
		trialErr     error
	)
	// noteTrialLanded records the verdict when an attempt lands cleanly at or
	// after the trial attempt and every outcome since the trial began was
	// definitive. A clean 409 in between proves nothing either way: the extra
	// stays dropped for the rest of the call, so the attempt that finally
	// lands sends the trial's request body, merely rebased. Without this a
	// busy multi-writer table on a refusing catalog would pay the rejected
	// round trip on every commit and never record the verdict. An ambiguous
	// outcome since the trial taints it: that commit may have landed
	// server-side, and the ambiguous-landed path (commit-id token found after
	// a reload) cannot tell WHICH attempt landed, so it never calls this and
	// the next call simply tries again.
	noteTrialLanded := func(attempt int) {
		if trial != 1 || attempt < trialAttempt || trialTainted {
			return
		}
		c.unsupported.refuseExpiry(c.cfg.UnsupportedUpdateRetryInterval)
		c.logger.Warnf("Catalog rejected a commit carrying snapshot expiry but accepted it without; pausing %s for this table for %s and leaving snapshot retention to external maintenance meanwhile. If this catalog does support snapshot expiry the rejection was transient and expiry resumes at the next probe. Rejection was: %v", ioFieldMaxSnapshotAge, c.cfg.UnsupportedUpdateRetryInterval, trialErr)
	}
	// budget is the number of attempts this call may make: cfg.MaxRetries plus
	// one per learn-and-strip retry and one per elimination trial.
	budget := c.cfg.MaxRetries
	attempt := 0
	reloaded := false
	for attempt < budget {
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
			// Stage failures ARE commit failures for observability: a
			// deterministic stage error (e.g. a config-vs-table conflict)
			// stalls the pipeline exactly like a rejected commit, and a
			// dashboard watching the commit-failure metric must see it.
			c.incrCommitFailure()
			return attempt > 1, joinUnresolvedUnknown(err, unresolvedUnknown)
		}
		// Snapshot retention, after the data so the new snapshot is the head;
		// rationale in stageSnapshotExpiry. expiryStaged records that expiry
		// was requested. Whether it changed anything is checked only if a 400
		// arrives (expiryChangedHistory), so the steady state pays no extra
		// metadata build.
		expiryStaged := false
		if c.cfg.MaxSnapshotAge > 0 {
			c.warnTableAgeMismatch()
			if !dropExpiry && !c.unsupported.refusesExpiry() && c.expiryHasWork() {
				expiryStaged = c.stageSnapshotExpiry(txn)
			}
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
			if trial != 0 {
				trialTainted = true
			}
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
				// With max_retries: 1 the stripped commit would otherwise never
				// run, and a catalog that names a key as prohibited would wedge
				// the table. It cannot loop: noteProhibitedKeys asks for a retry
				// only when it learns a key the commit sent, and the commit
				// sends a finite set of keys.
				commitErr = err
				budget++
				// The stripped body differs from a running trial's, so a
				// later landing cannot be pinned on the extra the trial
				// dropped.
				if trial != 0 {
					trialTainted = true
				}
				continue
			}
			// Optional-extras elimination (see trial above). Only a 400
			// qualifies: a 409 is an ordinary conflict for the reload branch
			// below, and auth/not-found verdicts are never about an extra.
			// Nothing landed (the rejection is definitive), so re-staging
			// from the same base without a reload is safe, exactly as for
			// the prohibited-keys retry. A table with nothing eligible, or
			// whose old snapshots are all pinned by refs, carries no expiry
			// update even though one was requested, so a 400 on it is never
			// blamed on expiry and costs no trial (expiryChangedHistory).
			if errors.Is(err, rest.ErrBadRequest) && trial == 0 && expiryStaged && c.expiryChangedHistory(txn) {
				dropExpiry = true
				trial, trialAttempt, trialTainted, trialErr, commitErr = 1, attempt+1, false, err, err
				budget++
				c.logger.Infof("Commit attempt %d rejected with optional updates staged; retrying without snapshot expiry to rule it out: %v", attempt, err)
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
			c.logger.Warnf("Commit attempt %d failed: %v", attempt, err)
			if reloadedTbl, reloadErr := c.reloadTable(ctx); reloadErr == nil {
				c.table = reloadedTbl
				reloaded = true
				// A trial's landing is evidence only if the extra it dropped
				// would still be staged on this new base. Another writer may
				// have expired the eligible snapshots itself, and landing
				// without expiry then proves nothing about the catalog.
				if trial == 1 && !c.expiryHasWork() {
					trialTainted = true
				}
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
		noteTrialLanded(attempt)
		return attempt > 1, nil
	}
	c.incrCommitFailure()
	return attempt > 1, fmt.Errorf("committing transaction after %d attempts: %w", attempt, joinUnresolvedUnknown(commitErr, unresolvedUnknown))
}

// expiryHasWork is the cheap pre-check for stageSnapshotExpiry: it reports
// whether the table's current metadata holds anything ExpireSnapshots could
// act on. It mirrors iceberg-go's retention chain: a branch's own
// max-snapshot-age-ms wins over the age in force (effectiveSnapshotAgeMs),
// and a ref's own max-ref-age-ms falls back to the table-level
// history.expire.max-ref-age-ms property. So it looks for a snapshot older
// than the shortest snapshot age in force on any branch, or a non-main ref
// older than its ref age. The head of a retained tag or branch is always
// kept, so an old snapshot that only a ref pins is not work: otherwise every
// commit to such a table would run the walk and assert every ref's position
// for nothing, and conflict with any concurrent ref mover. Main's head is
// not exempt, since the data commit moves it and it becomes an ordinary
// ancestor. These are necessary conditions for iceberg-go to stage an
// update, so when this is false the walk is skipped and the commit is known
// to carry no expiry update. It is conservative the other way: a snapshot
// older than a branch's age may not be on that branch, or
// min-snapshots-to-keep may retain it, so the walk can still stage nothing;
// expiryChangedHistory settles that if a 400 arrives.
func (c *committer) expiryHasWork() bool {
	meta := c.table.Metadata()
	nowMs := time.Now().UnixMilli()
	propMaxRefAgeMs, _ := lookupRetention(meta.Properties(), table.MaxRefAgeMsKey, "max-ref-age-ms", table.MaxRefAgeMsDefault)
	minSnapshotAgeMs, _ := c.effectiveSnapshotAgeMs()
	pinned := map[int64]struct{}{}
	for name, ref := range meta.Refs() {
		if ref.SnapshotRefType == table.BranchRef && ref.MaxSnapshotAgeMs != nil && *ref.MaxSnapshotAgeMs < minSnapshotAgeMs {
			minSnapshotAgeMs = *ref.MaxSnapshotAgeMs
		}
		if name == table.MainBranch {
			continue
		}
		pinned[ref.SnapshotID] = struct{}{}
		maxRefAgeMs := propMaxRefAgeMs
		if ref.MaxRefAgeMs != nil {
			maxRefAgeMs = *ref.MaxRefAgeMs
		}
		if snap := meta.SnapshotByID(ref.SnapshotID); snap != nil && nowMs-snap.TimestampMs > maxRefAgeMs {
			return true
		}
	}
	cutoff := nowMs - minSnapshotAgeMs
	for _, s := range meta.Snapshots() {
		if _, ok := pinned[s.SnapshotID]; ok {
			continue
		}
		if s.TimestampMs < cutoff {
			return true
		}
	}
	return false
}

// warnTableAgeMismatch logs, once per table, that the table's own retention
// property differs from the configured age and wins. It runs on every commit
// with retention enabled, not only when expiry has work, so the operator
// learns their setting is ignored even on a table where nothing is eligible
// yet.
func (c *committer) warnTableAgeMismatch() {
	tableMs, fromTable := c.effectiveSnapshotAgeMs()
	if !fromTable || tableMs == c.cfg.MaxSnapshotAge.Milliseconds() {
		return
	}
	c.unsupported.tableAgeWarnOnce.Do(func() {
		c.logger.Warnf("Table property %s is %dms and takes precedence over %s (%s) for this table", table.MaxSnapshotAgeMsKey, tableMs, ioFieldMaxSnapshotAge, c.cfg.MaxSnapshotAge)
	})
}

// effectiveSnapshotAgeMs is the snapshot age in force for this table: its
// own history.expire.max-snapshot-age-ms (or the legacy key) when it carries
// one, else the configured max_snapshot_age. The bool reports the former.
// A table-level property is an operator's explicit choice that other engines
// honour too, so it wins over the field, exactly as the table's
// commit.manifest-merge.enabled wins over manifest_merge_enabled.
func (c *committer) effectiveSnapshotAgeMs() (int64, bool) {
	if ms, ok := lookupRetention(c.table.Metadata().Properties(), table.MaxSnapshotAgeMsKey, "max-snapshot-age-ms", table.MaxSnapshotAgeMsDefault); ok {
		return ms, true
	}
	return c.cfg.MaxSnapshotAge.Milliseconds(), false
}

// lookupRetention reads a retention property exactly as iceberg-go's
// unexported getRetentionInt64 does: the standard key when present (an
// unparsable value yields the default), else the legacy unprefixed key that
// tables written by older Go versions carry, else the default. The bool
// reports whether either key was present.
func lookupRetention(props iceberg.Properties, key, legacyKey string, def int64) (int64, bool) {
	if _, ok := props[key]; ok {
		return props.GetInt64(key, def), true
	}
	if _, ok := props[legacyKey]; ok {
		return props.GetInt64(legacyKey, def), true
	}
	return def, false
}

// expiryChangedHistory reports whether the expiry staged on txn changes the
// table's history: it removes a base snapshot, or a base ref whose
// max-ref-age-ms has elapsed. It decides whether a 400 can be blamed on
// expiry at all, so it runs only on that path. It builds the staged
// metadata, which is linear in snapshots but small next to the full metadata
// JSON every REST commit serialises anyway. A failed Commit leaves the staged
// updates in place and StagedTable does not consult the transaction's
// committed flag, so it is safe to call after the rejection. An error
// building the staged metadata is answered conservatively with true: the
// trial then runs and, if expiry was innocent, fails identically and records
// nothing.
func (c *committer) expiryChangedHistory(txn *table.Transaction) bool {
	staged, err := txn.StagedTable()
	if err != nil {
		return true
	}
	stagedMeta := staged.Metadata()
	remaining := map[int64]struct{}{}
	for _, s := range stagedMeta.Snapshots() {
		remaining[s.SnapshotID] = struct{}{}
	}
	base := c.table.Metadata()
	for _, s := range base.Snapshots() {
		if _, ok := remaining[s.SnapshotID]; !ok {
			return true
		}
	}
	stagedRefs := map[string]struct{}{}
	for name := range stagedMeta.Refs() {
		stagedRefs[name] = struct{}{}
	}
	for name := range base.Refs() {
		if _, ok := stagedRefs[name]; !ok {
			return true
		}
	}
	return false
}

// stageSnapshotExpiry makes commit.max_snapshot_age take effect by staging
// Transaction.ExpireSnapshots on the data commit's own transaction: one
// catalog round trip, and the table is never observable with the new
// snapshot but without the expiry (or vice versa). A table that carries its
// own history.expire.max-snapshot-age-ms keeps that retention: no age is
// passed and iceberg-go reads the property (effectiveSnapshotAgeMs). Otherwise
// the configured age goes in as WithOlderThan. No table property is written
// either way. WithPostCommit(false) leaves the expired snapshots' files on
// storage for orphan-file maintenance: deleting them inline would race
// readers mid-scan on a streaming table, and this output must never delete
// a file a reader may hold. Like Iceberg's expire_snapshots procedure the
// expiry is table-wide; refs with their own retention properties keep them.
// iceberg-go also asserts EVERY ref's position for the commit, not just the
// branch being written, so on a table where another writer moves branches
// or tags concurrently an expiry-carrying commit can 409 and go through the
// ordinary reload-retry.
//
// It must run AFTER the data is staged so the new snapshot is the branch
// head the retention walk counts from. iceberg-go only stages a
// remove-snapshots update when something is actually eligible.
//
// A local staging error (metadata iceberg-go cannot walk, e.g. a ref left
// pointing at a snapshot another engine already removed) must not take the
// data commit down with it: retention is a maintenance concern, delivery is
// not. It is logged at most once per UnsupportedUpdateRetryInterval per
// table and the commit proceeds without expiry. The bool reports whether
// expiry was requested on txn.
func (c *committer) stageSnapshotExpiry(txn *table.Transaction) bool {
	opts := []table.ExpireSnapshotsOpt{table.WithPostCommit(false)}
	if _, fromTable := c.effectiveSnapshotAgeMs(); !fromTable {
		opts = append(opts, table.WithOlderThan(c.cfg.MaxSnapshotAge))
	}
	if err := txn.ExpireSnapshots(opts...); err != nil {
		if c.unsupported.warnExpiryStage(c.cfg.UnsupportedUpdateRetryInterval) {
			c.logger.Warnf("Could not stage snapshot expiry for this table; committing without it and leaving snapshot retention to external maintenance until the cause is fixed: %v", err)
		}
		return false
	}
	return true
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
// the caller should re-stage, letting the stripper filter the keys on the next
// commit. Only keys the failed commit actually sent (in its set-properties
// updates, tracked by the stripper) can be learned: a named key we never sent
// cannot be the cause of THIS rejection,
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
	c.logger.Warnf("Commit attempt %d rejected for prohibited table properties %v; retrying with them stripped", attempt, learned)
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
// newer than that. Snapshot expiry (ours or another writer's) may have removed
// that marker meanwhile; the scan then runs to the oldest snapshot, which only
// costs time. commitIDs are unique per call, so only a snapshot our own attempt
// produced can match.
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
