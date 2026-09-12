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
	"io/fs"
	"strings"
	"sync"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// writeRecorder is a mutex-guarded set of the .parquet paths written through a
// committer's recording filesystem (see newRecordingIO). It is the foundation
// of copy-on-write orphan cleanup's safety argument: because only files THIS
// committer authored are ever recorded, cleanup restricted to the recorded set
// can never touch a concurrent writer's in-flight files — by construction,
// rather than by any listing/diffing heuristic.
//
// Only .parquet paths are recorded: cleanup only ever reclaims data files
// (manifests and metadata of failed commits are left to Iceberg orphan-file
// maintenance, as before), and the filter keeps the set from growing without
// bound on committers that never run copy-on-write — the append and row-delta
// commit paths write only manifest/metadata files through the committer's
// filesystem (their parquet is written by the writer, through its own table
// handle), so they record nothing.
type writeRecorder struct {
	mu    sync.Mutex
	paths map[string]struct{}
}

func newWriteRecorder() *writeRecorder {
	return &writeRecorder{paths: map[string]struct{}{}}
}

// record notes a path created through the recording filesystem. Non-parquet
// paths are ignored (see the type comment). Recording happens at Create/
// WriteFile time regardless of whether the write later fails: a partially
// written file is still ours to clean up, and removing a path that never
// materialised is a harmless no-op.
func (r *writeRecorder) record(name string) {
	if !strings.HasSuffix(name, ".parquet") {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.paths[name] = struct{}{}
}

// reset clears the set. commitOverwrite calls it on entry (under commitMu,
// which serializes all commits) so the set holds exactly the files staged by
// the current copy-on-write call.
func (r *writeRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.paths = map[string]struct{}{}
}

// snapshot returns a copy of the current set.
func (r *writeRecorder) snapshot() map[string]struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]struct{}, len(r.paths))
	for p := range r.paths {
		out[p] = struct{}{}
	}
	return out
}

// recordingFSysF wraps a table's filesystem factory so every IO it yields
// records created paths into rec before delegating. It is installed on every
// table handle the committer retains (see rebindTableRecording), which is how
// the committer knows exactly which data files its own copy-on-write stage
// attempts wrote.
func recordingFSysF(inner table.FSysF, rec *writeRecorder) table.FSysF {
	return func(ctx context.Context) (iceio.IO, error) {
		fsys, err := inner(ctx)
		if err != nil {
			return nil, err
		}
		return newRecordingIO(fsys, rec), nil
	}
}

// rebindTableRecording is rebindTable plus write recording: the returned table
// is bound to cat for commits and to a recording wrapper of tbl's filesystem
// for I/O, so every data file written through it (iceberg-go's transaction
// paths resolve their WriteFileIO from the table's FSysF) lands in rec.
func rebindTableRecording(tbl *table.Table, cat table.CatalogIO, rec *writeRecorder) *table.Table {
	return table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), recordingFSysF(tbl.FS, rec), cat)
}

// newRecordingIO wraps inner so file creations are recorded into rec, while
// preserving the optional-interface surface iceberg-go probes for at runtime.
//
// Interface fidelity: iceberg-go's write paths REQUIRE iceio.WriteFileIO (the
// transaction asserts it unconditionally), and its read paths during a
// copy-on-write rewrite go through iceio.IO's Open. The optional interfaces a
// registered production IO actually implements are ListableIO and
// BulkRemovableIO (gocloud's blobFileIO implements both; LocalFS implements
// ListableIO); neither implements ReadFileIO, and the one ReadFileIO probe in
// iceberg-go falls back to Open when the interface is absent, so it is not
// forwarded here. The wrapper therefore comes in three shapes — WriteFileIO,
// +ListableIO, +ListableIO+BulkRemovableIO — chosen to match what inner
// satisfies, and never advertises an interface the underlying IO lacks.
//
// An inner IO without WriteFileIO is returned unwrapped: nothing can be
// written (or recorded) through it, and iceberg-go's commit paths would reject
// it themselves.
func newRecordingIO(inner iceio.IO, rec *writeRecorder) iceio.IO {
	w, ok := inner.(iceio.WriteFileIO)
	if !ok {
		return inner
	}
	base := recordingFS{inner: w, rec: rec}
	lister, ok := inner.(iceio.ListableIO)
	if !ok {
		return base
	}
	lfs := recordingListableFS{recordingFS: base, lister: lister}
	bulk, ok := inner.(iceio.BulkRemovableIO)
	if !ok {
		return lfs
	}
	return recordingListableBulkFS{recordingListableFS: lfs, bulk: bulk}
}

// recordingFS implements iceio.IO + iceio.WriteFileIO by delegation, recording
// every created path.
type recordingFS struct {
	inner iceio.WriteFileIO
	rec   *writeRecorder
}

func (f recordingFS) Open(name string) (iceio.File, error) { return f.inner.Open(name) }
func (f recordingFS) Remove(name string) error             { return f.inner.Remove(name) }

func (f recordingFS) Create(name string) (iceio.FileWriter, error) {
	f.rec.record(name)
	return f.inner.Create(name)
}

func (f recordingFS) WriteFile(name string, p []byte) error {
	f.rec.record(name)
	return f.inner.WriteFile(name, p)
}

// recordingListableFS additionally forwards iceio.ListableIO.
type recordingListableFS struct {
	recordingFS
	lister iceio.ListableIO
}

func (f recordingListableFS) WalkDir(root string, fn fs.WalkDirFunc) error {
	return f.lister.WalkDir(root, fn)
}

// recordingListableBulkFS additionally forwards iceio.BulkRemovableIO.
type recordingListableBulkFS struct {
	recordingListableFS
	bulk iceio.BulkRemovableIO
}

func (f recordingListableBulkFS) DeleteFiles(ctx context.Context, paths []string) ([]string, error) {
	return f.bulk.DeleteFiles(ctx, paths)
}
