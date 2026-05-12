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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// memCatalog is a minimal in-memory table.CatalogIO used to drive transaction
// commits without standing up a real catalog. It mirrors the pattern used in
// iceberg-go's own table_test.go.
type memCatalog struct {
	meta             table.Metadata
	metadataLocation string
	ident            table.Identifier
	location         string
}

func (m *memCatalog) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	return m.snapshot(), nil
}

func (m *memCatalog) CommitTable(_ context.Context, _ table.Identifier, _ []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	meta, err := table.UpdateTableMetadata(m.meta, updates, "")
	if err != nil {
		return nil, "", err
	}
	m.meta = meta
	return meta, m.metadataLocation, nil
}

func (m *memCatalog) snapshot() *table.Table {
	return table.New(
		m.ident,
		m.meta,
		m.metadataLocation,
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		m,
	)
}

// newTestTable creates an iceberg table backed by an in-memory catalog and the
// local filesystem under tb.TempDir().
func newTestTable(tb testing.TB) (*table.Table, *memCatalog) {
	tb.Helper()
	location := filepath.ToSlash(tb.TempDir())

	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
	)

	meta, err := table.NewMetadata(sc, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
		location, iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(tb, err)

	cat := &memCatalog{
		meta:             meta,
		metadataLocation: fmt.Sprintf("%s/metadata/00001-%s.metadata.json", location, uuid.New()),
		ident:            table.Identifier{"default", "t"},
		location:         location,
	}
	return cat.snapshot(), cat
}

// synthDataFile builds a DataFile referencing path. The path need not exist on
// disk — AddDataFiles validates metadata only.
func synthDataFile(tb testing.TB, spec iceberg.PartitionSpec, path string) iceberg.DataFile {
	tb.Helper()
	b, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		path,
		iceberg.ParquetFile,
		nil, nil, nil,
		1, 256,
	)
	require.NoError(tb, err)
	return b.Build()
}

// seedTable commits `count` separate single-file snapshots, approximating a
// table that has accumulated many manifests from many small writes. The
// committed paths each carry a fresh uuid, so they do not collide with any
// later "real" commit by the unit under test. Returns the latest table handle.
func seedTable(tb testing.TB, ctx context.Context, tbl *table.Table, count int) *table.Table {
	tb.Helper()
	for range count {
		df := synthDataFile(tb, tbl.Spec(),
			fmt.Sprintf("%s/data/seed-%s.parquet", tbl.Location(), uuid.New()))
		tx := tbl.NewTransaction()
		require.NoError(tb, tx.AddDataFiles(ctx, []iceberg.DataFile{df}, nil,
			table.WithoutAutoNameMapping(), table.WithoutDuplicateCheck()))
		next, err := tx.Commit(ctx)
		require.NoError(tb, err)
		tbl = next
	}
	return tbl
}

// TestCommitterSkipsDuplicateCheck pins the T6692 fix: the committer must
// bypass iceberg-go's default duplicate-path check on AddDataFiles. That check
// is an O(snapshot) scan of every manifest entry and dominated the commit hot
// path for Comcast. The writer already guarantees per-file path uniqueness via
// uuid stamping (writer.go), so the check is redundant.
//
// Behavioural assertion: committing a path that already exists in the table
// succeeds. Without the fix, AddDataFiles returns
// "cannot add files that are already referenced by table" and Commit fails.
func TestCommitterSkipsDuplicateCheck(t *testing.T) {
	ctx := t.Context()
	tbl, cat := newTestTable(t)

	tbl = seedTable(t, ctx, tbl, 1)

	logger := service.MockResources().Logger()
	c, err := NewCommitter(tbl, CommitConfig{
		ManifestMergeEnabled: false,
		MaxRetries:           1,
	}, func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, logger)
	require.NoError(t, err)
	defer c.Close()

	dupPath := fmt.Sprintf("%s/data/collision.parquet", tbl.Location())
	df1 := synthDataFile(t, tbl.Spec(), dupPath)
	require.NoError(t, c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df1}, SchemaID: c.currentSchemaID()}))

	// Second commit at the same path: only succeeds if the dup check is off.
	df2 := synthDataFile(t, tbl.Spec(), dupPath)
	require.NoError(t, c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df2}, SchemaID: c.currentSchemaID()}))
}

// BenchmarkAddDataFilesDupCheck measures the cost of Transaction.AddDataFiles
// against a snapshot pre-seeded with N existing data files, with iceberg-go's
// duplicate-path check on vs off. It is the local reproduction harness for
// T6692.
//
// Run locally with e.g.:
//
//	go test -bench BenchmarkAddDataFilesDupCheck -benchmem -benchtime=20x \
//	    ./internal/impl/iceberg
//
// The `dup_check=true` variants scale roughly linearly with seed; the
// `dup_check=false` variants are flat. The committer in this package ships
// with dup_check off (see committer.go doCommit) — the `dup_check=true`
// numbers are what production looked like before T6692.
func BenchmarkAddDataFilesDupCheck(b *testing.B) {
	ctx := b.Context()

	for _, seed := range []int{10, 100, 1000} {
		// Build the seeded table once per `seed` size and reuse it across the
		// inner sub-benchmarks. Seeding 1000 commits is the dominant cost, and
		// the AddDataFiles call we measure operates on the parent snapshot
		// without mutating it — so sharing is safe.
		tbl, _ := newTestTable(b)
		tbl = seedTable(b, ctx, tbl, seed)

		for _, dupCheck := range []bool{true, false} {
			opts := []table.WriteOption{table.WithoutAutoNameMapping()}
			if !dupCheck {
				opts = append(opts, table.WithoutDuplicateCheck())
			}

			b.Run(fmt.Sprintf("seed=%d/dup_check=%v", seed, dupCheck), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; b.Loop(); i++ {
					df := synthDataFile(b, tbl.Spec(),
						fmt.Sprintf("%s/data/bench-%d.parquet", tbl.Location(), i))
					tx := tbl.NewTransaction()
					if err := tx.AddDataFiles(ctx, []iceberg.DataFile{df}, nil, opts...); err != nil {
						b.Fatal(err)
					}
					// Discard the transaction — we are timing AddDataFiles, not Commit.
				}
			})
		}
	}
}
