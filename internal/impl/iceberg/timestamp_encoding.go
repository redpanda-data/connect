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

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/parquet-go/parquet-go"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
)

// resolveTimestampEncoding determines how no-timezone `timestamp` columns must
// be annotated in the parquet files written to tbl, guaranteeing an existing
// table never sees its encoding change or become mixed (a released connector
// version wrote the spec-incorrect isAdjustedToUTC=true "legacy" annotation).
//
// Resolution order:
//
//  1. Table property redpanda-connect.timestamp-encoding present → use it.
//     An unknown value is a hard error (guessing risks a mixed table).
//  2. Property absent → bootstrap by probing the table's own files
//     (probeTimestampEncoding), then STAMP the resolved value onto the table
//     as a SetProperties commit so the decision is permanent and visible to
//     every future writer.
//
// reload re-loads the table from the catalog; it is used to resolve a stamp
// race (two writers bootstrapping the same table concurrently). The returned
// table is tbl with the stamp applied when one was committed, otherwise tbl
// unchanged, so callers keep working with fresh metadata.
func resolveTimestampEncoding(ctx context.Context, tbl *table.Table, reload func(context.Context) (*table.Table, error), logger *service.Logger) (icebergx.TimestampEncoding, *table.Table, error) {
	if v, ok := tbl.Properties()[icebergx.TimestampEncodingProperty]; ok {
		enc, err := icebergx.ParseTimestampEncoding(v)
		if err != nil {
			return 0, nil, fmt.Errorf("table %v: %w", tbl.Identifier(), err)
		}
		return enc, tbl, nil
	}

	enc, err := probeTimestampEncoding(ctx, tbl)
	if err != nil {
		return 0, nil, fmt.Errorf("resolving timestamp encoding for table %v: %w", tbl.Identifier(), err)
	}

	stamped, err := stampTimestampEncoding(ctx, tbl, enc, reload)
	if err != nil {
		return 0, nil, fmt.Errorf("stamping timestamp encoding %q on table %v: %w", enc, tbl.Identifier(), err)
	}
	if logger != nil {
		logger.Infof("Pinned table %v to timestamp encoding %q (table property %s)", tbl.Identifier(), enc, icebergx.TimestampEncodingProperty)
	}
	return enc, stamped, nil
}

// probeTimestampEncoding bootstraps the encoding for a table that predates the
// pinning property, by inspecting what the table actually contains:
//
//   - schema has no no-tz `timestamp` column → spec. There is nothing the two
//     encodings disagree on, and it is correct for any timestamp column added
//     later by schema evolution, since no existing file can carry that column.
//   - no data files (empty table / no snapshot) → spec.
//   - otherwise → open a current-snapshot data file's parquet footer and read
//     the isAdjustedToUTC annotation off a no-tz timestamp column:
//     true → legacy, false → spec. Files that don't carry any such column
//     (e.g. written before the column was evolved in) are skipped; if no
//     parquet file carries one, resolve spec for the same reason as the
//     no-column case. Any read/parse failure is a hard error — guessing here
//     could silently mix annotations within the table.
func probeTimestampEncoding(ctx context.Context, tbl *table.Table) (icebergx.TimestampEncoding, error) {
	schema := tbl.Schema()
	if !icebergx.SchemaHasNoTZTimestamp(schema) {
		return icebergx.TimestampEncodingSpec, nil
	}
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return icebergx.TimestampEncodingSpec, nil
	}
	fsys, err := tbl.FS(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting table filesystem: %w", err)
	}
	manifests, err := snap.Manifests(fsys)
	if err != nil {
		return 0, fmt.Errorf("listing current-snapshot manifests: %w", err)
	}
	for _, m := range manifests {
		if m.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		for entry, err := range m.Entries(fsys, true) {
			if err != nil {
				return 0, fmt.Errorf("reading manifest entries: %w", err)
			}
			df := entry.DataFile()
			if df.FileFormat() != iceberg.ParquetFile {
				// A non-parquet data file was never written by this connector,
				// so it cannot carry the legacy encoding; skip it.
				continue
			}
			enc, found, err := probeParquetFooterEncoding(fsys, df.FilePath(), schema)
			if err != nil {
				return 0, fmt.Errorf("probing parquet footer of %s: %w", df.FilePath(), err)
			}
			if found {
				return enc, nil
			}
			// The file has no no-tz timestamp leaf (it predates the column);
			// keep scanning. In the common case the very first file decides.
		}
	}
	// Data files exist but none carries a no-tz timestamp column: the column
	// was added by evolution after every current file was written, so there is
	// no legacy-annotated file to stay consistent with.
	return icebergx.TimestampEncodingSpec, nil
}

// probeParquetFooterEncoding opens one parquet file's footer via the table's
// filesystem and inspects the logical-type annotation of the first leaf that
// is a no-tz `timestamp` column in the iceberg schema (matched by field ID).
// found is false when the file carries no such leaf.
func probeParquetFooterEncoding(fsys iceio.IO, path string, schema *iceberg.Schema) (enc icebergx.TimestampEncoding, found bool, err error) {
	f, err := fsys.Open(path)
	if err != nil {
		return 0, false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, false, err
	}
	// Only the footer is needed: skip the page-index and bloom-filter
	// sections so the probe stays a couple of small ranged reads.
	pf, err := parquet.OpenFile(f, info.Size(), parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		return 0, false, err
	}
	for _, el := range pf.Metadata().Schema {
		lt, ok := el.LogicalType.Get()
		if !ok || lt.Timestamp == nil {
			continue
		}
		field, ok := schema.FindFieldByID(int(el.FieldID))
		if !ok {
			continue
		}
		if _, noTZ := field.Type.(iceberg.TimestampType); !noTZ {
			continue
		}
		if lt.Timestamp.IsAdjustedToUTC {
			return icebergx.TimestampEncodingLegacy, true, nil
		}
		return icebergx.TimestampEncodingSpec, true, nil
	}
	return 0, false, nil
}

// stampTimestampEncoding commits the resolved encoding onto the table as the
// redpanda-connect.timestamp-encoding property, making the bootstrap decision
// permanent and visible. Two writers may race to stamp the same table: on a
// commit failure the table is reloaded and, if the property appeared
// meanwhile with our value, the race is benign and the reloaded table is
// used. A property that appeared with a DIFFERENT value is a hard error —
// both writers probed the same files, so a disagreement means something is
// wrong and writing could mix annotations.
func stampTimestampEncoding(ctx context.Context, tbl *table.Table, enc icebergx.TimestampEncoding, reload func(context.Context) (*table.Table, error)) (*table.Table, error) {
	txn := tbl.NewTransaction()
	if err := txn.SetProperties(iceberg.Properties{icebergx.TimestampEncodingProperty: enc.String()}); err != nil {
		return nil, err
	}
	stamped, commitErr := txn.Commit(ctx)
	if commitErr == nil {
		return stamped, nil
	}
	// The commit can fail because a concurrent writer stamped first (or wrote
	// anything else to the table). Reload and check whether the property is
	// now present and agrees with our resolution.
	if reload != nil {
		reloaded, reloadErr := reload(ctx)
		if reloadErr == nil {
			if v, ok := reloaded.Properties()[icebergx.TimestampEncodingProperty]; ok {
				if v == enc.String() {
					return reloaded, nil
				}
				return nil, fmt.Errorf("concurrent writer pinned %s=%q but this writer resolved %q (commit error: %v)", icebergx.TimestampEncodingProperty, v, enc, commitErr)
			}
		}
	}
	return nil, commitErr
}
