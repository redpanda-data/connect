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
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/shredder"
)

// writeCOW materialises a mutating batch as copy-on-write: it rewrites whole
// data files so the table only ever contains plain data files (no equality- or
// positional-delete files), which engine-backed catalogs such as Snowflake and
// the Databricks Unity Catalog can read.
//
// It reuses splitByOperation's parse + last-writer-wins per-key collapse:
//   - inserts = insert-op rows plus upsert-op rows (the rows to (re)write)
//   - deletes = one message per keyed op (upsert OR delete) — the keys whose
//     prior versions must be removed
//
// The whole batch is applied as a single atomic snapshot:
//   - only inserts (no keyed ops): plain append fast path (no rewrite)
//   - only deletes (no rows to write): txn.Delete(filter)
//   - otherwise: txn.Overwrite(reader, WithOverwriteFilter(filter)), which
//     deletes every existing row matching filter and appends the new rows in
//     one snapshot.
func (w *writer) writeCOW(ctx context.Context, batch service.MessageBatch) error {
	inserts, deletes, counts, err := w.splitByOperation(batch)
	if err != nil {
		return fmt.Errorf("splitting batch by row operation: %w", err)
	}

	// Fast path: no keyed operations — this is a plain append, so reuse the
	// data-file path and the append commit. No file rewrite is needed.
	if len(deletes) == 0 {
		if len(inserts) == 0 {
			return nil
		}
		files, err := w.writeDataFiles(ctx, inserts)
		if err != nil {
			return fmt.Errorf("writing data files: %w", err)
		}
		if err := w.committer.Commit(ctx, CommitInput{Files: files, SchemaID: w.table.Schema().ID}); err != nil {
			w.cleanupFiles(ctx, files)
			return fmt.Errorf("committing: %w", err)
		}
		w.metrics.incrInserted(counts.inserted)
		return nil
	}

	// Mutating (upsert/delete) copy-on-write rewrites existing data files, and
	// the rewrite cannot read back files whose no-tz `timestamp` columns carry
	// the legacy UTC-adjusted annotation (iceberg-go reads them as timestamptz
	// and refuses the timestamptz -> timestamp "promotion"). Fail upfront with
	// an actionable error — before any file writes — rather than surface the
	// library's cryptic one mid-commit. Insert-only batches on such a table are
	// fine (they took the append fast path above and keep writing the table's
	// own legacy encoding), as is merge-on-read (no file rewrites).
	if err := w.checkCOWTimestampEncoding(); err != nil {
		return err
	}

	// The remaining paths rewrite data files. Partitioned tables are supported:
	// iceberg-go's Overwrite/Delete route rows to partitions correctly end-to-end.
	//   - New/rewritten rows: recordsToDataFiles sends a partitioned spec through
	//     the partitioned fanout writer (partitioned_fanout_writer.go), which
	//     derives each row's partition tuple from the actual source-column value
	//     via PartitionField.Transform.Apply — so every transform (identity,
	//     bucket, truncate, year/month/day/hour) routes correctly. This is not the
	//     stats-inference path (fileToDataFile) that panics on non-order-preserving
	//     transforms; that path is only used by AddFiles.
	//   - Deletions: classifyFilesForFilteredDeletions evaluates the filter against
	//     every data file's stats across all partitions. A merge key on a
	//     non-partition column projects to AlwaysTrue in the partition space, so no
	//     partition is pruned and matching rows are found in every partition.
	// Unlike merge-on-read equality deletes (which are partition-scoped and so
	// require the partition source columns to be a subset of identifier_fields, see
	// writer.deleteRecordFields), copy-on-write rewrites whole files by filter and
	// appends real rows routed by value, so it carries no such constraint — the
	// merge key need not include (or be) the partition column.

	// The rewrite builds records through the Arrow JSON round-trip, so the whole
	// table schema must be faithfully representable that way.
	tableSchema := w.table.Schema()
	if err := checkCOWSchemaSupported(tableSchema); err != nil {
		return err
	}

	filter, err := w.buildCOWFilter(tableSchema, deletes)
	if err != nil {
		return fmt.Errorf("building copy-on-write filter: %w", err)
	}

	input := OverwriteInput{Filter: filter, SchemaID: tableSchema.ID}

	// Only deletes: delete the matching rows, write nothing new.
	if len(inserts) == 0 {
		if err := w.committer.commitOverwrite(ctx, input); err != nil {
			return fmt.Errorf("committing copy-on-write delete: %w", err)
		}
		w.metrics.incrDeleted(counts.deleted)
		return nil
	}

	// Detect columns present in the rows to write but absent from the table
	// schema and surface them as a schema-evolution error, so the router adds the
	// columns and retries — exactly as the append path does. The rewrite below
	// projects rows onto the current schema via array.RecordFromJSON, so without
	// this an unknown column's value would be silently dropped.
	if err := w.cowDetectNewColumns(tableSchema, inserts); err != nil {
		return err
	}

	// Deletes + new rows: one atomic overwrite. The reader factory rebuilds the
	// reader on every attempt because array.RecordReader is consumed once and
	// the commit stage can run multiple times on retry.
	factory, err := w.buildCOWRecordFactory(tableSchema, inserts)
	if err != nil {
		return fmt.Errorf("building copy-on-write records: %w", err)
	}
	input.NewReader = factory
	if err := w.committer.commitOverwrite(ctx, input); err != nil {
		return fmt.Errorf("committing copy-on-write overwrite: %w", err)
	}
	w.metrics.incrInserted(counts.inserted)
	w.metrics.incrUpserted(counts.upserted)
	w.metrics.incrDeleted(counts.deleted)
	return nil
}

// checkCOWTimestampEncoding guards mutating copy-on-write against tables
// pinned to the legacy timestamp encoding: their data files annotate no-tz
// `timestamp` columns with isAdjustedToUTC=true, which the copy-on-write
// rewrite cannot read back losslessly (iceberg-go maps the annotation to
// timestamptz and its strict rewrite visitor refuses timestamptz ->
// timestamp). Tables without any no-tz timestamp column are unaffected —
// there is no column the encodings disagree on.
func (w *writer) checkCOWTimestampEncoding() error {
	if w.tsEncoding != icebergx.TimestampEncodingLegacy || !icebergx.SchemaHasNoTZTimestamp(w.table.Schema()) {
		return nil
	}
	return fmt.Errorf(
		"table %s uses the legacy UTC-adjusted parquet encoding for its `timestamp` columns (table property %s=legacy), which copy-on-write cannot rewrite; "+
			"compact/rewrite the table's data files with an engine that writes the spec encoding and set the table property %s=spec "+
			"(stop or restart connector writers to the table around the migration — a running writer only re-reads the property when its writer is recreated), "+
			"or use merge_strategy: merge-on-read",
		strings.Join(w.table.Identifier(), "."), icebergx.TimestampEncodingProperty, icebergx.TimestampEncodingProperty,
	)
}

// checkCOWSchemaSupported rejects table schemas the copy-on-write path cannot
// faithfully round-trip through Arrow. The rewrite builds records via
// array.RecordFromJSON from the JSON produced by cowMassage; that projection is
// recursive, so nested struct/list/map columns are supported as long as every
// leaf is a supported primitive. Each type kind is checked by walking the type
// tree; any unsupported leaf fails loudly with an actionable message rather than
// risking a silent mis-write.
func checkCOWSchemaSupported(s *iceberg.Schema) error {
	for _, f := range s.Fields() {
		if err := checkCOWTypeSupported(f.Name, f.Type); err != nil {
			return err
		}
	}
	return nil
}

// checkCOWTypeSupported recurses an iceberg type, accepting nested
// struct/list/map whose leaves are all supported primitives and rejecting any
// unsupported leaf. path names the column position (dotted for nested fields) so
// the error points at the offending leaf.
func checkCOWTypeSupported(path string, t iceberg.Type) error {
	switch tt := t.(type) {
	case *iceberg.StructType:
		for _, f := range tt.FieldList {
			if err := checkCOWTypeSupported(path+"."+f.Name, f.Type); err != nil {
				return err
			}
		}
		return nil
	case *iceberg.ListType:
		return checkCOWTypeSupported(path+".element", tt.Element)
	case *iceberg.MapType:
		if err := checkCOWTypeSupported(path+".key", tt.KeyType); err != nil {
			return err
		}
		return checkCOWTypeSupported(path+".value", tt.ValueType)
	default:
		if _, ok := t.(iceberg.PrimitiveType); !ok {
			return fmt.Errorf("copy-on-write merge_strategy does not support column %q of unsupported non-primitive type %s (use merge-on-read for this schema)", path, t)
		}
		if !cowSupportedColumnType(t) {
			return fmt.Errorf("copy-on-write merge_strategy does not support column %q of type %s; supported leaf types are boolean, int, long, float, double, string, date, time, timestamp, timestamptz, decimal, uuid, binary, and fixed", path, t)
		}
		return nil
	}
}

// cowSupportedColumnType reports whether a primitive iceberg type round-trips
// faithfully through deleteKeyJSONValue + array.RecordFromJSON, as used by
// cowMassage/buildCOWRecordFactory. Every type in this set is guarded by a
// faithful round-trip in TestCOWColumnTypeRoundTrip (cow_type_roundtrip_test.go).
//
// binary and fixed are included: deleteKeyJSONValue passes a []byte through
// unchanged, json.Marshal base64-encodes it, and the Arrow Binary /
// FixedSizeBinary JSON readers base64-decode it back to the exact bytes.
//
// Nested struct/list/map are supported by recursing the type tree (see
// checkCOWTypeSupported) down to these primitive leaves: cowMassage produces the
// correct JSON shape at every depth — integers are emitted as strings at every
// leaf (fixing the historical >2^53 nested truncation) and maps are reshaped to
// Arrow's array-of-{key,value}-entries encoding. See cow_type_roundtrip_test.go
// for the round-trip evidence at each nesting.
func cowSupportedColumnType(t iceberg.Type) bool {
	switch t.(type) {
	case iceberg.BooleanType,
		iceberg.Int32Type, iceberg.Int64Type,
		iceberg.Float32Type, iceberg.Float64Type,
		iceberg.StringType,
		iceberg.DateType, iceberg.TimeType,
		iceberg.TimestampType, iceberg.TimestampTzType,
		iceberg.DecimalType,
		iceberg.UUIDType,
		iceberg.BinaryType, iceberg.FixedType:
		return true
	default:
		return false
	}
}

// buildCOWFilter builds the boolean expression selecting every row whose merge
// key appears in the keyed (upsert/delete) messages. For a single identifier
// column it is `col IN (v1, v2, ...)`; for a composite key it is an OR of
// per-tuple ANDs — `(a=a1 AND b=b1) OR (a=a2 AND b=b2) ...` — which is the
// correct semantics (an AND of per-column INs would match the cross product).
//
// Merge-key columns may be int/long/string, the temporal types
// (date/time/timestamp/timestamptz), or uuid. Every key literal is built so its
// encoding matches how buildCOWRecordFactory stores the same value (see
// cowKeyLiteral). decimal and boolean are intentionally excluded (iceberg-go's
// overwrite filter cannot apply either — decimal panics, boolean is
// unimplemented; use merge-on-read for those keys); other key types return a
// clear error.
func (w *writer) buildCOWFilter(tableSchema *iceberg.Schema, keyed service.MessageBatch) (iceberg.BooleanExpression, error) {
	idFields, err := w.cowKeyFields(tableSchema)
	if err != nil {
		return nil, err
	}

	if len(idFields) == 1 {
		f := idFields[0]
		lits := make([]iceberg.Literal, 0, len(keyed))
		for i, msg := range keyed {
			v, err := w.lookupKeyValue(msg, f, i)
			if err != nil {
				return nil, err
			}
			lit, err := cowKeyLiteral(f.Type, f.Name, v)
			if err != nil {
				return nil, err
			}
			lits = append(lits, lit)
		}
		// SetPredicate collapses duplicate literals and reduces to EqualTo/
		// AlwaysFalse for the degenerate cases.
		return iceberg.SetPredicate(iceberg.OpIn, iceberg.Reference(f.Name), lits), nil
	}

	clauses := make([]iceberg.BooleanExpression, 0, len(keyed))
	for i, msg := range keyed {
		ands := make([]iceberg.BooleanExpression, 0, len(idFields))
		for _, f := range idFields {
			v, err := w.lookupKeyValue(msg, f, i)
			if err != nil {
				return nil, err
			}
			lit, err := cowKeyLiteral(f.Type, f.Name, v)
			if err != nil {
				return nil, err
			}
			ands = append(ands, iceberg.LiteralPredicate(iceberg.OpEQ, iceberg.Reference(f.Name), lit))
		}
		var clause iceberg.BooleanExpression
		if len(ands) == 1 {
			clause = ands[0]
		} else {
			clause = iceberg.NewAnd(ands[0], ands[1], ands[2:]...)
		}
		clauses = append(clauses, clause)
	}

	if len(clauses) == 1 {
		return clauses[0], nil
	}
	return iceberg.NewOr(clauses[0], clauses[1], clauses[2:]...), nil
}

// cowKeyFields resolves the configured identifier_fields against the table
// schema, validating that each is a primitive column supported as a copy-on-
// write merge key.
func (w *writer) cowKeyFields(tableSchema *iceberg.Schema) ([]iceberg.NestedField, error) {
	if len(w.rowOpCfg.IdentifierFields) == 0 {
		return nil, fmt.Errorf("%s is required for upsert/delete", ioFieldIdentifierFields)
	}
	fields := make([]iceberg.NestedField, 0, len(w.rowOpCfg.IdentifierFields))
	for _, name := range w.rowOpCfg.IdentifierFields {
		field, ok := tableSchema.FindFieldByName(name)
		if !ok && !w.caseSensitive {
			field, ok = tableSchema.FindFieldByNameCaseInsensitive(name)
		}
		if !ok {
			return nil, fmt.Errorf("%s column %q not found in table schema", ioFieldIdentifierFields, name)
		}
		fields = append(fields, field)
	}
	return fields, nil
}

// lookupKeyValue extracts a single identifier column's value from a message,
// erroring on a missing or null key (a null merge key cannot select rows).
func (w *writer) lookupKeyValue(msg *service.Message, field iceberg.NestedField, idx int) (any, error) {
	structured, err := msg.AsStructured()
	if err != nil {
		return nil, fmt.Errorf("reading structured message %d for merge key: %w", idx, err)
	}
	row, ok := structured.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("message %d for upsert/delete must be an object, got %T", idx, structured)
	}
	v, ok := lookupField(row, field.Name, w.caseSensitive)
	if !ok || v == nil {
		return nil, fmt.Errorf("%s %q is missing or null in message %d", ioFieldIdentifierFields, field.Name, idx)
	}
	return v, nil
}

// cowKeyLiteral builds an iceberg filter literal for a merge-key value.
//
// The overriding invariant is that the literal's encoding MUST match how
// buildCOWRecordFactory stores the same value, or the overwrite filter selects
// no rows and the upsert/delete silently becomes a no-op (a silent no-op instead of a mutation).
// The rewrite stores every value by running it through deleteKeyJSONValue and
// then array.RecordFromJSON, so this function derives each literal from that
// same canonicalisation:
//
//   - int/long/string: built directly, mirroring the append path.
//   - date/time/uuid: canonicalised by deleteKeyJSONValue to the exact string
//     the data path stores, then parsed into the typed literal by iceberg's own
//     StringLiteral.To — so filter and storage share an encoding by construction
//     (date days, microsecond time-of-day, uuid bytes).
//   - timestamp/timestamptz: deleteKeyJSONValue requires a time.Time and rejects
//     a bare number (a numeric timestamp is ambiguous — the exact silent
//     no-match), so it is reused for that validation. The literal is then built
//     directly from the time.Time as UnixMicro, because StringLiteral.To's
//     timestamp parser does not accept the RFC3339 form the data path stores;
//     both encode microseconds since the epoch, so they still agree.
//
// decimal is deliberately NOT a supported merge key: iceberg-go's overwrite
// applies the filter through its substrait conversion, which panics on a decimal
// literal (toDecimalLiteral asserts *iceberg.DecimalType, but DecimalLiteral.Type
// returns a value DecimalType — a bug in the vendored library). Rather than let
// that panic reach a real table, decimal keys are rejected here with an
// actionable error. decimal remains valid as a merge-on-read equality-delete key
// (that path does not go through substrait).
//
// Other key types return a clear, actionable error.
func cowKeyLiteral(t iceberg.Type, name string, v any) (iceberg.Literal, error) {
	switch t.(type) {
	case iceberg.Int32Type:
		n, err := cowValueToInt64(v)
		if err != nil {
			return nil, fmt.Errorf("%s %q: %w", ioFieldIdentifierFields, name, err)
		}
		return iceberg.NewLiteral(int32(n)), nil
	case iceberg.Int64Type:
		n, err := cowValueToInt64(v)
		if err != nil {
			return nil, fmt.Errorf("%s %q: %w", ioFieldIdentifierFields, name, err)
		}
		return iceberg.NewLiteral(n), nil
	case iceberg.StringType:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("%s %q: string column given %T", ioFieldIdentifierFields, name, v)
		}
		return iceberg.NewLiteral(s), nil
	case iceberg.BooleanType:
		// Like decimal, a boolean merge key cannot be applied by iceberg-go's
		// copy-on-write rewrite: rewriteFilesWithFilter evaluates the (negated)
		// key predicate against each data file's rows to keep survivors, and that
		// row-level evaluation is unimplemented for BOOL ("not implemented:
		// unsupported type BOOL"), failing the whole overwrite. Refuse up front
		// with an actionable error rather than let it surface mid-commit. boolean
		// remains valid as a non-key column and as a merge-on-read key.
		return nil, fmt.Errorf("copy-on-write merge_strategy does not support merge key column %q of type %s; boolean is not a supported copy-on-write merge key (a known limitation in the underlying iceberg library's overwrite filter) — use merge-on-read for a boolean key", name, t)
	case iceberg.TimestampType, iceberg.TimestampTzType:
		// Reuse deleteKeyJSONValue purely for its validation: it requires a
		// time.Time and rejects a bare number with an actionable error.
		if _, err := deleteKeyJSONValue(t, v); err != nil {
			return nil, fmt.Errorf("%s %q: %w", ioFieldIdentifierFields, name, err)
		}
		tm := v.(time.Time)
		return iceberg.NewLiteral(iceberg.Timestamp(tm.UTC().UnixMicro())), nil
	case iceberg.DecimalType:
		// See the doc comment: an overwrite filter on a decimal column panics
		// inside iceberg-go's substrait conversion, so refuse loudly here.
		return nil, fmt.Errorf("copy-on-write merge_strategy does not support merge key column %q of type %s; decimal is not a supported copy-on-write merge key (a known limitation in the underlying iceberg library's overwrite filter) — use merge-on-read for a decimal key", name, t)
	case iceberg.DateType, iceberg.TimeType, iceberg.UUIDType:
		jv, err := deleteKeyJSONValue(t, v)
		if err != nil {
			return nil, fmt.Errorf("%s %q: %w", ioFieldIdentifierFields, name, err)
		}
		s, ok := jv.(string)
		if !ok {
			// deleteKeyJSONValue canonicalises all of these to a string; a
			// non-string means the incoming value could not be canonicalised
			// (e.g. a non-string uuid value), which cannot key a row.
			return nil, fmt.Errorf("%s %q: %s column requires a value convertible to its canonical string form, got %T", ioFieldIdentifierFields, name, t, v)
		}
		lit, err := iceberg.StringLiteral(s).To(t)
		if err != nil {
			return nil, fmt.Errorf("%s %q: %w", ioFieldIdentifierFields, name, err)
		}
		return lit, nil
	default:
		return nil, fmt.Errorf("copy-on-write merge_strategy does not support merge key column %q of type %s; supported merge-key types are int, long, string, date, time, timestamp, timestamptz, and uuid (use merge-on-read for other key types)", name, t)
	}
}

// cowValueToInt64 converts a JSON-decoded value into an int64 without silent
// precision loss, mirroring deleteKeyJSONValue's integer handling.
func cowValueToInt64(v any) (int64, error) {
	switch n := v.(type) {
	case json.Number:
		return n.Int64()
	case int:
		return int64(n), nil
	case int32:
		return int64(n), nil
	case int64:
		return n, nil
	case float64:
		if n != math.Trunc(n) {
			return 0, fmt.Errorf("integer column given non-integer value %v", n)
		}
		if math.Abs(n) >= 1<<53 {
			return 0, fmt.Errorf("integer column given value %v outside the range representable exactly as a float64 (provide it as an integer or string)", n)
		}
		return int64(n), nil
	case string:
		return strconv.ParseInt(n, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported value type %T for integer column", v)
	}
}

// cowDetectNewColumns returns a BatchSchemaEvolutionError naming any top-level
// field present in the rows to write but absent from the table schema. The
// copy-on-write rewrite projects rows onto the current schema, so an unknown
// column would otherwise be dropped without trace; returning this error lets the
// router evolve the table and retry, matching the shredder-based append path
// (writer.go writeDataFiles). Only top-level columns are detected here; new
// fields appearing inside an existing nested struct/list/map are not surfaced
// for evolution (nested schema evolution is out of scope for copy-on-write) —
// they are projected onto the current nested type by cowMassage.
func (w *writer) cowDetectNewColumns(tableSchema *iceberg.Schema, rows service.MessageBatch) error {
	var newErrs []*UnknownFieldError
	seen := make(map[string]struct{})
	for i, msg := range rows {
		structured, err := msg.AsStructured()
		if err != nil {
			return fmt.Errorf("reading structured message %d: %w", i, err)
		}
		row, ok := structured.(map[string]any)
		if !ok {
			return fmt.Errorf("message %d must be an object, got %T", i, structured)
		}
		for name, v := range row {
			_, known := tableSchema.FindFieldByName(name)
			if !known && !w.caseSensitive {
				_, known = tableSchema.FindFieldByNameCaseInsensitive(name)
			}
			if known {
				continue
			}
			dedup := name
			if !w.caseSensitive {
				dedup = strings.ToLower(name)
			}
			if _, dup := seen[dedup]; dup {
				continue
			}
			seen[dedup] = struct{}{}
			newErrs = append(newErrs, NewUnknownFieldError(nil, name, v))
		}
	}
	if len(newErrs) > 0 {
		return NewBatchSchemaEvolutionError(newErrs)
	}
	return nil
}

// buildCOWRecordFactory projects the rows to (re)write into JSON matching the
// full table schema and returns a factory that rebuilds a fresh RecordReader on
// each call. A factory (rather than a single reader) is required because
// array.RecordReader is consumed once and the commit stage can run multiple
// times on retry.
func (w *writer) buildCOWRecordFactory(tableSchema *iceberg.Schema, rows service.MessageBatch) (func() (array.RecordReader, error), error) {
	arrowSc, err := table.SchemaToArrowSchema(tableSchema, nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("building arrow schema: %w", err)
	}

	// Resolve the batch's schema metadata into the same fieldID -> schema.Common
	// map the insert path (messagesToParquet) hands the shredder, so a numeric
	// temporal DATA column is interpreted with the identical unit-aware
	// conversion rather than being rejected. We sample rows[0] and apply it to
	// every row, matching messagesToParquet's batch[0] assumption (Connect's
	// iceberg router groups by table before this point, so a batch shares one
	// schema). A parse failure is non-fatal: we log and fall back to the
	// schema-agnostic conversion, exactly as the insert path does.
	fieldCommons := w.cowFieldCommons(tableSchema, rows)

	fields := tableSchema.Fields()
	encoded := make([]map[string]any, 0, len(rows))
	for i, msg := range rows {
		structured, err := msg.AsStructured()
		if err != nil {
			return nil, fmt.Errorf("reading structured message %d: %w", i, err)
		}
		row, ok := structured.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("message %d must be an object, got %T", i, structured)
		}
		out := make(map[string]any, len(fields))
		for _, field := range fields {
			v, ok := lookupField(row, field.Name, w.caseSensitive)
			if !ok || v == nil {
				// Absent/null columns are left out so Arrow reads them as null.
				continue
			}
			jv, err := w.cowMassage(field.Type, field.ID, v, fieldCommons)
			if err != nil {
				return nil, fmt.Errorf("column %q in message %d: %w", field.Name, i, err)
			}
			out[field.Name] = jv
		}
		encoded = append(encoded, out)
	}

	jsonRows, err := json.Marshal(encoded)
	if err != nil {
		return nil, fmt.Errorf("encoding rows: %w", err)
	}

	return func() (array.RecordReader, error) {
		rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSc, bytes.NewReader(jsonRows))
		if err != nil {
			return nil, fmt.Errorf("building records: %w", err)
		}
		rdr, err := array.NewRecordReader(arrowSc, []arrow.RecordBatch{rec})
		if err != nil {
			rec.Release()
			return nil, fmt.Errorf("building record reader: %w", err)
		}
		// NewRecordReader retains rec, so drop our reference.
		rec.Release()
		return rdr, nil
	}, nil
}

// cowFieldCommons resolves the batch's schema_metadata into a leaf-field-ID ->
// schema.Common map, reusing exactly the parse + walk the insert path uses
// (typeResolver.parseSchemaMetadata + buildShredderFieldCommons). Returns nil
// when no resolver/metadata is configured, when the sampled message carries no
// metadata, or when the metadata does not parse — in every such case cowMassage
// falls back to the historical schema-agnostic conversion, matching
// messagesToParquet. Sampling rows[0] mirrors the insert path's batch[0]
// assumption.
func (w *writer) cowFieldCommons(tableSchema *iceberg.Schema, rows service.MessageBatch) map[int]*schema.Common {
	if w.resolver == nil || len(rows) == 0 {
		return nil
	}
	common, err := w.resolver.parseSchemaMetadata(rows[0])
	if err != nil {
		if w.logger != nil {
			w.logger.Warnf("parsing schema metadata for copy-on-write rewrite: %v (falling back to schema-agnostic conversion)", err)
		}
		return nil
	}
	if common == nil {
		return nil
	}
	return buildShredderFieldCommons(tableSchema, common, w.caseSensitive)
}

// cowMassage recursively projects a CDC value onto the JSON shape that
// SchemaToArrowSchema + array.RecordFromJSON expects for the given iceberg type,
// at every depth of the type tree. It is the nested generalisation of the flat
// deleteKeyJSONValue projection and exists so that copy-on-write can faithfully
// (re)write struct/list/map columns rather than either corrupting them or
// rejecting them outright.
//
// fieldID names the current leaf's iceberg field ID and fieldCommons carries the
// batch's schema metadata keyed by leaf field ID (see cowFieldCommons); together
// they let a temporal leaf interpret a numeric epoch value using the declared
// unit, exactly as the insert path's shredder does.
//
// Each type kind is handled as follows:
//
//   - primitive: for a temporal column (date/time/timestamp/timestamptz) a bare
//     numeric value is first resolved to a time.Time via
//     shredder.NumericTemporalToTime — the SAME unit-aware conversion the insert
//     path applies — so copy-on-write accepts the same numeric-epoch inputs as
//     inserts (CORR-1) instead of hard-erroring, and honours
//     require_schema_metadata identically. A time.Time value is unchanged. The
//     (possibly converted) value is then handed to deleteKeyJSONValue, which
//     applies the int->string, temporal, decimal and uuid canonicalisation at
//     every leaf. Doing this at every leaf (not just the top level) is what
//     fixes the historical silent truncation of integers nested beyond 2^53: a
//     nested int64 is emitted as a JSON string, which the Arrow Int32/Int64 JSON
//     builder parses back exactly, instead of decoding through a lossy float64.
//   - struct: the value is a map[string]any keyed by field name; recurse per
//     struct field, honouring the writer's case sensitivity, and emit a
//     map[string]any. Absent/null fields are omitted so Arrow reads them as null,
//     mirroring the top-level behaviour.
//   - list: the value is a []any; recurse per element with the element type and
//     emit a []any (nil elements pass through as JSON null).
//   - map: the CDC value is a map[string]any (the natural {"k": v} shape), but
//     Arrow encodes a map as List<Struct<key, value>> (see arrow.MapOf: the entry
//     struct fields are literally named "key" and "value", with value nullable).
//     So reshape to []any of {"key": k, "value": v} objects, recursing the key
//     and value types. A null map value stays null under its "value" key.
func (w *writer) cowMassage(t iceberg.Type, fieldID int, v any, fieldCommons map[int]*schema.Common) (any, error) {
	switch tt := t.(type) {
	case *iceberg.StructType:
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("struct value must be an object, got %T", v)
		}
		out := make(map[string]any, len(tt.FieldList))
		for _, f := range tt.FieldList {
			fv, ok := lookupField(m, f.Name, w.caseSensitive)
			if !ok || fv == nil {
				// Absent/null field: omit so Arrow reads it as null.
				continue
			}
			mv, err := w.cowMassage(f.Type, f.ID, fv, fieldCommons)
			if err != nil {
				return nil, fmt.Errorf("struct field %q: %w", f.Name, err)
			}
			out[f.Name] = mv
		}
		return out, nil
	case *iceberg.ListType:
		l, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("list value must be an array, got %T", v)
		}
		out := make([]any, len(l))
		for i, e := range l {
			if e == nil {
				out[i] = nil
				continue
			}
			me, err := w.cowMassage(tt.Element, tt.ElementID, e, fieldCommons)
			if err != nil {
				return nil, fmt.Errorf("list element %d: %w", i, err)
			}
			out[i] = me
		}
		return out, nil
	case *iceberg.MapType:
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("map value must be an object, got %T", v)
		}
		entries := make([]any, 0, len(m))
		for k, mv := range m {
			mk, err := w.cowMassage(tt.KeyType, tt.KeyID, k, fieldCommons)
			if err != nil {
				return nil, fmt.Errorf("map key %q: %w", k, err)
			}
			var vv any
			if mv != nil {
				vv, err = w.cowMassage(tt.ValueType, tt.ValueID, mv, fieldCommons)
				if err != nil {
					return nil, fmt.Errorf("map value for key %q: %w", k, err)
				}
			}
			entries = append(entries, map[string]any{"key": mk, "value": vv})
		}
		return entries, nil
	default:
		// Primitive leaf. For a temporal column, resolve a numeric epoch value to
		// a time.Time using the shredder's unit-aware conversion (honouring the
		// field's schema metadata and require_schema_metadata) so the encoded
		// value matches what the insert path stores; a time.Time passes through
		// untouched. Non-temporal leaves and time.Time values fall straight
		// through to the shared canonicalisation.
		if tm, ok, err := shredder.NumericTemporalToTime(v, t, fieldCommons[fieldID], w.requireSchemaMetadata); err != nil {
			return nil, err
		} else if ok {
			v = tm
		}
		// Iceberg date/time/timestamp columns are microsecond resolution, and the
		// Arrow JSON readers for those columns reject sub-microsecond strings. A
		// time.Time carrying nanoseconds is therefore truncated to microseconds so
		// it both encodes and matches exactly what the insert path stores (the
		// shredder uses UnixMicro, which truncates identically) — and, for a merge
		// key, what cowKeyLiteral's UnixMicro literal filters on. Truncation is the
		// only lossy step, and it is consistent across filter and storage, so it
		// never causes a silent no-match.
		if tm, ok := v.(time.Time); ok {
			v = tm.Truncate(time.Microsecond)
		}
		return deleteKeyJSONValue(t, v)
	}
}
