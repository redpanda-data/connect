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
	"math"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/redpanda-data/benthos/v4/public/service"
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

	// The remaining paths rewrite data files. Partitioned copy-on-write is not
	// yet validated in this prototype: iceberg-go's Overwrite routes rewritten
	// rows to partitions internally, but we have only proven the unpartitioned
	// case end-to-end, so fail loudly rather than risk mis-partitioned rewrites.
	spec := w.table.Spec()
	if spec.NumFields() > 0 {
		return errors.New("copy-on-write merge_strategy does not support upsert/delete on partitioned tables in this prototype; use merge-on-read, or an unpartitioned table")
	}

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

// checkCOWSchemaSupported rejects table schemas the prototype's copy-on-write
// path cannot faithfully round-trip through Arrow. The custom shredder handles
// nested/complex types on the append path, but the copy-on-write rewrite builds
// records via array.RecordFromJSON, which we have only verified for flat,
// primitive columns. Rather than silently mis-write, fail loudly with an
// actionable message.
func checkCOWSchemaSupported(s *iceberg.Schema) error {
	for _, f := range s.Fields() {
		if _, ok := f.Type.(iceberg.PrimitiveType); !ok {
			return fmt.Errorf("copy-on-write merge_strategy does not support column %q of non-primitive type %s; this prototype only supports tables whose columns are all flat primitive types (use merge-on-read for nested schemas)", f.Name, f.Type)
		}
		if !cowSupportedColumnType(f.Type) {
			return fmt.Errorf("copy-on-write merge_strategy does not support column %q of type %s; supported column types are boolean, int, long, float, double, string, date, time, timestamp, timestamptz, decimal, and uuid", f.Name, f.Type)
		}
	}
	return nil
}

// cowSupportedColumnType reports whether a primitive iceberg type is known to
// round-trip faithfully through deleteKeyJSONValue + array.RecordFromJSON. The
// set is deliberately conservative for the prototype; binary/fixed are excluded
// because we have not verified their JSON encoding.
func cowSupportedColumnType(t iceberg.Type) bool {
	switch t.(type) {
	case iceberg.BooleanType,
		iceberg.Int32Type, iceberg.Int64Type,
		iceberg.Float32Type, iceberg.Float64Type,
		iceberg.StringType,
		iceberg.DateType, iceberg.TimeType,
		iceberg.TimestampType, iceberg.TimestampTzType,
		iceberg.DecimalType,
		iceberg.UUIDType:
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
// For the prototype, merge-key columns are restricted to int/long/string/
// boolean so the filter literals are unambiguous; other key types return a
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

// cowKeyLiteral builds an iceberg filter literal for a merge-key value. Only
// int/long/string/boolean key columns are supported by the prototype's
// copy-on-write filter; other types return a clear, actionable error.
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
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("%s %q: boolean column given %T", ioFieldIdentifierFields, name, v)
		}
		return iceberg.NewLiteral(b), nil
	default:
		return nil, fmt.Errorf("copy-on-write merge_strategy does not support merge key column %q of type %s; supported merge-key types are int, long, string, and boolean (use merge-on-read for other key types)", name, t)
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
// (writer.go writeDataFiles). Copy-on-write is gated to flat-primitive schemas,
// so every new field is at the schema root.
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
			jv, err := deleteKeyJSONValue(field.Type, v)
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
