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
	"path"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/shredder"
)

// rowOperation is the row-level mutation applied to a message when written to
// the table. It is deliberately distinct from Iceberg's snapshot-level
// operation (append/replace/overwrite/delete).
type rowOperation string

const (
	rowOpInsert rowOperation = "insert"
	rowOpUpsert rowOperation = "upsert"
	rowOpDelete rowOperation = "delete"
)

// parseRowOperation validates s against the supported row operations. Unknown
// values are a hard error rather than a silent fallback to insert, so that a
// misconfigured mapping (e.g. an unmapped Debezium "c"/"u"/"d") fails loudly
// instead of silently appending duplicates.
func parseRowOperation(s string) (rowOperation, error) {
	switch op := rowOperation(s); op {
	case rowOpInsert, rowOpUpsert, rowOpDelete:
		return op, nil
	default:
		return "", fmt.Errorf("invalid %s %q: must be one of %q, %q, or %q", ioFieldRowOperation, s, rowOpInsert, rowOpUpsert, rowOpDelete)
	}
}

// RowOpConfig configures per-message row-level operations.
type RowOpConfig struct {
	// Operation resolves per message to insert, upsert, or delete. When it
	// resolves to the empty string the default (insert) is assumed.
	Operation *service.InterpolatedString
	// IdentifierFields are the table column names forming the equality-delete
	// key (the Iceberg identifier fields) used by upsert and delete. Empty for
	// append-only (insert) workloads.
	IdentifierFields []string
}

// mutating reports whether the configuration can ever produce a non-insert
// operation. A purely static insert (or unset) configuration takes the
// untouched append-only fast path.
func (c RowOpConfig) mutating() bool {
	if c.Operation == nil {
		return false
	}
	if static, ok := c.Operation.Static(); ok {
		return static != "" && rowOperation(static) != rowOpInsert
	}
	// Dynamic: assume it may resolve to a mutation.
	return true
}

// writer handles writing batches of messages to a single Iceberg table.
type writer struct {
	table                 *table.Table
	committer             *committer
	caseSensitive         bool
	writerOpts            []parquet.WriterOption
	resolver              *typeResolver
	requireSchemaMetadata bool
	rowOpCfg              RowOpConfig
	metrics               *opMetrics
	logger                *service.Logger

	// coerceLoggedFieldIDs tracks the iceberg field IDs we have already
	// logged a coerce-on-write notice for, so that a long-running writer
	// emits the divergence between schema metadata and existing column
	// type once per column rather than per batch. Single-goroutine access
	// per writer instance so no synchronisation is needed.
	coerceLoggedFieldIDs map[int]struct{}
}

// NewWriter creates a new writer for a specific table.
// The table and committer should use separate table references since they
// operate in different goroutines and the table object is mutable.
// caseSensitive controls how message keys are matched against the schema.
// resolver supplies optional per-message schema metadata used by the shredder
// to interpret numeric inputs into time-typed columns; pass nil to disable.
// requireSchemaMetadata enables shredder strict mode — see
// [shredder.RecordShredder.StrictTemporalMode].
func NewWriter(tbl *table.Table, comm *committer, caseSensitive bool, writerOpts []parquet.WriterOption, resolver *typeResolver, requireSchemaMetadata bool, rowOpCfg RowOpConfig, logger *service.Logger) *writer {
	return &writer{
		table:                 tbl,
		committer:             comm,
		caseSensitive:         caseSensitive,
		writerOpts:            writerOpts,
		resolver:              resolver,
		requireSchemaMetadata: requireSchemaMetadata,
		rowOpCfg:              rowOpCfg,
		logger:                logger,
		coerceLoggedFieldIDs:  map[int]struct{}{},
	}
}

// Write writes a batch of messages to the table.
func (w *writer) Write(ctx context.Context, batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}

	// Append-only fast path: when the configuration can never produce a
	// non-insert operation, write the whole batch as data files exactly as
	// before, with no per-message operation evaluation.
	if !w.rowOpCfg.mutating() {
		files, err := w.writeDataFiles(ctx, batch)
		if err != nil {
			return err
		}
		if err := w.committer.Commit(ctx, CommitInput{Files: files, SchemaID: w.table.Schema().ID}); err != nil {
			w.cleanupFiles(ctx, files)
			return fmt.Errorf("committing: %w", err)
		}
		return nil
	}

	// Row-level mutations: split the batch by operation, write inserted rows as
	// data files and deleted/upserted keys as equality-delete files, then commit
	// them together so a single snapshot reflects the whole batch.
	inserts, deletes, err := w.splitByOperation(batch)
	if err != nil {
		return err
	}

	var files, deleteFiles []iceberg.DataFile
	if len(inserts) > 0 {
		if files, err = w.writeDataFiles(ctx, inserts); err != nil {
			return err
		}
	}
	if len(deletes) > 0 {
		if deleteFiles, err = w.writeEqualityDeletes(ctx, deletes); err != nil {
			return err
		}
	}
	if len(files) == 0 && len(deleteFiles) == 0 {
		return nil
	}
	if err := w.committer.Commit(ctx, CommitInput{Files: files, DeleteFiles: deleteFiles, SchemaID: w.table.Schema().ID}); err != nil {
		w.cleanupFiles(ctx, files, deleteFiles)
		return fmt.Errorf("committing: %w", err)
	}
	return nil
}

// cleanupFiles best-effort removes written-but-uncommitted parquet files after a
// failed commit, to limit orphaned objects in storage. Failures are logged at
// debug and otherwise ignored (table maintenance / remove_orphan_files remains
// the backstop).
func (w *writer) cleanupFiles(ctx context.Context, groups ...[]iceberg.DataFile) {
	fs, err := w.table.FS(ctx)
	if err != nil {
		return
	}
	for _, group := range groups {
		for _, f := range group {
			if rmErr := fs.Remove(f.FilePath()); rmErr != nil {
				w.logger.Debugf("Failed to clean up uncommitted file %q: %v", f.FilePath(), rmErr)
			}
		}
	}
}

// writeDataFiles shreds a batch into parquet data files, writes them to table
// storage and returns the resulting iceberg data files (uncommitted).
func (w *writer) writeDataFiles(ctx context.Context, batch service.MessageBatch) ([]iceberg.DataFile, error) {
	// Convert messages to parquet (grouped by partition)
	parquetFiles, err := w.messagesToParquet(batch)
	if err != nil {
		return nil, fmt.Errorf("converting messages to parquet: %w", err)
	}

	// Get location provider for the table
	locProvider, err := w.table.LocationProvider()
	if err != nil {
		return nil, fmt.Errorf("getting location provider: %w", err)
	}

	// Write file using table's IO
	tableIO, err := w.table.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting table IO: %w", err)
	}
	writeIO, ok := tableIO.(icebergio.WriteFileIO)
	if !ok {
		return nil, fmt.Errorf("table IO does not support writing (got %T)", tableIO)
	}

	// Build field ID mappings for stats extraction and partition data
	_, fieldToCol, err := icebergx.BuildParquetSchema(w.table.Schema())
	if err != nil {
		return nil, fmt.Errorf("building parquet schema: %w", err)
	}
	colToFieldID := icebergx.ReverseFieldIDMap(fieldToCol)
	fieldIDToLogicalType, fieldIDToFixedSize := icebergx.PartitionFieldMaps(w.table.Spec(), w.table.Schema())

	// Write each partition file
	var files []iceberg.DataFile
	for _, pf := range parquetFiles {
		fileName := uuid.New().String() + ".parquet"
		// Generate data file path (partition path is empty for unpartitioned tables)
		var filePath string
		if len(pf.partitionKey) == 0 {
			filePath = locProvider.NewDataLocation(fileName)
		} else {
			partitionPath, err := icebergx.PartitionKeyToPath(w.table.Spec(), pf.partitionKey)
			if err != nil {
				return nil, fmt.Errorf("unable to compute partition key path: %w", err)
			}
			filePath = locProvider.NewDataLocation(path.Join(partitionPath, fileName))
		}

		if err := writeIO.WriteFile(filePath, pf.result.data); err != nil {
			return nil, fmt.Errorf("writing parquet file %q: %w", filePath, err)
		}

		w.logger.Debugf("Wrote parquet file: %s (%d bytes, %d rows)", filePath, len(pf.result.data), pf.result.footer.NumRows)

		// Extract partition data from key
		fieldIDToPartitionData := icebergx.PartitionDataFromKey(w.table.Spec(), pf.partitionKey)

		builder, err := iceberg.NewDataFileBuilder(
			w.table.Spec(),
			iceberg.EntryContentData,
			filePath,
			iceberg.ParquetFile,
			fieldIDToPartitionData,
			fieldIDToLogicalType,
			fieldIDToFixedSize,
			pf.result.footer.NumRows,
			int64(len(pf.result.data)),
		)
		if err != nil {
			return nil, fmt.Errorf("unable to create data file builder: %w", err)
		}

		// Extract parquet statistics
		stats, err := icebergx.ExtractParquetStats(pf.result.footer, w.table.Schema(), colToFieldID)
		if err != nil {
			return nil, fmt.Errorf("extracting parquet stats: %w", err)
		}
		builder = builder.
			ColumnSizes(stats.ColumnSizes).
			ValueCounts(stats.ValueCounts).
			NullValueCounts(stats.NullValueCounts).
			LowerBoundValues(stats.LowerBounds).
			UpperBoundValues(stats.UpperBounds).
			SplitOffsets(stats.SplitOffsets)

		files = append(files, builder.Build())
	}

	return files, nil
}

// splitByOperation partitions a batch by its per-message row_operation. Rows to
// insert go into inserts; messages whose identifier key should be equality-
// deleted go into deletes. Unknown operations, and upsert/delete without
// identifier_fields, are hard errors so a misconfiguration fails loudly rather
// than silently dropping or duplicating rows.
//
// Keyed operations (upsert/delete) are collapsed per identifier key to the last
// one in batch order. This is required for correctness: merge-on-read equality
// deletes only remove rows from earlier snapshots, never rows added in the same
// commit, so emitting more than one keyed operation per key in a single commit
// would otherwise leave duplicates (repeated upserts) or fail to delete (a
// delete following a same-batch upsert). `insert` is an unconditional append
// and is deliberately not keyed; mixing it with upsert/delete on the same key
// within one batch is not de-duplicated (map create events to `upsert`, not
// `insert`, for keyed data).
func (w *writer) splitByOperation(batch service.MessageBatch) (service.MessageBatch, service.MessageBatch, error) {
	var inserts service.MessageBatch

	type keyedOp struct {
		op  rowOperation
		msg *service.Message
	}
	order := make([]string, 0, len(batch))
	latest := make(map[string]keyedOp, len(batch))

	var nInsert, nUpsert, nDelete int64
	for i, msg := range batch {
		opStr, err := w.rowOpCfg.Operation.TryString(msg)
		if err != nil {
			return nil, nil, fmt.Errorf("evaluating %s for message %d: %w", ioFieldRowOperation, i, err)
		}
		if opStr == "" {
			opStr = string(rowOpInsert)
		}
		op, err := parseRowOperation(opStr)
		if err != nil {
			return nil, nil, err
		}
		if op == rowOpInsert {
			nInsert++
			inserts = append(inserts, msg)
			continue
		}
		if op == rowOpUpsert {
			nUpsert++
		} else {
			nDelete++
		}
		if len(w.rowOpCfg.IdentifierFields) == 0 {
			return nil, nil, fmt.Errorf("%s %q requires %s to be set", ioFieldRowOperation, op, ioFieldIdentifierFields)
		}
		key, err := w.dedupKey(msg)
		if err != nil {
			return nil, nil, fmt.Errorf("message %d: %w", i, err)
		}
		if _, seen := latest[key]; !seen {
			order = append(order, key)
		}
		latest[key] = keyedOp{op: op, msg: msg}
	}

	deletes := make(service.MessageBatch, 0, len(order))
	for _, key := range order {
		ko := latest[key]
		// Every keyed operation removes any prior version of the row.
		deletes = append(deletes, ko.msg)
		// An upsert also (re)writes the row; a delete does not.
		if ko.op == rowOpUpsert {
			inserts = append(inserts, ko.msg)
		}
	}

	if w.metrics != nil {
		w.metrics.inserted.Incr(nInsert)
		w.metrics.upserted.Incr(nUpsert)
		w.metrics.deleted.Incr(nDelete)
	}
	return inserts, deletes, nil
}

// dedupKey builds a stable identity string from a message's identifier_fields
// values, used to collapse multiple keyed operations on the same row within a
// batch. A missing or null identifier value is a hard error.
func (w *writer) dedupKey(msg *service.Message) (string, error) {
	structured, err := msg.AsStructured()
	if err != nil {
		return "", fmt.Errorf("reading structured message for delete key: %w", err)
	}
	row, ok := structured.(map[string]any)
	if !ok {
		return "", fmt.Errorf("message for upsert/delete must be an object, got %T", structured)
	}
	values := make([]any, len(w.rowOpCfg.IdentifierFields))
	for i, name := range w.rowOpCfg.IdentifierFields {
		v, ok := lookupField(row, name, w.caseSensitive)
		if !ok || v == nil {
			return "", fmt.Errorf("%s %q is missing or null", ioFieldIdentifierFields, name)
		}
		values[i] = v
	}
	b, err := json.Marshal(values)
	if err != nil {
		return "", fmt.Errorf("encoding delete key: %w", err)
	}
	return string(b), nil
}

// deleteKeyJSONValue converts a Go identifier value into a JSON-encodable form
// that array.RecordFromJSON parses losslessly for the column's iceberg type.
// Integers and decimals are emitted as strings to avoid the float64 precision
// loss that JSON number parsing would otherwise incur, and temporal time.Time
// values are formatted to the canonical string forms the Arrow JSON reader
// accepts. Other primitives (string, bool, float, binary, uuid) pass through
// json.Marshal unchanged.
func deleteKeyJSONValue(t iceberg.Type, v any) (any, error) {
	switch t.(type) {
	case iceberg.Int32Type, iceberg.Int64Type:
		switch n := v.(type) {
		case json.Number:
			return n.String(), nil
		case int:
			return strconv.FormatInt(int64(n), 10), nil
		case int32:
			return strconv.FormatInt(int64(n), 10), nil
		case int64:
			return strconv.FormatInt(n, 10), nil
		case float64:
			if n != math.Trunc(n) {
				return nil, fmt.Errorf("integer column given non-integer value %v", n)
			}
			return strconv.FormatInt(int64(n), 10), nil
		case string:
			return n, nil
		default:
			return nil, fmt.Errorf("unsupported value type %T for integer column", v)
		}
	case iceberg.DecimalType:
		// Format to the column's exact scale so the encoded key matches the
		// stored decimal; the shortest float representation would not.
		scale := t.(iceberg.DecimalType).Scale()
		switch n := v.(type) {
		case json.Number:
			return n.String(), nil
		case string:
			return n, nil
		case float64:
			return strconv.FormatFloat(n, 'f', scale, 64), nil
		case int:
			return strconv.Itoa(n), nil
		case int64:
			return strconv.FormatInt(n, 10), nil
		default:
			return nil, fmt.Errorf("unsupported value type %T for decimal column", v)
		}
	// Temporal keys must arrive as time.Time. A bare number is ambiguous (the
	// data path interprets it via schema_metadata or a seconds fallback, which
	// the delete path cannot reproduce), so accepting one would silently fail to
	// match the intended rows — reject it loudly instead.
	case iceberg.DateType:
		if tm, ok := v.(time.Time); ok {
			return tm.UTC().Format("2006-01-02"), nil
		}
		return nil, fmt.Errorf("date identifier column requires a time value, got %T", v)
	case iceberg.TimeType:
		if tm, ok := v.(time.Time); ok {
			return tm.UTC().Format("15:04:05.999999999"), nil
		}
		return nil, fmt.Errorf("time identifier column requires a time value, got %T", v)
	case iceberg.TimestampType, iceberg.TimestampTzType:
		if tm, ok := v.(time.Time); ok {
			return tm.UTC().Format(time.RFC3339Nano), nil
		}
		return nil, fmt.Errorf("timestamp identifier column requires a time value, got %T (a bare numeric timestamp is ambiguous as a delete key — convert it to a timestamp upstream)", v)
	default:
		return v, nil
	}
}

// deleteRecordFields returns the schema fields that must appear in an
// equality-delete record — the identifier fields (the equality key) plus, for
// partitioned tables, the partition source columns needed to route each delete
// to its partition — together with the identifier field IDs (the equality key).
// The field list is ordered and de-duplicated (an identifier field that is also
// a partition source appears once). Every contributing column must be a
// primitive type (a fundamental Iceberg constraint on identifier fields).
func (w *writer) deleteRecordFields(tableSchema *iceberg.Schema, spec *iceberg.PartitionSpec) ([]iceberg.NestedField, []int, error) {
	seen := map[int]struct{}{}
	var fields []iceberg.NestedField
	eqFieldIDs := make([]int, 0, len(w.rowOpCfg.IdentifierFields))

	for _, name := range w.rowOpCfg.IdentifierFields {
		field, ok := tableSchema.FindFieldByName(name)
		if !ok && !w.caseSensitive {
			field, ok = tableSchema.FindFieldByNameCaseInsensitive(name)
		}
		if !ok {
			return nil, nil, fmt.Errorf("%s column %q not found in table schema", ioFieldIdentifierFields, name)
		}
		if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
			return nil, nil, fmt.Errorf("%s column %q has non-primitive type %s; equality-delete keys must be primitive", ioFieldIdentifierFields, field.Name, field.Type)
		}
		switch field.Type.(type) {
		case iceberg.Float32Type, iceberg.Float64Type:
			// Floating-point equality is unreliable (NaN, -0.0, rounding), so a
			// float key would intermittently fail to match — Iceberg disallows
			// floats as identifier fields for the same reason.
			return nil, nil, fmt.Errorf("%s column %q has floating-point type %s, which is not a valid equality-delete key", ioFieldIdentifierFields, field.Name, field.Type)
		}
		eqFieldIDs = append(eqFieldIDs, field.ID)
		if _, dup := seen[field.ID]; !dup {
			seen[field.ID] = struct{}{}
			fields = append(fields, field)
		}
	}

	// Equality deletes are partition-scoped: a delete only matches data in the
	// same partition. If a partition is derived from a column outside
	// identifier_fields, that column can differ between a row's insert and a
	// later upsert/delete, routing the delete to the wrong partition and silently
	// missing the row. Require every partition source column to be an identifier
	// field (so it is part of the key and cannot vary for a given key). This
	// matches Iceberg/Flink's rule that partition sources must be a subset of the
	// equality fields. Identifier partition sources are already in `fields`.
	for _, pf := range spec.Fields() {
		for _, srcID := range pf.SourceIDs {
			if _, ok := seen[srcID]; ok {
				continue
			}
			name, _ := tableSchema.FindColumnName(srcID)
			return nil, nil, fmt.Errorf("table is partitioned by column %q (field %d), which is not in %s; upsert/delete is only supported when every partition source column is an identifier field (equality deletes are partition-scoped)", name, srcID, ioFieldIdentifierFields)
		}
	}

	return fields, eqFieldIDs, nil
}

// writeEqualityDeletes builds equality-delete files for the given messages,
// keyed on identifier_fields. The identifier columns are projected into Arrow
// records and written via the iceberg-go equality-delete writer, which owns the
// on-disk delete-file format. The returned files are uncommitted.
func (w *writer) writeEqualityDeletes(ctx context.Context, msgs service.MessageBatch) ([]iceberg.DataFile, error) {
	tableSchema := w.table.Schema()
	spec := w.table.Spec()

	delFields, eqFieldIDs, err := w.deleteRecordFields(tableSchema, &spec)
	if err != nil {
		return nil, err
	}

	// Project each message to its identifier columns, keyed by the canonical
	// schema field name so the JSON keys line up with the Arrow schema.
	rows := make([]map[string]any, 0, len(msgs))
	for i, msg := range msgs {
		structured, err := msg.AsStructured()
		if err != nil {
			return nil, fmt.Errorf("reading structured message %d for delete key: %w", i, err)
		}
		row, ok := structured.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("message %d for upsert/delete must be an object, got %T", i, structured)
		}
		key := make(map[string]any, len(delFields))
		for _, field := range delFields {
			v, ok := lookupField(row, field.Name, w.caseSensitive)
			if !ok || v == nil {
				return nil, fmt.Errorf("%s %q is missing or null in message %d", ioFieldIdentifierFields, field.Name, i)
			}
			jv, err := deleteKeyJSONValue(field.Type, v)
			if err != nil {
				return nil, fmt.Errorf("%s %q in message %d: %w", ioFieldIdentifierFields, field.Name, i, err)
			}
			key[field.Name] = jv
		}
		rows = append(rows, key)
	}

	jsonRows, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("encoding delete keys: %w", err)
	}

	delSchema := iceberg.NewSchema(0, delFields...)
	arrowSc, err := table.SchemaToArrowSchema(delSchema, nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("building delete arrow schema: %w", err)
	}

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSc, bytes.NewReader(jsonRows))
	if err != nil {
		return nil, fmt.Errorf("building delete records: %w", err)
	}
	defer rec.Release()

	records := func(yield func(arrow.RecordBatch, error) bool) {
		yield(rec, nil)
	}

	// WriteEqualityDeletes is functionally pure w.r.t. the transaction (it only
	// reads metadata and writes files), so a throwaway transaction is safe; the
	// committer commits the returned files via a RowDelta.
	deleteFiles, err := w.table.NewTransaction().WriteEqualityDeletes(ctx, eqFieldIDs, records)
	if err != nil {
		return nil, fmt.Errorf("writing equality deletes: %w", err)
	}
	return deleteFiles, nil
}

// lookupField finds a value in a structured row by column name, honouring case
// sensitivity the same way the shredder matches message keys to columns.
func lookupField(row map[string]any, name string, caseSensitive bool) (any, bool) {
	if v, ok := row[name]; ok {
		return v, true
	}
	if caseSensitive {
		return nil, false
	}
	for k, v := range row {
		if strings.EqualFold(k, name) {
			return v, true
		}
	}
	return nil, false
}

// parquetResult holds the output of parquet conversion for a partition.
type parquetResult struct {
	data   []byte
	footer *format.FileMetaData
}

// partitionFile pairs a partition path with its parquet data.
type partitionFile struct {
	partitionKey icebergx.PartitionKey
	result       parquetResult
}

// messagesToParquet converts messages to parquet format using the shredder.
// Returns a slice of partition files. For unpartitioned tables, returns a single
// file with an empty path.
func (w *writer) messagesToParquet(batch service.MessageBatch) ([]partitionFile, error) {
	schema := w.table.Schema()
	spec := w.table.Spec()

	// Build parquet schema and field ID to column index mapping
	pqSchema, fieldToCol, err := icebergx.BuildParquetSchema(schema)
	if err != nil {
		return nil, fmt.Errorf("building parquet schema: %w", err)
	}

	// Build sourceID -> partition index map
	partitionSourceIDs := make(map[int]int)
	for i := 0; i < spec.NumFields(); i++ {
		field := spec.Field(i)
		partitionSourceIDs[field.SourceID()] = i
	}
	numPartitionFields := spec.NumFields()

	// Create shredder for the schema. When schema metadata is configured
	// and the first message in the batch carries it, use it to inform the
	// shredder's numeric-to-temporal conversion. Schema metadata is the
	// authoritative source for "this BIGINT-shaped value is actually
	// timestamp-millis" and prevents the year-50000 silent corruption
	// that bloblang.ValueAsTimestamp's seconds-default would otherwise
	// produce.
	//
	// We sample the schema metadata from batch[0] only and apply it to
	// every message in the batch. Connect's iceberg router groups by
	// (namespace, table) before reaching this method, so messages in a
	// batch share a destination table and — in every supported upstream
	// (schema-registry decode, parquet decode, single-source streams) —
	// a single schema. If a future upstream genuinely interleaves
	// different schema metadata into one batch, this assumption breaks
	// silently for messages 1..N; in that case the writer must be
	// extended to per-message metadata lookup with a small cache.
	rs := shredder.NewRecordShredder(schema, w.caseSensitive)
	rs.StrictTemporalMode = w.requireSchemaMetadata
	if w.resolver != nil && len(batch) > 0 {
		if common, err := w.resolver.parseSchemaMetadata(batch[0]); err != nil {
			w.logger.Warnf("parsing schema metadata for shredder: %v (falling back to schema-agnostic conversion)", err)
		} else if common != nil {
			fieldCommons := buildShredderFieldCommons(schema, common, w.caseSensitive)
			w.logCoerceDecisions(schema, fieldCommons)
			rs.SetFieldSchemaMetadata(fieldCommons)
		}
	}

	// For unpartitioned tables, use a single writer
	if spec.IsUnpartitioned() {
		sink := newParquetSink(pqSchema, fieldToCol, w.caseSensitive, w.writerOpts...)

		for _, msg := range batch {
			structured, err := msg.AsStructured()
			if err != nil {
				return nil, fmt.Errorf("parsing message as structured: %w", err)
			}

			row, ok := structured.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("message is not an object, got %T", structured)
			}

			if err := rs.Shred(row, sink); err != nil {
				return nil, fmt.Errorf("shredding record: %w", err)
			}

			if err := sink.flush(); err != nil {
				return nil, fmt.Errorf("flushing row: %w", err)
			}
		}

		// Check for schema evolution before closing
		if newFields := sink.newFieldErrors(); len(newFields) > 0 {
			return nil, NewBatchSchemaEvolutionError(newFields)
		}

		result, err := sink.Close()
		if err != nil {
			return nil, fmt.Errorf("closing parquet writer: %w", err)
		}

		return []partitionFile{{partitionKey: nil, result: result}}, nil
	}

	// For partitioned tables, route rows to different writers
	// Use sorted slice with binary search (keyed by full partition key, not truncated path)
	type partitionEntry struct {
		key  icebergx.PartitionKey
		sink *parquetSink
	}
	var partitions []*partitionEntry

	// Create a buffering sink to capture values and partition key
	bufferSink := newBufferingSink(partitionSourceIDs, numPartitionFields, w.caseSensitive)

	for _, msg := range batch {
		structured, err := msg.AsStructured()
		if err != nil {
			return nil, fmt.Errorf("parsing message as structured: %w", err)
		}

		row, ok := structured.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("message is not an object, got %T", structured)
		}

		// Shred to buffer (captures values and partition key in one pass)
		bufferSink.reset()
		if err := rs.Shred(row, bufferSink); err != nil {
			return nil, fmt.Errorf("shredding record: %w", err)
		}

		// Compute partition key
		partitionKey, err := icebergx.NewPartitionKey(spec, schema, bufferSink.partitionValues)
		if err != nil {
			return nil, fmt.Errorf("computing partition key: %w", err)
		}

		idx, found := slices.BinarySearchFunc(partitions, partitionKey, func(e *partitionEntry, k icebergx.PartitionKey) int {
			return e.key.Compare(k)
		})

		var entry *partitionEntry
		if found {
			entry = partitions[idx]
		} else {
			entry = &partitionEntry{
				key:  partitionKey,
				sink: newParquetSink(pqSchema, fieldToCol, w.caseSensitive, w.writerOpts...),
			}
			// Insert at sorted position
			partitions = slices.Insert(partitions, idx, entry)
		}

		// Write buffered values to the correct partition
		if err := bufferSink.writeTo(entry.sink); err != nil {
			return nil, fmt.Errorf("writing row to partition: %w", err)
		}
	}

	// Check for schema evolution before closing partition sinks
	if newFields := bufferSink.newFieldErrors(); len(newFields) > 0 {
		return nil, NewBatchSchemaEvolutionError(newFields)
	}

	// Close all partition sinks and collect results (compute paths now)
	results := make([]partitionFile, 0, len(partitions))
	for _, entry := range partitions {
		result, err := entry.sink.Close()
		if err != nil {
			return nil, fmt.Errorf("closing parquet writer: %w", err)
		}
		results = append(results, partitionFile{partitionKey: entry.key, result: result})
	}

	return results, nil
}

// Close closes the writer and its committer.
func (w *writer) Close() {
	w.committer.Close()
}

// logCoerceDecisions inspects the resolved fieldID → schema.Common map
// against the live iceberg schema and emits one INFO-level log per field
// whose declared upstream type would have produced a different iceberg
// column type than the one the table currently holds.
//
// The intended audience is operators rolling out the #4399 metadata fix
// over existing tables whose columns were created under the pre-fix
// (degraded) metadata shape: the log surface tells them which columns the
// shredder is silently coerce-converting on write, so they can choose to
// rebuild the affected tables when they want native temporal columns.
//
// Per-field dedup is via w.coerceLoggedFieldIDs so a long-running writer
// emits each notice once, not per-batch.
func (w *writer) logCoerceDecisions(s *iceberg.Schema, fieldCommons map[int]*schema.Common) {
	if w.logger == nil || len(fieldCommons) == 0 {
		return
	}
	icebergTypeByID := map[int]iceberg.Type{}
	collectLeafIcebergTypes(s.AsStruct().FieldList, icebergTypeByID)

	for fieldID, common := range fieldCommons {
		if _, already := w.coerceLoggedFieldIDs[fieldID]; already {
			continue
		}
		existingType, ok := icebergTypeByID[fieldID]
		if !ok {
			continue
		}
		// The type inferrer is used by commonTypeToIcebergType only for
		// nested struct/list ID allocation, which we don't care about
		// when comparing leaf primitives — a throwaway inferrer is fine.
		impliedType, err := commonTypeToIcebergType(common, newTypeInferrer(w.caseSensitive))
		if err != nil || impliedType == nil {
			continue
		}
		if reflect.DeepEqual(existingType, impliedType) {
			continue
		}
		fieldName := lookupFieldName(s, fieldID)
		if w.requireSchemaMetadata {
			w.logger.Infof(
				"iceberg: field %q has existing column type %s but schema metadata declares %s; require_schema_metadata=true will reject writes for this column. Recreate the table to migrate to %s.",
				fieldName, existingType.String(), impliedType.String(), impliedType.String(),
			)
		} else {
			w.logger.Infof(
				"iceberg: coercing field %q on write: existing column type %s does not match the type implied by schema metadata (%s). "+
					"Values will be written using the existing column type. Recreate the table to migrate to %s.",
				fieldName, existingType.String(), impliedType.String(), impliedType.String(),
			)
		}
		w.coerceLoggedFieldIDs[fieldID] = struct{}{}
	}
}

// collectLeafIcebergTypes recursively walks the iceberg schema, populating
// out with fieldID → leaf type for every primitive-typed field.
func collectLeafIcebergTypes(fields []iceberg.NestedField, out map[int]iceberg.Type) {
	for _, f := range fields {
		switch t := f.Type.(type) {
		case *iceberg.StructType:
			collectLeafIcebergTypes(t.FieldList, out)
		case *iceberg.ListType:
			// Lists wrap a single element; treat the element as a leaf
			// candidate for completeness, even though primitive-element
			// lists are the common case.
			out[t.ElementID] = t.Element
			if st, ok := t.Element.(*iceberg.StructType); ok {
				collectLeafIcebergTypes(st.FieldList, out)
			}
		case *iceberg.MapType:
			out[t.ValueID] = t.ValueType
			if st, ok := t.ValueType.(*iceberg.StructType); ok {
				collectLeafIcebergTypes(st.FieldList, out)
			}
		default:
			out[f.ID] = f.Type
		}
	}
}

// lookupFieldName returns the human-readable name for an iceberg fieldID,
// or a synthesized "field_<id>" if the schema doesn't expose it directly.
func lookupFieldName(s *iceberg.Schema, fieldID int) string {
	if name, ok := s.FindColumnName(fieldID); ok {
		return name
	}
	return fmt.Sprintf("field_%d", fieldID)
}

// buildShredderFieldCommons walks an iceberg schema and the parallel
// schema.Common metadata that produced it, returning a fieldID → *schema.Common
// map keyed by the iceberg field IDs of every leaf column. The shredder
// consults this to interpret numeric inputs into time-typed columns using
// the unit/AdjustToUTC semantics declared by the upstream schema.
//
// Only leaf-level entries are emitted; struct/list/map containers are
// recursed into. Field matching uses the same case-sensitivity rule as
// shredding so the lookup behaves consistently with how values are routed.
// Fields present in the iceberg schema but absent from the metadata are
// skipped — those columns either pre-date the metadata or were added by
// schema evolution from a different source.
func buildShredderFieldCommons(s *iceberg.Schema, root *schema.Common, caseSensitive bool) map[int]*schema.Common {
	if root == nil {
		return nil
	}
	out := make(map[int]*schema.Common)
	visitIcebergSchemaFields(s.AsStruct().FieldList, root, caseSensitive, out)
	if len(out) == 0 {
		return nil
	}
	return out
}

func visitIcebergSchemaFields(fields []iceberg.NestedField, parent *schema.Common, caseSensitive bool, out map[int]*schema.Common) {
	if parent == nil || parent.Type != schema.Object {
		return
	}
	for _, f := range fields {
		child := lookupCommonChild(parent, f.Name, caseSensitive)
		if child == nil {
			continue
		}
		recordOrRecurseIcebergField(f.ID, f.Type, child, caseSensitive, out)
	}
}

// recordOrRecurseIcebergField is the leaf-vs-recurse decision for a single
// iceberg type/common pair. Leaves are registered in out; container types
// (struct, list, map) are recursed into so their leaf descendants pick up
// metadata too.
//
// When the iceberg-side container shape and the common-side type don't
// agree (e.g. iceberg has ListType but the common says Object), we skip
// rather than blindly consume children of the wrong shape. The shredder
// then falls back to the historical schema-agnostic conversion for those
// fields, which is the safe loss-of-precision rather than misinterpreting.
func recordOrRecurseIcebergField(fieldID int, typ iceberg.Type, common *schema.Common, caseSensitive bool, out map[int]*schema.Common) {
	switch t := typ.(type) {
	case *iceberg.StructType:
		if common.Type != schema.Object {
			return
		}
		visitIcebergSchemaFields(t.FieldList, common, caseSensitive, out)
	case *iceberg.ListType:
		// A schema.Common array carries the element schema as its single
		// child; skip if the shape doesn't match.
		if common.Type != schema.Array || len(common.Children) != 1 {
			return
		}
		recordOrRecurseIcebergField(t.ElementID, t.Element, &common.Children[0], caseSensitive, out)
	case *iceberg.MapType:
		// schema.Common maps are encoded as type Map with a single child
		// representing the value schema. Keys are always primitives in
		// our model, so they don't need metadata. Recurse into the value.
		if common.Type != schema.Map || len(common.Children) != 1 {
			return
		}
		recordOrRecurseIcebergField(t.ValueID, t.ValueType, &common.Children[0], caseSensitive, out)
	default:
		// Leaf — register the metadata so the shredder can consult it.
		out[fieldID] = common
	}
}

func lookupCommonChild(parent *schema.Common, name string, caseSensitive bool) *schema.Common {
	for i := range parent.Children {
		ch := &parent.Children[i]
		if caseSensitive {
			if ch.Name == name {
				return ch
			}
		} else if strings.EqualFold(ch.Name, name) {
			return ch
		}
	}
	return nil
}

// parquetColumn holds state for writing to a single parquet column.
type parquetColumn struct {
	writer *parquet.ColumnWriter
	colIdx int             // column index for parquet.Value.Level()
	values []parquet.Value // accumulated values for current row
}

// parquetSink implements shredder.Sink and writes values directly to column writers.
type parquetSink struct {
	buffer        *bytes.Buffer
	writer        *parquet.GenericWriter[any]
	columns       map[int]*parquetColumn // field ID -> column state
	rowCount      int
	caseSensitive bool

	// newFields collects unknown fields discovered during shredding for schema evolution.
	newFields  []*UnknownFieldError
	seenFields map[string]struct{} // dedup by full path
}

func newParquetSink(pqSchema *parquet.Schema, fieldToCol map[int]int, caseSensitive bool, writerOpts ...parquet.WriterOption) *parquetSink {
	buf := bytes.NewBuffer(nil)
	allOpts := make([]parquet.WriterOption, 0, 1+len(writerOpts))
	allOpts = append(allOpts, pqSchema)
	allOpts = append(allOpts, writerOpts...)
	pw := parquet.NewGenericWriter[any](buf, allOpts...)
	colWriters := pw.ColumnWriters()

	columns := make(map[int]*parquetColumn, len(fieldToCol))
	for fieldID, colIdx := range fieldToCol {
		columns[fieldID] = &parquetColumn{
			writer: colWriters[colIdx],
			colIdx: colIdx,
			values: make([]parquet.Value, 0, 8),
		}
	}
	return &parquetSink{
		buffer:        buf,
		writer:        pw,
		columns:       columns,
		caseSensitive: caseSensitive,
		newFields:     nil, // allocated lazily
	}
}

func (s *parquetSink) EmitValue(sv shredder.ShreddedValue) error {
	col, ok := s.columns[sv.FieldID]
	if !ok {
		return fmt.Errorf("unknown field ID: %d", sv.FieldID)
	}

	// Append the value with rep/def levels set
	val := sv.Value.Level(sv.RepLevel, sv.DefLevel, col.colIdx)
	col.values = append(col.values, val)

	return nil
}

func (s *parquetSink) OnNewField(parentPath icebergx.Path, name string, value any) {
	if !s.caseSensitive {
		name = strings.ToLower(name)
	}
	fe := NewUnknownFieldError(parentPath, name, value)
	key := dedupKey(fe.FullPath().String(), s.caseSensitive)
	if _, ok := s.seenFields[key]; ok {
		return
	}
	if s.seenFields == nil {
		s.seenFields = make(map[string]struct{})
	}
	s.seenFields[key] = struct{}{}
	s.newFields = append(s.newFields, fe)
}

// newFieldErrors returns the collected new field errors.
func (s *parquetSink) newFieldErrors() []*UnknownFieldError {
	return s.newFields
}

// flush writes the current row to column writers and increments the row count.
func (s *parquetSink) flush() error {
	for _, col := range s.columns {
		if _, err := col.writer.WriteRowValues(col.values); err != nil {
			return fmt.Errorf("writing to column %d: %w", col.colIdx, err)
		}
		col.values = col.values[:0]
	}
	s.rowCount++
	return nil
}

// Close closes the parquet writer and returns the result.
func (s *parquetSink) Close() (parquetResult, error) {
	if err := s.writer.Close(); err != nil {
		return parquetResult{}, err
	}
	return parquetResult{
		data:   s.buffer.Bytes(),
		footer: s.writer.File().Metadata(),
	}, nil
}

// bufferingSink captures shredded values and partition keys for later replay.
// This allows shredding once and then routing to the correct partition writer.
type bufferingSink struct {
	values             []shredder.ShreddedValue // buffered values in emission order
	partitionSourceIDs map[int]int              // sourceFieldID -> partition field index
	partitionValues    []parquet.Value          // captured partition values
	caseSensitive      bool

	// newFields collects unknown fields discovered during shredding for schema evolution.
	newFields  []*UnknownFieldError
	seenFields map[string]struct{} // dedup by full path
}

func newBufferingSink(partitionSourceIDs map[int]int, numPartitionFields int, caseSensitive bool) *bufferingSink {
	return &bufferingSink{
		values:             make([]shredder.ShreddedValue, 0, 64),
		partitionSourceIDs: partitionSourceIDs,
		partitionValues:    make([]parquet.Value, numPartitionFields),
		caseSensitive:      caseSensitive,
		newFields:          nil, // allocated lazily
	}
}

func (s *bufferingSink) reset() {
	s.values = s.values[:0]
	for i := range s.partitionValues {
		s.partitionValues[i] = parquet.Value{}
	}
	// Don't reset newFields - we want to accumulate across all messages in the batch
}

func (s *bufferingSink) EmitValue(sv shredder.ShreddedValue) error {
	// Buffer the value
	s.values = append(s.values, sv)

	// Capture partition values (only top-level fields, rep level 0)
	if idx, ok := s.partitionSourceIDs[sv.FieldID]; ok && sv.RepLevel == 0 {
		s.partitionValues[idx] = sv.Value
	}

	return nil
}

func (s *bufferingSink) OnNewField(parentPath icebergx.Path, name string, value any) {
	if !s.caseSensitive {
		name = strings.ToLower(name)
	}
	fe := NewUnknownFieldError(parentPath, name, value)
	key := dedupKey(fe.FullPath().String(), s.caseSensitive)
	if _, ok := s.seenFields[key]; ok {
		return
	}
	if s.seenFields == nil {
		s.seenFields = make(map[string]struct{})
	}
	s.seenFields[key] = struct{}{}
	s.newFields = append(s.newFields, fe)
}

// dedupKey returns the key used to dedup new-field errors across messages in
// a batch. In case-insensitive mode it folds to lowercase so two messages
// reporting new fields differing only in case (e.g. "FOO" and "foo") collapse
// to a single schema-evolution attempt instead of racing each other.
func dedupKey(path string, caseSensitive bool) string {
	if caseSensitive {
		return path
	}
	return strings.ToLower(path)
}

// newFieldErrors returns the collected new field errors.
func (s *bufferingSink) newFieldErrors() []*UnknownFieldError {
	return s.newFields
}

// writeTo replays buffered values to the target sink and flushes.
func (s *bufferingSink) writeTo(target *parquetSink) error {
	for _, sv := range s.values {
		if err := target.EmitValue(sv); err != nil {
			return err
		}
	}
	return target.flush()
}
