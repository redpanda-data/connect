// Copyright 2025 Redpanda Data, Inc.
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
	"fmt"
	"time"

	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// writer handles writing batches of messages to a single Iceberg table.
type writer struct {
	table     *table.Table
	committer *committer
	logger    *service.Logger
}

// NewWriter creates a new writer for a specific table.
func NewWriter(tbl *table.Table, logger *service.Logger) (*writer, error) {
	comm, err := newCommitter(tbl, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create committer: %w", err)
	}

	return &writer{
		table:     tbl,
		committer: comm,
		logger:    logger,
	}, nil
}

// Write writes a batch of messages to the table.
func (w *writer) Write(ctx context.Context, batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}

	// Convert messages to parquet
	parquetData, rowCount, err := w.messagesToParquet(batch)
	if err != nil {
		return fmt.Errorf("failed to convert messages to parquet: %w", err)
	}

	// Generate unique file name
	fileName := fmt.Sprintf("%s-%d.parquet", uuid.New().String(), time.Now().UnixNano())

	// Get location provider for the table
	locProvider, err := w.table.LocationProvider()
	if err != nil {
		return fmt.Errorf("failed to get location provider: %w", err)
	}

	// Generate data file path
	filePath := locProvider.NewDataLocation(fileName)

	// Write file using table's IO
	tableIO, err := w.table.FS(ctx)
	if err != nil {
		return fmt.Errorf("failed to get table IO: %w", err)
	}
	writeIO, ok := tableIO.(icebergio.WriteFileIO)
	if !ok {
		return fmt.Errorf("table IO does not support writing (got %T)", tableIO)
	}

	if err := writeIO.WriteFile(filePath, parquetData); err != nil {
		return fmt.Errorf("failed to write parquet file: %w", err)
	}

	w.logger.Debugf("Wrote parquet file: %s (%d bytes, %d rows)", filePath, len(parquetData), rowCount)

	// Submit to committer
	file := dataFile{
		path:     filePath,
		rowCount: int64(rowCount),
		fileSize: int64(len(parquetData)),
	}

	if err := w.committer.Commit(ctx, file); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	return nil
}

// messagesToParquet converts messages to parquet format.
func (w *writer) messagesToParquet(batch service.MessageBatch) ([]byte, int, error) {
	schema := w.table.Schema()

	// Build parquet schema from iceberg schema
	pqSchema, err := icebergSchemaToParquet(schema)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to build parquet schema: %w", err)
	}

	// Convert messages to rows
	rows := make([]map[string]any, 0, len(batch))
	for _, msg := range batch {
		structured, err := msg.AsStructured()
		if err != nil {
			return nil, 0, fmt.Errorf("failed to parse message as structured: %w", err)
		}

		row, ok := structured.(map[string]any)
		if !ok {
			return nil, 0, fmt.Errorf("message is not a map, got %T", structured)
		}

		rows = append(rows, row)
	}

	// Write parquet
	buf := bytes.NewBuffer(nil)
	pw := parquet.NewGenericWriter[map[string]any](buf, pqSchema)

	if _, err := pw.Write(rows); err != nil {
		return nil, 0, fmt.Errorf("failed to write parquet rows: %w", err)
	}

	if err := pw.Close(); err != nil {
		return nil, 0, fmt.Errorf("failed to close parquet writer: %w", err)
	}

	return buf.Bytes(), len(rows), nil
}

// Close closes the writer and its committer.
func (w *writer) Close() {
	if w.committer != nil {
		w.committer.Close()
	}
}

// icebergSchemaToParquet converts an iceberg schema to a parquet schema.
func icebergSchemaToParquet(schema *iceberg.Schema) (*parquet.Schema, error) {
	group := make(parquet.Group)

	for _, field := range schema.Fields() {
		node, err := icebergFieldToParquet(field)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", field.Name, err)
		}
		group[field.Name] = node
	}

	return parquet.NewSchema("iceberg", group), nil
}

// icebergFieldToParquet converts an iceberg field to a parquet node.
// Note: We don't add field IDs because iceberg-go's AddFiles method doesn't support them.
// The AddFiles method resolves field IDs by matching column names to the schema.
func icebergFieldToParquet(field iceberg.NestedField) (parquet.Node, error) {
	node, err := icebergTypeToParquet(field.Type)
	if err != nil {
		return nil, err
	}

	// Add optional wrapper if not required
	if !field.Required {
		node = parquet.Optional(node)
	}

	return node, nil
}

// icebergTypeToParquet converts an iceberg type to a parquet node.
func icebergTypeToParquet(t iceberg.Type) (parquet.Node, error) {
	switch t := t.(type) {
	case iceberg.BooleanType:
		return parquet.Leaf(parquet.BooleanType), nil
	case iceberg.Int32Type:
		return parquet.Int(32), nil
	case iceberg.Int64Type:
		return parquet.Int(64), nil
	case iceberg.Float32Type:
		return parquet.Leaf(parquet.FloatType), nil
	case iceberg.Float64Type:
		return parquet.Leaf(parquet.DoubleType), nil
	case iceberg.StringType:
		return parquet.String(), nil
	case iceberg.BinaryType:
		return parquet.Leaf(parquet.ByteArrayType), nil
	case iceberg.DateType:
		return parquet.Date(), nil
	case iceberg.TimeType:
		return parquet.Time(parquet.Microsecond), nil
	case iceberg.TimestampType:
		return parquet.Timestamp(parquet.Microsecond), nil
	case iceberg.TimestampTzType:
		return parquet.Timestamp(parquet.Microsecond), nil
	case iceberg.UUIDType:
		return parquet.UUID(), nil
	case *iceberg.StructType:
		group := make(parquet.Group)
		for _, f := range t.Fields() {
			node, err := icebergFieldToParquet(f)
			if err != nil {
				return nil, err
			}
			group[f.Name] = node
		}
		return group, nil
	case *iceberg.ListType:
		elem, err := icebergTypeToParquet(t.Element)
		if err != nil {
			return nil, err
		}
		return parquet.List(elem), nil
	case *iceberg.MapType:
		key, err := icebergTypeToParquet(t.KeyType)
		if err != nil {
			return nil, err
		}
		val, err := icebergTypeToParquet(t.ValueType)
		if err != nil {
			return nil, err
		}
		return parquet.Map(key, val), nil
	default:
		return nil, fmt.Errorf("unsupported iceberg type: %T", t)
	}
}
