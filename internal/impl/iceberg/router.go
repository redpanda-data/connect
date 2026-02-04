// Copyright 2025 Redpanda Data, Inc.
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
	"strings"
	"sync"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/catalogx"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
)

// tableKey uniquely identifies an Iceberg table.
type tableKey struct {
	namespace string // dot-separated namespace
	table     string
}

// SchemaEvolutionConfig holds configuration for automatic table creation and schema evolution.
type SchemaEvolutionConfig struct {
	// Enabled controls whether auto-creation and schema evolution are active.
	Enabled bool
	// PartitionSpec is an interpolated string that produces a partition spec expression
	// when evaluated against the first message (e.g., "(year(ts), bucket(16, id))").
	PartitionSpec *service.InterpolatedString
}

const maxSchemaEvolutionRetries = 10

// tableEntry holds a writer and its associated lock for a single table.
// The RWMutex allows concurrent writes (RLock) while serializing
// schema evolution operations (Lock).
type tableEntry struct {
	mu     sync.RWMutex
	writer *writer
}

// router routes message batches to per-table writers.
type router struct {
	catalogCfg   catalogx.Config
	namespaceStr *service.InterpolatedString
	tableStr     *service.InterpolatedString
	schemaEvoCfg SchemaEvolutionConfig

	entries sync.Map // tableKey -> *tableEntry

	logger *service.Logger
}

// NewRouter creates a new router.
func NewRouter(
	catalogCfg catalogx.Config,
	namespaceStr *service.InterpolatedString,
	tableStr *service.InterpolatedString,
	schemaEvoCfg SchemaEvolutionConfig,
	logger *service.Logger,
) *router {
	return &router{
		catalogCfg:   catalogCfg,
		namespaceStr: namespaceStr,
		tableStr:     tableStr,
		schemaEvoCfg: schemaEvoCfg,
		logger:       logger,
	}
}

// getOrCreateEntry returns the entry for a table, creating one if needed.
func (r *router) getOrCreateEntry(key tableKey) *tableEntry {
	if v, ok := r.entries.Load(key); ok {
		return v.(*tableEntry)
	}
	entry := &tableEntry{}
	actual, _ := r.entries.LoadOrStore(key, entry)
	return actual.(*tableEntry)
}

// Route routes a batch of messages to the appropriate writers.
func (r *router) Route(ctx context.Context, batch service.MessageBatch) error {
	// fast path if static namespace + table is used.
	if ns, ok := r.namespaceStr.Static(); ok {
		if tbl, ok := r.tableStr.Static(); ok {
			key := tableKey{namespace: ns, table: tbl}
			return r.writeWithRetry(ctx, key, batch)
		}
	}

	// Group messages by table key
	groups := make(map[tableKey]service.MessageBatch)

	nsExec := batch.InterpolationExecutor(r.namespaceStr)
	tableExec := batch.InterpolationExecutor(r.tableStr)
	for i, msg := range batch {
		ns, err := nsExec.TryString(i)
		if err != nil {
			return fmt.Errorf("failed to interpolate namespace: %w", err)
		}

		tbl, err := tableExec.TryString(i)
		if err != nil {
			return fmt.Errorf("failed to interpolate table: %w", err)
		}

		key := tableKey{namespace: ns, table: tbl}
		groups[key] = append(groups[key], msg)
	}

	// Write each group to its writer with retry loop
	for key, groupBatch := range groups {
		if err := r.writeWithRetry(ctx, key, groupBatch); err != nil {
			return err
		}
	}

	return nil
}

// writeWithRetry writes a batch to a table with retry loop for schema evolution.
func (r *router) writeWithRetry(ctx context.Context, key tableKey, batch service.MessageBatch) error {
	entry := r.getOrCreateEntry(key)

	for range maxSchemaEvolutionRetries {
		err := r.doWrite(ctx, key, entry, batch)
		if err == nil {
			return nil
		}

		// Check if schema evolution is disabled - fail immediately
		if !r.schemaEvoCfg.Enabled {
			return fmt.Errorf("failed to write to %s.%s: %w", key.namespace, key.table, err)
		}

		// Handle specific errors with schema evolution
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			if err := r.createNamespace(ctx, key, entry); err != nil {
				return fmt.Errorf("failed to create namespace %s: %w", key.namespace, err)
			}
			continue
		}

		if errors.Is(err, catalog.ErrNoSuchTable) {
			createErr := r.createTable(ctx, key, batch, entry)
			if createErr != nil {
				// If table creation failed because namespace doesn't exist, create it first
				if errors.Is(createErr, catalog.ErrNoSuchNamespace) {
					if nsErr := r.createNamespace(ctx, key, entry); nsErr != nil {
						return fmt.Errorf("failed to create namespace %s: %w", key.namespace, nsErr)
					}
					// Don't return error, continue retry loop to create table
				} else {
					return fmt.Errorf("failed to create table %s.%s: %w", key.namespace, key.table, createErr)
				}
			}
			continue
		}

		var schemaErr *BatchSchemaEvolutionError
		if errors.As(err, &schemaErr) {
			if err := r.evolveSchema(ctx, key, schemaErr, entry); err != nil {
				return fmt.Errorf("failed to evolve schema for %s.%s: %w", key.namespace, key.table, err)
			}
			continue
		}

		// Unhandled error - fail
		return fmt.Errorf("failed to write to %s.%s: %w", key.namespace, key.table, err)
	}

	return fmt.Errorf("failed to write to %s.%s after %d retries", key.namespace, key.table, maxSchemaEvolutionRetries)
}

// doWrite performs a single write attempt, creating the writer if needed.
func (r *router) doWrite(ctx context.Context, key tableKey, entry *tableEntry, batch service.MessageBatch) error {
	for {
		// Fast path: writer exists, use RLock for concurrent writes
		entry.mu.RLock()
		w := entry.writer
		if w != nil {
			err := w.Write(ctx, batch)
			entry.mu.RUnlock()
			return err
		}
		entry.mu.RUnlock()

		// Slow path: create writer under exclusive lock
		entry.mu.Lock()
		if entry.writer != nil {
			// Another goroutine created it, retry with RLock
			entry.mu.Unlock()
			continue
		}
		w, err := r.createWriter(ctx, key)
		if err != nil {
			entry.mu.Unlock()
			return err
		}
		entry.writer = w
		entry.mu.Unlock()
		// Loop back to write with RLock
	}
}

// createNamespace creates the namespace for a table.
func (r *router) createNamespace(ctx context.Context, key tableKey, entry *tableEntry) error {
	entry.mu.Lock()
	defer entry.mu.Unlock()

	nsParts := strings.Split(key.namespace, ".")
	client, err := catalogx.NewCatalogClient(r.catalogCfg, nsParts)
	if err != nil {
		return fmt.Errorf("failed to create catalog client: %w", err)
	}
	defer client.Close()

	// Check if namespace already exists (race protection)
	exists, err := client.CheckNamespaceExists(ctx)
	if err != nil {
		return fmt.Errorf("failed to check namespace existence: %w", err)
	}
	if exists {
		r.logger.Debugf("Namespace %s already exists (created by another process)", key.namespace)
		return nil
	}

	// Create the namespace
	if err := client.CreateNamespace(ctx, nil); err != nil {
		return err
	}

	r.logger.Infof("Created namespace: %s", key.namespace)
	return nil
}

// createTable creates a new table with schema inferred from the first message.
func (r *router) createTable(ctx context.Context, key tableKey, batch service.MessageBatch, entry *tableEntry) error {
	entry.mu.Lock()
	defer entry.mu.Unlock()

	nsParts := strings.Split(key.namespace, ".")
	client, err := catalogx.NewCatalogClient(r.catalogCfg, nsParts)
	if err != nil {
		return fmt.Errorf("failed to create catalog client: %w", err)
	}
	defer client.Close()

	// Check if table already exists (race protection)
	exists, err := client.CheckTableExists(ctx, key.table)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}
	if exists {
		r.logger.Debugf("Table %s.%s already exists (created by another process)", key.namespace, key.table)
		// Invalidate cached writer so it gets recreated with the new table
		r.closeWriter(entry)
		return nil
	}

	// Get first message to infer schema
	if len(batch) == 0 {
		return fmt.Errorf("cannot create table from empty batch")
	}

	firstMsg := batch[0]
	structured, err := firstMsg.AsStructured()
	if err != nil {
		return fmt.Errorf("failed to parse first message: %w", err)
	}

	record, ok := structured.(map[string]any)
	if !ok {
		return fmt.Errorf("first message is not an object, got %T", structured)
	}

	// Build schema from record
	schema, err := BuildSchemaFromRecord(record)
	if err != nil {
		return fmt.Errorf("failed to build schema from record: %w", err)
	}

	// Parse partition spec if configured
	var partitionSpec *iceberg.PartitionSpec
	if r.schemaEvoCfg.PartitionSpec != nil {
		specStr, err := batch.TryInterpolatedString(0, r.schemaEvoCfg.PartitionSpec)
		if err != nil {
			return fmt.Errorf("failed to interpolate partition spec: %w", err)
		}
		if specStr != "" {
			spec, err := icebergx.ParsePartitionSpec(specStr, schema)
			if err != nil {
				return fmt.Errorf("failed to parse partition spec %q: %w", specStr, err)
			}
			partitionSpec = &spec
		}
	}

	// Create the table
	_, err = client.CreateTableWithSpec(ctx, key.table, schema, partitionSpec)
	if err != nil {
		// Check if table was created by another process
		if errors.Is(err, catalog.ErrTableAlreadyExists) {
			r.logger.Debugf("Table %s.%s already exists (created by another process)", key.namespace, key.table)
			r.closeWriter(entry)
			return nil
		}
		return err
	}

	r.logger.Infof("Created table: %s.%s with %d columns", key.namespace, key.table, len(schema.Fields()))
	// Invalidate cached writer so it gets recreated with the new table
	r.closeWriter(entry)
	return nil
}

// evolveSchema adds new columns to the table.
func (r *router) evolveSchema(ctx context.Context, key tableKey, schemaErr *BatchSchemaEvolutionError, entry *tableEntry) error {
	entry.mu.Lock()
	defer entry.mu.Unlock()

	nsParts := strings.Split(key.namespace, ".")
	client, err := catalogx.NewCatalogClient(r.catalogCfg, nsParts)
	if err != nil {
		return fmt.Errorf("failed to create catalog client: %w", err)
	}
	defer client.Close()

	// Load current table
	tbl, err := client.LoadTable(ctx, key.table)
	if err != nil {
		return fmt.Errorf("failed to load table: %w", err)
	}

	// Group new fields by parent path for efficient updates
	groups := schemaErr.GroupByParentPath()

	// Update schema with new columns
	_, err = client.UpdateSchema(ctx, tbl, func(us *table.UpdateSchema) {
		for _, fields := range groups {
			for _, field := range fields {
				// Infer type from sample value
				fieldType, err := InferIcebergTypeForAddColumn(field.Value())
				if err != nil {
					r.logger.Warnf("Failed to infer type for field %q: %v, using string", field.FieldName(), err)
					fieldType = iceberg.StringType{}
				}

				// Build column path
				path := field.FullPath()
				colPath := make([]string, len(path))
				for i, seg := range path {
					colPath[i] = seg.Name
				}

				// Add column (all new columns are optional)
				us.AddColumn(colPath, fieldType, "", false, nil)
			}
		}
	})
	if err != nil {
		return fmt.Errorf("failed to update schema: %w", err)
	}

	r.logger.Infof("Evolved schema for %s.%s: added %d columns", key.namespace, key.table, len(schemaErr.Errors))

	// Invalidate cached writer so it gets recreated with the new schema
	r.closeWriter(entry)
	return nil
}

// closeWriter closes and nils the writer in an entry.
// Caller must hold entry.mu.Lock().
func (r *router) closeWriter(entry *tableEntry) {
	if entry.writer != nil {
		entry.writer.Close()
		entry.writer = nil
	}
}

// createWriter creates a new writer for a table.
// Caller must ensure this is only called when entry.writer is nil.
func (r *router) createWriter(ctx context.Context, key tableKey) (*writer, error) {
	// Parse namespace into parts
	nsParts := strings.Split(key.namespace, ".")

	// Create catalog client for this namespace
	client, err := catalogx.NewCatalogClient(r.catalogCfg, nsParts)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog client: %w", err)
	}

	// Load the table twice - writer and committer need separate references
	// since the table object is mutable and they operate in different goroutines
	writerTbl, err := client.LoadTable(ctx, key.table)
	if err != nil {
		_ = client.Close()
		// Return the error directly - the retry loop will handle it
		return nil, err
	}

	committerTbl, err := client.LoadTable(ctx, key.table)
	if err != nil {
		_ = client.Close()
		return nil, err
	}

	// Create committer with its own table reference
	comm, err := NewCommitter(committerTbl, r.logger)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("failed to create committer: %w", err)
	}

	// Create writer with its own table reference and the committer
	w := NewWriter(writerTbl, comm, r.logger)
	r.logger.Debugf("Created writer for table %s.%s", key.namespace, key.table)

	return w, nil
}

// Close closes all cached writers.
func (r *router) Close() {
	r.entries.Range(func(k, v any) bool {
		key := k.(tableKey)
		entry := v.(*tableEntry)
		entry.mu.Lock()
		if entry.writer != nil {
			entry.writer.Close()
			entry.writer = nil
			r.logger.Debugf("Closed writer for table %s.%s", key.namespace, key.table)
		}
		entry.mu.Unlock()
		return true
	})
}
