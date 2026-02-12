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
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/shredder"
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

// Router routes message batches to per-table writers.
type Router struct {
	catalogCfg   catalogx.Config
	namespaceStr *service.InterpolatedString
	tableStr     *service.InterpolatedString
	schemaEvoCfg SchemaEvolutionConfig
	commitCfg    CommitConfig

	entries sync.Map // tableKey -> *tableEntry

	logger *service.Logger
}

// NewRouter creates a new router.
func NewRouter(
	catalogCfg catalogx.Config,
	namespaceStr *service.InterpolatedString,
	tableStr *service.InterpolatedString,
	schemaEvoCfg SchemaEvolutionConfig,
	commitCfg CommitConfig,
	logger *service.Logger,
) *Router {
	return &Router{
		catalogCfg:   catalogCfg,
		namespaceStr: namespaceStr,
		tableStr:     tableStr,
		schemaEvoCfg: schemaEvoCfg,
		commitCfg:    commitCfg,
		logger:       logger,
	}
}

// getOrCreateEntry returns the entry for a table, creating one if needed.
func (r *Router) getOrCreateEntry(key tableKey) *tableEntry {
	if v, ok := r.entries.Load(key); ok {
		return v.(*tableEntry)
	}
	entry := &tableEntry{}
	actual, _ := r.entries.LoadOrStore(key, entry)
	return actual.(*tableEntry)
}

// Route routes a batch of messages to the appropriate writers.
func (r *Router) Route(ctx context.Context, batch service.MessageBatch) error {
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
			return fmt.Errorf("interpolating namespace: %w", err)
		}

		tbl, err := tableExec.TryString(i)
		if err != nil {
			return fmt.Errorf("interpolating table: %w", err)
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
func (r *Router) writeWithRetry(ctx context.Context, key tableKey, batch service.MessageBatch) error {
	entry := r.getOrCreateEntry(key)

	for range maxSchemaEvolutionRetries {
		err := r.doWrite(ctx, key, entry, batch)
		if err == nil {
			return nil
		}

		// Stale schema: the writer was using an outdated schema.
		// Recreate the writer to pick up the current schema and retry.
		var staleErr *StaleSchemaError
		if errors.As(err, &staleErr) {
			entry.mu.Lock()
			r.closeWriter(entry)
			entry.mu.Unlock()
			r.logger.Debugf("Stale schema for %s.%s: %v, recreating writer", key.namespace, key.table, err)
			continue
		}

		// Check if schema evolution is disabled - fail immediately
		if !r.schemaEvoCfg.Enabled {
			return fmt.Errorf("writing to %s.%s: %w", key.namespace, key.table, err)
		}

		// Handle specific errors with schema evolution
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			if err := r.createNamespace(ctx, key, entry); err != nil {
				return fmt.Errorf("creating namespace %s: %w", key.namespace, err)
			}
			continue
		}

		if errors.Is(err, catalog.ErrNoSuchTable) {
			createErr := r.createTable(ctx, key, batch, entry)
			if createErr != nil {
				// If table creation failed because namespace doesn't exist, create it first
				if errors.Is(createErr, catalog.ErrNoSuchNamespace) {
					if nsErr := r.createNamespace(ctx, key, entry); nsErr != nil {
						return fmt.Errorf("creating namespace %s: %w", key.namespace, nsErr)
					}
					// Don't return error, continue retry loop to create table
				} else {
					return fmt.Errorf("creating table %s.%s: %w", key.namespace, key.table, createErr)
				}
			}
			continue
		}

		var schemaErr *BatchSchemaEvolutionError
		if errors.As(err, &schemaErr) {
			if err := r.evolveSchema(ctx, key, schemaErr, entry); err != nil {
				return fmt.Errorf("evolving schema for %s.%s: %w", key.namespace, key.table, err)
			}
			continue
		}

		var reqNullErr *shredder.RequiredFieldNullError
		if errors.As(err, &reqNullErr) {
			if err := r.makeColumnOptional(ctx, key, reqNullErr, entry); err != nil {
				return fmt.Errorf("making column optional for %s.%s: %w", key.namespace, key.table, err)
			}
			continue
		}

		// Unhandled error - fail
		return fmt.Errorf("writing to %s.%s: %w", key.namespace, key.table, err)
	}

	return fmt.Errorf("writing to %s.%s: exhausted %d retries", key.namespace, key.table, maxSchemaEvolutionRetries)
}

// doWrite performs a single write attempt, creating the writer if needed.
func (r *Router) doWrite(ctx context.Context, key tableKey, entry *tableEntry, batch service.MessageBatch) error {
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
func (r *Router) createNamespace(ctx context.Context, key tableKey, entry *tableEntry) error {
	entry.mu.Lock()
	defer entry.mu.Unlock()

	nsParts := strings.Split(key.namespace, ".")
	client, err := catalogx.NewCatalogClient(r.catalogCfg, nsParts)
	if err != nil {
		return fmt.Errorf("creating catalog client: %w", err)
	}
	defer client.Close()

	// Check if namespace already exists (race protection)
	exists, err := client.CheckNamespaceExists(ctx)
	if err != nil {
		return fmt.Errorf("checking namespace existence: %w", err)
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
func (r *Router) createTable(ctx context.Context, key tableKey, batch service.MessageBatch, entry *tableEntry) error {
	entry.mu.Lock()
	defer entry.mu.Unlock()

	nsParts := strings.Split(key.namespace, ".")
	client, err := catalogx.NewCatalogClient(r.catalogCfg, nsParts)
	if err != nil {
		return fmt.Errorf("creating catalog client: %w", err)
	}
	defer client.Close()

	// Check if table already exists (race protection)
	exists, err := client.CheckTableExists(ctx, key.table)
	if err != nil {
		return fmt.Errorf("checking table existence: %w", err)
	}
	if exists {
		r.logger.Debugf("Table %s.%s already exists (created by another process)", key.namespace, key.table)
		// Invalidate cached writer so it gets recreated with the new table
		r.closeWriter(entry)
		return nil
	}

	// Get first message to infer schema
	if len(batch) == 0 {
		return errors.New("cannot create table from empty batch")
	}

	firstMsg := batch[0]
	structured, err := firstMsg.AsStructured()
	if err != nil {
		return fmt.Errorf("parsing first message: %w", err)
	}

	record, ok := structured.(map[string]any)
	if !ok {
		return fmt.Errorf("first message is not an object, got %T", structured)
	}

	// Build schema from record
	schema, err := BuildSchemaFromRecord(record)
	if err != nil {
		return fmt.Errorf("building schema from record: %w", err)
	}

	// Parse partition spec if configured
	var partitionSpec *iceberg.PartitionSpec
	if r.schemaEvoCfg.PartitionSpec != nil {
		specStr, err := batch.TryInterpolatedString(0, r.schemaEvoCfg.PartitionSpec)
		if err != nil {
			return fmt.Errorf("interpolating partition spec: %w", err)
		}
		if specStr != "" {
			spec, err := icebergx.ParsePartitionSpec(specStr, schema)
			if err != nil {
				return fmt.Errorf("parsing partition spec %q: %w", specStr, err)
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
func (r *Router) evolveSchema(ctx context.Context, key tableKey, schemaErr *BatchSchemaEvolutionError, entry *tableEntry) error {
	entry.mu.Lock()
	defer entry.mu.Unlock()

	nsParts := strings.Split(key.namespace, ".")
	client, err := catalogx.NewCatalogClient(r.catalogCfg, nsParts)
	if err != nil {
		return fmt.Errorf("creating catalog client: %w", err)
	}
	defer client.Close()

	// Load current table
	tbl, err := client.LoadTable(ctx, key.table)
	if err != nil {
		return fmt.Errorf("loading table: %w", err)
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
		return fmt.Errorf("updating schema: %w", err)
	}

	r.logger.Infof("Evolved schema for %s.%s: added %d columns", key.namespace, key.table, len(schemaErr.Errors))

	// Invalidate cached writer so it gets recreated with the new schema
	r.closeWriter(entry)
	return nil
}

// makeColumnOptional changes a required column to optional in the table schema.
func (r *Router) makeColumnOptional(ctx context.Context, key tableKey, reqNullErr *shredder.RequiredFieldNullError, entry *tableEntry) error {
	entry.mu.Lock()
	defer entry.mu.Unlock()

	nsParts := strings.Split(key.namespace, ".")
	client, err := catalogx.NewCatalogClient(r.catalogCfg, nsParts)
	if err != nil {
		return fmt.Errorf("creating catalog client: %w", err)
	}
	defer client.Close()

	// Load current table
	tbl, err := client.LoadTable(ctx, key.table)
	if err != nil {
		return fmt.Errorf("loading table: %w", err)
	}

	// Build column path from the error's path + field name.
	// Only include PathField segments - skip PathListElement/PathMapEntry
	// which don't correspond to named columns in the schema.
	colPath := make([]string, 0, len(reqNullErr.Path)+1)
	for _, seg := range reqNullErr.Path {
		if seg.Kind == icebergx.PathField {
			colPath = append(colPath, seg.Name)
		}
	}
	colPath = append(colPath, reqNullErr.Field.Name)

	// Update schema to make the column optional
	_, err = client.UpdateSchema(ctx, tbl, func(us *table.UpdateSchema) {
		us.UpdateColumn(colPath, table.ColumnUpdate{
			Required: iceberg.Optional[bool]{Val: false, Valid: true},
		})
	})
	if err != nil {
		return fmt.Errorf("updating schema: %w", err)
	}

	r.logger.Infof("Made column %q optional for %s.%s", reqNullErr.Field.Name, key.namespace, key.table)

	// Invalidate cached writer so it gets recreated with the new schema
	r.closeWriter(entry)
	return nil
}

// closeWriter closes and nils the writer in an entry.
// Caller must hold entry.mu.Lock().
func (*Router) closeWriter(entry *tableEntry) {
	if entry.writer != nil {
		entry.writer.Close()
		entry.writer = nil
	}
}

// createWriter creates a new writer for a table.
// Caller must ensure this is only called when entry.writer is nil.
func (r *Router) createWriter(ctx context.Context, key tableKey) (*writer, error) {
	// Parse namespace into parts
	nsParts := strings.Split(key.namespace, ".")

	// Create catalog client for this namespace
	client, err := catalogx.NewCatalogClient(r.catalogCfg, nsParts)
	if err != nil {
		return nil, fmt.Errorf("creating catalog client: %w", err)
	}
	defer client.Close()

	// Load the table twice - writer and committer need separate references
	// since the table object is mutable and they operate in different goroutines
	writerTbl, err := client.LoadTable(ctx, key.table)
	if err != nil {
		// Return the error directly - the retry loop will handle it
		return nil, err
	}

	committerTbl, err := client.LoadTable(ctx, key.table)
	if err != nil {
		return nil, err
	}

	// Create committer with its own table reference
	comm, err := NewCommitter(committerTbl, r.commitCfg, r.logger)
	if err != nil {
		return nil, fmt.Errorf("creating committer: %w", err)
	}

	// Create writer with its own table reference and the committer
	w := NewWriter(writerTbl, comm, r.logger)
	r.logger.Debugf("Created writer for table %s.%s", key.namespace, key.table)

	return w, nil
}

// Close closes all cached writers.
func (r *Router) Close() {
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
