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
	"fmt"
	"strings"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/catalogx"
)

// tableKey uniquely identifies an Iceberg table.
type tableKey struct {
	namespace string // dot-separated namespace
	table     string
}

// router routes message batches to per-table writers.
type router struct {
	catalogCfg   catalogx.Config
	namespaceStr *service.InterpolatedString
	tableStr     *service.InterpolatedString

	writers   map[tableKey]*writer
	writersMu sync.RWMutex

	logger *service.Logger
}

// NewRouter creates a new router.
func NewRouter(
	catalogCfg catalogx.Config,
	namespaceStr *service.InterpolatedString,
	tableStr *service.InterpolatedString,
	logger *service.Logger,
) *router {
	return &router{
		catalogCfg:   catalogCfg,
		namespaceStr: namespaceStr,
		tableStr:     tableStr,
		writers:      make(map[tableKey]*writer),
		logger:       logger,
	}
}

// Route routes a batch of messages to the appropriate writers.
func (r *router) Route(ctx context.Context, batch service.MessageBatch) error {
	// Group messages by table key
	groups := make(map[tableKey]service.MessageBatch)

	for _, msg := range batch {
		ns, err := r.namespaceStr.TryString(msg)
		if err != nil {
			return fmt.Errorf("failed to interpolate namespace: %w", err)
		}

		tbl, err := r.tableStr.TryString(msg)
		if err != nil {
			return fmt.Errorf("failed to interpolate table: %w", err)
		}

		key := tableKey{namespace: ns, table: tbl}
		groups[key] = append(groups[key], msg)
	}

	// Write each group to its writer
	for key, groupBatch := range groups {
		w, err := r.getOrCreateWriter(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get writer for %s.%s: %w", key.namespace, key.table, err)
		}

		if err := w.Write(ctx, groupBatch); err != nil {
			return fmt.Errorf("failed to write to %s.%s: %w", key.namespace, key.table, err)
		}
	}

	return nil
}

// getOrCreateWriter returns a cached writer or creates a new one.
func (r *router) getOrCreateWriter(ctx context.Context, key tableKey) (*writer, error) {
	// Fast path: check if writer exists
	r.writersMu.RLock()
	w, ok := r.writers[key]
	r.writersMu.RUnlock()
	if ok {
		return w, nil
	}

	// Slow path: create writer under write lock
	r.writersMu.Lock()
	defer r.writersMu.Unlock()

	// Double-check after acquiring write lock
	if w, ok := r.writers[key]; ok {
		return w, nil
	}

	// Parse namespace into parts
	nsParts := strings.Split(key.namespace, ".")

	// Create catalog client for this namespace
	client, err := catalogx.NewCatalogClient(r.catalogCfg, nsParts)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog client: %w", err)
	}

	// Load the table
	tbl, err := client.LoadTable(ctx, key.table)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("failed to load table: %w", err)
	}

	// Create writer for this table
	w, err = NewWriter(tbl, r.logger)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("failed to create writer: %w", err)
	}

	r.writers[key] = w
	r.logger.Debugf("Created writer for table %s.%s", key.namespace, key.table)

	return w, nil
}

// Close closes all cached writers.
func (r *router) Close() {
	r.writersMu.Lock()
	defer r.writersMu.Unlock()

	for key, w := range r.writers {
		w.Close()
		r.logger.Debugf("Closed writer for table %s.%s", key.namespace, key.table)
	}

	r.writers = make(map[tableKey]*writer)
}
