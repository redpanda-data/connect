// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package bigquery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"golang.org/x/sync/singleflight"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// resolvedSchema holds both descriptor forms (DescriptorProto for the managed
// writer, MessageDescriptor for JSON-to-proto conversion) plus the table's
// declared primary keys (nil when the table has no PK constraint).
type resolvedSchema struct {
	descriptorProto   *descriptorpb.DescriptorProto
	messageDescriptor protoreflect.MessageDescriptor
	primaryKeys       []string
}

// schemaResolver caches resolved schemas per table and deduplicates concurrent
// resolves of the same table via a singleflight group.
type schemaResolver struct {
	cache sync.Map // key: string -> *resolvedSchema
	sf    singleflight.Group
	log   *service.Logger
	// resolveTimeout bounds a single BQ Metadata fetch inside the singleflight
	// so a wedged backend cannot pin the singleflight slot indefinitely. Set
	// from the schema_resolve_timeout config field.
	resolveTimeout time.Duration
	// generation increments on every Evict so an in-flight singleflight whose
	// fetch started before the eviction can detect it raced with a writer and
	// refuse to store its now-stale result. atomic to avoid taking a lock on
	// the cache-miss path.
	generation atomic.Int64
	// creator is non-nil when auto_create_table is enabled. On a 404 from
	// Metadata, Resolve calls creator.Ensure then retries the fetch.
	creator *tableCreator
}

// Resolve returns a resolved schema for the given table by fetching the
// table metadata from BigQuery. Results are cached per table ID, and concurrent
// cache misses for the same table are coalesced into a single BQ call.
//
// The fetch runs under a context detached from the caller — singleflight runs
// the function once for the *first* caller, and if that caller's per-batch ctx
// cancelled mid-fetch every concurrent waiter would receive the cancellation
// error too. Detaching ensures one slow batch cannot poison resolution for
// every other batch routing to the same table.
//
// Evict-during-flight protection: the in-flight fetch reads the generation
// counter before starting and compares it before storing. If Evict bumped the
// counter in between (e.g. after a schema evolution invalidated the cached
// descriptor), the result is discarded so the next Resolve sees a clean miss
// rather than a re-stored stale schema.
func (r *schemaResolver) Resolve(ctx context.Context, client *bq.Client, datasetID, tableID string) (*resolvedSchema, error) {
	if cached, ok := r.cache.Load(tableID); ok {
		return cached.(*resolvedSchema), nil
	}

	if client == nil {
		return nil, errors.New("no schema source available: ensure the table exists in BigQuery")
	}

	v, err, _ := r.sf.Do(tableID, func() (any, error) {
		// Re-check cache: another goroutine may have populated it while we
		// were queued in singleflight.
		if cached, ok := r.cache.Load(tableID); ok {
			return cached.(*resolvedSchema), nil
		}
		genBefore := r.generation.Load()
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.resolveTimeout)
		defer cancel()
		rs, err := resolveFromBQTable(fetchCtx, client, r.creator, datasetID, tableID)
		if err != nil {
			return nil, err
		}
		// Only store if no Evict raced with this fetch. A bumped generation
		// means the cached descriptor we were about to write is already
		// known-stale (typically: schema evolved during this resolve), so we
		// return the value to satisfy current waiters but skip the cache so
		// the next Resolve re-fetches with the post-evolution schema.
		if r.generation.Load() == genBefore {
			r.cache.Store(tableID, rs)
		}
		return rs, nil
	})
	if err != nil {
		return nil, fmt.Errorf("resolving schema for table %q: %w", tableID, err)
	}
	return v.(*resolvedSchema), nil
}

// Evict removes the cached schema for a table, forcing re-resolution on the
// next call to Resolve. Bumps the generation counter so an in-flight Resolve
// for the same table detects it raced with the eviction and refuses to write
// its now-stale result back to the cache.
func (r *schemaResolver) Evict(tableID string) {
	r.generation.Add(1)
	r.cache.Delete(tableID)
}

func resolveFromBQTable(ctx context.Context, client *bq.Client, creator *tableCreator, datasetID, tableID string) (*resolvedSchema, error) {
	meta, err := client.Dataset(datasetID).Table(tableID).Metadata(ctx)
	if err != nil {
		// 404 + auto_create_table → create the table and retry. Any other
		// error short-circuits.
		var apiErr *googleapi.Error
		if creator != nil && errors.As(err, &apiErr) && apiErr.Code == http.StatusNotFound {
			if cerr := creator.Ensure(ctx, client, datasetID, tableID); cerr != nil {
				return nil, fmt.Errorf("auto-create on 404: %w", cerr)
			}
			meta, err = client.Dataset(datasetID).Table(tableID).Metadata(ctx)
		}
		if err != nil {
			return nil, err
		}
	}

	tableSchema, err := adapt.BQSchemaToStorageTableSchema(meta.Schema)
	if err != nil {
		return nil, fmt.Errorf("converting BQ schema to storage schema: %w", err)
	}

	descriptor, err := adapt.StorageSchemaToProto2Descriptor(tableSchema, "root")
	if err != nil {
		return nil, fmt.Errorf("converting storage schema to proto descriptor: %w", err)
	}

	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, errors.New("schema descriptor is not a MessageDescriptor")
	}

	normalized, err := adapt.NormalizeDescriptor(messageDescriptor)
	if err != nil {
		return nil, fmt.Errorf("normalizing descriptor: %w", err)
	}

	md, err := descriptorProtoToMessageDescriptor(normalized)
	if err != nil {
		return nil, err
	}

	return &resolvedSchema{
		descriptorProto:   normalized,
		messageDescriptor: md,
		primaryKeys:       extractPrimaryKeysFromMetadata(meta),
	}, nil
}

// extractPrimaryKeysFromMetadata reads the PRIMARY KEY declaration from a
// fetched BigQuery table metadata. Returns nil if the table has no primary
// key declared. The returned slice preserves the column order declared in
// BigQuery, which is significant for composite keys.
func extractPrimaryKeysFromMetadata(meta *bq.TableMetadata) []string {
	if meta == nil || meta.TableConstraints == nil || meta.TableConstraints.PrimaryKey == nil {
		return nil
	}
	cols := meta.TableConstraints.PrimaryKey.Columns
	if len(cols) == 0 {
		return nil
	}
	out := make([]string, len(cols))
	copy(out, cols)
	return out
}
