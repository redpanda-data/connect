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
	"sync"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// resolvedSchema holds both descriptor forms (DescriptorProto for the managed
// writer, MessageDescriptor for JSON-to-proto conversion).
type resolvedSchema struct {
	descriptorProto   *descriptorpb.DescriptorProto
	messageDescriptor protoreflect.MessageDescriptor
}

// schemaResolver caches resolved schemas per table and deduplicates concurrent
// resolves of the same table via a singleflight group.
type schemaResolver struct {
	cache sync.Map // key: string -> *resolvedSchema
	sf    singleflight.Group
	log   *service.Logger
}

// Resolve returns a resolved schema for the given table by fetching the
// table metadata from BigQuery. Results are cached per table ID, and concurrent
// cache misses for the same table are coalesced into a single BQ call.
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
		rs, err := resolveFromBQTable(ctx, client, datasetID, tableID)
		if err != nil {
			return nil, err
		}
		r.cache.Store(tableID, rs)
		return rs, nil
	})
	if err != nil {
		return nil, fmt.Errorf("resolving schema for table %q: %w", tableID, err)
	}
	return v.(*resolvedSchema), nil
}

// Evict removes the cached schema for a table, forcing re-resolution on the
// next call to Resolve.
func (r *schemaResolver) Evict(tableID string) {
	r.cache.Delete(tableID)
}

func resolveFromBQTable(ctx context.Context, client *bq.Client, datasetID, tableID string) (*resolvedSchema, error) {
	meta, err := client.Dataset(datasetID).Table(tableID).Metadata(ctx)
	if err != nil {
		return nil, err
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
	}, nil
}
