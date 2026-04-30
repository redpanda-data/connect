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
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// resolvedSchema holds both descriptor forms (DescriptorProto for the managed
// writer, MessageDescriptor for JSON-to-proto conversion) and an optional field
// name mapping from original (unsanitized) names to sanitized proto field names.
type resolvedSchema struct {
	descriptorProto   *descriptorpb.DescriptorProto
	messageDescriptor protoreflect.MessageDescriptor
	fieldNameMapping  map[string]string // original -> sanitized; nil when no renaming needed
}

type schemaResolver struct {
	bqClient  *bq.Client
	datasetID string
	cache     sync.Map // key: string -> *resolvedSchema
	log       *service.Logger
}

// Resolve returns a resolved schema for the given table by fetching the
// table metadata from BigQuery. Results are cached per table ID.
func (r *schemaResolver) Resolve(ctx context.Context, tableID string, _ *service.Message) (*resolvedSchema, error) {
	if cached, ok := r.cache.Load(tableID); ok {
		return cached.(*resolvedSchema), nil
	}

	if r.bqClient == nil {
		return nil, errors.New("no schema source available: ensure the table exists in BigQuery")
	}

	rs, err := r.resolveFromBQTable(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("resolving schema for table %q: %w", tableID, err)
	}
	r.cache.Store(tableID, rs)
	return rs, nil
}

// Evict removes the cached schema for a table, forcing re-resolution on the
// next call to Resolve.
func (r *schemaResolver) Evict(tableID string) {
	r.cache.Delete(tableID)
}

func (r *schemaResolver) resolveFromBQTable(ctx context.Context, tableID string) (*resolvedSchema, error) {
	meta, err := r.bqClient.Dataset(r.datasetID).Table(tableID).Metadata(ctx)
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
		fieldNameMapping:  nil,
	}, nil
}
