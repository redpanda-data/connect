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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestResolverCacheHit(t *testing.T) {
	// Given a resolver with a pre-populated cache entry.
	r := &schemaResolver{log: service.MockResources().Logger()}
	expected := &resolvedSchema{
		messageDescriptor: buildTestMessageDescriptor(t),
	}
	r.cache.Store("my_table", expected)

	// When we resolve the same table.
	rs, err := r.Resolve(context.Background(), "my_table", service.NewMessage(nil))

	// Then the cached value is returned without error.
	require.NoError(t, err)
	assert.Same(t, expected, rs)
}

func TestResolverCacheMissNoBQClient(t *testing.T) {
	// Given a resolver with no BQ client.
	r := &schemaResolver{log: service.MockResources().Logger()}

	// When we resolve a table with no cache entry.
	rs, err := r.Resolve(context.Background(), "missing", service.NewMessage(nil))

	// Then it returns an error about no schema source.
	require.Error(t, err)
	assert.Nil(t, rs)
	assert.Contains(t, err.Error(), "no schema source available")
}

func TestResolverEvict(t *testing.T) {
	// Given a resolver with a cached entry.
	r := &schemaResolver{log: service.MockResources().Logger()}
	r.cache.Store("my_table", &resolvedSchema{})

	// When we evict that table.
	r.Evict("my_table")

	// Then the cache entry is gone.
	_, ok := r.cache.Load("my_table")
	assert.False(t, ok)
}

func TestResolverEvictNonexistent(t *testing.T) {
	// Given a resolver with no cache entries.
	r := &schemaResolver{log: service.MockResources().Logger()}

	// When we evict a nonexistent key, it does not panic.
	assert.NotPanics(t, func() {
		r.Evict("no_such_table")
	})
}

// buildTestMessageDescriptor creates a simple (name STRING, age INT64)
// message descriptor for unit tests.
func buildTestMessageDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	dp := &descriptorpb.DescriptorProto{
		Name: new("TestMessage"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{
				Name:   new("name"),
				Number: new(int32(1)),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			},
			{
				Name:   new("age"),
				Number: new(int32(2)),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			},
		},
	}
	md, err := descriptorProtoToMessageDescriptor(dp)
	require.NoError(t, err)
	return md
}
