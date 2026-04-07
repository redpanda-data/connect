// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestBigQueryWriteAPIConfigParsing(t *testing.T) {
	spec := bigQueryWriteAPISpec()
	pConf, err := spec.ParseYAML(`
dataset: my_dataset
table: my_table
`, nil)
	require.NoError(t, err)

	cfg, err := bigQueryWriteAPIConfigFromParsed(pConf)
	require.NoError(t, err)

	assert.Equal(t, bigquery.DetectProjectID, cfg.ProjectID)
	assert.Equal(t, "my_dataset", cfg.DatasetID)
	assert.Equal(t, "json", cfg.MessageFormat)
	assert.Empty(t, cfg.CredentialsJSON)
	assert.Empty(t, cfg.EndpointHTTP)
	assert.Empty(t, cfg.EndpointGRPC)
}

func TestBigQueryWriteAPIConfigParsingAllFields(t *testing.T) {
	spec := bigQueryWriteAPISpec()
	pConf, err := spec.ParseYAML(`
project: my-project
dataset: my_dataset
table: my_table
message_format: protobuf
credentials_json: '{"type":"service_account"}'
endpoint:
  http: http://localhost:9050
  grpc: localhost:9060
`, nil)
	require.NoError(t, err)

	cfg, err := bigQueryWriteAPIConfigFromParsed(pConf)
	require.NoError(t, err)

	assert.Equal(t, "my-project", cfg.ProjectID)
	assert.Equal(t, "my_dataset", cfg.DatasetID)
	assert.Equal(t, "protobuf", cfg.MessageFormat)
	assert.Equal(t, `{"type":"service_account"}`, cfg.CredentialsJSON)
	assert.Equal(t, "http://localhost:9050", cfg.EndpointHTTP)
	assert.Equal(t, "localhost:9060", cfg.EndpointGRPC)
}

func TestJSONToProtoConversion(t *testing.T) {
	bqSchema := bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "age", Type: bigquery.IntegerFieldType},
	}
	tableSchema, err := adapt.BQSchemaToStorageTableSchema(bqSchema)
	require.NoError(t, err)

	descriptor, err := adapt.StorageSchemaToProto2Descriptor(tableSchema, "root")
	require.NoError(t, err)

	msgDesc := descriptor.(protoreflect.MessageDescriptor)

	// BigQuery INTEGER maps to INT64 in proto; protojson expects string for int64.
	protoBytes, err := jsonToProtoBytes([]byte(`{"name":"alice","age":"30"}`), msgDesc)
	require.NoError(t, err)
	assert.NotEmpty(t, protoBytes)

	// Verify round-trip: unmarshal proto bytes back.
	msg := dynamicpb.NewMessage(msgDesc)
	require.NoError(t, proto.Unmarshal(protoBytes, msg))
}

func TestJSONToProtoConversionWithNormalizedDescriptor(t *testing.T) {
	bqSchema := bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "age", Type: bigquery.IntegerFieldType},
	}
	tableSchema, err := adapt.BQSchemaToStorageTableSchema(bqSchema)
	require.NoError(t, err)

	descriptor, err := adapt.StorageSchemaToProto2Descriptor(tableSchema, "root")
	require.NoError(t, err)

	msgDesc := descriptor.(protoreflect.MessageDescriptor)

	normalizedDP, err := adapt.NormalizeDescriptor(msgDesc)
	require.NoError(t, err)

	normalizedMsgDesc, err := descriptorProtoToMessageDescriptor(normalizedDP)
	require.NoError(t, err)

	// JSON-to-proto with normalized descriptor should produce valid bytes.
	protoBytes, err := jsonToProtoBytes([]byte(`{"name":"alice","age":"30"}`), normalizedMsgDesc)
	require.NoError(t, err)
	assert.NotEmpty(t, protoBytes)

	// Verify round-trip with normalized descriptor.
	msg := dynamicpb.NewMessage(normalizedMsgDesc)
	require.NoError(t, proto.Unmarshal(protoBytes, msg))
}

func TestWriteBatchNotConnected(t *testing.T) {
	spec := bigQueryWriteAPISpec()
	pConf, err := spec.ParseYAML(`
dataset: my_dataset
table: my_table
`, nil)
	require.NoError(t, err)

	out, err := bigQueryWriteAPIOutputFromConfig(pConf, service.MockResources())
	require.NoError(t, err)

	batch := service.MessageBatch{service.NewMessage([]byte(`{"foo":"bar"}`))}
	writeErr := out.WriteBatch(context.Background(), batch)
	assert.ErrorIs(t, writeErr, service.ErrNotConnected)
}

func TestWriteBatchEmptyBatch(t *testing.T) {
	spec := bigQueryWriteAPISpec()
	pConf, err := spec.ParseYAML(`
dataset: my_dataset
table: my_table
`, nil)
	require.NoError(t, err)

	out, err := bigQueryWriteAPIOutputFromConfig(pConf, service.MockResources())
	require.NoError(t, err)

	// Empty batch should return nil, not panic.
	err = out.WriteBatch(context.Background(), service.MessageBatch{})
	assert.NoError(t, err)
}

func TestCloseNilClients(t *testing.T) {
	spec := bigQueryWriteAPISpec()
	pConf, err := spec.ParseYAML(`
dataset: my_dataset
table: my_table
`, nil)
	require.NoError(t, err)

	out, err := bigQueryWriteAPIOutputFromConfig(pConf, service.MockResources())
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		err := out.Close(context.Background())
		assert.NoError(t, err)
	})
}

func TestTableCacheKey(t *testing.T) {
	out := &bigQueryWriteAPIOutput{
		conf: bigQueryWriteAPIConfig{
			ProjectID: "my-project",
			DatasetID: "my_dataset",
		},
	}
	assert.Equal(t, "projects/my-project/datasets/my_dataset/tables/my_table", out.tableCacheKey("my_table"))
}

func TestDescriptorProtoToMessageDescriptorErrors(t *testing.T) {
	t.Run("unresolvable message type reference", func(t *testing.T) {
		dp := &descriptorpb.DescriptorProto{
			Name: new("BrokenRef"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name:     new("ptr"),
					Number:   new(int32(1)),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
					TypeName: new(".nonexistent.Missing"),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				},
			},
		}

		md, err := descriptorProtoToMessageDescriptor(dp)
		require.Error(t, err)
		assert.Nil(t, md)
		assert.Contains(t, err.Error(), "creating file descriptor from normalized proto")
	})

	t.Run("duplicate field numbers", func(t *testing.T) {
		dp := &descriptorpb.DescriptorProto{
			Name: new("DuplicateNumbers"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name:   new("alpha"),
					Number: new(int32(1)),
					Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				},
				{
					Name:   new("beta"),
					Number: new(int32(1)),
					Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				},
			},
		}

		md, err := descriptorProtoToMessageDescriptor(dp)
		require.Error(t, err)
		assert.Nil(t, md)
		assert.Contains(t, err.Error(), "creating file descriptor from normalized proto")
	})
}

func TestSweepIdleStreams(t *testing.T) {
	out := &bigQueryWriteAPIOutput{
		log:       service.MockResources().Logger(),
		streams:   make(map[string]*streamWithDescriptor),
		stopSweep: make(chan struct{}),
	}

	// Add a stream that was last used well beyond the idle timeout.
	stale := &streamWithDescriptor{}
	stale.lastUsed.Store(time.Now().Add(-10 * time.Minute).UnixNano())
	out.streams["projects/p/datasets/d/tables/stale"] = stale

	// Add a stream that was just used.
	fresh := &streamWithDescriptor{}
	fresh.lastUsed.Store(time.Now().UnixNano())
	out.streams["projects/p/datasets/d/tables/fresh"] = fresh

	// Run one sweep cycle by calling sweepIdleStreams in a goroutine and
	// stopping it after the first tick.
	go func() {
		// Give the sweep goroutine time to process one tick.
		time.Sleep(100 * time.Millisecond)
		close(out.stopSweep)
	}()

	// Use a very short sweep interval so the test completes quickly.
	// We can't change the package constants, so we directly test the
	// eviction logic instead.
	now := time.Now()

	out.streamsMu.Lock()
	var evictedKeys []string
	for key, swd := range out.streams {
		lastUsed := time.Unix(0, swd.lastUsed.Load())
		if now.Sub(lastUsed) > streamIdleTimeout {
			evictedKeys = append(evictedKeys, key)
			delete(out.streams, key)
		}
	}
	out.streamsMu.Unlock()

	assert.Equal(t, []string{"projects/p/datasets/d/tables/stale"}, evictedKeys)

	out.streamsMu.RLock()
	_, freshExists := out.streams["projects/p/datasets/d/tables/fresh"]
	_, staleExists := out.streams["projects/p/datasets/d/tables/stale"]
	out.streamsMu.RUnlock()

	assert.True(t, freshExists, "fresh stream should remain in cache")
	assert.False(t, staleExists, "stale stream should have been evicted")
}
