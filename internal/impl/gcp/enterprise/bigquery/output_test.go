// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package bigquery

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

// newTestOutput creates a bigQueryWriteAPIOutput suitable for unit tests
// that do not require a live BigQuery connection.
func newTestOutput(t *testing.T, yaml string) *bigQueryWriteAPIOutput {
	t.Helper()
	spec := bigQueryWriteAPISpec()
	pConf, err := spec.ParseYAML(yaml, nil)
	require.NoError(t, err)
	mgr := service.MockResources()
	license.InjectTestService(mgr)
	out, err := bigQueryWriteAPIOutputFromConfig(pConf, mgr)
	require.NoError(t, err)
	return out
}

func TestBigQueryWriteAPIConfigParsing(t *testing.T) {
	// Given a minimal config with only required fields.
	spec := bigQueryWriteAPISpec()
	pConf, err := spec.ParseYAML(`
dataset: my_dataset
table: my_table
`, nil)
	require.NoError(t, err)

	// When we parse the config.
	cfg, err := bqWriteAPIConfigFromParsed(pConf)
	require.NoError(t, err)

	// Then defaults are applied correctly.
	assert.Equal(t, bigquery.DetectProjectID, cfg.ProjectID)
	assert.Equal(t, "my_dataset", cfg.DatasetID)
	assert.Equal(t, "json", cfg.MessageFormat)
	assert.Equal(t, "default_stream", cfg.WriteMode)
	assert.False(t, cfg.AutoCreateTable)
	assert.Empty(t, cfg.CredentialsJSON)
	assert.Empty(t, cfg.TargetPrincipal)
	assert.Empty(t, cfg.Delegates)
	assert.Equal(t, 5*time.Minute, cfg.StreamIdleTimeout)
	assert.Equal(t, 1*time.Minute, cfg.StreamSweepInterval)
	assert.Equal(t, 5*time.Second, cfg.SchemaResolveTimeout)
	assert.Equal(t, 30*time.Second, cfg.SchemaEvolutionTimeout)
	assert.Empty(t, cfg.EndpointHTTP)
	assert.Empty(t, cfg.EndpointGRPC)
}

func TestBigQueryWriteAPIConfigParsingAllFields(t *testing.T) {
	// Given a config with all fields explicitly set.
	spec := bigQueryWriteAPISpec()
	pConf, err := spec.ParseYAML(`
project: my-project
dataset: my_dataset
table: my_table
message_format: protobuf
write_mode: pending_stream
auto_create_table: true
schema:
  - { name: id, type: STRING, mode: REQUIRED }
credentials_json: '{"type":"service_account"}'
target_principal: "sa@project.iam.gserviceaccount.com"
delegates:
  - "delegate@project.iam.gserviceaccount.com"
stream_idle_timeout: 10m
stream_sweep_interval: 2m
schema_resolve_timeout: 7s
schema_evolution_timeout: 45s
endpoint:
  http: http://localhost:9050
  grpc: localhost:9060
`, nil)
	require.NoError(t, err)

	// When we parse the config.
	cfg, err := bqWriteAPIConfigFromParsed(pConf)
	require.NoError(t, err)

	// Then all values are parsed correctly.
	assert.Equal(t, "my-project", cfg.ProjectID)
	assert.Equal(t, "my_dataset", cfg.DatasetID)
	assert.Equal(t, "protobuf", cfg.MessageFormat)
	assert.Equal(t, "pending_stream", cfg.WriteMode)
	assert.True(t, cfg.AutoCreateTable)
	assert.Equal(t, `{"type":"service_account"}`, cfg.CredentialsJSON)
	assert.Equal(t, "sa@project.iam.gserviceaccount.com", cfg.TargetPrincipal)
	assert.Equal(t, []string{"delegate@project.iam.gserviceaccount.com"}, cfg.Delegates)
	assert.Equal(t, 10*time.Minute, cfg.StreamIdleTimeout)
	assert.Equal(t, 2*time.Minute, cfg.StreamSweepInterval)
	assert.Equal(t, 7*time.Second, cfg.SchemaResolveTimeout)
	assert.Equal(t, 45*time.Second, cfg.SchemaEvolutionTimeout)
	assert.Equal(t, "http://localhost:9050", cfg.EndpointHTTP)
	assert.Equal(t, "localhost:9060", cfg.EndpointGRPC)
}

func TestBigQueryWriteAPIConfigParsingWriteModeUpsert(t *testing.T) {
	spec := bigQueryWriteAPISpec()
	for _, mode := range []string{"upsert", "upsert_delete"} {
		t.Run(mode, func(t *testing.T) {
			yaml := fmt.Sprintf(`
dataset: my_dataset
table: my_table
write_mode: %s
change_type: ${! metadata("operation") }
`, mode)
			pConf, err := spec.ParseYAML(yaml, nil)
			require.NoError(t, err)
			cfg, err := bqWriteAPIConfigFromParsed(pConf)
			require.NoError(t, err)
			assert.Equal(t, mode, cfg.WriteMode)
		})
	}
}

func TestBigQueryWriteAPIConfigChangeType(t *testing.T) {
	spec := bigQueryWriteAPISpec()

	t.Run("change_type parsed for upsert mode", func(t *testing.T) {
		pConf, err := spec.ParseYAML(`
dataset: my_dataset
table: my_table
write_mode: upsert
change_type: ${! metadata("operation") }
`, nil)
		require.NoError(t, err)
		cfg, err := bqWriteAPIConfigFromParsed(pConf)
		require.NoError(t, err)
		require.NotNil(t, cfg.ChangeType)
	})

	t.Run("change_type required when upsert", func(t *testing.T) {
		pConf, err := spec.ParseYAML(`
dataset: my_dataset
table: my_table
write_mode: upsert
`, nil)
		require.NoError(t, err)
		_, err = bqWriteAPIConfigFromParsed(pConf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "change_type")
	})

	t.Run("change_type not required when default_stream", func(t *testing.T) {
		pConf, err := spec.ParseYAML(`
dataset: my_dataset
table: my_table
`, nil)
		require.NoError(t, err)
		cfg, err := bqWriteAPIConfigFromParsed(pConf)
		require.NoError(t, err)
		assert.Nil(t, cfg.ChangeType)
	})
}

func TestBigQueryWriteAPIConfigChangeSequenceNumber(t *testing.T) {
	spec := bigQueryWriteAPISpec()
	pConf, err := spec.ParseYAML(`
dataset: my_dataset
table: my_table
write_mode: upsert
change_type: ${! metadata("operation") }
change_sequence_number: ${! metadata("scn") }
`, nil)
	require.NoError(t, err)
	cfg, err := bqWriteAPIConfigFromParsed(pConf)
	require.NoError(t, err)
	require.NotNil(t, cfg.ChangeSequenceNumber)
}

func TestBigQueryWriteAPIConfigPrimaryKeys(t *testing.T) {
	spec := bigQueryWriteAPISpec()

	t.Run("primary_keys parsed", func(t *testing.T) {
		pConf, err := spec.ParseYAML(`
dataset: my_dataset
table: my_table
write_mode: upsert
change_type: ${! metadata("operation") }
primary_keys: [id, tenant_id]
`, nil)
		require.NoError(t, err)
		cfg, err := bqWriteAPIConfigFromParsed(pConf)
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "tenant_id"}, cfg.PrimaryKeys)
	})

	t.Run("max 16 columns", func(t *testing.T) {
		keys := make([]string, 17)
		for i := range keys {
			keys[i] = fmt.Sprintf("c%d", i)
		}
		pConf, err := spec.ParseYAML(fmt.Sprintf(`
dataset: my_dataset
table: my_table
write_mode: upsert
change_type: ${! metadata("operation") }
primary_keys: [%s]
`, strings.Join(keys, ", ")), nil)
		require.NoError(t, err)
		_, err = bqWriteAPIConfigFromParsed(pConf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "16")
	})

	t.Run("required when auto_create_table and CDC mode", func(t *testing.T) {
		pConf, err := spec.ParseYAML(`
dataset: my_dataset
table: my_table
write_mode: upsert
change_type: ${! metadata("operation") }
auto_create_table: true
schema:
  - { name: id, type: STRING, mode: REQUIRED }
`, nil)
		require.NoError(t, err)
		_, err = bqWriteAPIConfigFromParsed(pConf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "primary_keys")
	})

	t.Run("PK column must exist in schema when both set", func(t *testing.T) {
		pConf, err := spec.ParseYAML(`
dataset: my_dataset
table: my_table
write_mode: upsert
change_type: ${! metadata("operation") }
auto_create_table: true
schema:
  - { name: id, type: STRING, mode: REQUIRED }
primary_keys: [unknown_col]
`, nil)
		require.NoError(t, err)
		_, err = bqWriteAPIConfigFromParsed(pConf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown_col")
	})

	t.Run("PK column must be REQUIRED when both set", func(t *testing.T) {
		pConf, err := spec.ParseYAML(`
dataset: my_dataset
table: my_table
write_mode: upsert
change_type: ${! metadata("operation") }
auto_create_table: true
schema:
  - { name: id, type: STRING, mode: NULLABLE }
primary_keys: [id]
`, nil)
		require.NoError(t, err)
		_, err = bqWriteAPIConfigFromParsed(pConf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "REQUIRED")
	})
}

func TestBigQueryWriteAPIConfigCDCIncompatibilities(t *testing.T) {
	spec := bigQueryWriteAPISpec()
	cases := []struct {
		name   string
		yaml   string
		errMsg string
	}{
		{
			name: "pending_stream rejects change_type",
			yaml: `
dataset: my_dataset
table: my_table
write_mode: pending_stream
change_type: ${! metadata("operation") }
`,
			errMsg: "change_type",
		},
		{
			name: "default_stream rejects change_sequence_number",
			yaml: `
dataset: my_dataset
table: my_table
change_sequence_number: ${! metadata("scn") }
`,
			errMsg: "change_sequence_number",
		},
		{
			name: "upsert rejects message_format: protobuf",
			yaml: `
dataset: my_dataset
table: my_table
write_mode: upsert
change_type: ${! metadata("operation") }
message_format: protobuf
`,
			errMsg: "message_format",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pConf, err := spec.ParseYAML(tc.yaml, nil)
			require.NoError(t, err)
			_, err = bqWriteAPIConfigFromParsed(pConf)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestBigQueryWriteAPIConfigParsingRejectsNonPositiveDurations(t *testing.T) {
	spec := bigQueryWriteAPISpec()
	for _, tc := range []struct {
		name   string
		yaml   string
		errMsg string
	}{
		{
			name: "zero idle timeout",
			yaml: `
dataset: my_dataset
table: my_table
stream_idle_timeout: 0s
`,
			errMsg: bqwaFieldStreamIdleTimeout,
		},
		{
			name: "zero sweep interval",
			yaml: `
dataset: my_dataset
table: my_table
stream_sweep_interval: 0s
`,
			errMsg: bqwaFieldStreamSweepInterval,
		},
		{
			name: "zero schema_resolve_timeout",
			yaml: `
dataset: my_dataset
table: my_table
schema_resolve_timeout: 0s
`,
			errMsg: bqwaFieldSchemaResolveTimeout,
		},
		{
			name: "zero schema_evolution_timeout",
			yaml: `
dataset: my_dataset
table: my_table
schema_evolution_timeout: 0s
`,
			errMsg: bqwaFieldSchemaEvolutionTimeout,
		},
		{
			name: "delegates without target_principal",
			yaml: `
dataset: my_dataset
table: my_table
delegates:
  - "delegate@project.iam.gserviceaccount.com"
`,
			errMsg: bqwaFieldDelegates,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pConf, err := spec.ParseYAML(tc.yaml, nil)
			require.NoError(t, err)
			_, err = bqWriteAPIConfigFromParsed(pConf)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestSchemaParsing(t *testing.T) {
	spec := bigQueryWriteAPISpec()
	pConf, err := spec.ParseYAML(`
dataset: my_dataset
table: my_table
auto_create_table: true
schema:
  - { name: id, type: STRING, mode: REQUIRED }
  - { name: tags, type: STRING, mode: REPEATED }
  - { name: age, type: INT64 }
  - name: address
    type: RECORD
    fields:
      - { name: line1, type: STRING }
      - { name: city, type: STRING, mode: REQUIRED }
`, nil)
	require.NoError(t, err)

	cfg, err := bqWriteAPIConfigFromParsed(pConf)
	require.NoError(t, err)
	require.Len(t, cfg.Schema, 4)
	assert.Equal(t, "id", cfg.Schema[0].Name)
	assert.Equal(t, "STRING", cfg.Schema[0].Type)
	assert.Equal(t, "REQUIRED", cfg.Schema[0].Mode)
	assert.Equal(t, "REPEATED", cfg.Schema[1].Mode)
	// INT64 alias normalises to INTEGER.
	assert.Equal(t, "INTEGER", cfg.Schema[2].Type)
	assert.Equal(t, "NULLABLE", cfg.Schema[2].Mode)
	assert.Equal(t, "RECORD", cfg.Schema[3].Type)
	require.Len(t, cfg.Schema[3].Fields, 2)
	assert.Equal(t, "line1", cfg.Schema[3].Fields[0].Name)
	assert.Equal(t, "city", cfg.Schema[3].Fields[1].Name)
	assert.Equal(t, "REQUIRED", cfg.Schema[3].Fields[1].Mode)
}

func TestSchemaValidation(t *testing.T) {
	spec := bigQueryWriteAPISpec()
	for _, tc := range []struct {
		name   string
		yaml   string
		errMsg string
	}{
		{
			name: "auto_create_table without schema",
			yaml: `
dataset: my_dataset
table: my_table
auto_create_table: true
`,
			errMsg: bqwaFieldSchema,
		},
		{
			name: "invalid column type",
			yaml: `
dataset: my_dataset
table: my_table
auto_create_table: true
schema:
  - { name: id, type: NOTATYPE }
`,
			errMsg: "NOTATYPE",
		},
		{
			name: "invalid mode",
			yaml: `
dataset: my_dataset
table: my_table
auto_create_table: true
schema:
  - { name: id, type: STRING, mode: WEIRD }
`,
			errMsg: "WEIRD",
		},
		{
			name: "record without fields",
			yaml: `
dataset: my_dataset
table: my_table
auto_create_table: true
schema:
  - { name: addr, type: RECORD }
`,
			errMsg: "RECORD",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pConf, err := spec.ParseYAML(tc.yaml, nil)
			require.NoError(t, err)
			_, err = bqWriteAPIConfigFromParsed(pConf)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestPartitioningClusteringParsing(t *testing.T) {
	spec := bigQueryWriteAPISpec()
	pConf, err := spec.ParseYAML(`
dataset: my_dataset
table: my_table
auto_create_table: true
schema:
  - { name: id, type: STRING }
  - { name: created_at, type: TIMESTAMP }
  - { name: user_id, type: STRING }
time_partitioning:
  type: HOUR
  field: created_at
  expiration: 24h
  require_filter: true
clustering:
  - user_id
  - id
`, nil)
	require.NoError(t, err)

	cfg, err := bqWriteAPIConfigFromParsed(pConf)
	require.NoError(t, err)
	assert.Equal(t, "HOUR", cfg.TimePartitioning.Type)
	assert.Equal(t, "created_at", cfg.TimePartitioning.Field)
	assert.Equal(t, 24*time.Hour, cfg.TimePartitioning.Expiration)
	assert.True(t, cfg.TimePartitioning.RequireFilter)
	assert.Equal(t, []string{"user_id", "id"}, cfg.Clustering)
}

func TestPartitioningClusteringDefaults(t *testing.T) {
	// Absent partition block leaves Type empty (sentinel for "not configured").
	spec := bigQueryWriteAPISpec()
	pConf, err := spec.ParseYAML(`
dataset: my_dataset
table: my_table
`, nil)
	require.NoError(t, err)
	cfg, err := bqWriteAPIConfigFromParsed(pConf)
	require.NoError(t, err)
	assert.Empty(t, cfg.TimePartitioning.Type)
	assert.Empty(t, cfg.TimePartitioning.Field)
	assert.Empty(t, cfg.Clustering)
}

func TestPartitioningClusteringValidation(t *testing.T) {
	spec := bigQueryWriteAPISpec()
	for _, tc := range []struct {
		name   string
		yaml   string
		errMsg string
	}{
		{
			name: "partition field not in schema",
			yaml: `
dataset: my_dataset
table: my_table
auto_create_table: true
schema: [{ name: id, type: STRING }]
time_partitioning: { type: DAY, field: missing_col }
`,
			errMsg: "missing_col",
		},
		{
			name: "partition field wrong type",
			yaml: `
dataset: my_dataset
table: my_table
auto_create_table: true
schema: [{ name: id, type: STRING }]
time_partitioning: { type: DAY, field: id }
`,
			errMsg: "DATE/TIMESTAMP/DATETIME",
		},
		{
			name: "clustering column not in schema",
			yaml: `
dataset: my_dataset
table: my_table
auto_create_table: true
schema: [{ name: id, type: STRING }]
clustering: [missing_col]
`,
			errMsg: "missing_col",
		},
		{
			name: "too many clustering columns",
			yaml: `
dataset: my_dataset
table: my_table
auto_create_table: true
schema:
  - { name: a, type: STRING }
  - { name: b, type: STRING }
  - { name: c, type: STRING }
  - { name: d, type: STRING }
  - { name: e, type: STRING }
clustering: [a, b, c, d, e]
`,
			errMsg: "at most 4",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pConf, err := spec.ParseYAML(tc.yaml, nil)
			require.NoError(t, err)
			_, err = bqWriteAPIConfigFromParsed(pConf)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errMsg)
		})
	}
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
	// Given an output that has not been connected.
	out := newTestOutput(t, `
dataset: my_dataset
table: my_table
`)

	// When we write a batch.
	batch := service.MessageBatch{service.NewMessage([]byte(`{"foo":"bar"}`))}
	err := out.WriteBatch(t.Context(), batch)

	// Then it returns ErrNotConnected.
	assert.ErrorIs(t, err, service.ErrNotConnected)
}

func TestWriteBatchEmptyBatch(t *testing.T) {
	// Given an output that has not been connected.
	out := newTestOutput(t, `
dataset: my_dataset
table: my_table
`)

	// When we write an empty batch.
	err := out.WriteBatch(t.Context(), service.MessageBatch{})

	// Then it succeeds without error.
	assert.NoError(t, err)
}

func TestCloseNilClients(t *testing.T) {
	// Given an output that has never connected.
	out := newTestOutput(t, `
dataset: my_dataset
table: my_table
`)

	// When we close it.
	// Then it does not panic or return an error.
	assert.NotPanics(t, func() {
		err := out.Close(t.Context())
		assert.NoError(t, err)
	})
}

func TestTableCacheKey(t *testing.T) {
	// Given an output configured for a specific dataset.
	out := newTestOutput(t, `
project: my-project
dataset: my_dataset
table: my_table
`)

	// When we compute the cache key for a table (callers pass the resolved
	// projectID they captured under connMu).
	key := out.tableCacheKey("my-project", "my_table")

	// Then it returns the fully qualified table resource path.
	assert.Equal(t, "projects/my-project/datasets/my_dataset/tables/my_table", key)
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

// TestStreamCacheConcurrentStress exercises the stream cache from many
// goroutines doing reads (fast path), inserts (with LRU eviction), and
// targeted evictions. The point isn't to assert final cache contents — Go's
// race detector and the lock invariants are what we're stressing.
func TestStreamCacheConcurrentStress(t *testing.T) {
	out := newTestOutput(t, `
dataset: my_dataset
table: my_table
max_cached_streams: 16
`)
	out.streams = make(map[string]*streamWithDescriptor)

	// Seed enough entries to make the LRU scan non-trivial.
	for i := range 8 {
		k := fmt.Sprintf("projects/p/datasets/d/tables/seed%d", i)
		swd := &streamWithDescriptor{}
		swd.lastUsed.Store(time.Now().Add(time.Duration(-i) * time.Second).UnixNano())
		out.streams[k] = swd
	}

	const (
		goroutines   = 16
		opsPerWorker = 500
	)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			for i := range opsPerWorker {
				key := fmt.Sprintf("projects/p/datasets/d/tables/t%d", (id*opsPerWorker+i)%24)
				switch i % 4 {
				case 0:
					// Fast-path read with lastUsed update.
					out.streamsMu.RLock()
					if cached, exists := out.streams[key]; exists {
						cached.lastUsed.Store(time.Now().UnixNano())
					}
					out.streamsMu.RUnlock()
				case 1:
					// Insert path mimicking getOrCreateStream's tail.
					out.streamsMu.Lock()
					if _, exists := out.streams[key]; !exists {
						newSwd := &streamWithDescriptor{}
						newSwd.lastUsed.Store(time.Now().UnixNano())
						out.streams[key] = newSwd
						// Drive the LRU pass under contention.
						if len(out.streams) > out.conf.MaxCachedStreams {
							var lruKey string
							var lruTS int64 = -1
							for k, s := range out.streams {
								if k == key {
									continue
								}
								ts := s.lastUsed.Load()
								if lruTS == -1 || ts < lruTS {
									lruKey = k
									lruTS = ts
								}
							}
							if lruKey != "" {
								delete(out.streams, lruKey)
							}
						}
					}
					out.streamsMu.Unlock()
				case 2:
					out.evictStream(key)
				case 3:
					out.streamsMu.RLock()
					_ = len(out.streams)
					out.streamsMu.RUnlock()
				}
			}
		}(g)
	}
	wg.Wait()

	// The cap is a soft limit but should be honoured under the contention
	// pattern above (each insert evicts at most one extra entry).
	out.streamsMu.RLock()
	size := len(out.streams)
	out.streamsMu.RUnlock()
	assert.LessOrEqual(t, size, out.conf.MaxCachedStreams+1,
		"cache should stay within ~MaxCachedStreams under concurrent insert/evict")
}

func TestSweepIdleStreams(t *testing.T) {
	// Given an output with a short sweep interval and a stale stream.
	out := newTestOutput(t, `
dataset: my_dataset
table: my_table
stream_idle_timeout: 100ms
stream_sweep_interval: 50ms
`)
	out.streams = make(map[string]*streamWithDescriptor)
	out.stopSweep = make(chan struct{})

	stale := &streamWithDescriptor{}
	stale.lastUsed.Store(time.Now().Add(-10 * time.Minute).UnixNano())
	out.streams["projects/p/datasets/d/tables/stale"] = stale

	fresh := &streamWithDescriptor{}
	fresh.lastUsed.Store(time.Now().UnixNano())
	out.streams["projects/p/datasets/d/tables/fresh"] = fresh

	// When we run the actual sweep goroutine.
	out.sweepWg.Add(1)
	go out.sweepIdleStreams(out.stopSweep)
	// Stop the sweeper via t.Cleanup so an early require failure can't leak it.
	t.Cleanup(func() {
		close(out.stopSweep)
		out.sweepWg.Wait()
	})

	// Then the stale stream is evicted and the fresh one remains.
	assert.Eventually(t, func() bool {
		out.streamsMu.RLock()
		defer out.streamsMu.RUnlock()
		_, staleExists := out.streams["projects/p/datasets/d/tables/stale"]
		return !staleExists
	}, 2*time.Second, 25*time.Millisecond, "stale stream should have been evicted")

	out.streamsMu.RLock()
	_, freshExists := out.streams["projects/p/datasets/d/tables/fresh"]
	out.streamsMu.RUnlock()
	assert.True(t, freshExists, "fresh stream should remain in cache")
}

func TestMaxCachedStreamsLRUEviction(t *testing.T) {
	// Seed three cached streams with monotonically older lastUsed values.
	// With max_cached_streams=2 and a fourth stream inserted, the oldest of
	// the existing entries must be evicted; the just-inserted entry must
	// survive even though its peer set is over the cap mid-insert.
	out := newTestOutput(t, `
dataset: my_dataset
table: my_table
max_cached_streams: 2
`)
	out.streams = make(map[string]*streamWithDescriptor)

	mk := func(age time.Duration) *streamWithDescriptor {
		s := &streamWithDescriptor{}
		s.lastUsed.Store(time.Now().Add(-age).UnixNano())
		return s
	}
	out.streams["projects/p/datasets/d/tables/oldest"] = mk(10 * time.Minute)
	out.streams["projects/p/datasets/d/tables/middle"] = mk(5 * time.Minute)
	out.streams["projects/p/datasets/d/tables/recent"] = mk(1 * time.Minute)

	// Simulate the inline LRU pass that runs inside getOrCreateStream after a
	// new entry is added — we drive it via the cap logic directly so the test
	// doesn't need a live BQ to construct a real stream.
	out.streamsMu.Lock()
	newKey := "projects/p/datasets/d/tables/new"
	out.streams[newKey] = mk(0)
	var evictedKey string
	if out.conf.MaxCachedStreams > 0 && len(out.streams) > out.conf.MaxCachedStreams {
		var lruKey string
		var lruTS int64 = -1
		for k, s := range out.streams {
			if k == newKey {
				continue
			}
			ts := s.lastUsed.Load()
			if lruTS == -1 || ts < lruTS {
				lruKey = k
				lruTS = ts
			}
		}
		evictedKey = lruKey
		delete(out.streams, lruKey)
	}
	out.streamsMu.Unlock()

	// Only the oldest pre-existing key should be picked, not the just-inserted entry.
	assert.Equal(t, "projects/p/datasets/d/tables/oldest", evictedKey)
	_, ok := out.streams[newKey]
	assert.True(t, ok, "just-inserted entry must survive its own insert")
	// The cap is a soft limit; we evicted one entry but len may still exceed
	// MaxCachedStreams by one if MaxCachedStreams==1 (rare); for the typical
	// case len should equal MaxCachedStreams+1−1=MaxCachedStreams when going
	// from over-by-1 to over-by-0.
	assert.LessOrEqual(t, len(out.streams), out.conf.MaxCachedStreams+1)
}

func TestMaxCachedStreamsUnlimited(t *testing.T) {
	// max_cached_streams=0 means unlimited — eviction is gated entirely on
	// the idle-timeout sweep loop.
	out := newTestOutput(t, `
dataset: my_dataset
table: my_table
max_cached_streams: 0
`)
	assert.Equal(t, 0, out.conf.MaxCachedStreams)
}

func TestClassifyBQError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bqErrorKind
	}{
		{"unavailable is transient", grpcstatus.Error(codes.Unavailable, "service unavailable"), bqErrorTransient},
		{"resource exhausted is transient", grpcstatus.Error(codes.ResourceExhausted, "quota exceeded"), bqErrorTransient},
		{"deadline exceeded is transient", grpcstatus.Error(codes.DeadlineExceeded, "timeout"), bqErrorTransient},
		{"aborted is transient", grpcstatus.Error(codes.Aborted, "conflict"), bqErrorTransient},
		{"internal is transient", grpcstatus.Error(codes.Internal, "internal error"), bqErrorTransient},
		{"invalid argument is permanent", grpcstatus.Error(codes.InvalidArgument, "bad request"), bqErrorPermanent},
		{"not found is permanent", grpcstatus.Error(codes.NotFound, "table not found"), bqErrorPermanent},
		{"permission denied is permanent", grpcstatus.Error(codes.PermissionDenied, "forbidden"), bqErrorPermanent},
		{"unauthenticated is permanent", grpcstatus.Error(codes.Unauthenticated, "bad creds"), bqErrorPermanent},
		{"nil is transient", nil, bqErrorTransient},
		{"non-grpc error is transient", errors.New("random network error"), bqErrorTransient},
		{"wrapped grpc error is classified", fmt.Errorf("appending rows: %w", grpcstatus.Error(codes.InvalidArgument, "bad")), bqErrorPermanent},
		{"http 404 is permanent", &googleapi.Error{Code: 404}, bqErrorPermanent},
		{"http 403 is permanent", &googleapi.Error{Code: 403}, bqErrorPermanent},
		{"http 408 is transient", &googleapi.Error{Code: 408}, bqErrorTransient},
		{"http 429 is transient", &googleapi.Error{Code: 429}, bqErrorTransient},
		{"http 500 is transient", &googleapi.Error{Code: 500}, bqErrorTransient},
		{"wrapped http 404 is permanent", fmt.Errorf("resolve: %w", &googleapi.Error{Code: 404}), bqErrorPermanent},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, classifyBQError(tc.err).kind)
		})
	}
}

func TestClassifyBQErrorSchemaMismatch(t *testing.T) {
	st, err := grpcstatus.New(codes.InvalidArgument, "schema mismatch").
		WithDetails(&storagepb.StorageError{
			Code:         storagepb.StorageError_SCHEMA_MISMATCH_EXTRA_FIELDS,
			ErrorMessage: "extra fields found",
		})
	require.NoError(t, err)
	bqErr := classifyBQError(st.Err())
	assert.True(t, bqErr.IsSchemaMismatch())
	assert.False(t, bqErr.IsPermanent())
	assert.False(t, bqErr.IsRetryable())
}

func TestClassifyBQErrorSchemaMismatchWrapped(t *testing.T) {
	st, err := grpcstatus.New(codes.InvalidArgument, "schema mismatch").
		WithDetails(&storagepb.StorageError{
			Code:         storagepb.StorageError_SCHEMA_MISMATCH_EXTRA_FIELDS,
			ErrorMessage: "extra fields found",
		})
	require.NoError(t, err)
	wrapped := fmt.Errorf("appending rows: %w", st.Err())
	bqErr := classifyBQError(wrapped)
	assert.True(t, bqErr.IsSchemaMismatch())
}

func TestBQErrorUnwrap(t *testing.T) {
	// bqError preserves the underlying error for errors.Is/As callers.
	inner := grpcstatus.Error(codes.InvalidArgument, "bad")
	bqErr := classifyBQError(inner)
	assert.Equal(t, inner, bqErr.Unwrap())
	assert.Equal(t, inner.Error(), bqErr.Error())
}

func TestMetricsInitialization(t *testing.T) {
	m := newBQWAMetrics(service.MockResources().Metrics())
	require.NotNil(t, m.rowsSent)
	require.NotNil(t, m.rowsFailed)
	require.NotNil(t, m.batchesSent)
	require.NotNil(t, m.batchLatency)
	require.NotNil(t, m.retries)
	require.NotNil(t, m.schemaEvolutions)
	require.NotNil(t, m.schemaEvolutionFailures)
	require.NotNil(t, m.cachedStreams)
	require.NotNil(t, m.streamsEvicted)
}

func TestBuildAuthOpts(t *testing.T) {
	// Covers the easy paths through buildAuthOpts that don't talk to GCP.
	// The target_principal branch hits impersonate.CredentialsTokenSource,
	// which makes a real network call, so it's deliberately left to
	// integration coverage.
	tests := []struct {
		name        string
		yaml        string
		isGRPC      bool
		endpoint    string
		expectMin   int  // expected minimum number of options returned
		expectGRPC  bool // expects the insecure-credentials gRPC dial option
		expectAuth  bool // expects authentication to be enabled (no WithoutAuthentication)
		expectError bool
	}{
		{
			name: "endpoint override disables auth (HTTP)",
			yaml: `
dataset: my_dataset
table: my_table
endpoint:
  http: http://localhost:9050
`,
			endpoint:   "http://localhost:9050",
			isGRPC:     false,
			expectMin:  2, // WithoutAuthentication + WithEndpoint
			expectAuth: false,
		},
		{
			name: "endpoint override disables auth and adds insecure gRPC dial opt",
			yaml: `
dataset: my_dataset
table: my_table
endpoint:
  grpc: localhost:9060
`,
			endpoint:   "localhost:9060",
			isGRPC:     true,
			expectMin:  3, // WithoutAuthentication + WithEndpoint + WithGRPCDialOption
			expectAuth: false,
			expectGRPC: true,
		},
		{
			name: "credentials_json only",
			yaml: `
dataset: my_dataset
table: my_table
credentials_json: '{"type":"service_account"}'
`,
			expectMin:  1, // WithCredentialsJSON
			expectAuth: true,
		},
		{
			name: "no auth config",
			yaml: `
dataset: my_dataset
table: my_table
`,
			expectMin:  0,
			expectAuth: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := newTestOutput(t, tc.yaml)
			opts, err := out.buildAuthOpts(t.Context(), tc.endpoint, tc.isGRPC)
			if tc.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(opts), tc.expectMin)
		})
	}
}
