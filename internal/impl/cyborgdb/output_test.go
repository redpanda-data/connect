// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cyborgdb

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/cyborginc/cyborgdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Mock client implementation for testing
type mockClient struct {
	indexes map[string]*mockIndex
	err     error
}

func newMockClient() *mockClient {
	return &mockClient{
		indexes: make(map[string]*mockIndex),
	}
}

func (c *mockClient) ListIndexes(ctx context.Context) ([]string, error) {
	if c.err != nil {
		return nil, c.err
	}
	
	var names []string
	for name := range c.indexes {
		names = append(names, name)
	}
	return names, nil
}

func (c *mockClient) CreateIndex(ctx context.Context, indexName string, indexKey []byte) (*cyborgdb.EncryptedIndex, error) {
	if c.err != nil {
		return nil, c.err
	}
	
	idx := &mockIndex{
		name:    indexName,
		vectors: make(map[string]*cyborgdb.VectorItem),
		closed:  false,
	}
	c.indexes[indexName] = idx
	
	// Return a fake EncryptedIndex - in real tests we'd need to properly mock this
	return nil, nil
}

func (c *mockClient) GetIndex(ctx context.Context, indexName string, indexKey []byte) (*cyborgdb.EncryptedIndex, error) {
	if c.err != nil {
		return nil, c.err
	}
	
	if _, exists := c.indexes[indexName]; !exists {
		return nil, fmt.Errorf("index not found")
	}
	
	// Return a fake EncryptedIndex - in real tests we'd need to properly mock this
	return nil, nil
}

type mockIndex struct {
	name    string
	vectors map[string]*cyborgdb.VectorItem
	closed  bool
}

type mockIndexClient struct {
	index *mockIndex
}

func (m *mockIndexClient) Upsert(ctx context.Context, items []cyborgdb.VectorItem) error {
	if m.index.closed {
		return fmt.Errorf("index is closed")
	}
	
	for _, item := range items {
		m.index.vectors[item.Id] = &cyborgdb.VectorItem{
			Id:       item.Id,
			Vector:   item.Vector,
			Metadata: item.Metadata,
		}
	}
	return nil
}

func (m *mockIndexClient) Delete(ctx context.Context, ids []string) error {
	if m.index.closed {
		return fmt.Errorf("index is closed")
	}
	
	for _, id := range ids {
		delete(m.index.vectors, id)
	}
	return nil
}

func (m *mockIndexClient) Close() error {
	// Don't actually close the index in tests
	return nil
}

// Test helper functions
func generateTestKey() string {
	key := make([]byte, 32)
	rand.Read(key)
	return base64.StdEncoding.EncodeToString(key)
}

func createTestMessage(id string, vector []float32, metadata map[string]interface{}) *service.Message {
	msg := service.NewMessage(nil)
	
	// Convert vector to interface slice
	vecInterface := make([]interface{}, len(vector))
	for i, v := range vector {
		vecInterface[i] = v
	}
	
	structured := map[string]interface{}{
		"id":     id,
		"vector": vecInterface,
	}
	
	// Add metadata fields to structured data for mapping
	if metadata != nil {
		for k, v := range metadata {
			structured[k] = v
		}
	}
	
	msg.SetStructuredMut(structured)
	
	return msg
}

func TestOutputWriter_Connect(t *testing.T) {
	tests := []struct {
		name            string
		createIfMissing bool
		indexExists     bool
		expectError     bool
		errorContains   string
	}{
		{
			name:            "existing index loads successfully",
			createIfMissing: false,
			indexExists:     true,
			expectError:     false,
		},
		{
			name:            "missing index without create flag fails",
			createIfMissing: false,
			indexExists:     false,
			expectError:     true,
			errorContains:   "does not exist and create_if_missing is false",
		},
		{
			name:            "missing index with create flag succeeds",
			createIfMissing: true,
			indexExists:     false,
			expectError:     false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := newMockClient()
			
			if tt.indexExists {
				// Pre-create the index
				mockClient.indexes["test-index"] = &mockIndex{
					name:      "test-index",
					vectors:   make(map[string]*cyborgdb.VectorItem),
				}
			}
			
			indexKey, _ := base64.StdEncoding.DecodeString(generateTestKey())
			
			w := &outputWriter{
				client:          mockClient,
				indexName:       "test-index",
				indexKey:        indexKey,
				createIfMissing: tt.createIfMissing,
				logger:          service.MockResources().Logger(),
			}
			
			err := w.Connect(context.Background())
			
			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
				assert.True(t, w.init)
				
				if !tt.indexExists && tt.createIfMissing {
					// Verify index was created
					_, exists := mockClient.indexes["test-index"]
					assert.True(t, exists)
				}
			}
		})
	}
}

func TestOutputWriter_UpsertBatch(t *testing.T) {
	mockClient := newMockClient()
	mockIndex := &mockIndex{
		name:    "test-index",
		vectors: make(map[string]*cyborgdb.VectorItem),
	}
	mockClient.indexes["test-index"] = mockIndex
	
	indexKey, _ := base64.StdEncoding.DecodeString(generateTestKey())
	
	// Don't use vector mapping in the test - it doesn't work with structured messages
	// The real usage would have JSON content, not structured
	var vectorMapping *bloblang.Executor = nil
	var metadataMapping *bloblang.Executor = nil
	
	idField, _ := service.NewInterpolatedString("${! json(\"id\") }")
	
	w := &outputWriter{
		client:          mockClient,
		index:           &mockIndexClient{mockIndex},
		indexName:       "test-index",
		indexKey:        indexKey,
		op:              operationUpsert,
		id:              idField,
		vectorMapping:   vectorMapping,
		metadataMapping: metadataMapping,
		logger:          service.MockResources().Logger(),
		init:            true,
	}
	
	// Create test batch
	batch := service.MessageBatch{
		createTestMessage("vec1", []float32{0.1, 0.2, 0.3}, map[string]interface{}{
			"category": "test",
			"score":    0.95,
		}),
		createTestMessage("vec2", []float32{0.4, 0.5, 0.6}, map[string]interface{}{
			"category": "example", 
			"score":    0.87,
		}),
	}
	
	err := w.WriteBatch(context.Background(), batch)
	require.NoError(t, err)
	
	// Verify vectors were upserted
	assert.Equal(t, 2, len(mockIndex.vectors))
	
	vec1 := mockIndex.vectors["vec1"]
	assert.NotNil(t, vec1)
	assert.Equal(t, []float32{0.1, 0.2, 0.3}, vec1.Vector)
	assert.Equal(t, "test", vec1.Metadata["category"])
	assert.Equal(t, float64(0.95), vec1.Metadata["score"])
	
	vec2 := mockIndex.vectors["vec2"]
	assert.NotNil(t, vec2)
	assert.Equal(t, []float32{0.4, 0.5, 0.6}, vec2.Vector)
	assert.Equal(t, "example", vec2.Metadata["category"])
	assert.Equal(t, float64(0.87), vec2.Metadata["score"])
}

func TestOutputWriter_DeleteBatch(t *testing.T) {
	mockClient := newMockClient()
	mockIndex := &mockIndex{
		name:    "test-index",
		vectors: make(map[string]*cyborgdb.VectorItem),
	}
	
	// Pre-populate some vectors
	mockIndex.vectors["vec1"] = &cyborgdb.VectorItem{
		Id:     "vec1",
		Vector: []float32{0.1, 0.2, 0.3},
	}
	mockIndex.vectors["vec2"] = &cyborgdb.VectorItem{
		Id:     "vec2",
		Vector: []float32{0.4, 0.5, 0.6},
	}
	mockIndex.vectors["vec3"] = &cyborgdb.VectorItem{
		Id:     "vec3",
		Vector: []float32{0.7, 0.8, 0.9},
	}
	
	mockClient.indexes["test-index"] = mockIndex
	
	indexKey, _ := base64.StdEncoding.DecodeString(generateTestKey())
	idField, _ := service.NewInterpolatedString("${! json(\"id\") }")
	
	w := &outputWriter{
		client:    mockClient,
		index:     &mockIndexClient{mockIndex},
		indexName: "test-index",
		indexKey:  indexKey,
		op:        operationDelete,
		id:        idField,
		logger:    service.MockResources().Logger(),
		init:      true,
	}
	
	// Create test batch for deletion
	batch := service.MessageBatch{
		createTestMessage("vec1", nil, nil),
		createTestMessage("vec3", nil, nil),
	}
	
	err := w.WriteBatch(context.Background(), batch)
	require.NoError(t, err)
	
	// Verify vectors were deleted
	assert.Equal(t, 1, len(mockIndex.vectors))
	assert.Nil(t, mockIndex.vectors["vec1"])
	assert.NotNil(t, mockIndex.vectors["vec2"]) // Should still exist
	assert.Nil(t, mockIndex.vectors["vec3"])
}

func TestOutputWriter_VectorTypeConversion(t *testing.T) {
	mockClient := newMockClient()
	mockIndex := &mockIndex{
		name:    "test-index",
		vectors: make(map[string]*cyborgdb.VectorItem),
	}
	mockClient.indexes["test-index"] = mockIndex
	
	indexKey, _ := base64.StdEncoding.DecodeString(generateTestKey())
	var vectorMapping *bloblang.Executor = nil
	idField, _ := service.NewInterpolatedString("${! json(\"id\") }")
	
	w := &outputWriter{
		client:        mockClient,
		index:         &mockIndexClient{mockIndex},
		indexName:     "test-index",
		indexKey:      indexKey,
		op:            operationUpsert,
		id:            idField,
		vectorMapping: vectorMapping,
		logger:        service.MockResources().Logger(),
		init:          true,
	}
	
	// Test different numeric types
	msg := service.NewMessage(nil)
	msg.SetStructuredMut(map[string]interface{}{
		"id": "test-vec",
		"vector": []interface{}{
			float64(0.1),
			float32(0.2),
			int(3),
			int64(4),
		},
	})
	
	batch := service.MessageBatch{msg}
	err := w.WriteBatch(context.Background(), batch)
	require.NoError(t, err)
	
	// Verify all values were converted to float32
	vec := mockIndex.vectors["test-vec"]
	assert.NotNil(t, vec)
	assert.Equal(t, []float32{0.1, 0.2, 3.0, 4.0}, vec.Vector)
}

func TestOutputWriter_InvalidVectorType(t *testing.T) {
	mockClient := newMockClient()
	mockIndex := &mockIndex{
		name:    "test-index",
		vectors: make(map[string]*cyborgdb.VectorItem),
	}
	mockClient.indexes["test-index"] = mockIndex
	
	indexKey, _ := base64.StdEncoding.DecodeString(generateTestKey())
	var vectorMapping *bloblang.Executor = nil
	idField, _ := service.NewInterpolatedString("${! json(\"id\") }")
	
	w := &outputWriter{
		client:        mockClient,
		index:         &mockIndexClient{mockIndex},
		indexName:     "test-index",
		indexKey:      indexKey,
		op:            operationUpsert,
		id:            idField,
		vectorMapping: vectorMapping,
		logger:        service.MockResources().Logger(),
		init:          true,
	}
	
	// Test with invalid vector element type
	msg := service.NewMessage(nil)
	msg.SetStructuredMut(map[string]interface{}{
		"id": "test-vec",
		"vector": []interface{}{
			0.1,
			"invalid", // String instead of number
			0.3,
		},
	})
	
	batch := service.MessageBatch{msg}
	err := w.WriteBatch(context.Background(), batch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "vector element 1 is not a number")
}

func TestOutputWriter_EmptyBatch(t *testing.T) {
	mockClient := newMockClient()
	mockIndex := &mockIndex{
		name:    "test-index",
		vectors: make(map[string]*cyborgdb.VectorItem),
	}
	mockClient.indexes["test-index"] = mockIndex
	
	indexKey, _ := base64.StdEncoding.DecodeString(generateTestKey())
	
	w := &outputWriter{
		client:    mockClient,
		index:     &mockIndexClient{mockIndex},
		indexName: "test-index",
		indexKey:  indexKey,
		op:        operationUpsert,
		logger:    service.MockResources().Logger(),
		init:      true,
	}
	
	// Test with empty batch
	batch := service.MessageBatch{}
	err := w.WriteBatch(context.Background(), batch)
	require.NoError(t, err)
	
	// Verify no vectors were added
	assert.Equal(t, 0, len(mockIndex.vectors))
}

func TestOutputWriter_Close(t *testing.T) {
	mockIndex := &mockIndex{
		name:    "test-index",
		vectors: make(map[string]*cyborgdb.VectorItem),
	}

	w := &outputWriter{
		index:  &mockIndexClient{mockIndex},
		logger: service.MockResources().Logger(),
	}

	err := w.Close(context.Background())
	require.NoError(t, err)

	// Test Close with no index
	w2 := &outputWriter{
		logger: service.MockResources().Logger(),
	}

	err = w2.Close(context.Background())
	require.NoError(t, err)
}



// Constructor tests
func TestNewOutputWriter(t *testing.T) {
	// Clean up any existing keys
	os.RemoveAll(".cyborgdb")
	defer os.RemoveAll(".cyborgdb")

	t.Run("valid config", func(t *testing.T) {
		config := `
host: api.cyborg.com
api_key: test-key
index_name: test-index
operation: upsert
id: ${! json("id") }
vector_mapping: root = this.vector
create_if_missing: true
`
		spec := outputSpec()
		parsedConf, err := spec.ParseYAML(config, nil)
		require.NoError(t, err)

		writer, err := newOutputWriter(parsedConf, service.MockResources())
		require.NoError(t, err)
		assert.NotNil(t, writer)
		assert.Equal(t, operationUpsert, writer.op)
	})

	t.Run("missing required field", func(t *testing.T) {
		config := `
api_key: test-key
index_name: test-index
operation: upsert
id: ${! json("id") }
vector_mapping: root = this.vector
`
		spec := outputSpec()
		_, err := spec.ParseYAML(config, nil)
		assert.Error(t, err) // Should fail during YAML parsing
	})
}

func TestResolveIndexKey(t *testing.T) {
	// Clean up before and after
	os.RemoveAll(".cyborgdb")
	defer os.RemoveAll(".cyborgdb")

	logger := service.MockResources().Logger()

	t.Run("generate new key", func(t *testing.T) {
		key, err := resolveIndexKey("test-index", logger)
		require.NoError(t, err)
		assert.Len(t, key, 32)

		// Check file was created
		keyFile := filepath.Join(".cyborgdb", "test-index.key")
		_, err = os.Stat(keyFile)
		assert.NoError(t, err)
	})

	t.Run("use environment variable", func(t *testing.T) {
		testKey := generateTestKey()
		os.Setenv("CYBORGDB_INDEX_KEY", testKey)
		defer os.Unsetenv("CYBORGDB_INDEX_KEY")

		key, err := resolveIndexKey("test-index", logger)
		require.NoError(t, err)
		assert.Len(t, key, 32)
	})
}


