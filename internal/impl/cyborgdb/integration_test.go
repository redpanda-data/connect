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

//go:build integration
// +build integration

package cyborgdb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cyborginc/cyborgdb-go"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegration(t *testing.T) {
	integration.CheckSkip(t)
	
	// Get environment variables for CyborgDB connection
	baseURL := os.Getenv("CYBORGDB_BASE_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8000"
	}
	
	apiKey := os.Getenv("CYBORGDB_API_KEY")
	if apiKey == "" {
		t.Skip("CYBORGDB_API_KEY not set")
	}
	
	// Generate a unique index name for this test run
	indexName := fmt.Sprintf("test-index-%d", time.Now().Unix())

	// Generate encryption key
	indexKey, err := cyborgdb.GenerateKey()
	require.NoError(t, err)
	indexKeyStr := base64.StdEncoding.EncodeToString(indexKey)

	// Register cleanup to always run, even on test failures
	t.Cleanup(func() {
		cleanupTestIndex(t, baseURL, apiKey, indexName, indexKeyStr)
	})

	t.Run("OutputOperations", func(t *testing.T) {
		testOutputOperations(t, baseURL, apiKey, indexName, indexKeyStr)
	})

	t.Run("BatchOperations", func(t *testing.T) {
		testBatchOperations(t, baseURL, apiKey, indexName, indexKeyStr)
	})
}

func testOutputOperations(t *testing.T, baseURL, apiKey, indexName, indexKey string) {
	// Create output config
	outputConf := fmt.Sprintf(`
host: %s
api_key: %s
index_name: %s
create_if_missing: true
operation: upsert
id: ${! json("id") }
vector_mapping: root = this.vector
metadata_mapping: root = this.metadata
`, baseURL, apiKey, indexName)
	
	// Parse output config
	outputSpec := outputSpec()
	outputParsedConf, err := outputSpec.ParseYAML(outputConf, nil)
	require.NoError(t, err)
	
	mgr := service.MockResources()
	
	// Create output
	
	writer, err := newOutputWriter(outputParsedConf, mgr)
	require.NoError(t, err)
	
	// Connect
	ctx := context.Background()
	err = writer.Connect(ctx)
	require.NoError(t, err)
	
	// Create test messages
	testVectors := []struct {
		id       string
		vector   []float32
		metadata map[string]interface{}
	}{
		{
			id:     "vec1",
			vector: []float32{0.1, 0.2, 0.3},
			metadata: map[string]interface{}{
				"category": "test",
				"score":    0.95,
			},
		},
		{
			id:     "vec2",
			vector: []float32{0.4, 0.5, 0.6},
			metadata: map[string]interface{}{
				"category": "example",
				"score":    0.87,
			},
		},
		{
			id:     "vec3",
			vector: []float32{0.7, 0.8, 0.9},
			metadata: map[string]interface{}{
				"category": "sample",
				"score":    0.92,
			},
		},
	}
	
	// Write vectors
	for _, tv := range testVectors {
		msg := createIntegrationTestMessage(tv.id, tv.vector, tv.metadata)
		batch := service.MessageBatch{msg}
		err = writer.WriteBatch(ctx, batch)
		require.NoError(t, err)
	}
	
	// Verify vectors were written successfully
	t.Logf("Successfully wrote %d vectors to CyborgDB index", len(testVectors))
	
	// Close connections
	err = writer.Close(ctx)
	require.NoError(t, err)
}

func testBatchOperations(t *testing.T, baseURL, apiKey, indexName, indexKey string) {
	ctx := context.Background()
	mgr := service.MockResources()
	
	// Create output for batch upsert
	outputConf := fmt.Sprintf(`
host: %s
api_key: %s
index_name: %s
operation: upsert
id: ${! json("id") }
vector_mapping: root = this.vector
batching:
  count: 3
  period: 1s
`, baseURL, apiKey, indexName)
	
	outputSpec := outputSpec()
	outputParsedConf, err := outputSpec.ParseYAML(outputConf, nil)
	require.NoError(t, err)
	
	writer, err := newOutputWriter(outputParsedConf, mgr)
	require.NoError(t, err)
	
	err = writer.Connect(ctx)
	require.NoError(t, err)
	
	// Create batch of messages
	batch := service.MessageBatch{}
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("batch-vec-%d", i)
		vector := []float32{float32(i) * 0.1, float32(i) * 0.2, float32(i) * 0.3}
		msg := createIntegrationTestMessage(id, vector, nil)
		batch = append(batch, msg)
	}
	
	// Write batch
	err = writer.WriteBatch(ctx, batch)
	require.NoError(t, err)
	
	// Verify batch was written successfully
	t.Logf("Successfully wrote batch of %d vectors", len(batch))
	
	// Test batch delete
	deleteConf := fmt.Sprintf(`
host: %s
api_key: %s
index_name: %s
operation: delete
id: ${! json("id") }
`, baseURL, apiKey, indexName)
	
	deleteParsedConf, err := outputSpec.ParseYAML(deleteConf, nil)
	require.NoError(t, err)
	
	deleter, err := newOutputWriter(deleteParsedConf, mgr)
	require.NoError(t, err)
	
	err = deleter.Connect(ctx)
	require.NoError(t, err)
	
	// Delete batch
	deleteBatch := service.MessageBatch{}
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("batch-vec-%d", i)
		msg := createIntegrationTestMessage(id, nil, nil)
		deleteBatch = append(deleteBatch, msg)
	}
	
	err = deleter.WriteBatch(ctx, deleteBatch)
	require.NoError(t, err)
	
	// Close connections
	err = writer.Close(ctx)
	require.NoError(t, err)
	
	err = deleter.Close(ctx)
	require.NoError(t, err)
}

func createIntegrationTestMessage(id string, vector []float32, metadata map[string]interface{}) *service.Message {
	data := map[string]interface{}{
		"id": id,
	}

	if vector != nil {
		// Convert []float32 to []interface{} for proper JSON serialization
		vecInterface := make([]interface{}, len(vector))
		for i, v := range vector {
			vecInterface[i] = v
		}
		data["vector"] = vecInterface
	}

	if metadata != nil {
		data["metadata"] = metadata
	}

	// Create message with JSON bytes instead of SetStructuredMut
	// This ensures bloblang can properly access the fields
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		panic(fmt.Sprintf("Failed to marshal test data: %v", err))
	}

	return service.NewMessage(jsonBytes)
}

func cleanupTestIndex(t *testing.T, baseURL, apiKey, indexName, indexKeyStr string) {
	// Create a client to delete the test index
	client, err := cyborgdb.NewClient(baseURL, apiKey)
	require.NoError(t, err)

	// Try to load the key from the auto-generated file
	keyFile := filepath.Join(".cyborgdb", fmt.Sprintf("%s.key", indexName))
	var indexKey []byte

	keyData, err := os.ReadFile(keyFile)
	if err == nil {
		// Parse the key from the file (skip comments, extract base64 key)
		lines := strings.Split(string(keyData), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" && !strings.HasPrefix(line, "#") {
				indexKey, err = base64.StdEncoding.DecodeString(line)
				if err != nil {
					t.Logf("Failed to decode key from file: %v", err)
					// Fall back to the provided key
					indexKey, err = base64.StdEncoding.DecodeString(indexKeyStr)
					require.NoError(t, err)
				}
				break
			}
		}
	} else {
		// Fall back to the provided key if file doesn't exist
		indexKey, err = base64.StdEncoding.DecodeString(indexKeyStr)
		require.NoError(t, err)
	}
	
	ctx := context.Background()
	
	// Load and delete the index
	index, err := client.LoadIndex(ctx, indexName, indexKey)
	if err != nil {
		// Index might not exist, that's okay
		t.Logf("Could not load index for cleanup: %v", err)
		return
	}
	
	err = index.DeleteIndex(ctx)
	if err != nil {
		t.Logf("Could not delete index: %v", err)
	} else {
		t.Logf("Successfully deleted test index: %s", indexName)
	}

	// Clean up the auto-generated key file
	if _, err := os.Stat(keyFile); err == nil {
		if err := os.Remove(keyFile); err != nil {
			t.Logf("Could not remove key file: %v", err)
		} else {
			t.Logf("Removed auto-generated key file: %s", keyFile)
		}
	}

	// Try to remove the .cyborgdb directory if it's empty
	_ = os.Remove(".cyborgdb")
}