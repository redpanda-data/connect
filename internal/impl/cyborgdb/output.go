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
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cyborginc/cyborgdb-go"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	poFieldBatching        = "batching"
	poFieldHost            = "host"
	poFieldAPIKey          = "api_key"
	poFieldIndexName       = "index_name"
	poFieldID              = "id"
	poFieldOp              = "operation"
	poFieldVectorMapping   = "vector_mapping"
	poFieldMetadataMapping = "metadata_mapping"
	poFieldCreateIfMissing = "create_if_missing"
	
	// KeySize is the required size for CyborgDB encryption keys (32 bytes for AES-256)
	KeySize = 32
)

func outputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("4.40.0").
		Categories("AI").
		Summary("Inserts items into a CyborgDB encrypted vector index.").
		Description(`
This output allows you to write vectors to a CyborgDB encrypted index. CyborgDB provides
end-to-end encrypted vector storage with automatic dimension detection and index optimization.

All vector data is encrypted client-side before being sent to the server, ensuring complete
data privacy. The encryption key never leaves your infrastructure.
`).
		Fields(
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(poFieldBatching),
			service.NewStringField(poFieldHost).
				Description("The host for the CyborgDB instance.").
				Example("api.cyborg.com").
				Example("localhost:8000"),
			service.NewStringField(poFieldAPIKey).
				Secret().
				Description("The CyborgDB API key for authentication."),
			service.NewStringField(poFieldIndexName).
				Default("redpanda-vectors").
				Description("The name of the index to write to. Encryption keys are auto-generated and stored securely in .cyborgdb/ directory."),
			service.NewBoolField(poFieldCreateIfMissing).
				Default(false).
				Advanced().
				Description("If true, create the index if it doesn't exist. CyborgDB will auto-detect dimension and optimize the index."),
			service.NewStringEnumField(poFieldOp, "upsert", "delete").
				Default("upsert").
				Description("The operation to perform against the CyborgDB index."),
			service.NewInterpolatedStringField(poFieldID).
				Description("The ID for the vector entry in CyborgDB."),
			service.NewBloblangField(poFieldVectorMapping).
				Optional().
				Description("The mapping to extract out the vector from the document. The result must be a floating point array. Required for upsert operations.").
				Example("root = this.embeddings_vector").
				Example("root = [1.2, 0.5, 0.76]"),
			service.NewBloblangField(poFieldMetadataMapping).
				Optional().
				Description("An optional mapping of message to metadata for the vector entry.").
				Example(`root = @`).
				Example(`root = metadata()`).
				Example(`root = {"summary": this.summary, "category": this.category}`),
		)
}

func init() {
	service.MustRegisterBatchOutput(
		"cyborgdb",
		outputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPol service.BatchPolicy, mif int, err error) {
			if batchPol, err = conf.FieldBatchPolicy(poFieldBatching); err != nil {
				return
			}
			if mif, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if out, err = newOutputWriter(conf, mgr); err != nil {
				return
			}
			return
		})
}

type operation string

const (
	operationUpsert operation = "upsert"
	operationDelete operation = "delete"
)

type outputWriter struct {
	client client
	index  indexClient
	
	host      string
	indexName string
	indexKey  []byte
	op        operation
	logger    *service.Logger

	createIfMissing bool

	id              *service.InterpolatedString
	vectorMapping   *bloblang.Executor
	metadataMapping *bloblang.Executor

	mu   sync.Mutex
	init bool
}

func newOutputWriter(conf *service.ParsedConfig, mgr *service.Resources) (*outputWriter, error) {
	host, err := conf.FieldString(poFieldHost)
	if err != nil {
		return nil, err
	}
	
	// Build base URL from host
	baseURL := host
	if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
		baseURL = "https://" + host
	}
	
	apiKey, err := conf.FieldString(poFieldAPIKey)
	if err != nil {
		return nil, err
	}
	
	cyborgClient, err := cyborgdb.NewClient(baseURL, apiKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create CyborgDB client: %w", err)
	}
	
	indexName, err := conf.FieldString(poFieldIndexName)
	if err != nil {
		return nil, err
	}
	
	// Generate or load encryption key from local storage
	indexKey, err := resolveIndexKey(indexName, mgr.Logger())
	if err != nil {
		return nil, err
	}
	
	rawOp, err := conf.FieldString(poFieldOp)
	if err != nil {
		return nil, err
	}
	
	var op operation
	switch rawOp {
	case string(operationUpsert):
		op = operationUpsert
	case string(operationDelete):
		op = operationDelete
	default:
		return nil, fmt.Errorf("invalid operation: %s", rawOp)
	}
	
	id, err := conf.FieldInterpolatedString(poFieldID)
	if err != nil {
		return nil, err
	}
	
	createIfMissing, err := conf.FieldBool(poFieldCreateIfMissing)
	if err != nil {
		return nil, err
	}
	
	var vectorMapping *bloblang.Executor
	var metadataMapping *bloblang.Executor
	
	if op == operationUpsert {
		vectorMapping, err = conf.FieldBloblang(poFieldVectorMapping)
		if err != nil {
			return nil, err
		}
		
		if conf.Contains(poFieldMetadataMapping) {
			metadataMapping, err = conf.FieldBloblang(poFieldMetadataMapping)
			if err != nil {
				return nil, err
			}
		}
	}
	
	w := outputWriter{
		client:          &cyborgdbClient{cyborgClient},
		host:            host,
		indexName:       indexName,
		indexKey:        indexKey,
		op:              op,
		logger:          mgr.Logger(),
		createIfMissing: createIfMissing,
		id:              id,
		vectorMapping:   vectorMapping,
		metadataMapping: metadataMapping,
	}
	
	return &w, nil
}

// resolveIndexKey resolves encryption key with priority: ENV > existing file > generate new
func resolveIndexKey(indexName string, logger *service.Logger) ([]byte, error) {
	// Check environment variable
	if envKey := os.Getenv("CYBORGDB_INDEX_KEY"); envKey != "" {
		logger.Infof("Using encryption key from CYBORGDB_INDEX_KEY environment variable")
		return decodeBase64Key(envKey)
	}
	
	// Load from existing local file (reuse generated key)
	keyFile := filepath.Join(".cyborgdb", fmt.Sprintf("%s.key", indexName))
	if keyData, err := os.ReadFile(keyFile); err == nil {
		logger.Debugf("Reusing existing encryption key from: %s", keyFile)
		return decodeKeyFromFile(string(keyData))
	}
	
	// Generate and store new key using CyborgDB SDK (DEV/TEST ONLY)
	logger.Infof("No existing key found. Generating new encryption key for index: %s", indexName)
	logger.Warnf("Auto-generating encryption keys is for DEVELOPMENT and TESTING only!")
	logger.Warnf("For production, set CYBORGDB_INDEX_KEY environment variable!")
	
	// Generate a new 32-byte key for AES-256 encryption
	key := make([]byte, KeySize)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("failed to generate encryption key: %w", err)
	}
	
	keyStr := base64.StdEncoding.EncodeToString(key)
	
	// Create secure storage directory
	if err := os.MkdirAll(".cyborgdb", 0700); err != nil {
		return nil, fmt.Errorf("failed to create .cyborgdb directory: %w", err)
	}
	
	keyFile = filepath.Join(".cyborgdb", fmt.Sprintf("%s.key", indexName))
	keyFileContent := fmt.Sprintf(`# CyborgDB Development/Testing Encryption Key
# 
#   WARNING: This is an AUTO-GENERATED key for DEVELOPMENT and TESTING only!
#   DO NOT use auto-generated keys in production environments!
#   DO NOT commit this file to version control!
#
# Generated for index: %s
# Created by: CyborgDB Go SDK
# 
# This key is required to decrypt your vector data.
# For development/testing: Keep this file secure and back it up safely!
# Add .cyborgdb/ to your .gitignore to avoid committing keys.
#
# For PRODUCTION, use environment variable instead:
#   export CYBORGDB_INDEX_KEY="your-secure-production-key"
#
%s`, indexName, keyStr)
	
	if err := os.WriteFile(keyFile, []byte(keyFileContent), 0600); err != nil {
		logger.Errorf("Failed to save encryption key to %s: %v", keyFile, err)
		logger.Warnf("Key will only be available for this session!")
	} else {
		logger.Infof("Development key saved to: %s", keyFile)
		logger.Warnf("Add .cyborgdb/ to your .gitignore file!")
		logger.Warnf("Keep this development key secure - it's needed to decrypt your data!")
		logger.Warnf("For production: export CYBORGDB_INDEX_KEY=\"your-secure-key\"")
	}
	
	return key, nil
}

// decodeKeyFromFile decodes a base64-encoded key from a key file
func decodeKeyFromFile(content string) ([]byte, error) {
	// Extract the key from file content
	lines := strings.Split(content, "\n")
	var keyStr string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			keyStr = line
			break
		}
	}
	
	if keyStr == "" {
		return nil, fmt.Errorf("no key found in file")
	}
	
	indexKey, err := base64.StdEncoding.DecodeString(keyStr)
	if err != nil {
		return nil, fmt.Errorf("invalid key encoding (must be base64): %w", err)
	}
	
	if len(indexKey) != KeySize {
		return nil, fmt.Errorf("key must be exactly %d bytes, got %d", KeySize, len(indexKey))
	}
	
	return indexKey, nil
}

// decodeBase64Key decodes and validates a base64-encoded key string
func decodeBase64Key(keyStr string) ([]byte, error) {
	keyStr = strings.TrimSpace(keyStr)
	if keyStr == "" {
		return nil, fmt.Errorf("key string is empty")
	}
	
	indexKey, err := base64.StdEncoding.DecodeString(keyStr)
	if err != nil {
		return nil, fmt.Errorf("invalid key encoding (must be base64): %w", err)
	}
	
	if len(indexKey) != KeySize {
		return nil, fmt.Errorf("key must be exactly %d bytes, got %d", KeySize, len(indexKey))
	}
	
	return indexKey, nil
}

func (w *outputWriter) Connect(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.init {
		return nil
	}
	
	w.logger.Tracef("Connecting to CyborgDB index %s", w.indexName)
	
	// Check if index exists first
	indexes, err := w.client.ListIndexes(ctx)
	if err != nil {
		return fmt.Errorf("failed to list indexes: %w", err)
	}
	
	indexExists := false
	for _, name := range indexes {
		if name == w.indexName {
			indexExists = true
			break
		}
	}
	
	var index *cyborgdb.EncryptedIndex
	
	if indexExists {
		// Get existing index
		w.logger.Tracef("Getting existing index %s", w.indexName)
		index, err = w.client.GetIndex(ctx, w.indexName, w.indexKey)
		if err != nil {
			return fmt.Errorf("failed to get index %s: %w", w.indexName, err)
		}
		w.logger.Tracef("Successfully got index %s", w.indexName)
	} else {
		if !w.createIfMissing {
			return fmt.Errorf("index %s does not exist and create_if_missing is false", w.indexName)
		}
		
		// Create new index with hardcoded ivfflat type
		// CyborgDB will auto-detect dimension and auto-train
		w.logger.Infof("Creating new CyborgDB index %s with IVFFlat (auto-dimension, auto-train)", w.indexName)
		
		index, err = w.client.CreateIndex(ctx, w.indexName, w.indexKey)
		if err != nil {
			return fmt.Errorf("failed to create index %s: %w", w.indexName, err)
		}
		
		w.logger.Infof("Successfully created CyborgDB index %s", w.indexName)
	}
	
	w.index = &cyborgdbEncryptedIndex{index}
	w.init = true
	w.logger.Tracef("Connected to CyborgDB index %s", w.indexName)
	
	return nil
}

func (w *outputWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if !w.init {
		if err := w.Connect(ctx); err != nil {
			return err
		}
	}
	
	switch w.op {
	case operationUpsert:
		return w.upsertBatch(ctx, batch)
	case operationDelete:
		return w.deleteBatch(ctx, batch)
	default:
		return fmt.Errorf("unsupported operation: %s", w.op)
	}
}

func (w *outputWriter) upsertBatch(ctx context.Context, batch service.MessageBatch) error {
	batchSize := len(batch)
	if batchSize == 0 {
		return nil // Nothing to do for empty batch
	}

	// Pre-allocate
	items := make([]cyborgdb.VectorItem, 0, batchSize)

	// Use batch executors
	idExec := batch.InterpolationExecutor(w.id)
	var vectorExec *service.MessageBatchBloblangExecutor
	if w.vectorMapping != nil {
		vectorExec = batch.BloblangExecutor(w.vectorMapping)
	}
	var metadataExec *service.MessageBatchBloblangExecutor
	if w.metadataMapping != nil {
		metadataExec = batch.BloblangExecutor(w.metadataMapping)
	}

	for i := range batch {
		id, err := idExec.TryString(i)
		if err != nil {
			return fmt.Errorf("failed to interpolate id: %w", err)
		}

		var vecResult interface{}

		if vectorExec != nil {
			// Execute vector mapping using batch executor
			rawVec, err := vectorExec.Query(i)
			if err != nil {
				return fmt.Errorf("failed to execute vector mapping: %w", err)
			}
			if rawVec == nil {
				continue // Skip if no vector returned
			}
			vecResult, err = rawVec.AsStructured()
			if err != nil {
				return fmt.Errorf("vector mapping extraction failed: %w", err)
			}
		} else {
			// Fall back to extracting "vector" field from structured message
			msg := batch[i]
			structured, err := msg.AsStructured()
			if err != nil {
				return fmt.Errorf("failed to parse message: %w", err)
			}

			// If it's a map, try to extract the "vector" field
			if structMap, ok := structured.(map[string]interface{}); ok {
				if vec, exists := structMap["vector"]; exists {
					vecResult = vec
				} else {
					return fmt.Errorf("no 'vector' field found in structured message")
				}
			} else {
				// Otherwise assume the entire structured message is the vector
				vecResult = structured
			}
		}
		
		// Handle different vector result types
		var vector []float32
		switch v := vecResult.(type) {
		case []float32:
			vector = v
		case []float64:
			vector = make([]float32, len(v))
			for i, val := range v {
				vector[i] = float32(val)
			}
		case []interface{}:
			vector = make([]float32, len(v))
			for i, elem := range v {
				switch val := elem.(type) {
				case float64:
					vector[i] = float32(val)
				case float32:
					vector[i] = val
				case int:
					vector[i] = float32(val)
				case int64:
					vector[i] = float32(val)
				case json.Number:
					f, err := val.Float64()
					if err != nil {
						return fmt.Errorf("vector element %d cannot be converted to float: %w", i, err)
					}
					vector[i] = float32(f)
				default:
					return fmt.Errorf("vector element %d is not a number: %T", i, val)
				}
			}
		case nil:
			return fmt.Errorf("vector mapping returned nil - check that vector field exists in message")
		default:
			return fmt.Errorf("vector mapping must return an array, got %T", vecResult)
		}
		
		item := cyborgdb.VectorItem{
			Id:     id,
			Vector: vector,
		}
		
		// Process metadata
		if metadataExec != nil {
			// Use metadata mapping with batch executor
			rawMeta, err := metadataExec.Query(i)
			if err != nil {
				return fmt.Errorf("failed to execute metadata mapping: %w", err)
			}

			if rawMeta != nil {
				metaResult, err := rawMeta.AsStructured()
				if err != nil {
					return fmt.Errorf("metadata mapping extraction failed: %w", err)
				}

				if metaMap, ok := metaResult.(map[string]interface{}); ok {
					item.Metadata = metaMap
				}
			}
		} else if w.metadataMapping == nil {
			// Extract metadata from structured message only if no mapping provided
			msg := batch[i]
			structured, err := msg.AsStructured()
			if err == nil {
				if structMap, ok := structured.(map[string]interface{}); ok {
					// Count metadata fields first to avoid allocation if none
					metaCount := 0
					for k := range structMap {
						if k != "id" && k != "vector" {
							metaCount++
						}
					}

					if metaCount > 0 {
						metadata := make(map[string]interface{}, metaCount)
						for k, v := range structMap {
							if k != "id" && k != "vector" {
								metadata[k] = v
							}
						}
						item.Metadata = metadata
					}
				}
			}
		}
		
		items = append(items, item)
	}
	
	if err := w.index.Upsert(ctx, items); err != nil {
		return fmt.Errorf("failed to upsert vectors: %w", err)
	}
	
	return nil
}

func (w *outputWriter) deleteBatch(ctx context.Context, batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}

	ids := make([]string, 0, len(batch))

	// Use batch executor for consistency
	idExec := batch.InterpolationExecutor(w.id)

	for i := range batch {
		id, err := idExec.TryString(i)
		if err != nil {
			return fmt.Errorf("failed to interpolate id: %w", err)
		}
		ids = append(ids, id)
	}
	
	if err := w.index.Delete(ctx, ids); err != nil {
		return fmt.Errorf("failed to delete vectors: %w", err)
	}
	
	return nil
}

func (w *outputWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.index != nil {
		return w.index.Close()
	}
	return nil
}