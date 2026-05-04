// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforcegrpc

import (
	"context"
	"fmt"
	"sync"

	"github.com/twmb/avro"
	"google.golang.org/grpc/metadata"
)

// SchemaCache provides thread-safe caching of Avro schemas keyed by schema ID.
// On cache miss it fetches the schema from the Salesforce Pub/Sub API via gRPC.
type SchemaCache struct {
	mu      sync.RWMutex
	schemas map[string]*avro.Schema
	pubsub  PubSubClient

	bearerToken string
	instanceURL string
	tenantID    string
}

// NewSchemaCache creates a new SchemaCache that uses the given PubSubClient to fetch schemas.
func NewSchemaCache(pubsub PubSubClient, bearerToken, instanceURL, tenantID string) *SchemaCache {
	return &SchemaCache{
		schemas:     make(map[string]*avro.Schema),
		pubsub:      pubsub,
		bearerToken: bearerToken,
		instanceURL: instanceURL,
		tenantID:    tenantID,
	}
}

// UpdateAuth updates the authentication credentials used for schema fetches.
func (sc *SchemaCache) UpdateAuth(bearerToken, instanceURL, tenantID string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.bearerToken = bearerToken
	sc.instanceURL = instanceURL
	sc.tenantID = tenantID
}

// GetSchema returns the cached Avro schema for the given schema ID, or fetches and caches it.
func (sc *SchemaCache) GetSchema(ctx context.Context, schemaID string) (*avro.Schema, error) {
	sc.mu.RLock()
	if s, ok := sc.schemas[schemaID]; ok {
		sc.mu.RUnlock()
		return s, nil
	}
	sc.mu.RUnlock()

	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Double-check after acquiring write lock
	if s, ok := sc.schemas[schemaID]; ok {
		return s, nil
	}

	md := metadata.Pairs(
		"accesstoken", sc.bearerToken,
		"instanceurl", sc.instanceURL,
		"tenantid", sc.tenantID,
	)
	rpcCtx := metadata.NewOutgoingContext(ctx, md)

	schemaInfo, err := sc.pubsub.GetSchema(rpcCtx, &SchemaRequest{SchemaId: schemaID})
	if err != nil {
		return nil, fmt.Errorf("fetch schema %s: %w", schemaID, err)
	}

	s, err := avro.Parse(schemaInfo.SchemaJson)
	if err != nil {
		return nil, fmt.Errorf("compile Avro schema %s: %w", schemaID, err)
	}

	sc.schemas[schemaID] = s
	return s, nil
}

// DecodeAvroPayload decodes a binary Avro payload using the given schema.
func DecodeAvroPayload(schema *avro.Schema, payload []byte) (map[string]any, error) {
	var result map[string]any
	if _, err := schema.Decode(payload, &result, avro.TaggedUnions()); err != nil {
		return nil, fmt.Errorf("decode Avro payload: %w", err)
	}
	return result, nil
}

// UnwrapAvroString returns the string value from an Avro union-encoded value.
// `avro.TaggedUnions()` represents a string member of a union as
// `{"string": "..."}`; non-union string fields decode directly to a string.
// Returns "" / false for missing or non-string values.
func UnwrapAvroString(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case map[string]any:
		s, ok := x["string"].(string)
		return s, ok
	}
	return "", false
}

// UnwrapAvroArray returns the slice value from an Avro union-encoded value.
// Tagged unions wrap arrays as `{"array": [...]}`; non-union array fields
// decode directly to `[]any`. Returns nil / false for missing or non-array
// values.
func UnwrapAvroArray(v any) ([]any, bool) {
	switch x := v.(type) {
	case []any:
		return x, true
	case map[string]any:
		arr, ok := x["array"].([]any)
		return arr, ok
	}
	return nil, false
}
