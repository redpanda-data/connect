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
