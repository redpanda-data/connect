// Copyright 2024 Redpanda Data, Inc.
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

	"github.com/linkedin/goavro/v2"
	"google.golang.org/grpc/metadata"
)

// SchemaCache provides thread-safe caching of Avro codecs keyed by schema ID.
// On cache miss it fetches the schema from the Salesforce Pub/Sub API via gRPC.
type SchemaCache struct {
	mu     sync.RWMutex
	codecs map[string]*goavro.Codec
	pubsub PubSubClient

	bearerToken string
	instanceURL string
	tenantID    string
}

// NewSchemaCache creates a new SchemaCache that uses the given PubSubClient to fetch schemas.
func NewSchemaCache(pubsub PubSubClient, bearerToken, instanceURL, tenantID string) *SchemaCache {
	return &SchemaCache{
		codecs:      make(map[string]*goavro.Codec),
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

// GetCodec returns the cached Avro codec for the given schema ID, or fetches and caches it.
func (sc *SchemaCache) GetCodec(ctx context.Context, schemaID string) (*goavro.Codec, error) {
	sc.mu.RLock()
	if codec, ok := sc.codecs[schemaID]; ok {
		sc.mu.RUnlock()
		return codec, nil
	}
	sc.mu.RUnlock()

	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Double-check after acquiring write lock
	if codec, ok := sc.codecs[schemaID]; ok {
		return codec, nil
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

	codec, err := goavro.NewCodec(schemaInfo.SchemaJson)
	if err != nil {
		return nil, fmt.Errorf("compile Avro schema %s: %w", schemaID, err)
	}

	sc.codecs[schemaID] = codec
	return codec, nil
}

// DecodeAvroPayload decodes a binary Avro payload using the given codec.
func DecodeAvroPayload(codec *goavro.Codec, payload []byte) (map[string]any, error) {
	native, _, err := codec.NativeFromBinary(payload)
	if err != nil {
		return nil, fmt.Errorf("decode Avro payload: %w", err)
	}

	result, ok := native.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("decoded Avro payload is not a map, got %T", native)
	}

	return result, nil
}
