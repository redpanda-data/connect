// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cohere

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

type jsonSchema = map[string]any

type jsonSchemaProvider interface {
	GetJSONSchema(context.Context) (jsonSchema, error)
}

type fixedSchemaProvider struct {
	jsonSchema
}

func (s *fixedSchemaProvider) GetJSONSchema(context.Context) (jsonSchema, error) {
	return s.jsonSchema, nil
}

func newFixedSchema(raw string) (jsonSchemaProvider, error) {
	p := &fixedSchemaProvider{}
	if err := json.Unmarshal([]byte(raw), &p.jsonSchema); err != nil {
		return nil, fmt.Errorf("invalid JSON schema: %w", err)
	}
	return p, nil
}

type dynamicSchemaProvider struct {
	cached          jsonSchema
	nextRefreshTime time.Time
	refreshInterval time.Duration
	mu              sync.Mutex

	client  *sr.Client
	subject string
}

func (p *dynamicSchemaProvider) GetJSONSchema(ctx context.Context) (jsonSchema, error) {
	if time.Now().Before(p.nextRefreshTime) {
		return p.cached, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// Double check since we now have the lock that we didn't race with other requests
	if time.Now().Before(p.nextRefreshTime) {
		return p.cached, nil
	}
	info, err := p.client.GetSchemaBySubjectAndVersion(ctx, p.subject, nil, false)
	if err != nil {
		return nil, fmt.Errorf("unable to load latest schema for subject %q: %w", p.subject, err)
	}
	var schema jsonSchema
	if err := json.Unmarshal([]byte(info.Schema), &schema); err != nil {
		return nil, fmt.Errorf("unable to parse json schema from schema with ID=%d", info.ID)
	}
	p.cached = schema
	p.nextRefreshTime = time.Now().Add(p.refreshInterval)
	return p.cached, nil
}

func newDynamicSchema(client *sr.Client, subject string, refreshInterval time.Duration) jsonSchemaProvider {
	return &dynamicSchemaProvider{
		cached:          nil,
		nextRefreshTime: time.UnixMilli(0),
		refreshInterval: refreshInterval,
		client:          client,
		subject:         subject,
	}
}
