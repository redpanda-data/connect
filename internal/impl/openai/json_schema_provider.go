// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package openai

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	oai "github.com/sashabaranov/go-openai"
	"github.com/sashabaranov/go-openai/jsonschema"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

type jsonSchemaProvider interface {
	GetJSONSchema(context.Context) (*oai.ChatCompletionResponseFormatJSONSchema, error)
}

type fixedSchemaProvider struct {
	oai.ChatCompletionResponseFormatJSONSchema
}

func (s *fixedSchemaProvider) GetJSONSchema(context.Context) (*oai.ChatCompletionResponseFormatJSONSchema, error) {
	return &s.ChatCompletionResponseFormatJSONSchema, nil
}

func newFixedSchema(name, description, raw string) (jsonSchemaProvider, error) {
	p := &fixedSchemaProvider{}
	p.Name = name
	p.Description = description
	if err := json.Unmarshal([]byte(raw), &p.Schema); err != nil {
		return nil, fmt.Errorf("invalid JSON schema: %w", err)
	}
	p.Strict = true
	return p, nil
}

type dynamicSchemaProvider struct {
	cached          *oai.ChatCompletionResponseFormatJSONSchema
	nextRefreshTime time.Time
	refreshInterval time.Duration
	mu              sync.Mutex

	client     *sr.Client
	subject    string
	namePrefix string
}

func (p *dynamicSchemaProvider) GetJSONSchema(ctx context.Context) (*oai.ChatCompletionResponseFormatJSONSchema, error) {
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
	var schema jsonschema.Definition
	if err := json.Unmarshal([]byte(info.Schema.Schema), &schema); err != nil {
		return nil, fmt.Errorf("unable to parse json schema from schema with ID=%d", info.ID)
	}
	name := fmt.Sprintf("%s%d", p.namePrefix, info.ID)
	p.cached = &oai.ChatCompletionResponseFormatJSONSchema{
		Name:   name,
		Schema: &schema,
		Strict: true,
	}
	p.nextRefreshTime = time.Now().Add(p.refreshInterval)
	return p.cached, nil
}

func newDynamicSchema(client *sr.Client, subject, namePrefix string, refreshInterval time.Duration) jsonSchemaProvider {
	return &dynamicSchemaProvider{
		cached:          nil,
		nextRefreshTime: time.UnixMilli(0),
		refreshInterval: refreshInterval,
		client:          client,
		subject:         subject,
		namePrefix:      namePrefix,
	}
}
