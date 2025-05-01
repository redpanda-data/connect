// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package ollama

import (
	"bytes"
	"net/http"
	"net/url"
	"testing"

	"github.com/ollama/ollama/api"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/ollama"
)

func createCompletionProcessorForTest(t *testing.T, addr string) *ollamaCompletionProcessor {
	t.Helper()
	url, err := url.Parse(addr)
	assert.NoError(t, err)
	return &ollamaCompletionProcessor{
		baseOllamaProcessor: &baseOllamaProcessor{
			// use smallest model possible to make it cheaper
			model:  "tinyllama",
			client: api.NewClient(url, http.DefaultClient),
		},
		userPrompt:   nil,
		systemPrompt: nil,
	}
}

func TestOllamaCompletionIntegration(t *testing.T) {
	integration.CheckSkip(t)

	ctx := t.Context()
	ollamaContainer, err := ollama.Run(ctx, "ollama/ollama:0.5.1")
	assert.NoError(t, err)
	defer func() {
		if err := ollamaContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()
	addr, err := ollamaContainer.ConnectionString(ctx)
	assert.NoError(t, err)
	proc := createCompletionProcessorForTest(t, addr)
	err = proc.pullModel(t.Context())
	assert.NoError(t, err)
	msg := service.NewMessage([]byte("In one word what color is snow?"))
	batch, err := proc.Process(ctx, msg)
	assert.NoError(t, err)
	assert.Len(t, batch, 1)
	msg = batch[0]
	b, err := msg.AsBytes()
	assert.NoError(t, err)
	assert.NoError(t, msg.GetError())
	require.Contains(t, string(bytes.ToLower(b)), "white")
}
