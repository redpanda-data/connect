// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package ollama

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/ollama/ollama/api"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/ollama"
)

func createEmbeddingsProcessorForTest(t *testing.T, addr string) *ollamaEmbeddingProcessor {
	t.Helper()
	url, err := url.Parse(addr)
	assert.NoError(t, err)
	return &ollamaEmbeddingProcessor{
		baseOllamaProcessor: &baseOllamaProcessor{
			// use smallest model possible to make it cheaper
			model:  "all-minilm",
			cmd:    nil,
			client: api.NewClient(url, http.DefaultClient),
			logger: nil,
		},
		text: nil,
	}
}

func TestOllamaEmbeddingsIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	ctx := context.Background()
	ollamaContainer, err := ollama.Run(ctx, "ollama/ollama:0.2.5")
	assert.NoError(t, err)
	defer func() {
		if err := ollamaContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()
	addr, err := ollamaContainer.ConnectionString(ctx)
	assert.NoError(t, err)
	proc := createEmbeddingsProcessorForTest(t, addr)
	err = proc.pullModel(context.Background())
	assert.NoError(t, err)
	msg := service.NewMessage([]byte("Redpanda is the fastest and best streaming platform"))
	batch, err := proc.Process(ctx, msg)
	assert.NoError(t, err)
	assert.Len(t, batch, 1)
	msg = batch[0]
	embd, err := msg.AsStructured()
	assert.NoError(t, err)
	assert.NoError(t, msg.GetError())
	require.IsType(t, []any{}, embd)
	require.Len(t, embd, 384)
	for i := 0; i < 384; i++ {
		require.IsType(t, float64(0), embd.([]any)[i])
	}
}
