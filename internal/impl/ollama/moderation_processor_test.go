// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package ollama

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/ollama/ollama/api"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/ollama"
)

func createModerationProcessorForTest(t *testing.T, model, addr, prompt, response string) *ollamaModerationProcessor {
	t.Helper()
	url, err := url.Parse(addr)
	require.NoError(t, err)
	p, err := service.NewInterpolatedString(prompt)
	require.NoError(t, err)
	r, err := service.NewInterpolatedString(response)
	require.NoError(t, err)
	return &ollamaModerationProcessor{
		baseOllamaProcessor: &baseOllamaProcessor{
			model:  model,
			client: api.NewClient(url, http.DefaultClient),
		},
		prompt:   p,
		response: r,
	}
}

func TestOllamaModerationIntegration(t *testing.T) {
	integration.CheckSkip(t)
	ctx := t.Context()

	ollamaContainer, err := ollama.Run(
		ctx,
		"ollama/ollama:0.4.2",
		testcontainers.WithLogger(testcontainers.TestLogger(t)),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := ollamaContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})
	addr, err := ollamaContainer.ConnectionString(ctx)
	require.NoError(t, err)
	for _, model := range []string{"llama-guard3:1b", "shieldgemma:2b"} {
		proc := createModerationProcessorForTest(t, model, addr, "How can I adopt my own llama?", "${!content()}")
		err = proc.pullModel(ctx)
		require.NoError(t, err)
		msg := service.NewMessage([]byte("Go to the zoo and steal one!"))
		batch, err := proc.Process(ctx, msg)
		require.NoError(t, err)
		require.Len(t, batch, 1)
		msg = batch[0]
		s, ok := msg.MetaGet("safe")
		require.True(t, ok)
		assert.Equal(t, "no", s)
		if model == "llama-guard3" {
			s, ok := msg.MetaGet("category")
			require.True(t, ok)
			assert.Equal(t, "S2", s)
		}

		msg = service.NewMessage([]byte("Go to a llama rescue facility and learn more about llamas first."))
		batch, err = proc.Process(ctx, msg)
		require.NoError(t, err)
		require.Len(t, batch, 1)
		msg = batch[0]
		s, ok = msg.MetaGet("safe")
		require.True(t, ok)
		assert.Equal(t, "yes", s)
		if model == "llama-guard3" {
			_, ok := msg.MetaGet("category")
			require.False(t, ok)
		}
	}
}
