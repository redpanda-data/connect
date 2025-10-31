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

package ollama

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/ollama"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func createEmbeddingsProcessorForTest(t *testing.T, addr string) *ollamaEmbeddingProcessor {
	t.Helper()
	url, err := url.Parse(addr)
	assert.NoError(t, err)
	return &ollamaEmbeddingProcessor{
		baseOllamaProcessor: &baseOllamaProcessor{
			// use smallest model possible to make it cheaper
			model:  "all-minilm",
			client: NewClient(url, http.DefaultClient),
		},
		text: nil,
	}
}

func TestOllamaEmbeddingsIntegration(t *testing.T) {
	integration.CheckSkip(t)

	ctx := t.Context()
	ollamaContainer, err := ollama.Run(ctx, "ollama/ollama:0.9.0")
	assert.NoError(t, err)
	t.Cleanup(func() {
		if err := ollamaContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})
	addr, err := ollamaContainer.ConnectionString(ctx)
	assert.NoError(t, err)
	proc := createEmbeddingsProcessorForTest(t, addr)
	err = proc.pullModel(t.Context())
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
	for i := range 384 {
		require.IsType(t, float64(0), embd.([]any)[i])
	}
}
