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
	"bytes"
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

func createCompletionProcessorForTest(t *testing.T, addr string) *ollamaCompletionProcessor {
	t.Helper()
	url, err := url.Parse(addr)
	assert.NoError(t, err)
	return &ollamaCompletionProcessor{
		baseOllamaProcessor: &baseOllamaProcessor{
			// use smallest model possible to make it cheaper
			model:  "tinyllama",
			client: NewClient(url, http.DefaultClient),
		},
		userPrompt:   nil,
		systemPrompt: nil,
	}
}

func TestOllamaCompletionIntegration(t *testing.T) {
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
