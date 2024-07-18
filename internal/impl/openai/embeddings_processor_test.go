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

package openai

import (
	"context"
	"testing"

	oai "github.com/Azure/azure-sdk-for-go/sdk/ai/azopenai"
	"github.com/go-faker/faker/v4"
	"github.com/go-faker/faker/v4/pkg/options"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockEmbeddingsClient struct {
	stubClient
}

func mockEmbeddings(text string) []float32 {
	embd := make([]float32, len(text))
	for i, r := range text {
		embd[i] = float32(r)
	}
	return embd
}

func (m *mockEmbeddingsClient) GetEmbeddings(ctx context.Context, body oai.EmbeddingsOptions, options *oai.GetEmbeddingsOptions) (resp oai.GetEmbeddingsResponse, err error) {
	for i, text := range body.Input {
		idx := int32(i)
		resp.Data = append(resp.Data, oai.EmbeddingItem{
			Embedding: mockEmbeddings(text),
			Index:     &idx,
		})
	}
	return
}

func TestEmbedding(t *testing.T) {
	text, err := bloblang.GlobalEnvironment().Parse(`content().string()`)
	assert.NoError(t, err)
	p := embeddingsProcessor{
		baseProcessor: &baseProcessor{
			client: &mockEmbeddingsClient{},
			model:  "text-embedding-ada-002",
		},
		text: text,
	}
	input := service.NewMessage([]byte(faker.Paragraph(options.WithGenerateUniqueValues(true))))
	output, err := p.Process(context.Background(), input)
	assert.NoError(t, err)
	assert.Len(t, output, 1)
	msg := output[0]
	require.NoError(t, msg.GetError())
}

func TestEmbeddingInterpolationError(t *testing.T) {
	text, err := bloblang.GlobalEnvironment().Parse(`throw("kaboom!")`)
	assert.NoError(t, err)
	p := embeddingsProcessor{
		baseProcessor: &baseProcessor{
			client: &mockEmbeddingsClient{},
			model:  "text-embedding-ada-002",
		},
		text: text,
	}
	input := service.NewMessage([]byte(faker.Paragraph(options.WithGenerateUniqueValues(true))))
	_, err = p.Process(context.Background(), input)
	assert.Error(t, err)
}
