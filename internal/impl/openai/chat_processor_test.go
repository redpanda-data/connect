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
	"encoding/json"
	"testing"

	"github.com/go-faker/faker/v4"
	oai "github.com/sashabaranov/go-openai"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type mockChatClient struct {
	stubClient
}

func (*mockChatClient) CreateChatCompletion(_ context.Context, body oai.ChatCompletionRequest) (resp oai.ChatCompletionResponse, err error) {
	resp.ID = faker.UUIDHyphenated()
	resp.Model = body.Model
	resp.Choices = []oai.ChatCompletionChoice{
		{
			Message: oai.ChatCompletionMessage{
				Role:    "assistant",
				Content: faker.Paragraph(),
			},
		},
	}
	return
}

func TestChat(t *testing.T) {
	p := chatProcessor{
		baseProcessor: &baseProcessor{
			client: &mockChatClient{},
			model:  "gpt-4o",
		},
	}
	input := service.NewMessage([]byte(faker.Paragraph()))
	output, err := p.Process(t.Context(), input)
	assert.NoError(t, err)
	assert.Len(t, output, 1)
	msg := output[0]
	require.NoError(t, msg.GetError())
}

func TestChatInterpolationError(t *testing.T) {
	text, err := service.NewInterpolatedString(`${!throw("kaboom!")}`)
	assert.NoError(t, err)
	p := chatProcessor{
		baseProcessor: &baseProcessor{
			client: &mockChatClient{},
			model:  "gpt-4o",
		},
		userPrompt: text,
	}
	input := service.NewMessage([]byte(faker.Paragraph()))
	_, err = p.Process(t.Context(), input)
	assert.Error(t, err)
}

type captureChatClient struct {
	mockChatClient
	body oai.ChatCompletionRequest
}

func (c *captureChatClient) CreateChatCompletion(ctx context.Context, body oai.ChatCompletionRequest) (oai.ChatCompletionResponse, error) {
	c.body = body
	return c.mockChatClient.CreateChatCompletion(ctx, body)
}

func TestChatZeroSamplingValuesAreSent(t *testing.T) {
	zero := float32(0)
	client := &captureChatClient{}
	p := chatProcessor{
		baseProcessor: &baseProcessor{
			client: client,
			model:  "gpt-4o",
		},
		temperature: &zero,
		topP:        &zero,
	}
	_, err := p.Process(t.Context(), service.NewMessage([]byte("hi")))
	require.NoError(t, err)

	raw, err := json.Marshal(client.body)
	require.NoError(t, err)
	var sent map[string]any
	require.NoError(t, json.Unmarshal(raw, &sent))

	// An explicit 0 must reach the API instead of being dropped by omitempty,
	// otherwise the API default (1) applies.
	for _, field := range []string{"temperature", "top_p"} {
		v, ok := sent[field].(float64)
		require.Truef(t, ok, "%s missing from request body: %s", field, raw)
		assert.Less(t, v, 1e-30, field)
	}
}
