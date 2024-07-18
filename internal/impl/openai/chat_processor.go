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
	"errors"
	"fmt"

	oai "github.com/Azure/azure-sdk-for-go/sdk/ai/azopenai"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	ocpFieldUserPrompt   = "prompt"
	ocpFieldSystemPrompt = "system_prompt"
)

func init() {
	err := service.RegisterProcessor(
		"openai_chat_completion",
		chatProcessorConfig(),
		makeChatProcessor,
	)
	if err != nil {
		panic(err)
	}
}

func chatProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("AI").
		Summary("Processor that uses the OpenAI API to create a model response for the given chat conversation.").
		Description(`
This processor calls the OpenAI API, creating a chat completion based on the user prompt. By default the entire message's payload as a string is submitted, and the `+"`"+ocpFieldUserPrompt+"`"+` configuration field allows customizing that.

You can learn more about chat completion here: https://platform.openai.com/docs/guides/chat-completions[https://platform.openai.com/docs/guides/chat-completions^]`).
		Version("4.32.0").
		Fields(
			baseConfigFieldsWithModels(
				"gpt-4o",
				"gpt-4o-mini",
				"gpt-4",
				"gpt4-turbo",
			)...,
		).
		Fields(
			service.NewBloblangField(ocpFieldUserPrompt).
				Description("The prompt to generate text from. By default the entire payload as a string is submitted.").
				Optional(),
			service.NewInterpolatedStringField(ocpFieldSystemPrompt).
				Description("The system prompt to submit along with the prompt.").
				Optional(),
		)
}

func makeChatProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	b, err := newBaseProcessor(conf)
	if err != nil {
		return nil, err
	}
	var up *bloblang.Executor
	if conf.Contains(ocpFieldUserPrompt) {
		up, err = conf.FieldBloblang(ocpFieldUserPrompt)
		if err != nil {
			return nil, err
		}
	}
	var sp *service.InterpolatedString
	if conf.Contains(ocpFieldSystemPrompt) {
		sp, err = conf.FieldInterpolatedString(ocpFieldSystemPrompt)
		if err != nil {
			return nil, err
		}
	}
	return &chatProcessor{b, up, sp}, nil
}

type chatProcessor struct {
	*baseProcessor

	userPrompt   *bloblang.Executor
	systemPrompt *service.InterpolatedString
}

func (p *chatProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	var body oai.ChatCompletionsOptions
	body.DeploymentName = &p.model
	if p.systemPrompt != nil {
		s, err := p.systemPrompt.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", ocpFieldSystemPrompt, err)
		}
		body.Messages = append(body.Messages, &oai.ChatRequestSystemMessage{
			Content: &s,
		})
	}
	if p.userPrompt != nil {
		s, err := msg.BloblangQueryValue(p.userPrompt)
		if err != nil {
			return nil, fmt.Errorf("%s execution error: %w", ocpFieldUserPrompt, err)
		}
		body.Messages = append(body.Messages, &oai.ChatRequestUserMessage{
			Content: oai.NewChatRequestUserMessageContent(bloblang.ValueToString(s)),
		})
	} else {
		b, err := msg.AsBytes()
		if err != nil {
			return nil, err
		}
		body.Messages = append(body.Messages, &oai.ChatRequestUserMessage{
			Content: oai.NewChatRequestUserMessageContent(string(b)),
		})
	}
	var opts oai.GetChatCompletionsOptions
	resp, err := p.client.GetChatCompletions(ctx, body, &opts)
	if err != nil {
		return nil, err
	}
	if len(resp.Choices) != 1 {
		return nil, fmt.Errorf("invalid number of choices in response: %d", len(resp.Choices))
	}
	if resp.Choices[0].Message == nil {
		return nil, errors.New("invalid missing message in chat response")
	}
	if resp.Choices[0].Message.Content == nil {
		return nil, errors.New("invalid missing message content in chat response")
	}
	msg = msg.Copy()
	msg.SetBytes([]byte(*resp.Choices[0].Message.Content))
	return service.MessageBatch{msg}, nil
}
