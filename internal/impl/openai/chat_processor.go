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
		Summary("Generates responses to messages in a chat conversation, using the OpenAI API.").
		Description(`
This processor sends the contents of user prompts to the OpenAI API, which generates responses. By default, the processor submits the entire payload of each message as a string, unless you use the `+"`"+ocpFieldUserPrompt+"`"+` configuration field to customize it.

To learn more about chat completion, see the https://platform.openai.com/docs/guides/chat-completions[OpenAI API documentation^].`).
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
				Description("The user prompt you want to generate a response for. By default, the processor submits the entire payload as a string.").
				Optional(),
			service.NewInterpolatedStringField(ocpFieldSystemPrompt).
				Description("The system prompt to submit along with the user prompt.").
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
