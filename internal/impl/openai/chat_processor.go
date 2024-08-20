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
	ocpFieldUserPrompt       = "prompt"
	ocpFieldSystemPrompt     = "system_prompt"
	ocpFieldMaxTokens        = "max_tokens"
	ocpFieldTemp             = "temperature"
	ocpFieldUser             = "user"
	ocpFieldTopP             = "top_p"
	ocpFieldSeed             = "seed"
	ocpFieldStop             = "stop"
	ocpFieldPresencePenalty  = "presence_penalty"
	ocpFieldFrequencyPenalty = "frequency_penalty"
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
			service.NewIntField(ocpFieldMaxTokens).
				Optional().
				Description("The maximum number of tokens that can be generated in the chat completion."),
			service.NewFloatField(ocpFieldTemp).
				Optional().
				Description(`What sampling temperature to use, between 0 and 2. Higher values like 0.8 will make the output more random, while lower values like 0.2 will make it more focused and deterministic.

We generally recommend altering this or top_p but not both.`).
				LintRule(`root = if this > 2 || this < 0 { [ "field must be between 0 and 2" ] }`),
			service.NewInterpolatedStringField(ocpFieldUser).
				Optional().
				Description("A unique identifier representing your end-user, which can help OpenAI to monitor and detect abuse."),
			service.NewFloatField(ocpFieldTopP).
				Optional().
				Advanced().
				Description(`An alternative to sampling with temperature, called nucleus sampling, where the model considers the results of the tokens with top_p probability mass. So 0.1 means only the tokens comprising the top 10% probability mass are considered.

We generally recommend altering this or temperature but not both.`).
				LintRule(`root = if this > 1 || this < 0 { [ "field must be between 0 and 1" ] }`),
			service.NewFloatField(ocpFieldFrequencyPenalty).
				Optional().
				Advanced().
				Description("Number between -2.0 and 2.0. Positive values penalize new tokens based on their existing frequency in the text so far, decreasing the model's likelihood to repeat the same line verbatim.").
				LintRule(`root = if this > 2 || this < -2 { [ "field must be less than 2 and greater than -2" ] }`),
			service.NewFloatField(ocpFieldPresencePenalty).
				Optional().
				Advanced().
				Description("Number between -2.0 and 2.0. Positive values penalize new tokens based on whether they appear in the text so far, increasing the model's likelihood to talk about new topics.").
				LintRule(`root = if this > 2 || this < -2 { [ "field must be less than 2 and greater than -2" ] }`),
			service.NewIntField(ocpFieldSeed).
				Advanced().
				Optional().
				Description("If specified, our system will make a best effort to sample deterministically, such that repeated requests with the same seed and parameters should return the same result. Determinism is not guaranteed."),
			service.NewStringListField(ocpFieldStop).
				Optional().
				Advanced().
				Description("Up to 4 sequences where the API will stop generating further tokens."),
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
	var maxTokens *int32
	if conf.Contains(ocpFieldMaxTokens) {
		mt, err := conf.FieldInt(ocpFieldMaxTokens)
		if err != nil {
			return nil, err
		}
		m := int32(mt)
		maxTokens = &m
	}
	var temp *float32
	if conf.Contains(ocpFieldTemp) {
		ft, err := conf.FieldFloat(ocpFieldTemp)
		if err != nil {
			return nil, err
		}
		t := float32(ft)
		temp = &t
	}
	var user *service.InterpolatedString
	if conf.Contains(ocpFieldUser) {
		user, err = conf.FieldInterpolatedString(ocpFieldUser)
		if err != nil {
			return nil, err
		}
	}
	var topP *float32
	if conf.Contains(ocpFieldTopP) {
		v, err := conf.FieldFloat(ocpFieldTopP)
		if err != nil {
			return nil, err
		}
		tp := float32(v)
		topP = &tp
	}
	var frequencyPenalty *float32
	if conf.Contains(ocpFieldFrequencyPenalty) {
		v, err := conf.FieldFloat(ocpFieldFrequencyPenalty)
		if err != nil {
			return nil, err
		}
		fp := float32(v)
		frequencyPenalty = &fp
	}
	var presencePenalty *float32
	if conf.Contains(ocpFieldPresencePenalty) {
		v, err := conf.FieldFloat(ocpFieldPresencePenalty)
		if err != nil {
			return nil, err
		}
		pp := float32(v)
		presencePenalty = &pp
	}
	var seed *int64
	if conf.Contains(ocpFieldSeed) {
		intSeed, err := conf.FieldInt(ocpFieldSeed)
		if err != nil {
			return nil, err
		}
		s := int64(intSeed)
		seed = &s
	}
	var stop []string
	if conf.Contains(ocpFieldStop) {
		stop, err = conf.FieldStringList(ocpFieldStop)
		if err != nil {
			return nil, err
		}
	}
	return &chatProcessor{b, up, sp, maxTokens, temp, user, topP, frequencyPenalty, presencePenalty, seed, stop}, nil
}

type chatProcessor struct {
	*baseProcessor

	userPrompt       *bloblang.Executor
	systemPrompt     *service.InterpolatedString
	maxTokens        *int32
	temperature      *float32
	user             *service.InterpolatedString
	topP             *float32
	frequencyPenalty *float32
	presencePenalty  *float32
	seed             *int64
	stop             []string
}

func (p *chatProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	var body oai.ChatCompletionsOptions
	body.DeploymentName = &p.model
	body.MaxTokens = p.maxTokens
	body.Temperature = p.temperature
	body.TopP = p.topP
	body.Seed = p.seed
	body.FrequencyPenalty = p.frequencyPenalty
	body.PresencePenalty = p.presencePenalty
	body.Stop = p.stop
	if p.user != nil {
		u, err := p.user.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", ocpFieldUser, err)
		}
		body.User = &u
	}
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
