// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package aws

import (
	"context"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	bedrocktypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
)

const (
	bedFieldModel        = "model"
	bedFieldUserPrompt   = "prompt"
	bedFieldSystemPrompt = "system_prompt"
)

func init() {
	err := service.RegisterProcessor("aws_bedrock_chat", newBedrockConfigSpec(), newBedrockProcessor)
	if err != nil {
		panic(err)
	}
}

func newBedrockConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Generates responses to messages in a chat conversation, using the AWS Bedrock API.").
		Description(`This processor sends prompts to your chosen large language model (LLM) and generates text from the responses, using the AWS Bedrock API.
For more information, see the https://docs.aws.amazon.com/bedrock/latest/userguide[AWS Bedrock documentation^].`).
		Categories("AI").
		Version("4.33.0").
		Fields(config.SessionFields()...).
		Field(service.NewStringField(bedFieldModel).
			Examples("amazon.titan-text-express-v1", "anthropic.claude-3-5-sonnet-20240620-v1:0", "cohere.command-text-v14", "meta.llama3-1-70b-instruct-v1:0", "mistral.mistral-large-2402-v1:0").
			Description("The model ID to use. For a full list see the https://docs.aws.amazon.com/bedrock/latest/userguide/model-ids.html[AWS Bedrock documentation^].")).
		Field(service.NewStringField(bedFieldUserPrompt).
			Description("The prompt you want to generate a response for. By default, the processor submits the entire payload as a string.").
			Optional()).
		Field(service.NewStringField(bedFieldSystemPrompt).
			Optional().
			Description("The system prompt to submit to the AWS Bedrock LLM."))

}

func newBedrockProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	aconf, err := GetSession(context.Background(), conf)
	if err != nil {
		return nil, err
	}
	client := bedrockruntime.NewFromConfig(aconf)
	model, err := conf.FieldString(bedFieldModel)
	if err != nil {
		return nil, err
	}
	p := &bedrockProcessor{
		client: client,
		model:  model,
	}
	if conf.Contains(bedFieldUserPrompt) {
		pf, err := conf.FieldInterpolatedString(bedFieldUserPrompt)
		if err != nil {
			return nil, err
		}
		p.userPrompt = pf
	}
	if conf.Contains(bedFieldSystemPrompt) {
		pf, err := conf.FieldInterpolatedString(bedFieldSystemPrompt)
		if err != nil {
			return nil, err
		}
		p.systemPrompt = pf
	}
	return p, nil
}

type bedrockProcessor struct {
	client *bedrockruntime.Client
	model  string

	userPrompt   *service.InterpolatedString
	systemPrompt *service.InterpolatedString
}

func (b *bedrockProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	prompt, err := b.computePrompt(msg)
	if err != nil {
		return nil, err
	}
	input := &bedrockruntime.ConverseInput{
		Messages: []bedrocktypes.Message{
			{
				Role: bedrocktypes.ConversationRoleUser,
				Content: []bedrocktypes.ContentBlock{
					&bedrocktypes.ContentBlockMemberText{
						Value: prompt,
					},
				},
			},
		},
		ModelId: &b.model,
	}
	if b.systemPrompt != nil {
		prompt, err := b.systemPrompt.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("unable to interpolate `%s`: %w", bedFieldSystemPrompt, err)
		}
		input.System = []bedrocktypes.SystemContentBlock{
			&bedrocktypes.SystemContentBlockMemberText{Value: prompt},
		}
	}
	resp, err := b.client.Converse(ctx, input)
	if err != nil {
		return nil, err
	}
	respOut, ok := resp.Output.(*bedrocktypes.ConverseOutputMemberMessage)
	if !ok {
		return nil, fmt.Errorf("unexpected output: %T", resp)
	}
	content := respOut.Value.Content
	if len(content) != 1 {
		return nil, fmt.Errorf("unexpected number of response content: %d", len(content))
	}
	out := msg.Copy()
	switch c := content[0].(type) {
	case *bedrocktypes.ContentBlockMemberText:
		out.SetStructured(c.Value)
	default:
		return nil, fmt.Errorf("unsupported response content type: %T", content[0])
	}
	return service.MessageBatch{out}, nil
}

func (b *bedrockProcessor) computePrompt(msg *service.Message) (string, error) {
	if b.userPrompt != nil {
		return b.userPrompt.TryString(msg)
	}
	buf, err := msg.AsBytes()
	if err != nil {
		return "", err
	}
	if !utf8.Valid(buf) {
		return "", errors.New("message payload contained invalid UTF8")
	}
	return string(buf), nil
}

func (b *bedrockProcessor) Close(ctx context.Context) error {
	return nil
}
