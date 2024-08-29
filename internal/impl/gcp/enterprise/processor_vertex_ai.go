// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package gcp

import (
	"context"
	"errors"
	"fmt"
	"unicode/utf8"

	"cloud.google.com/go/vertexai/genai"
	"github.com/redpanda-data/benthos/v4/public/service"
	"google.golang.org/api/option"
)

const (
	vaipFieldProject          = "project"
	vaipFieldCredentialsJSON  = "credentials_json"
	vaipFieldModel            = "model"
	vaipFieldLocation         = "location"
	vaipFieldPrompt           = "prompt"
	vaipFieldSystemPrompt     = "system_prompt"
	vaipFieldTemp             = "temperature"
	vaipFieldTopP             = "top_p"
	vaipFieldTopK             = "top_k"
	vaipFieldMaxTokens        = "max_tokens"
	vaipFieldStop             = "stop"
	vaipFieldPresencePenalty  = "presence_penalty"
	vaipFieldFrequencyPenalty = "frequency_penalty"
	vaipFieldResponseFormat   = "response_format"
)

func init() {
	err := service.RegisterProcessor(
		"gcp_vertex_ai_chat",
		newVertexAIProcessorConfig(),
		newVertexAIProcessor,
	)
	if err != nil {
		panic(err)
	}
}

func newVertexAIProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("AI").
		Summary("Generates responses to messages in a chat conversation, using the Vertex AI API.").
		Description(`This processor sends prompts to your chosen large language model (LLM) and generates text from the responses, using the Vertex AI API.

For more information, see the https://cloud.google.com/vertex-ai/docs[Vertex AI documentation^].`).
		Version("4.34.0").
		Fields(
			service.NewStringField(vaipFieldProject).
				Description("GCP project ID to use"),
			service.NewStringField(vaipFieldCredentialsJSON).
				Description("An optional field to set google Service Account Credentials json.").
				Secret().
				Optional(),
			service.NewStringField(vaipFieldLocation).
				Description("The location of the model if using a fined tune model. For base models this can be omitted").
				Optional().
				Examples("us-central1"),
			service.NewStringField(vaipFieldModel).
				Description("The name of the LLM to use. For a full list of models, see the https://console.cloud.google.com/vertex-ai/model-garden[Vertex AI Model Garden].").
				Examples("gemini-1.5-pro-001", "gemini-1.5-flash-001"),
			service.NewInterpolatedStringField(vaipFieldPrompt).
				Description("The prompt you want to generate a response for. By default, the processor submits the entire payload as a string.").
				Optional(),
			service.NewInterpolatedStringField(vaipFieldSystemPrompt).
				Description("The system prompt to submit to the Vertex AI LLM.").
				Advanced().
				Optional(),
			service.NewFloatField(vaipFieldTemp).
				Description("Controls the randomness of predications.").
				Optional().
				LintRule(`root = if this < 0 || this > 2 { ["field must be between 0.0-2.0"] }`),
			service.NewIntField(vaipFieldMaxTokens).
				Description("The maximum number of output tokens to generate per message.").
				Optional(),
			service.NewStringEnumField(vaipFieldResponseFormat, "text", "json").
				Description("The response format of generated type, the model must also be prompted to output the appropriate response type.").
				Default("text"),
			service.NewFloatField(vaipFieldTopP).
				Advanced().
				Description("If specified, nucleus sampling will be used.").
				Optional().
				LintRule(`root = if this < 0 || this > 1 { ["field must be between 0.0-1.0"] }`),
			service.NewIntField(vaipFieldTopK).
				Advanced().
				Description("If specified top-k sampling will be used.").
				Optional().
				LintRule(`root = if this < 1 || this > 40 { ["field must be between 1-40"] }`),
			service.NewStringListField(vaipFieldStop).
				Advanced().
				Description("Stop sequences to when the model will stop generating further tokens.").
				Optional(),
			service.NewFloatField(vaipFieldPresencePenalty).
				Advanced().
				Description("Positive values penalize new tokens based on whether they appear in the text so far, increasing the model's likelihood to talk about new topics.").
				Optional().
				LintRule(`root = if this < -2 || this > 2 { ["field must be greater than -2.0 and less than 2.0"] }`),
			service.NewFloatField(vaipFieldFrequencyPenalty).
				Advanced().
				Description("Positive values penalize new tokens based on their existing frequency in the text so far, decreasing the model's likelihood to repeat the same line verbatim.").
				Optional().
				LintRule(`root = if this < -2 || this > 2 { ["field must be greater than -2.0 and less than 2.0"] }`),
		)
}

func newVertexAIProcessor(conf *service.ParsedConfig, mgr *service.Resources) (p service.Processor, err error) {
	ctx := context.Background()
	proc := &vertexAIChatProcessor{}
	var project string
	project, err = conf.FieldString(vaipFieldProject)
	if err != nil {
		return
	}
	var location string
	if conf.Contains(vaipFieldLocation) {
		location, err = conf.FieldString(vaipFieldLocation)
		if err != nil {
			return
		}
	}
	opts := []option.ClientOption{}
	if conf.Contains(vaipFieldCredentialsJSON) {
		var jsonObject string
		jsonObject, err = conf.FieldString(vaipFieldCredentialsJSON)
		if err != nil {
			return
		}
		opts = append(opts, option.WithCredentialsJSON([]byte(jsonObject)))
	}
	proc.client, err = genai.NewClient(ctx, project, location, opts...)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			_ = proc.client.Close()
		}
	}()
	proc.model, err = conf.FieldString(vaipFieldModel)
	if err != nil {
		return
	}
	if conf.Contains(vaipFieldPrompt) {
		proc.userPrompt, err = conf.FieldInterpolatedString(vaipFieldPrompt)
		if err != nil {
			return
		}
	}
	if conf.Contains(vaipFieldSystemPrompt) {
		proc.systemPrompt, err = conf.FieldInterpolatedString(vaipFieldSystemPrompt)
		if err != nil {
			return
		}
	}
	if conf.Contains(vaipFieldTemp) {
		var temp float64
		temp, err = conf.FieldFloat(vaipFieldTemp)
		if err != nil {
			return
		}
		proc.temp = genai.Ptr(float32(temp))
	}
	if conf.Contains(vaipFieldTopP) {
		var topP float64
		topP, err = conf.FieldFloat(vaipFieldTopP)
		if err != nil {
			return
		}
		proc.topP = genai.Ptr(float32(topP))
	}
	if conf.Contains(vaipFieldTopK) {
		var topK int
		topK, err = conf.FieldInt(vaipFieldTopK)
		if err != nil {
			return
		}
		proc.topK = genai.Ptr(int32(topK))
	}
	if conf.Contains(vaipFieldMaxTokens) {
		var maxTokens int
		maxTokens, err = conf.FieldInt(vaipFieldMaxTokens)
		if err != nil {
			return
		}
		proc.maxTokens = genai.Ptr(int32(maxTokens))
	}
	if conf.Contains(vaipFieldStop) {
		proc.stopSequences, err = conf.FieldStringList(vaipFieldStop)
		if err != nil {
			return
		}
	}
	if conf.Contains(vaipFieldPresencePenalty) {
		var pp float64
		pp, err = conf.FieldFloat(vaipFieldPresencePenalty)
		if err != nil {
			return
		}
		proc.presencePenalty = genai.Ptr(float32(pp))
	}
	if conf.Contains(vaipFieldFrequencyPenalty) {
		var fp float64
		fp, err = conf.FieldFloat(vaipFieldFrequencyPenalty)
		if err != nil {
			return
		}
		proc.frequencyPenalty = genai.Ptr(float32(fp))
	}
	var format string
	format, err = conf.FieldString(vaipFieldResponseFormat)
	if format == "json" {
		proc.responseMIMEType = "application/json"
	} else if format == "text" {
		proc.responseMIMEType = "text/plain"
	} else {
		return nil, fmt.Errorf("invalid value %q for `%s`", format, vaipFieldResponseFormat)
	}
	p = proc
	return
}

type vertexAIChatProcessor struct {
	client *genai.Client
	model  string

	userPrompt       *service.InterpolatedString
	systemPrompt     *service.InterpolatedString
	temp             *float32
	topP             *float32
	topK             *int32
	maxTokens        *int32
	stopSequences    []string
	presencePenalty  *float32
	frequencyPenalty *float32
	responseMIMEType string
}

func (p *vertexAIChatProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	m := p.client.GenerativeModel(p.model)
	m.Temperature = p.temp
	m.TopP = p.topP
	m.TopK = p.topK
	m.MaxOutputTokens = p.maxTokens
	m.StopSequences = p.stopSequences
	m.PresencePenalty = p.presencePenalty
	m.FrequencyPenalty = p.frequencyPenalty
	m.ResponseMIMEType = p.responseMIMEType
	if p.systemPrompt != nil {
		p, err := p.systemPrompt.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("unable to evaluate `%s`: %w", vaipFieldSystemPrompt, err)
		}
		m.SystemInstruction = &genai.Content{
			Role:  "system",
			Parts: []genai.Part{genai.Text(p)},
		}
	}
	chat := m.StartChat()
	prompt, err := p.computePrompt(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to compute prompt: %w", err)
	}
	resp, err := chat.SendMessage(ctx, genai.Text(prompt))
	if err != nil {
		return nil, fmt.Errorf("failed to generate response: %w", err)
	}
	if len(resp.Candidates) != 1 {
		if resp.PromptFeedback != nil && resp.PromptFeedback.BlockReasonMessage != "" {
			return nil, fmt.Errorf("response blocked due to: %s", resp.PromptFeedback.BlockReasonMessage)
		}
		return nil, errors.New("no candidate responses returned")
	}
	parts := resp.Candidates[0].Content.Parts
	if len(parts) != 1 {
		if resp.PromptFeedback != nil && resp.PromptFeedback.BlockReasonMessage != "" {
			return nil, fmt.Errorf("response blocked due to: %s", resp.PromptFeedback.BlockReasonMessage)
		}
		return nil, errors.New("no candidate response parts returned")
	}
	out := msg.Copy()
	switch p := parts[0].(type) {
	case genai.Text:
		out.SetStructured(string(p))
	case genai.Blob:
		out.SetBytes(p.Data)
	case genai.FileData:
		out.SetStructured(p.FileURI)
	default:
		return nil, fmt.Errorf("unknown response content: %T", parts[0])
	}
	return service.MessageBatch{out}, nil
}

func (p *vertexAIChatProcessor) computePrompt(msg *service.Message) (string, error) {
	if p.userPrompt != nil {
		return p.userPrompt.TryString(msg)
	}
	b, err := msg.AsBytes()
	if err != nil {
		return "", err
	}
	if !utf8.Valid(b) {
		return "", errors.New("message payload contained invalid UTF8")
	}
	return string(b), nil
}

func (p *vertexAIChatProcessor) Close(ctx context.Context) error {
	return p.client.Close()
}
