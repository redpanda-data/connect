// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"unicode/utf8"

	"cloud.google.com/go/vertexai/genai"
	"google.golang.org/api/option"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	vaicpFieldProject          = "project"
	vaicpFieldCredentialsJSON  = "credentials_json"
	vaicpFieldModel            = "model"
	vaicpFieldLocation         = "location"
	vaicpFieldPrompt           = "prompt"
	vaicpFieldSystemPrompt     = "system_prompt"
	vaicpFieldAttachment       = "attachment"
	vaicpFieldTemp             = "temperature"
	vaicpFieldTopP             = "top_p"
	vaicpFieldTopK             = "top_k"
	vaicpFieldMaxTokens        = "max_tokens"
	vaicpFieldStop             = "stop"
	vaicpFieldPresencePenalty  = "presence_penalty"
	vaicpFieldFrequencyPenalty = "frequency_penalty"
	vaicpFieldResponseFormat   = "response_format"
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
			service.NewStringField(vaicpFieldProject).
				Description("GCP project ID to use"),
			service.NewStringField(vaicpFieldCredentialsJSON).
				Description("An optional field to set google Service Account Credentials json.").
				Secret().
				Optional(),
			service.NewStringField(vaicpFieldLocation).
				Description("The location of the model if using a fined tune model. For base models this can be omitted").
				Optional().
				Examples("us-central1"),
			service.NewStringField(vaicpFieldModel).
				Description("The name of the LLM to use. For a full list of models, see the https://console.cloud.google.com/vertex-ai/model-garden[Vertex AI Model Garden].").
				Examples("gemini-1.5-pro-001", "gemini-1.5-flash-001"),
			service.NewInterpolatedStringField(vaicpFieldPrompt).
				Description("The prompt you want to generate a response for. By default, the processor submits the entire payload as a string.").
				Optional(),
			service.NewInterpolatedStringField(vaicpFieldSystemPrompt).
				Description("The system prompt to submit to the Vertex AI LLM.").
				Advanced().
				Optional(),
			service.NewBloblangField(vaicpFieldAttachment).
				Description("Additional data like an image to send with the prompt to the model. The result of the mapping must be a byte array, and the content type is automatically detected.").
				Version("4.38.0").
				Example(`root = this.image.decode("base64") # decode base64 encoded image`).
				Optional(),
			service.NewFloatField(vaicpFieldTemp).
				Description("Controls the randomness of predications.").
				Optional().
				LintRule(`root = if this < 0 || this > 2 { ["field must be between 0.0-2.0"] }`),
			service.NewIntField(vaicpFieldMaxTokens).
				Description("The maximum number of output tokens to generate per message.").
				Optional(),
			service.NewStringEnumField(vaicpFieldResponseFormat, "text", "json").
				Description("The response format of generated type, the model must also be prompted to output the appropriate response type.").
				Default("text"),
			service.NewFloatField(vaicpFieldTopP).
				Advanced().
				Description("If specified, nucleus sampling will be used.").
				Optional().
				LintRule(`root = if this < 0 || this > 1 { ["field must be between 0.0-1.0"] }`),
			service.NewIntField(vaicpFieldTopK).
				Advanced().
				Description("If specified top-k sampling will be used.").
				Optional().
				LintRule(`root = if this < 1 || this > 40 { ["field must be between 1-40"] }`),
			service.NewStringListField(vaicpFieldStop).
				Advanced().
				Description("Stop sequences to when the model will stop generating further tokens.").
				Optional(),
			service.NewFloatField(vaicpFieldPresencePenalty).
				Advanced().
				Description("Positive values penalize new tokens based on whether they appear in the text so far, increasing the model's likelihood to talk about new topics.").
				Optional().
				LintRule(`root = if this < -2 || this > 2 { ["field must be greater than -2.0 and less than 2.0"] }`),
			service.NewFloatField(vaicpFieldFrequencyPenalty).
				Advanced().
				Description("Positive values penalize new tokens based on their existing frequency in the text so far, decreasing the model's likelihood to repeat the same line verbatim.").
				Optional().
				LintRule(`root = if this < -2 || this > 2 { ["field must be greater than -2.0 and less than 2.0"] }`),
		)
}

func newVertexAIProcessor(conf *service.ParsedConfig, mgr *service.Resources) (p service.Processor, err error) {
	if err = license.CheckRunningEnterprise(mgr); err != nil {
		return
	}

	ctx := context.Background()
	proc := &vertexAIChatProcessor{}
	var project string
	project, err = conf.FieldString(vaicpFieldProject)
	if err != nil {
		return
	}
	var location string
	if conf.Contains(vaicpFieldLocation) {
		location, err = conf.FieldString(vaicpFieldLocation)
		if err != nil {
			return
		}
	}
	opts := []option.ClientOption{}
	if conf.Contains(vaicpFieldCredentialsJSON) {
		var jsonObject string
		jsonObject, err = conf.FieldString(vaicpFieldCredentialsJSON)
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
	proc.model, err = conf.FieldString(vaicpFieldModel)
	if err != nil {
		return
	}
	if conf.Contains(vaicpFieldPrompt) {
		proc.userPrompt, err = conf.FieldInterpolatedString(vaicpFieldPrompt)
		if err != nil {
			return
		}
	}
	if conf.Contains(vaicpFieldSystemPrompt) {
		proc.systemPrompt, err = conf.FieldInterpolatedString(vaicpFieldSystemPrompt)
		if err != nil {
			return
		}
	}
	if conf.Contains(vaicpFieldAttachment) {
		proc.attachment, err = conf.FieldBloblang(vaicpFieldAttachment)
		if err != nil {
			return
		}
	}
	if conf.Contains(vaicpFieldTemp) {
		var temp float64
		temp, err = conf.FieldFloat(vaicpFieldTemp)
		if err != nil {
			return
		}
		proc.temp = genai.Ptr(float32(temp))
	}
	if conf.Contains(vaicpFieldTopP) {
		var topP float64
		topP, err = conf.FieldFloat(vaicpFieldTopP)
		if err != nil {
			return
		}
		proc.topP = genai.Ptr(float32(topP))
	}
	if conf.Contains(vaicpFieldTopK) {
		var topK int
		topK, err = conf.FieldInt(vaicpFieldTopK)
		if err != nil {
			return
		}
		proc.topK = genai.Ptr(int32(topK))
	}
	if conf.Contains(vaicpFieldMaxTokens) {
		var maxTokens int
		maxTokens, err = conf.FieldInt(vaicpFieldMaxTokens)
		if err != nil {
			return
		}
		proc.maxTokens = genai.Ptr(int32(maxTokens))
	}
	if conf.Contains(vaicpFieldStop) {
		proc.stopSequences, err = conf.FieldStringList(vaicpFieldStop)
		if err != nil {
			return
		}
	}
	if conf.Contains(vaicpFieldPresencePenalty) {
		var pp float64
		pp, err = conf.FieldFloat(vaicpFieldPresencePenalty)
		if err != nil {
			return
		}
		proc.presencePenalty = genai.Ptr(float32(pp))
	}
	if conf.Contains(vaicpFieldFrequencyPenalty) {
		var fp float64
		fp, err = conf.FieldFloat(vaicpFieldFrequencyPenalty)
		if err != nil {
			return
		}
		proc.frequencyPenalty = genai.Ptr(float32(fp))
	}
	var format string
	format, err = conf.FieldString(vaicpFieldResponseFormat)
	switch format {
	case "json":
		proc.responseMIMEType = "application/json"
	case "text":
		proc.responseMIMEType = "text/plain"
	default:
		return nil, fmt.Errorf("invalid value %q for `%s`", format, vaicpFieldResponseFormat)
	}
	p = proc
	return
}

type vertexAIChatProcessor struct {
	client *genai.Client
	model  string

	userPrompt       *service.InterpolatedString
	systemPrompt     *service.InterpolatedString
	attachment       *bloblang.Executor
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
			return nil, fmt.Errorf("unable to evaluate `%s`: %w", vaicpFieldSystemPrompt, err)
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
	parts := []genai.Part{genai.Text(prompt)}
	if p.attachment != nil {
		v, err := msg.BloblangQuery(p.attachment)
		if err != nil {
			return nil, fmt.Errorf("unable to evaluate `%s`: %w", vaicpFieldAttachment, err)
		}
		i, err := v.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("unable to convert `%s` to bytes: %w", vaicpFieldAttachment, err)
		}
		contentType := http.DetectContentType(i)
		if contentType == "application/octet-stream" {
			return nil, fmt.Errorf("unable to detect content-type of `%s`", vaicpFieldAttachment)
		}
		parts = append(parts, genai.Blob{MIMEType: contentType, Data: i})
	}
	resp, err := chat.SendMessage(ctx, parts...)
	if err != nil {
		return nil, fmt.Errorf("failed to generate response: %w", err)
	}
	if len(resp.Candidates) != 1 {
		if resp.PromptFeedback != nil && resp.PromptFeedback.BlockReasonMessage != "" {
			return nil, fmt.Errorf("response blocked due to: %s", resp.PromptFeedback.BlockReasonMessage)
		}
		return nil, errors.New("no candidate responses returned")
	}
	parts = resp.Candidates[0].Content.Parts
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
