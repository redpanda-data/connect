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
	vaiFieldProject         = "project"
	vaiFieldCredentialsJSON = "credentials_json"
	vaiFieldModel           = "model"
	vaiFieldLocation        = "location"
	vaiFieldPrompt          = "prompt"
	vaiFieldSystemPrompt    = "system_prompt"
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
		Version("4.33.0").
		Fields(
			service.NewStringField(vaiFieldProject).
				Description("GCP project ID to use"),
			service.NewStringField(vaiFieldCredentialsJSON).
				Description("An optional field to set google Service Account Credentials json.").
				Secret().
				Optional(),
			service.NewStringField(vaiFieldLocation).
				Description("The location of the model if using a fined tune model. For base models this can be omitted").
				Optional().
				Examples("us-central1"),
			service.NewStringField(vaiFieldModel).
				Description("The name of the LLM to use. For a full list of models, see the https://console.cloud.google.com/vertex-ai/model-garden[Vertex AI Model Garden].").
				Examples("gemini-1.5-pro-001", "gemini-1.5-flash-001"),
			service.NewInterpolatedStringField(vaiFieldPrompt).
				Description("The prompt you want to generate a response for. By default, the processor submits the entire payload as a string.").
				Optional(),
			service.NewInterpolatedStringField(vaiFieldSystemPrompt).
				Description("The system prompt to submit to the Vertex AI LLM.").
				Advanced().
				Optional(),
		)
}

func newVertexAIProcessor(conf *service.ParsedConfig, mgr *service.Resources) (p service.Processor, err error) {
	ctx := context.Background()
	proc := &vertexAIChatProcessor{}
	var project string
	project, err = conf.FieldString(vaiFieldProject)
	if err != nil {
		return
	}
	var location string
	if conf.Contains(vaiFieldLocation) {
		location, err = conf.FieldString(vaiFieldLocation)
		if err != nil {
			return
		}
	}
	opts := []option.ClientOption{}
	if conf.Contains(vaiFieldCredentialsJSON) {
		var jsonFile string
		jsonFile, err = conf.FieldString(vaiFieldCredentialsJSON)
		if err != nil {
			return
		}
		opts, err = getClientOptionWithCredential(jsonFile, opts)
		if err != nil {
			return
		}
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
	proc.model, err = conf.FieldString(vaiFieldModel)
	if err != nil {
		return
	}
	if conf.Contains(vaiFieldPrompt) {
		proc.userPrompt, err = conf.FieldInterpolatedString(vaiFieldPrompt)
		if err != nil {
			return
		}
	}
	if conf.Contains(vaiFieldSystemPrompt) {
		proc.systemPrompt, err = conf.FieldInterpolatedString(vaiFieldSystemPrompt)
		if err != nil {
			return
		}
	}
	p = proc
	return
}

type vertexAIChatProcessor struct {
	client *genai.Client
	model  string

	userPrompt   *service.InterpolatedString
	systemPrompt *service.InterpolatedString
}

func (p *vertexAIChatProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	m := p.client.GenerativeModel(p.model)
	if p.systemPrompt != nil {
		p, err := p.systemPrompt.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("unable to evaluate `%s`: %w", vaiFieldSystemPrompt, err)
		}
		m.SystemInstruction = &genai.Content{
			Role:  "system",
			Parts: []genai.Part{genai.Text(p)},
		}
	}
	chat := m.StartChat()
	prompt, err := p.computePrompt(msg)
	if err != nil {
		return nil, err
	}
	resp, err := chat.SendMessage(ctx, genai.Text(prompt))
	if err != nil {
		return nil, err
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
