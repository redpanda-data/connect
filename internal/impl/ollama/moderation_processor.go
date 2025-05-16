// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package ollama

import (
	"context"
	"fmt"
	"strings"

	"github.com/ollama/ollama/api"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	ompFieldUserPrompt        = "prompt"
	ompFieldAssistantResponse = "response"
)

func init() {
	service.MustRegisterProcessor(
		"ollama_moderation",
		ollamaModerationProcessorConfig(),
		makeOllamaModerationProcessor,
	)

}

func ollamaModerationProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("AI").
		Summary("Generates responses to messages in a chat conversation, using the Ollama API.").
		Description(`This processor checks LLM response safety using either `+"`llama-guard3`"+` or `+"`shieldgemma`"+`. If you want to check if a given prompt is safe, then that can be done with the `+"`ollama_chat`"+` processor - this processor is for response classification only.

By default, the processor starts and runs a locally installed Ollama server. Alternatively, to use an already running Ollama server, add your server details to the `+"`"+bopFieldServerAddress+"`"+` field. You can https://ollama.com/download[download and install Ollama from the Ollama website^].

For more information, see the https://github.com/ollama/ollama/tree/main/docs[Ollama documentation^].`).
		Version("4.42.0").
		Fields(
			service.NewStringAnnotatedEnumField(bopFieldModel, map[string]string{
				"llama-guard3": "When using llama-guard3, two pieces of metadata is added: @safe with the value of `yes` or `no` and the second being @category for the safety category violation. For more information see the https://ollama.com/library/llama-guard3[Llama Guard 3 Model Card].",
				"shieldgemma":  "When using shieldgemma, the model output is a single piece of metadata of @safe with a value of `yes` or `no` if the response is not in violation of its defined safety policies.",
			}).
				Description("The name of the Ollama LLM to use.").
				Examples("llama-guard3", "shieldgemma"),
			service.NewInterpolatedStringField(ompFieldUserPrompt).
				Description("The input prompt that was used with the LLM. If using `ollama_chat` the you can use `save_prompt_metadata` to safe the prompt as metadata."),
			service.NewInterpolatedStringField(ompFieldAssistantResponse).
				Description("The LLM's response to classify if it contains safe or unsafe content."),
		).Fields(commonFields()...).
		Example(
			"Use Llama Guard 3 classify a LLM response",
			"This example uses Llama Guard 3 to check if another model responded with a safe or unsafe content.",
			`
input:
  stdin:
    scanner:
      lines: {}
pipeline:
  processors:
    - ollama_chat:
        model: llava
        prompt: "${!content().string()}"
        save_prompt_metadata: true
    - ollama_moderation:
        model: llama-guard3
        prompt: "${!@prompt}"
        response: "${!content().string()}"
    - mapping: |
        root.response = content().string()
        root.is_safe = @safe
output:
  stdout:
    codec: lines
`)
}

func makeOllamaModerationProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	p := ollamaModerationProcessor{}
	var err error
	p.prompt, err = conf.FieldInterpolatedString(ompFieldUserPrompt)
	if err != nil {
		return nil, err
	}
	p.response, err = conf.FieldInterpolatedString(ompFieldAssistantResponse)
	if err != nil {
		return nil, err
	}
	b, err := newBaseProcessor(conf, mgr)
	if err != nil {
		return nil, err
	}
	p.baseOllamaProcessor = b
	return &p, nil
}

type ollamaModerationProcessor struct {
	*baseOllamaProcessor

	prompt   *service.InterpolatedString
	response *service.InterpolatedString
}

func (o *ollamaModerationProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	p, err := o.prompt.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("interpolation error for %s: %w", ompFieldUserPrompt, err)
	}
	r, err := o.response.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("interpolation error for %s: %w", ompFieldAssistantResponse, err)
	}
	g, err := o.generateCompletion(ctx, p, r)
	if err != nil {
		return nil, err
	}
	m := msg.Copy()
	lines := strings.Split(g, "\n")
	if len(lines) == 0 {
		return nil, fmt.Errorf("unable to extract a moderation response from %s", o.model)
	}
	var safe string
	switch lines[0] {
	case "Yes", "unsafe":
		safe = "no"
	case "No", "safe":
		safe = "yes"
	default:
		return nil, fmt.Errorf("unexpected moderation response from %s: %q", o.model, lines[0])
	}
	m.MetaSet("safe", safe)
	if len(lines) > 1 {
		m.MetaSet("category", lines[1])
	}
	return service.MessageBatch{m}, nil
}

func (o *ollamaModerationProcessor) generateCompletion(ctx context.Context, prompt, response string) (string, error) {
	var req api.ChatRequest
	req.Model = o.model
	req.Options = o.opts
	req.Messages = append(req.Messages, api.Message{
		Role:    "user",
		Content: prompt,
	})
	req.Messages = append(req.Messages, api.Message{
		Role:    "assistant",
		Content: response,
	})
	shouldStream := false
	req.Stream = &shouldStream
	var g string
	err := o.client.Chat(ctx, &req, func(resp api.ChatResponse) error {
		g = resp.Message.Content
		return nil
	})
	return g, err
}

func (o *ollamaModerationProcessor) Close(ctx context.Context) error {
	return o.baseOllamaProcessor.Close(ctx)
}
