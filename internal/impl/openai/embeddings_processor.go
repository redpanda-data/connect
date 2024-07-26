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
	"fmt"

	oai "github.com/Azure/azure-sdk-for-go/sdk/ai/azopenai"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	oepFieldTextMapping = "text_mapping"
)

func init() {
	err := service.RegisterProcessor(
		"openai_embeddings",
		embeddingProcessorConfig(),
		makeEmbeddingsProcessor,
	)
	if err != nil {
		panic(err)
	}
}

func embeddingProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("AI").
		Summary("Generates vector embeddings to represent input text, using the OpenAI API.").
		Description(`
This processor sends text strings to the OpenAI API, which generates vector embeddings. By default, the processor submits the entire payload of each message as a string, unless you use the ` + "`" + oepFieldTextMapping + "`" + ` configuration field to customize it.

To learn more about vector embeddings, see the https://platform.openai.com/docs/guides/embeddings[OpenAI API documentation^].`).
		Version("4.32.0").
		Fields(
			baseConfigFieldsWithModels(
				"text-embedding-3-large",
				"text-embedding-3-small",
				"text-embedding-ada-002",
			)...,
		).
		Field(
			service.NewBloblangField(oepFieldTextMapping).
				Description("The text you want to generate a vector embedding for. By default, the processor submits the entire payload as a string.").
				Optional(),
		)
}

func makeEmbeddingsProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	b, err := newBaseProcessor(conf)
	if err != nil {
		return nil, err
	}
	var t *bloblang.Executor
	if conf.Contains(oepFieldTextMapping) {
		t, err = conf.FieldBloblang(oepFieldTextMapping)
		if err != nil {
			return nil, err
		}
	}
	return &embeddingsProcessor{b, t}, nil
}

type embeddingsProcessor struct {
	*baseProcessor

	text *bloblang.Executor
}

func (p *embeddingsProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	var body oai.EmbeddingsOptions
	body.DeploymentName = &p.model
	if p.text != nil {
		s, err := msg.BloblangQueryValue(p.text)
		if err != nil {
			return nil, fmt.Errorf("%s execution error: %w", oepFieldTextMapping, err)
		}
		body.Input = append(body.Input, bloblang.ValueToString(s))
	} else {
		b, err := msg.AsBytes()
		if err != nil {
			return nil, err
		}
		body.Input = append(body.Input, string(b))
	}
	resp, err := p.client.GetEmbeddings(ctx, body, nil)
	if err != nil {
		return nil, err
	}
	if len(resp.Data) != 1 {
		return nil, fmt.Errorf("expected a single embeddings response, got: %d", len(resp.Data))
	}
	embd := resp.Data[0]
	data := make([]any, len(embd.Embedding))
	for i, f := range embd.Embedding {
		data[i] = f
	}
	msg = msg.Copy()
	msg.SetStructuredMut(data)
	return service.MessageBatch{msg}, nil
}
