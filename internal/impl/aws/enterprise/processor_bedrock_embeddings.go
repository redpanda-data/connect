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
	"encoding/json"
	"errors"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/redpanda-data/benthos/v4/public/service"

	amzn "github.com/aws/aws-sdk-go-v2/aws"

	"github.com/redpanda-data/connect/v4/internal/impl/aws"
	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
)

const (
	bedepFieldModel = "model"
	bedepFieldText  = "text"
)

func init() {
	err := service.RegisterProcessor("aws_bedrock_embeddings", newBedrockEmbeddingsConfigSpec(), newBedrockEmbeddingsProcessor)
	if err != nil {
		panic(err)
	}
}

func newBedrockEmbeddingsConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Computes vector embeddings on text, using the AWS Bedrock API.").
		Description(`This processor sends text to your chosen large language model (LLM) and computes vector embeddings, using the AWS Bedrock API.
For more information, see the https://docs.aws.amazon.com/bedrock/latest/userguide[AWS Bedrock documentation^].`).
		Categories("AI").
		Version("4.37.0").
		Fields(config.SessionFields()...).
		Field(service.NewStringField(bedepFieldModel).
			Examples("amazon.titan-embed-text-v1", "amazon.titan-embed-text-v2:0", "cohere.embed-english-v3", "cohere.embed-multilingual-v3").
			Description("The model ID to use. For a full list see the https://docs.aws.amazon.com/bedrock/latest/userguide/model-ids.html[AWS Bedrock documentation^].")).
		Field(service.NewStringField(bedepFieldText).
			Description("The prompt you want to generate a response for. By default, the processor submits the entire payload as a string.").
			Optional()).
		Example(
			"Store embedding vectors in Clickhouse",
			"Compute embeddings for some generated data and store it within https://clickhouse.com/[Clickhouse^]",
			`input:
  generate:
    interval: 1s
    mapping: |
      root = {"text": fake("paragraph")}
pipeline:
  processors:
  - branch:
      request_map: |
        root = this.text
      processors:
      - aws_bedrock_embeddings:
          model: amazon.titan-embed-text-v1
      result_map: |
        root.embeddings = this
output:
  sql_insert:
    driver: clickhouse
    dsn: "clickhouse://localhost:9000"
    table: searchable_text
    columns: ["id", "text", "vector"]
    args_mapping: "root = [uuid_v4(), this.text, this.embeddings]"
`)
}

func newBedrockEmbeddingsProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	aconf, err := aws.GetSession(context.Background(), conf)
	if err != nil {
		return nil, err
	}
	client := bedrockruntime.NewFromConfig(aconf)
	model, err := conf.FieldString(bedepFieldModel)
	if err != nil {
		return nil, err
	}
	p := &bedrockEmbeddingsProcessor{
		client: client,
		model:  model,
	}
	if conf.Contains(bedepFieldText) {
		p.text, err = conf.FieldInterpolatedString(bedepFieldText)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}

type bedrockEmbeddingsProcessor struct {
	client *bedrockruntime.Client
	model  string

	text *service.InterpolatedString
}

type embeddingsRequest struct {
	InputText string `json:"inputText"`
}

type embeddingsResponse struct {
	Embedding           []float64 `json:"embedding"`
	InputTextTokenCount int       `json:"inputTextTokenCount"`
}

func (b *bedrockEmbeddingsProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	prompt, err := b.computeText(msg)
	if err != nil {
		return nil, err
	}
	payload := embeddingsRequest{prompt}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	output, err := b.client.InvokeModel(ctx, &bedrockruntime.InvokeModelInput{
		Body:        payloadBytes,
		ModelId:     amzn.String(b.model),
		ContentType: amzn.String("application/json"),
	})
	if err != nil {
		return nil, err
	}
	var resp embeddingsResponse
	if err = json.Unmarshal(output.Body, &resp); err != nil {
		return nil, err
	}
	if resp.Embedding == nil {
		return nil, errors.New("response did not contain any embeddings")
	}
	vec := make([]any, len(resp.Embedding))
	for i, e := range resp.Embedding {
		vec[i] = e
	}
	out := msg.Copy()
	out.SetStructured(vec)
	return service.MessageBatch{out}, nil
}

func (b *bedrockEmbeddingsProcessor) computeText(msg *service.Message) (string, error) {
	if b.text != nil {
		return b.text.TryString(msg)
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

func (b *bedrockEmbeddingsProcessor) Close(ctx context.Context) error {
	return nil
}
