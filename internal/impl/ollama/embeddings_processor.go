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
	"errors"
	"unicode/utf8"

	"github.com/ollama/ollama/api"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	oepFieldText = "text"
)

func init() {
	service.MustRegisterProcessor(
		"ollama_embeddings",
		ollamaEmbeddingProcessorConfig(),
		makeOllamaEmbeddingProcessor,
	)
}

func ollamaEmbeddingProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("AI").
		Summary("Generates vector embeddings from text, using the Ollama API.").
		Description(`This processor sends text to your chosen Ollama large language model (LLM) and creates vector embeddings, using the Ollama API. Vector embeddings are long arrays of numbers that represent values or objects, in this case text. 

By default, the processor starts and runs a locally installed Ollama server. Alternatively, to use an already running Ollama server, add your server details to the `+"`"+bopFieldServerAddress+"`"+` field. You can https://ollama.com/download[download and install Ollama from the Ollama website^].

For more information, see the https://github.com/ollama/ollama/tree/main/docs[Ollama documentation^].`).
		Version("4.32.0").
		Fields(
			service.NewStringField(bopFieldModel).
				Description("The name of the Ollama LLM to use. For a full list of models, see the https://ollama.com/models[Ollama website].").
				Examples("nomic-embed-text", "mxbai-embed-large", "snowflake-artic-embed", "all-minilm"),
			service.NewInterpolatedStringField(oepFieldText).
				Description("The text you want to create vector embeddings for. By default, the processor submits the entire payload as a string.").
				Optional(),
		).Fields(commonFields()...).
		Example(
			"Store embedding vectors in Qdrant",
			"Compute embeddings for some generated data and store it within xrefs:component:outputs/qdrant.adoc[Qdrant]",
			`input:
  generate:
    interval: 1s
    mapping: |
      root = {"text": fake("paragraph")}
pipeline:
  processors:
  - ollama_embeddings:
      model: snowflake-artic-embed
      text: "${!this.text}"
output:
  qdrant:
    grpc_host: localhost:6334
    collection_name: "example_collection"
    id: "root = uuid_v4()"
    vector_mapping: "root = this"
`).
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
      processors:
      - ollama_embeddings:
          model: snowflake-artic-embed
          text: "${!this.text}"
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

func makeOllamaEmbeddingProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	p := ollamaEmbeddingProcessor{}
	if conf.Contains(oepFieldText) {
		pf, err := conf.FieldInterpolatedString(oepFieldText)
		if err != nil {
			return nil, err
		}
		p.text = pf
	}
	b, err := newBaseProcessor(conf, mgr)
	if err != nil {
		return nil, err
	}
	p.baseOllamaProcessor = b
	return &p, nil
}

type ollamaEmbeddingProcessor struct {
	*baseOllamaProcessor

	text *service.InterpolatedString
}

func (o *ollamaEmbeddingProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	p, err := o.computeText(msg)
	if err != nil {
		return nil, err
	}
	e, err := o.generateEmbedding(ctx, p)
	if err != nil {
		return nil, err
	}
	m := msg.Copy()
	s := make([]any, len(e))
	for i, f := range e {
		s[i] = f
	}
	m.SetStructuredMut(s)
	return service.MessageBatch{m}, nil
}

func (o *ollamaEmbeddingProcessor) computeText(msg *service.Message) (string, error) {
	if o.text != nil {
		return o.text.TryString(msg)
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

func (o *ollamaEmbeddingProcessor) generateEmbedding(ctx context.Context, text string) ([]float64, error) {
	var req api.EmbeddingRequest
	req.Model = o.model
	req.Prompt = text
	req.Options = o.opts
	resp, err := o.client.Embeddings(ctx, &req)
	if err != nil {
		return nil, err
	}
	return resp.Embedding, nil
}

func (o *ollamaEmbeddingProcessor) Close(ctx context.Context) error {
	return o.baseOllamaProcessor.Close(ctx)
}
