// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cohere

import (
	"context"
	"errors"
	"fmt"

	cohere "github.com/cohere-ai/cohere-go/v2"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	crpFieldDocuments = "documents"
	crpFieldQuery     = "query"
	crpFieldTopN      = "top_n"
	crpFieldMaxTokens = "max_tokens_per_doc"
)

func init() {
	err := service.RegisterProcessor(
		"cohere_rerank",
		rerankProcessorConfig(),
		makeRerankProcessor,
	)
	if err != nil {
		panic(err)
	}
}

func rerankProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("AI").
		Summary("Generates vector embeddings to represent input text, using the Cohere API.").
		Description(`
This processor sends document strings to the Cohere API, which reranks them based on the relevance to the query.

To learn more about reranking, see the https://docs.cohere.com/docs/rerank-2[Cohere API documentation^].

The output of this processor is an array of strings that are ordered by their relevance.

== Metadata

relevance_scores: an array of scores for each document, indicating how relevant it is to the query. The scores are in the same order as the documents in the input. The higher the score, the more relevant the document is to the query.

		`).
		Version("4.37.0").
		Fields(
			baseConfigFieldsWithModels(
				"rerank-v3.5",
			)...,
		).
		Fields(
			service.NewInterpolatedStringField(crpFieldQuery).Description("The search query"),
			service.NewBloblangField(crpFieldDocuments).Description("A list of texts that will be compared to the query. For optimal performance Cohere recommends against sending more than 1000 documents in a single request. NOTE: structured data should be formatted as YAML for best performance."),
			service.NewIntField(crpFieldTopN).Default(0).Description("The number of documents to return, if 0 all documents are returned."),
			service.NewIntField(crpFieldMaxTokens).Default(4096).Description("Long documents will be automatically truncated to the specified number of tokens."),
		).
		Example(
			"Rerank some documents based on a query",
			"Rerank some documents based on a query",
			`input:
  generate:
    interval: 1s
    mapping: |
      root = {
        "query": fake("sentence"),
        "docs": [fake("paragraph"), fake("paragraph"), fake("paragraph")],
      }
pipeline:
  processors:
  - cohere_rerank:
      model: rerank-v3.5
      api_key: "${COHERE_API_KEY}"
      query: "${!this.query}"
      documents: "root = this.docs"
output:
  stdout: {}`)
}

func makeRerankProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	b, err := newBaseProcessor(conf)
	if err != nil {
		return nil, err
	}
	q, err := conf.FieldInterpolatedString(crpFieldQuery)
	if err != nil {
		return nil, err
	}
	d, err := conf.FieldBloblang(crpFieldDocuments)
	if err != nil {
		return nil, err
	}
	t, err := conf.FieldInt(crpFieldTopN)
	if err != nil {
		return nil, err
	}
	m, err := conf.FieldInt(crpFieldMaxTokens)
	if err != nil {
		return nil, err
	}
	return &rerankProcessor{b, q, d, t, m}, nil
}

type rerankProcessor struct {
	*baseProcessor

	query     *service.InterpolatedString
	documents *bloblang.Executor
	topN      int
	maxTokens int
}

func (p *rerankProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	q, err := p.query.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to interpolate query: %w", err)
	}
	docsMsg, err := msg.BloblangQuery(p.documents)
	if err != nil {
		return nil, fmt.Errorf("failed to execute documents: %w", err)
	}
	v, err := docsMsg.AsStructured()
	if err != nil {
		return nil, fmt.Errorf("failed to extract documents response: %w", err)
	}
	docs, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("failed to extract documents response as array: %T", v)
	}
	if len(docs) == 0 {
		return nil, errors.New("no documents to rerank")
	}
	req := cohere.V2RerankRequest{
		Model:           p.model,
		Query:           q,
		MaxTokensPerDoc: &p.maxTokens,
	}
	if p.topN > 0 {
		req.TopN = &p.topN
	}
	for _, d := range docs {
		req.Documents = append(req.Documents, bloblang.ValueToString(d))
	}
	resp, err := p.client.Rerank(ctx, &req)
	if err != nil {
		return nil, fmt.Errorf("failed to rerank documents: %w", err)
	}
	rankedDocs := []any{}
	scores := []any{}
	for _, result := range resp.Results {
		if result.Index < 0 || result.Index >= len(docs) {
			return nil, fmt.Errorf("invalid API response: out of range index %d for documents array of length %d", result.Index, len(docs))
		}
		rankedDocs = append(rankedDocs, docs[result.Index])
		scores = append(scores, result.RelevanceScore)
	}
	msg = msg.Copy()
	msg.SetStructured(rankedDocs)
	msg.MetaSetMut("relevance_scores", scores)
	return service.MessageBatch{msg}, nil
}
