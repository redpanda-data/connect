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
	"unicode/utf8"

	"github.com/redpanda-data/benthos/v4/public/service"

	aiplatform "cloud.google.com/go/aiplatform/apiv1"
	"cloud.google.com/go/aiplatform/apiv1/aiplatformpb"
	"cloud.google.com/go/vertexai/genai"

	"google.golang.org/protobuf/types/known/structpb"

	"google.golang.org/api/option"
)

const (
	vaiepFieldProject         = "project"
	vaiepFieldCredentialsJSON = "credentials_json"
	vaiepFieldModel           = "model"
	vaiepFieldLocation        = "location"
	vaiepFieldText            = "text"
	vaiepFieldTaskType        = "task_type"
	vaiepFieldDims            = "output_dimensions"
)

func init() {
	err := service.RegisterProcessor(
		"gcp_vertex_ai_embeddings",
		newVertexAIEmbeddingsProcessorConfig(),
		newVertexAIEmbeddingsProcessor,
	)
	if err != nil {
		panic(err)
	}
}

func newVertexAIEmbeddingsProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("AI").
		Summary("Generates vector embeddings to represent input text, using the Vertex AI API.").
		Description(`This processor sends text strings to the Vertex AI API, which generates vector embeddings. By default, the processor submits the entire payload of each message as a string, unless you use the `+"`"+vaiepFieldText+"`"+` configuration field to customize it.

For more information, see the https://cloud.google.com/vertex-ai/generative-ai/docs/embeddings[Vertex AI documentation^].`).
		Version("4.37.0").
		Fields(
			service.NewStringField(vaiepFieldProject).
				Description("GCP project ID to use"),
			service.NewStringField(vaiepFieldCredentialsJSON).
				Description("An optional field to set google Service Account Credentials json.").
				Secret().
				Optional(),
			service.NewStringField(vaiepFieldLocation).
				Description("The location of the model.").
				Default("us-central1"),
			service.NewStringField(vaiepFieldModel).
				Description("The name of the LLM to use. For a full list of models, see the https://console.cloud.google.com/vertex-ai/model-garden[Vertex AI Model Garden].").
				Examples("text-embedding-004", "text-multilingual-embedding-002"),
			service.NewStringAnnotatedEnumField(vaiepFieldTaskType, map[string]string{
				"SEMANTIC_SIMILARITY": "optimize for text similarity",
				"CLASSIFICATION":      "optimize for being able classify texts according to preset labels",
				"CLUSTERING":          "optimize for clustering texts based on their similarities",
				"RETRIEVAL_DOCUMENT":  "optimize for documents that will be searched (also known as a corpus)",
				"RETRIEVAL_QUERY":     `optimize for queries such as "What is the best fish recipe?" or "best restaurant in Chicago"`,
				"QUESTION_ANSWERING":  `optimize for search proper questions such as "Why is the sky blue?"`,
				"FACT_VERIFICATION":   `optimize for queries that are proving or disproving a fact such as "apples grow underground"`,
			}).
				Default("RETRIEVAL_DOCUMENT").
				Description("The way to optimize embeddings that the model generates for specific use cases."),
			service.NewInterpolatedStringField(vaiepFieldText).
				Description("The text you want to compute vector embeddings for. By default, the processor submits the entire payload as a string.").
				Optional(),
			service.NewIntField(vaiepFieldDims).
				Description("The maximum length for the output embedding size. If set, the output embeddings will be truncated to this size.").
				Optional(),
		)
}

func newVertexAIEmbeddingsProcessor(conf *service.ParsedConfig, mgr *service.Resources) (p service.Processor, err error) {
	ctx := context.Background()
	proc := &vertexAIEmbeddingsProcessor{}
	var project string
	project, err = conf.FieldString(vaiepFieldProject)
	if err != nil {
		return
	}
	var location string
	location, err = conf.FieldString(vaiepFieldLocation)
	if err != nil {
		return
	}
	opts := []option.ClientOption{
		option.WithEndpoint(location + "-aiplatform.googleapis.com:443"),
	}
	if conf.Contains(vaiepFieldCredentialsJSON) {
		var jsonObject string
		jsonObject, err = conf.FieldString(vaiepFieldCredentialsJSON)
		if err != nil {
			return
		}
		opts = append(opts, option.WithCredentialsJSON([]byte(jsonObject)))
	}
	proc.client, err = aiplatform.NewPredictionClient(ctx, opts...)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			_ = proc.client.Close()
		}
	}()
	var model string
	model, err = conf.FieldString(vaiepFieldModel)
	if err != nil {
		return
	}
	proc.endpoint = fmt.Sprintf("projects/%s/locations/%s/publishers/google/models/%s", project, location, model)
	if conf.Contains(vaiepFieldText) {
		proc.text, err = conf.FieldInterpolatedString(vaiepFieldText)
		if err != nil {
			return
		}
	}
	var taskType string
	taskType, err = conf.FieldString(vaiepFieldTaskType)
	if err != nil {
		return
	}
	proc.taskType = taskType
	if conf.Contains(vaiepFieldDims) {
		var dims int
		dims, err = conf.FieldInt(vaiepFieldDims)
		if err != nil {
			return
		}
		proc.dims = genai.Ptr(float64(dims))
	}
	p = proc
	return
}

type vertexAIEmbeddingsProcessor struct {
	client   *aiplatform.PredictionClient
	endpoint string
	taskType string
	dims     *float64

	text *service.InterpolatedString
}

func (p *vertexAIEmbeddingsProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	text, err := p.computeText(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to compute prompt: %w", err)
	}
	input := structpb.NewStructValue(&structpb.Struct{
		Fields: map[string]*structpb.Value{
			"content":   structpb.NewStringValue(text),
			"task_type": structpb.NewStringValue(p.taskType),
		},
	})
	var fields map[string]*structpb.Value
	if p.dims != nil {
		fields = map[string]*structpb.Value{"output_dimensionality": structpb.NewNumberValue(*p.dims)}
	}
	params := structpb.NewStructValue(&structpb.Struct{Fields: fields})
	req := &aiplatformpb.PredictRequest{
		Endpoint:   p.endpoint,
		Instances:  []*structpb.Value{input},
		Parameters: params,
	}
	resp, err := p.client.Predict(ctx, req)
	if err != nil {
		return nil, err
	}
	if len(resp.Predictions) != 1 {
		return nil, fmt.Errorf("expected a single embedding response got %d", len(resp.Predictions))
	}
	prediction := resp.Predictions[0].GetStructValue()
	if prediction == nil {
		return nil, errors.New("expected predictions to be a struct")
	}
	embeddingspb := prediction.Fields["embeddings"]
	if embeddingspb == nil {
		return nil, errors.New("expected embeddings struct field")
	}
	embeddings := embeddingspb.GetStructValue()
	if embeddings == nil {
		return nil, errors.New("expected embeddings struct field")
	}
	vectorpb := embeddings.Fields["values"]
	if vectorpb == nil {
		return nil, errors.New("expected values list field")
	}
	vector := vectorpb.GetListValue()
	if vector == nil {
		return nil, errors.New("expected values list field")
	}
	slice := vector.GetValues()
	output := make([]any, len(slice))
	for i, value := range slice {
		output[i] = float32(value.GetNumberValue())
	}
	out := msg.Copy()
	out.SetStructured(output)
	return service.MessageBatch{out}, nil
}

func (p *vertexAIEmbeddingsProcessor) computeText(msg *service.Message) (string, error) {
	if p.text != nil {
		return p.text.TryString(msg)
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

func (p *vertexAIEmbeddingsProcessor) Close(ctx context.Context) error {
	return p.client.Close()
}
