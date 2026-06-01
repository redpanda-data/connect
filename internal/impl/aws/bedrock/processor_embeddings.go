// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bedrock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"

	"github.com/redpanda-data/benthos/v4/public/service"

	amzn "github.com/aws/aws-sdk-go-v2/aws"

	baws "github.com/redpanda-data/connect/v4/internal/impl/aws"
	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
)

const (
	bedepFieldModel     = "model"
	bedepFieldText      = "text"
	bedepFieldInputType = "input_type"
)

func init() {
	service.MustRegisterProcessor("aws_bedrock_embeddings", newBedrockEmbeddingsConfigSpec(), newBedrockEmbeddingsProcessor)
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
			Examples("amazon.titan-embed-text-v1", "amazon.titan-embed-text-v2:0", "cohere.embed-english-v3", "cohere.embed-multilingual-v3", "cohere.embed-v4:0").
			Description("The model ID to use. For a full list see the https://docs.aws.amazon.com/bedrock/latest/userguide/model-ids.html[AWS Bedrock documentation^].")).
		Field(service.NewStringField(bedepFieldText).
			Description("The prompt you want to generate a response for. By default, the processor submits the entire payload as a string.").
			Optional()).
		Field(service.NewStringAnnotatedEnumField(bedepFieldInputType, map[string]string{
			"search_document": "Used for embeddings stored in a vector database for search use-cases.",
			"search_query":    "Used for embeddings of search queries run against a vector DB to find relevant documents.",
			"classification":  "Used for embeddings passed through a text classifier.",
			"clustering":      "Used for the embeddings run through a clustering algorithm.",
		}).
			Description("Specifies the type of input passed to the model. Required by Cohere embedding models; ignored by Amazon Titan models.").
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

func newBedrockEmbeddingsProcessor(conf *service.ParsedConfig, _ *service.Resources) (service.Processor, error) {
	aconf, err := baws.GetSession(context.Background(), conf)
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
	if conf.Contains(bedepFieldInputType) {
		p.inputType, err = conf.FieldString(bedepFieldInputType)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}

type bedrockEmbeddingsProcessor struct {
	client *bedrockruntime.Client
	model  string

	text      *service.InterpolatedString
	inputType string
}

// isCohereModel reports whether the model ID targets a Cohere embedding model
// on Bedrock. The check uses a substring match so it covers both bare model
// IDs (e.g. cohere.embed-english-v3) and regional inference profiles
// (e.g. us.cohere.embed-v4:0).
func isCohereModel(model string) bool {
	return strings.Contains(model, "cohere")
}

func buildEmbeddingsRequest(model, text, inputType string) ([]byte, error) {
	if isCohereModel(model) {
		req := map[string]any{
			"texts":           []string{text},
			"embedding_types": []string{"float"},
		}
		if inputType != "" {
			req["input_type"] = inputType
		}
		return json.Marshal(req)
	}
	return json.Marshal(map[string]any{"inputText": text})
}

func parseEmbeddingsResponse(model string, body []byte) ([]float64, error) {
	if isCohereModel(model) {
		return parseCohereEmbeddingsResponse(body)
	}
	var resp struct {
		Embedding []float64 `json:"embedding"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if resp.Embedding == nil {
		return nil, errors.New("response did not contain any embeddings")
	}
	return resp.Embedding, nil
}

// parseCohereEmbeddingsResponse handles both the legacy Cohere v3 schema
// where `embeddings` is an array of vectors, and the Cohere v4 schema
// where `embeddings` is an object keyed by embedding type
// (e.g. {"float": [[...]]}).
func parseCohereEmbeddingsResponse(body []byte) ([]float64, error) {
	var raw struct {
		Embeddings json.RawMessage `json:"embeddings"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	if len(raw.Embeddings) == 0 {
		return nil, errors.New("response did not contain any embeddings")
	}

	var vectors [][]float64
	switch raw.Embeddings[0] {
	case '[':
		if err := json.Unmarshal(raw.Embeddings, &vectors); err != nil {
			return nil, fmt.Errorf("parsing cohere embeddings array: %w", err)
		}
	case '{':
		var typed struct {
			Float [][]float64 `json:"float"`
		}
		if err := json.Unmarshal(raw.Embeddings, &typed); err != nil {
			return nil, fmt.Errorf("parsing cohere embeddings object: %w", err)
		}
		vectors = typed.Float
	default:
		return nil, fmt.Errorf("unexpected cohere embeddings shape: %s", string(raw.Embeddings))
	}

	if len(vectors) == 0 {
		return nil, errors.New("response did not contain any embeddings")
	}
	if len(vectors) > 1 {
		return nil, fmt.Errorf("expected a single embeddings response, got: %d", len(vectors))
	}
	return vectors[0], nil
}

func (b *bedrockEmbeddingsProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	prompt, err := b.computeText(msg)
	if err != nil {
		return nil, err
	}
	payloadBytes, err := buildEmbeddingsRequest(b.model, prompt, b.inputType)
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
	embedding, err := parseEmbeddingsResponse(b.model, output.Body)
	if err != nil {
		return nil, err
	}
	vec := make([]any, len(embedding))
	for i, e := range embedding {
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

func (*bedrockEmbeddingsProcessor) Close(context.Context) error {
	return nil
}
