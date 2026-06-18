// Copyright 2026 Redpanda Data, Inc.
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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestBuildEmbeddingsRequest(t *testing.T) {
	tests := []struct {
		name      string
		model     string
		text      string
		inputType string
		want      map[string]any
	}{
		{
			name:  "titan request uses inputText",
			model: "amazon.titan-embed-text-v2:0",
			text:  "hello world",
			want:  map[string]any{"inputText": "hello world"},
		},
		{
			name:  "titan ignores input_type",
			model: "amazon.titan-embed-text-v1",
			text:  "hello",
			// inputType is set but should not appear for titan
			inputType: "search_document",
			want:      map[string]any{"inputText": "hello"},
		},
		{
			name:      "cohere v3 request includes texts and input_type",
			model:     "cohere.embed-english-v3",
			text:      "hello world",
			inputType: "search_document",
			want: map[string]any{
				"texts":           []any{"hello world"},
				"input_type":      "search_document",
				"embedding_types": []any{"float"},
			},
		},
		{
			name:      "cohere v4 regional inference profile is detected",
			model:     "us.cohere.embed-v4:0",
			text:      "query text",
			inputType: "search_query",
			want: map[string]any{
				"texts":           []any{"query text"},
				"input_type":      "search_query",
				"embedding_types": []any{"float"},
			},
		},
		{
			name:  "cohere without input_type omits the field",
			model: "cohere.embed-multilingual-v3",
			text:  "hola",
			want: map[string]any{
				"texts":           []any{"hola"},
				"embedding_types": []any{"float"},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildEmbeddingsRequest(tc.model, tc.text, tc.inputType)
			require.NoError(t, err)
			var asMap map[string]any
			require.NoError(t, json.Unmarshal(got, &asMap))
			assert.Equal(t, tc.want, asMap)
		})
	}
}

func TestParseEmbeddingsResponseTitan(t *testing.T) {
	body := []byte(`{"embedding":[0.1,0.2,0.3],"inputTextTokenCount":3}`)
	got, err := parseEmbeddingsResponse("amazon.titan-embed-text-v2:0", body)
	require.NoError(t, err)
	assert.Equal(t, []float64{0.1, 0.2, 0.3}, got)
}

func TestParseEmbeddingsResponseTitanMissingEmbedding(t *testing.T) {
	_, err := parseEmbeddingsResponse("amazon.titan-embed-text-v1", []byte(`{"inputTextTokenCount":0}`))
	assert.ErrorContains(t, err, "did not contain any embeddings")
}

func TestParseEmbeddingsResponseCohereV3(t *testing.T) {
	body := []byte(`{"embeddings":[[0.1,0.2,0.3]],"id":"abc"}`)
	got, err := parseEmbeddingsResponse("cohere.embed-english-v3", body)
	require.NoError(t, err)
	assert.Equal(t, []float64{0.1, 0.2, 0.3}, got)
}

func TestParseEmbeddingsResponseCohereV4(t *testing.T) {
	body := []byte(`{"embeddings":{"float":[[0.4,0.5,0.6]]},"id":"xyz"}`)
	got, err := parseEmbeddingsResponse("us.cohere.embed-v4:0", body)
	require.NoError(t, err)
	assert.Equal(t, []float64{0.4, 0.5, 0.6}, got)
}

func TestParseEmbeddingsResponseCohereMissingEmbeddings(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"no field", `{"id":"abc"}`},
		{"empty array", `{"embeddings":[]}`},
		{"empty float", `{"embeddings":{"float":[]}}`},
		{"unexpected shape", `{"embeddings":"oops"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseEmbeddingsResponse("cohere.embed-english-v3", []byte(tc.body))
			assert.Error(t, err)
		})
	}
}

func TestParseEmbeddingsResponseCohereMultipleVectorsRejected(t *testing.T) {
	body := []byte(`{"embeddings":[[0.1],[0.2]]}`)
	_, err := parseEmbeddingsResponse("cohere.embed-english-v3", body)
	assert.ErrorContains(t, err, "expected a single embeddings response")
}

func TestNewProcessor_CohereRequiresInputType(t *testing.T) {
	parseAndBuild := func(t *testing.T, yaml string) error {
		t.Helper()
		conf, err := newBedrockEmbeddingsConfigSpec().ParseYAML(yaml, nil)
		require.NoError(t, err)
		_, err = newBedrockEmbeddingsProcessor(conf, service.MockResources())
		return err
	}

	t.Run("cohere without input_type fails at parse time", func(t *testing.T) {
		err := parseAndBuild(t, `
region: us-east-1
credentials: {id: test, secret: test}
model: cohere.embed-english-v3
`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "input_type")
	})

	t.Run("cohere regional profile without input_type fails at parse time", func(t *testing.T) {
		err := parseAndBuild(t, `
region: us-east-1
credentials: {id: test, secret: test}
model: us.cohere.embed-v4:0
`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "input_type")
	})

	t.Run("cohere with input_type builds", func(t *testing.T) {
		require.NoError(t, parseAndBuild(t, `
region: us-east-1
credentials: {id: test, secret: test}
model: cohere.embed-english-v3
input_type: search_document
`))
	})

	t.Run("titan without input_type builds", func(t *testing.T) {
		require.NoError(t, parseAndBuild(t, `
region: us-east-1
credentials: {id: test, secret: test}
model: amazon.titan-embed-text-v1
`))
	})
}
