// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cohere

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/license"
)

func TestCohereRerankProcessor(t *testing.T) {
	type testCase struct {
		name               string
		query              string
		documents          []string
		topN               int
		mockResponse       map[string]any
		expectedResults    int
		expectedFirstDoc   string
		expectedFirstScore float64
		expectError        bool
		expectedErr        string
	}

	tests := []testCase{
		{
			name:            "basic rerank test",
			query:           "What is machine learning?",
			documents:       []string{"Machine learning is a subset of AI", "Cooking recipes", "Weather forecast"},
			topN:            0, // return all
			expectedResults: 3,
			mockResponse: map[string]any{
				"results": []any{
					map[string]any{"index": 0, "relevance_score": 0.95},
					map[string]any{"index": 2, "relevance_score": 0.3},
					map[string]any{"index": 1, "relevance_score": 0.1},
				},
			},
			expectedFirstDoc:   "Machine learning is a subset of AI",
			expectedFirstScore: 0.95,
		},
		{
			name:            "top n filtering",
			query:           "What is machine learning?",
			documents:       []string{"Machine learning is a subset of AI", "Cooking recipes", "Weather forecast"},
			topN:            2,
			expectedResults: 2,
			mockResponse: map[string]any{
				"results": []any{
					map[string]any{"index": 0, "relevance_score": 0.95},
					map[string]any{"index": 2, "relevance_score": 0.3},
				},
			},
			expectedFirstDoc:   "Machine learning is a subset of AI",
			expectedFirstScore: 0.95,
		},
		{
			name:  "top n much smaller than document count",
			query: "What is artificial intelligence?",
			documents: []string{
				"Doc 0: AI is artificial intelligence",
				"Doc 1: Cooking pasta with tomatoes",
				"Doc 2: Weather is sunny today",
				"Doc 3: Machine learning algorithms",
				"Doc 4: Basketball game scores",
				"Doc 5: Artificial neural networks",
				"Doc 6: Music theory basics",
				"Doc 7: Deep learning concepts",
				"Doc 8: Restaurant menu items",
				"Doc 9: Travel destinations",
				"Doc 10: Programming languages",
				"Doc 11: Computer vision tasks",
				"Doc 12: Shopping list items",
				"Doc 13: Natural language processing",
				"Doc 14: Sports news updates",
				"Doc 15: Data science methods",
				"Doc 16: Movie recommendations",
				"Doc 17: AI ethics principles",
				"Doc 18: Social media posts",
				"Doc 19: Technology trends",
			},
			topN:            3,
			expectedResults: 3,
			mockResponse: map[string]any{
				"results": []any{
					// Cohere returns results in relevance order, but with original indices
					map[string]any{"index": 17, "relevance_score": 0.98}, // "AI ethics principles"
					map[string]any{"index": 0, "relevance_score": 0.95},  // "Doc 0: AI is artificial intelligence"
					map[string]any{"index": 5, "relevance_score": 0.87},  // "Artificial neural networks"
				},
			},
			expectedFirstDoc:   "Doc 17: AI ethics principles",
			expectedFirstScore: 0.98,
		},
		{
			name:      "invalid index in response",
			query:     "test query",
			documents: []string{"doc1", "doc2"},
			mockResponse: map[string]any{
				"results": []any{
					map[string]any{"index": 5, "relevance_score": 0.95}, // invalid index
				},
			},
			expectError: true,
			expectedErr: "invalid API response: out of range index 5 for documents array of length 2",
		},
		{
			name:      "negative index in response",
			query:     "test query",
			documents: []string{"doc1", "doc2"},
			mockResponse: map[string]any{
				"results": []any{
					map[string]any{"index": -1, "relevance_score": 0.95}, // negative index
				},
			},
			expectError: true,
			expectedErr: "invalid API response: out of range index -1 for documents array of length 2",
		},
		{
			name:        "empty documents",
			query:       "test query",
			documents:   []string{},
			expectError: true,
			expectedErr: "no documents to rerank",
		},
	}

	for i, test := range tests {
		t.Run(test.name+"/"+strconv.Itoa(i), func(t *testing.T) {
			var server *httptest.Server

			// Only create mock server if we have a mock response
			if test.mockResponse != nil {
				server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					require.Equal(t, "POST", r.Method)
					require.Equal(t, "/v2/rerank", r.URL.Path)

					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)

					responseBytes, err := json.Marshal(test.mockResponse)
					require.NoError(t, err)
					_, err = w.Write(responseBytes)
					require.NoError(t, err)
				}))
				defer server.Close()
			}

			// Create input message
			inputData := map[string]any{
				"query": test.query,
				"docs":  test.documents,
			}
			inputBytes, err := json.Marshal(inputData)
			require.NoError(t, err)

			// Create processor config
			baseURL := "https://api.cohere.com"
			if server != nil {
				baseURL = server.URL
			}

			topNStr := ""
			if test.topN > 0 {
				topNStr = fmt.Sprintf("top_n: %d", test.topN)
			}

			conf, err := rerankProcessorConfig().ParseYAML(fmt.Sprintf(`
base_url: %s
api_key: test-key
model: rerank-v3.5
query: "${!this.query}"
documents: "root = this.docs"
%s
`, baseURL, topNStr), nil)
			require.NoError(t, err)

			// Create processor with license service
			resources := service.MockResources()
			license.InjectTestService(resources)
			proc, err := makeRerankProcessor(conf, resources)
			require.NoError(t, err)

			// Process message
			msgs, err := proc.Process(t.Context(), service.NewMessage(inputBytes))

			if test.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.expectedErr)
				return
			}

			require.NoError(t, err)
			require.Len(t, msgs, 1)

			// Get result
			result, err := msgs[0].AsStructured()
			require.NoError(t, err)

			resultArray, ok := result.([]any)
			require.True(t, ok, "Expected result to be an array")
			require.Len(t, resultArray, test.expectedResults)

			// Check first result
			firstResult, ok := resultArray[0].(map[string]any)
			require.True(t, ok, "Expected first result to be a map")

			assert.Equal(t, test.expectedFirstDoc, firstResult["document"])
			assert.Equal(t, test.expectedFirstScore, firstResult["relevance_score"])

			// Verify all results have the correct structure and document-score mapping
			mockResults, ok := test.mockResponse["results"].([]any)
			require.True(t, ok, "Mock response should have results array")

			for i, item := range resultArray {
				resultItem, ok := item.(map[string]any)
				require.True(t, ok, "Expected result item %d to be a map", i)

				document, hasDocument := resultItem["document"]
				assert.True(t, hasDocument, "Result item %d should have 'document' field", i)

				score, hasScore := resultItem["relevance_score"]
				assert.True(t, hasScore, "Result item %d should have 'relevance_score' field", i)

				// Verify the document matches the expected index from mock response
				mockResult := mockResults[i].(map[string]any)
				expectedIndex := mockResult["index"].(int)
				expectedScore := mockResult["relevance_score"].(float64)
				expectedDocument := test.documents[expectedIndex]

				assert.Equal(t, expectedDocument, document, "Document at position %d should match expected document from index %d", i, expectedIndex)
				assert.Equal(t, expectedScore, score, "Score at position %d should match expected score", i)
			}

			require.NoError(t, msgs[0].GetError())
		})
	}
}

func TestCohereRerankProcessorIntegration(t *testing.T) {
	integration.CheckSkip(t)

	apiKey := os.Getenv("COHERE_API_KEY")
	if apiKey == "" {
		t.Skip("Skipping integration test: COHERE_API_KEY environment variable not set")
	}

	// Test data from the example
	testQuery := "What is the capital of the United States?"
	testDocuments := []string{
		"Carson City is the capital city of the American state of Nevada.",
		"The Commonwealth of the Northern Mariana Islands is a group of islands in the Pacific Ocean. Its capital is Saipan.",
		"Capitalization or capitalisation in English grammar is the use of a capital letter at the start of a word. English usage varies from capitalization in other languages.",
		"Washington, D.C. (also known as simply Washington or D.C., and officially as the District of Columbia) is the capital of the United States. It is a federal district.",
		"Capital punishment has existed in the United States since before the United States was a country. As of 2017, capital punishment is legal in 30 of the 50 states.",
	}

	// Create input message
	inputData := map[string]any{
		"query": testQuery,
		"docs":  testDocuments,
	}
	inputBytes, err := json.Marshal(inputData)
	require.NoError(t, err)

	// Create processor config with real API
	conf, err := rerankProcessorConfig().ParseYAML(fmt.Sprintf(`
api_key: %s
model: rerank-v3.5
query: "${!this.query}"
documents: "root = this.docs"
top_n: 3
`, apiKey), nil)
	require.NoError(t, err)

	// Create processor with license service
	resources := service.MockResources()
	license.InjectTestService(resources)
	proc, err := makeRerankProcessor(conf, resources)
	require.NoError(t, err)

	// Process message
	msgs, res := proc.Process(t.Context(), service.NewMessage(inputBytes))
	require.NoError(t, res)
	require.Len(t, msgs, 1)

	// Get result
	result, err := msgs[0].AsStructured()
	require.NoError(t, err)

	resultArray, ok := result.([]any)
	require.True(t, ok, "Expected result to be an array")
	require.Len(t, resultArray, 3, "Expected exactly 3 results due to top_n=3")

	// Verify structure of all results
	for i, item := range resultArray {
		resultItem, ok := item.(map[string]any)
		require.True(t, ok, "Expected result item %d to be a map", i)

		document, hasDocument := resultItem["document"]
		assert.True(t, hasDocument, "Result item %d should have 'document' field", i)

		score, hasScore := resultItem["relevance_score"]
		assert.True(t, hasScore, "Result item %d should have 'relevance_score' field", i)

		scoreFloat, ok := score.(float64)
		require.True(t, ok, "Score should be a float64")

		t.Logf("Result %d: score=%.6f, doc=%s", i, scoreFloat, document.(string)[:50]+"...")
	}

	// The first result should be about Washington D.C. (index 3)
	firstResult := resultArray[0].(map[string]any)
	firstDoc := firstResult["document"].(string)
	assert.Contains(t, firstDoc, "Washington, D.C.", "First result should be about Washington D.C.")

	require.NoError(t, msgs[0].GetError())
}
