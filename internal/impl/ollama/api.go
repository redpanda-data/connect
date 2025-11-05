// Copyright 2025 Redpanda Data, Inc.
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

package ollama

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
)

// Options represents model options for Ollama
type Options struct {
	NumPredict       int      `json:"num_predict,omitempty"`
	Temperature      float32  `json:"temperature,omitempty"`
	NumKeep          int      `json:"num_keep,omitempty"`
	Seed             int      `json:"seed,omitempty"`
	TopK             int      `json:"top_k,omitempty"`
	TopP             float32  `json:"top_p,omitempty"`
	RepeatPenalty    float32  `json:"repeat_penalty,omitempty"`
	FrequencyPenalty float32  `json:"frequency_penalty,omitempty"`
	PresencePenalty  float32  `json:"presence_penalty,omitempty"`
	Stop             []string `json:"stop,omitempty"`
	NumCtx           int      `json:"num_ctx,omitempty"`
	NumBatch         int      `json:"num_batch,omitempty"`
	NumGPU           int      `json:"num_gpu,omitempty"`
	NumThread        int      `json:"num_thread,omitempty"`
	UseMMap          *bool    `json:"use_mmap,omitempty"`
}

// ImageData represents image data as bytes
type ImageData []byte

// Message represents a chat message
type Message struct {
	Role      string      `json:"role"`
	Content   string      `json:"content"`
	Images    []ImageData `json:"images,omitempty"`
	ToolCalls []ToolCall  `json:"tool_calls,omitempty"`
}

// ToolCall represents a tool call in a message
type ToolCall struct {
	Function ToolCallFunction `json:"function"`
}

// ToolCallArguments represents the arguments of a tool call
type ToolCallArguments map[string]interface{}

func (t ToolCallArguments) String() string {
	b, _ := json.Marshal(t)
	return string(b)
}

// ToolCallFunction represents the function part of a tool call
type ToolCallFunction struct {
	Name      string            `json:"name"`
	Arguments ToolCallArguments `json:"arguments"`
}

// PropertyType represents the type of a tool property
type PropertyType []string

// ToolProperty represents a property of a tool
type ToolProperty struct {
	Type        PropertyType `json:"type"`
	Description string       `json:"description"`
	Enum        []any        `json:"enum,omitempty"`
}

// ToolParameters represents the parameters of a tool
type ToolParameters struct {
	Type       string                  `json:"type"`
	Required   []string                `json:"required"`
	Properties map[string]ToolProperty `json:"properties"`
}

// ToolFunction represents a tool function
type ToolFunction struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  ToolParameters `json:"parameters"`
}

// Tool represents a tool that can be called by the LLM
type Tool struct {
	Type     string       `json:"type"`
	Function ToolFunction `json:"function"`
}

// ChatRequest represents a chat request
type ChatRequest struct {
	Model    string          `json:"model"`
	Messages []Message       `json:"messages"`
	Stream   *bool           `json:"stream,omitempty"`
	Format   json.RawMessage `json:"format,omitempty"`
	Options  map[string]any  `json:"options,omitempty"`
	Tools    []Tool          `json:"tools,omitempty"`
}

// ChatResponse represents a chat response
type ChatResponse struct {
	Model     string  `json:"model"`
	Message   Message `json:"message"`
	Done      bool    `json:"done"`
	CreatedAt string  `json:"created_at"`
}

// EmbeddingRequest represents an embedding request
type EmbeddingRequest struct {
	Model   string         `json:"model"`
	Prompt  string         `json:"prompt"`
	Options map[string]any `json:"options,omitempty"`
}

// EmbeddingResponse represents an embedding response
type EmbeddingResponse struct {
	Embedding []float64 `json:"embedding"`
}

// PullRequest represents a model pull request
type PullRequest struct {
	Model string `json:"model"`
}

// ProgressResponse represents a progress response during model pull
type ProgressResponse struct {
	Status    string `json:"status"`
	Completed int64  `json:"completed"`
	Total     int64  `json:"total"`
}

// Client represents an Ollama API client
type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
}

// NewClient creates a new Ollama client
func NewClient(baseURL *url.URL, httpClient *http.Client) *Client {
	return &Client{
		baseURL:    baseURL,
		httpClient: httpClient,
	}
}

// ClientFromEnvironment creates a client from environment variables
func ClientFromEnvironment() (*Client, error) {
	host := os.Getenv("OLLAMA_HOST")
	if host == "" {
		host = "http://127.0.0.1:11434"
	}
	u, err := url.Parse(host)
	if err != nil {
		return nil, fmt.Errorf("invalid OLLAMA_HOST: %w", err)
	}
	return NewClient(u, http.DefaultClient), nil
}

// Heartbeat checks if the Ollama server is reachable
func (c *Client) Heartbeat(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL.String(), nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

// Pull pulls a model from the Ollama registry
func (c *Client) Pull(ctx context.Context, req *PullRequest, fn func(ProgressResponse) error) error {
	reqURL := c.baseURL.JoinPath("/api/pull")
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, "POST", reqURL.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("pull failed with status %d: %s", resp.StatusCode, string(body))
	}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		var progress ProgressResponse
		if err := json.Unmarshal(scanner.Bytes(), &progress); err != nil {
			return err
		}
		if fn != nil {
			if err := fn(progress); err != nil {
				return err
			}
		}
	}
	return scanner.Err()
}

// Chat sends a chat request to Ollama
func (c *Client) Chat(ctx context.Context, req *ChatRequest, fn func(ChatResponse) error) error {
	reqURL := c.baseURL.JoinPath("/api/chat")
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, "POST", reqURL.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("chat failed with status %d: %s", resp.StatusCode, string(body))
	}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		var chatResp ChatResponse
		if err := json.Unmarshal(scanner.Bytes(), &chatResp); err != nil {
			return err
		}
		if fn != nil {
			if err := fn(chatResp); err != nil {
				return err
			}
		}
	}
	return scanner.Err()
}

// Embeddings generates embeddings for the given text
func (c *Client) Embeddings(ctx context.Context, req *EmbeddingRequest) (*EmbeddingResponse, error) {
	reqURL := c.baseURL.JoinPath("/api/embeddings")
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, "POST", reqURL.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("embeddings failed with status %d: %s", resp.StatusCode, string(body))
	}

	var embeddingResp EmbeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&embeddingResp); err != nil {
		return nil, err
	}
	return &embeddingResp, nil
}
