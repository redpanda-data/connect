// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package a2a

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"strings"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
)

// httpTransport implements a2aclient.Transport using HTTP/JSON-RPC 2.0.
type httpTransport struct {
	baseURL    string
	httpClient *http.Client
}

// NewHTTPTransport creates a new HTTP transport for A2A protocol.
func NewHTTPTransport(baseURL string, httpClient *http.Client) a2aclient.Transport {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &httpTransport{
		baseURL:    baseURL,
		httpClient: httpClient,
	}
}

// jsonRPCRequest represents a JSON-RPC 2.0 request.
type jsonRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
	ID      string `json:"id,omitempty"`
}

// jsonRPCResponse represents a JSON-RPC 2.0 response.
type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
	ID      string          `json:"id,omitempty"`
}

// jsonRPCError represents a JSON-RPC 2.0 error object.
type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (e *jsonRPCError) Error() string {
	return fmt.Sprintf("JSON-RPC error %d: %s", e.Code, e.Message)
}

// doRequest performs an HTTP POST request with JSON-RPC payload.
func (t *httpTransport) doRequest(ctx context.Context, method string, params any) (*jsonRPCResponse, error) {
	// Build JSON-RPC request
	req := jsonRPCRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      "1",
	}

	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON-RPC request: %w", err)
	}

	// Create HTTP request
	httpReq, err := http.NewRequestWithContext(ctx, "POST", t.baseURL, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	// Apply auth headers from CallMeta (set by interceptors)
	if meta, ok := a2aclient.CallMetaFrom(ctx); ok {
		for k, values := range meta {
			for _, v := range values {
				httpReq.Header.Add(k, v)
			}
		}
	}

	// Execute request
	resp, err := t.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(body))
	}

	// Parse JSON-RPC response
	var jsonResp jsonRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&jsonResp); err != nil {
		return nil, fmt.Errorf("failed to decode JSON-RPC response: %w", err)
	}

	if jsonResp.Error != nil {
		return nil, jsonResp.Error
	}

	return &jsonResp, nil
}

// SendMessage implements the message/send method.
func (t *httpTransport) SendMessage(ctx context.Context, params *a2a.MessageSendParams) (a2a.SendMessageResult, error) {
	resp, err := t.doRequest(ctx, "message/send", params)
	if err != nil {
		return nil, err
	}

	// Try to unmarshal as Task first, then Message
	var task a2a.Task
	if err := json.Unmarshal(resp.Result, &task); err == nil && task.ID != "" {
		return &task, nil
	}

	var message a2a.Message
	if err := json.Unmarshal(resp.Result, &message); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result as Task or Message: %w", err)
	}

	return &message, nil
}

// GetTask implements the tasks/get method.
func (t *httpTransport) GetTask(ctx context.Context, query *a2a.TaskQueryParams) (*a2a.Task, error) {
	resp, err := t.doRequest(ctx, "tasks/get", query)
	if err != nil {
		return nil, err
	}

	var task a2a.Task
	if err := json.Unmarshal(resp.Result, &task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}

	return &task, nil
}

// SendStreamingMessage implements the message/stream method with SSE support.
func (t *httpTransport) SendStreamingMessage(ctx context.Context, params *a2a.MessageSendParams) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		req := jsonRPCRequest{
			JSONRPC: "2.0",
			Method:  "message/stream",
			Params:  params,
			ID:      "1",
		}

		reqBody, err := json.Marshal(req)
		if err != nil {
			yield(nil, fmt.Errorf("failed to marshal request: %w", err))
			return
		}

		httpReq, err := http.NewRequestWithContext(ctx, "POST", t.baseURL, bytes.NewReader(reqBody))
		if err != nil {
			yield(nil, fmt.Errorf("failed to create HTTP request: %w", err))
			return
		}

		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Accept", "text/event-stream")

		if meta, ok := a2aclient.CallMetaFrom(ctx); ok {
			for k, values := range meta {
				for _, v := range values {
					httpReq.Header.Add(k, v)
				}
			}
		}

		resp, err := t.httpClient.Do(httpReq)
		if err != nil {
			yield(nil, fmt.Errorf("HTTP request failed: %w", err))
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			yield(nil, fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(body)))
			return
		}

		if !strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
			body, _ := io.ReadAll(resp.Body)
			yield(nil, fmt.Errorf("expected text/event-stream, got %s: %s", resp.Header.Get("Content-Type"), string(body)))
			return
		}

		t.parseSSEStream(ctx, resp.Body, yield)
	}
}

// ResubscribeToTask implements the tasks/resubscribe method.
func (t *httpTransport) ResubscribeToTask(ctx context.Context, id *a2a.TaskIDParams) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		req := jsonRPCRequest{
			JSONRPC: "2.0",
			Method:  "tasks/resubscribe",
			Params:  id,
			ID:      "1",
		}

		reqBody, err := json.Marshal(req)
		if err != nil {
			yield(nil, fmt.Errorf("failed to marshal request: %w", err))
			return
		}

		httpReq, err := http.NewRequestWithContext(ctx, "POST", t.baseURL, bytes.NewReader(reqBody))
		if err != nil {
			yield(nil, fmt.Errorf("failed to create HTTP request: %w", err))
			return
		}

		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Accept", "text/event-stream")

		if meta, ok := a2aclient.CallMetaFrom(ctx); ok {
			for k, values := range meta {
				for _, v := range values {
					httpReq.Header.Add(k, v)
				}
			}
		}

		resp, err := t.httpClient.Do(httpReq)
		if err != nil {
			yield(nil, fmt.Errorf("HTTP request failed: %w", err))
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			yield(nil, fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(body)))
			return
		}

		if !strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
			body, _ := io.ReadAll(resp.Body)
			yield(nil, fmt.Errorf("expected text/event-stream, got %s: %s", resp.Header.Get("Content-Type"), string(body)))
			return
		}

		t.parseSSEStream(ctx, resp.Body, yield)
	}
}

// parseSSEStream parses SSE events from a reader and yields them to the provided function.
func (t *httpTransport) parseSSEStream(ctx context.Context, body io.Reader, yield func(a2a.Event, error) bool) {
	scanner := bufio.NewScanner(body)
	var eventType string
	var eventData strings.Builder

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			yield(nil, ctx.Err())
			return
		default:
		}

		line := scanner.Text()

		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if eventData.Len() > 0 {
				eventData.WriteString("\n")
			}
			eventData.WriteString(data)
		} else if line == "" && eventData.Len() > 0 {
			data := eventData.String()
			eventData.Reset()

			event, err := t.parseEventByType([]byte(data), eventType)
			if err != nil {
				yield(nil, fmt.Errorf("failed to parse SSE event (type=%s): %w", eventType, err))
				return
			}

			if event != nil {
				if !yield(event, nil) {
					return
				}
			}

			eventType = ""
		}
	}

	if err := scanner.Err(); err != nil {
		yield(nil, fmt.Errorf("SSE stream error: %w", err))
	}
}

// parseEventByType parses an SSE event data based on the event type.
func (*httpTransport) parseEventByType(data []byte, eventType string) (a2a.Event, error) {
	var jsonResp jsonRPCResponse
	if err := json.Unmarshal(data, &jsonResp); err == nil && jsonResp.JSONRPC == "2.0" {
		if jsonResp.Error != nil {
			return nil, jsonResp.Error
		}
		data = jsonResp.Result
	}

	switch eventType {
	case "task_status_update":
		var evt a2a.TaskStatusUpdateEvent
		if err := json.Unmarshal(data, &evt); err != nil {
			return nil, fmt.Errorf("failed to unmarshal TaskStatusUpdateEvent: %w", err)
		}
		return &evt, nil

	case "task_artifact_update":
		var evt a2a.TaskArtifactUpdateEvent
		if err := json.Unmarshal(data, &evt); err != nil {
			return nil, fmt.Errorf("failed to unmarshal TaskArtifactUpdateEvent: %w", err)
		}
		return &evt, nil

	case "task", "":
		var task a2a.Task
		if err := json.Unmarshal(data, &task); err == nil && task.ID != "" {
			return &task, nil
		}

		var msg a2a.Message
		if err := json.Unmarshal(data, &msg); err == nil && msg.ID != "" {
			return &msg, nil
		}

		return nil, errors.New("failed to parse event as Task or Message")

	case "message":
		var msg a2a.Message
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal Message: %w", err)
		}
		return &msg, nil

	default:
		var raw map[string]any
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, fmt.Errorf("failed to parse event JSON: %w", err)
		}

		if _, hasArtifact := raw["artifact"]; hasArtifact {
			var evt a2a.TaskArtifactUpdateEvent
			if err := json.Unmarshal(data, &evt); err != nil {
				return nil, err
			}
			return &evt, nil
		}

		if _, hasStatus := raw["status"]; hasStatus {
			if _, hasTaskID := raw["taskId"]; hasTaskID {
				var evt a2a.TaskStatusUpdateEvent
				if err := json.Unmarshal(data, &evt); err != nil {
					return nil, err
				}
				return &evt, nil
			}

			var task a2a.Task
			if err := json.Unmarshal(data, &task); err == nil && task.ID != "" {
				return &task, nil
			}
		}

		if _, hasMessageID := raw["messageId"]; hasMessageID {
			var msg a2a.Message
			if err := json.Unmarshal(data, &msg); err != nil {
				return nil, err
			}
			return &msg, nil
		}

		return nil, fmt.Errorf("unknown event type: %s", eventType)
	}
}

// CancelTask implements the tasks/cancel method.
func (t *httpTransport) CancelTask(_ context.Context, id *a2a.TaskIDParams) (*a2a.Task, error) {
	return nil, a2aclient.ErrNotImplemented
}

// GetTaskPushConfig implements the tasks/pushNotificationConfig/get method.
func (t *httpTransport) GetTaskPushConfig(_ context.Context, params *a2a.GetTaskPushConfigParams) (*a2a.TaskPushConfig, error) {
	return nil, a2aclient.ErrNotImplemented
}

// ListTaskPushConfig implements the tasks/pushNotificationConfig/list method.
func (t *httpTransport) ListTaskPushConfig(_ context.Context, params *a2a.ListTaskPushConfigParams) ([]*a2a.TaskPushConfig, error) {
	return nil, a2aclient.ErrNotImplemented
}

// SetTaskPushConfig implements the tasks/pushNotificationConfig/set method.
func (t *httpTransport) SetTaskPushConfig(_ context.Context, params *a2a.TaskPushConfig) (*a2a.TaskPushConfig, error) {
	return nil, a2aclient.ErrNotImplemented
}

// DeleteTaskPushConfig implements the tasks/pushNotificationConfig/delete method.
func (t *httpTransport) DeleteTaskPushConfig(_ context.Context, params *a2a.DeleteTaskPushConfigParams) error {
	return a2aclient.ErrNotImplemented
}

// GetAgentCard retrieves the agent card from /.well-known/agent.json.
func (t *httpTransport) GetAgentCard(_ context.Context) (*a2a.AgentCard, error) {
	return nil, a2aclient.ErrNotImplemented
}

// Destroy cleans up resources.
func (*httpTransport) Destroy() error {
	return nil
}
