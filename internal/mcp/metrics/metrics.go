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

package metrics

import (
	"context"
	"encoding/json"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Metrics contains counters, gauges, and timers for tracking MCP operations.
type Metrics struct {
	// Tool metrics
	toolInvocations          *service.MetricCounter
	toolExecutionDuration    *service.MetricTimer
	toolConcurrentExecutions *service.MetricGauge
	toolRequestSize          *service.MetricTimer
	toolResponseSize         *service.MetricTimer
	toolErrors               *service.MetricCounter

	// Message metrics
	messagesReceived *service.MetricCounter
	messagesSent     *service.MetricCounter
}

// NewMetrics creates a new Metrics instance using the provided service Metrics.
func NewMetrics(m *service.Metrics) *Metrics {
	return &Metrics{
		// Tool metrics
		toolInvocations:          m.NewCounter("mcp_tool_invocations_total", "tool_name", "status"),
		toolExecutionDuration:    m.NewTimer("mcp_tool_execution_duration_ns", "tool_name"),
		toolConcurrentExecutions: m.NewGauge("mcp_tool_concurrent_executions", "tool_name"),
		toolRequestSize:          m.NewTimer("mcp_tool_request_size_bytes", "tool_name"),
		toolResponseSize:         m.NewTimer("mcp_tool_response_size_bytes", "tool_name"),
		toolErrors:               m.NewCounter("mcp_tool_errors_total", "tool_name", "error_type"),

		// Message metrics
		messagesReceived: m.NewCounter("mcp_messages_received_total", "method"),
		messagesSent:     m.NewCounter("mcp_messages_sent_total", "method"),
	}
}

// Middleware returns an MCP method handler that tracks metrics for all MCP method calls.
func (m *Metrics) Middleware(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (result mcp.Result, err error) {
		start := time.Now()
		m.messagesReceived.Incr(1, method)

		// Track tool-specific metrics for tools/call
		if method == "tools/call" {
			return m.handleToolCall(ctx, next, req, start)
		}

		// Call the next handler
		result, err = next(ctx, method, req)

		// Track response metrics
		m.messagesSent.Incr(1, method)
		if err != nil {
			m.toolErrors.Incr(1, method, "method_error")
		}

		return result, err
	}
}

// handleToolCall handles metrics for tool invocations specifically.
func (m *Metrics) handleToolCall(ctx context.Context, next mcp.MethodHandler, req mcp.Request, start time.Time) (result mcp.Result, err error) {
	// Extract tool name from request
	toolName := extractToolName(req)
	if toolName == "" {
		toolName = "unknown"
	}

	// Track concurrent executions
	m.toolConcurrentExecutions.Incr(1, toolName)
	defer m.toolConcurrentExecutions.Decr(1, toolName)

	// Track request size
	if reqBytes, err := json.Marshal(req); err == nil {
		m.toolRequestSize.Timing(int64(len(reqBytes)), toolName)
	}

	// Call the next handler
	result, err = next(ctx, "tools/call", req)

	// Track execution duration
	m.toolExecutionDuration.Timing(time.Since(start).Nanoseconds(), toolName)

	// Track response
	m.messagesSent.Incr(1, "tools/call")
	if err != nil {
		m.toolInvocations.Incr(1, toolName, "error")
		m.toolErrors.Incr(1, toolName, "invocation_error")
	} else {
		m.toolInvocations.Incr(1, toolName, "success")

		// Track response size
		if respBytes, err := json.Marshal(result); err == nil {
			m.toolResponseSize.Timing(int64(len(respBytes)), toolName)
		}
	}

	return result, err
}

// extractToolName extracts the tool name from a tools/call request.
func extractToolName(req mcp.Request) string {
	// Get params and try to type assert to CallToolParams
	params := req.GetParams()
	if callToolParams, ok := params.(*mcp.CallToolParams); ok {
		return callToolParams.Name
	}
	return ""
}
